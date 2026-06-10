#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Predicate statistics, RawMergeTree style: every optimized plan is scanned
// for filters pushed into base table scans and for grouping columns, and the
// (table, column) usage counts feed raw_optimize()'s ORDER BY selection.
//===--------------------------------------------------------------------===//

struct RawColumnUsage {
	idx_t filters = 0;
	idx_t groups = 0;
};

class RawStatsCache : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rawduck_filter_stats";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		// statistics must never be evicted
		return optional_idx();
	}

	mutex lock;
	// fully qualified table key -> column name -> observed usage
	map<string, map<string, RawColumnUsage>> usage;
};

static RawStatsCache &GetStatsCache(ClientContext &context) {
	return *ObjectCache::GetObjectCache(context).GetOrCreate<RawStatsCache>(RawStatsCache::ObjectType());
}

static string TableKey(const TableCatalogEntry &table) {
	return StringUtil::Lower(table.ParentCatalog().GetName()) + "." + StringUtil::Lower(table.ParentSchema().name) +
	       "." + StringUtil::Lower(table.name);
}

// Resolves a scan output column (binding into column_ids) back to its name.
static bool ResolveGetColumn(const LogicalGet &get, idx_t column_index, string &name) {
	auto &column_ids = get.GetColumnIds();
	if (column_index >= column_ids.size()) {
		return false;
	}
	auto table_column = column_ids[column_index].GetPrimaryIndex();
	if (table_column >= get.names.size()) {
		return false;
	}
	name = get.names[table_column];
	return true;
}

static void CollectGets(LogicalOperator &op, unordered_map<idx_t, optional_ptr<LogicalGet>> &gets) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		gets[get.table_index] = &get;
	}
	for (auto &child : op.children) {
		CollectGets(*child, gets);
	}
}

static void CollectStats(ClientContext &context, LogicalOperator &op,
                         const unordered_map<idx_t, optional_ptr<LogicalGet>> &gets) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (table && !get.table_filters.filters.empty()) {
			auto key = TableKey(*table);
			auto &stats = GetStatsCache(context);
			lock_guard<mutex> guard(stats.lock);
			for (auto &filter : get.table_filters.filters) {
				// table filters are keyed by the table's logical column index,
				// and LogicalGet::names covers the full table schema
				if (filter.first >= get.names.size()) {
					continue;
				}
				stats.usage[key][get.names[filter.first]].filters++;
			}
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		// grouping columns that reference a base table scan directly
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (auto &group : aggregate.groups) {
			if (group->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			auto &column_ref = group->Cast<BoundColumnRefExpression>();
			auto entry = gets.find(column_ref.binding.table_index);
			if (entry == gets.end() || !entry->second) {
				continue;
			}
			auto &get = *entry->second;
			auto table = get.GetTable();
			string column_name;
			if (!table || !ResolveGetColumn(get, column_ref.binding.column_index, column_name)) {
				continue;
			}
			auto &stats = GetStatsCache(context);
			lock_guard<mutex> guard(stats.lock);
			stats.usage[TableKey(*table)][column_name].groups++;
		}
	}
	for (auto &child : op.children) {
		CollectStats(context, *child, gets);
	}
}

static void RawDuckOptimizeHook(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	// observation must never break a query
	try {
		unordered_map<idx_t, optional_ptr<LogicalGet>> gets;
		CollectGets(*plan, gets);
		CollectStats(input.context, *plan, gets);
	} catch (...) { // NOLINT: best-effort statistics collection
	}
}

OptimizerExtension GetRawDuckOptimizerExtension() {
	OptimizerExtension extension;
	extension.optimize_function = RawDuckOptimizeHook;
	return extension;
}

//===--------------------------------------------------------------------===//
// raw_stats(): dump observed predicate statistics
//===--------------------------------------------------------------------===//

struct RawStatsState : public GlobalTableFunctionState {
	vector<std::tuple<string, string, idx_t, idx_t>> entries;
	idx_t next = 0;
};

static unique_ptr<FunctionData> RawStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
	names = {"table", "column", "filter_count", "group_count"};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> RawStatsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<RawStatsState>();
	auto &stats = GetStatsCache(context);
	lock_guard<mutex> guard(stats.lock);
	for (auto &table : stats.usage) {
		for (auto &column : table.second) {
			state->entries.emplace_back(table.first, column.first, column.second.filters, column.second.groups);
		}
	}
	return std::move(state);
}

static void RawStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RawStatsState>();
	idx_t count = 0;
	while (state.next < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.next++];
		output.SetValue(0, count, Value(std::get<0>(entry)));
		output.SetValue(1, count, Value(std::get<1>(entry)));
		output.SetValue(2, count, Value::BIGINT(NumericCast<int64_t>(std::get<2>(entry))));
		output.SetValue(3, count, Value::BIGINT(NumericCast<int64_t>(std::get<3>(entry))));
		count++;
	}
	output.SetCardinality(count);
}

TableFunction GetRawStatsFunction() {
	return TableFunction("raw_stats", {}, RawStatsFunction, RawStatsBind, RawStatsInit);
}

//===--------------------------------------------------------------------===//
// raw_optimize(table): physically reorder by the hottest predicate columns
//===--------------------------------------------------------------------===//

struct RawOptimizeBindData : public TableFunctionData {
	string target;
};

struct RawOptimizeState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> RawOptimizeBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawOptimizeBindData>();
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("RawDuck: raw_optimize(table) argument may not be NULL");
	}
	result->target = input.inputs[0].GetValue<string>();
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
	names = {"table", "order_by", "rows"};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawOptimizeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawOptimizeState>();
}

static void RawOptimizeFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawOptimizeBindData>();
	auto &state = data.global_state->Cast<RawOptimizeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	// resolve the table within the calling query's transaction
	auto qname = QualifiedName::Parse(bind_data.target);
	auto catalog_name = qname.catalog.empty() ? INVALID_CATALOG : qname.catalog;
	auto schema_name = qname.schema.empty() ? DEFAULT_SCHEMA : qname.schema;
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, catalog_name, schema_name, qname.name);

	// rank columns by how often queries filtered on them
	vector<pair<string, idx_t>> ranked;
	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		auto entry = stats.usage.find(TableKey(table));
		if (entry != stats.usage.end()) {
			for (auto &column : entry->second) {
				// filters benefit most from zone-map pruning; grouping
				// locality is a secondary win
				ranked.emplace_back(column.first, 2 * column.second.filters + column.second.groups);
			}
		}
	}
	std::stable_sort(ranked.begin(), ranked.end(),
	                 [](const pair<string, idx_t> &a, const pair<string, idx_t> &b) { return a.second > b.second; });

	output.SetValue(0, 0, Value(bind_data.target));
	if (ranked.empty()) {
		// nothing observed yet: no-op
		output.SetValue(1, 0, Value(LogicalType::VARCHAR));
		output.SetValue(2, 0, Value::BIGINT(0));
		output.SetCardinality(1);
		return;
	}

	string order_by;
	for (idx_t i = 0; i < MinValue<idx_t>(ranked.size(), 2); i++) {
		order_by += (i ? ", " : "") + RawQuoteIdentifier(ranked[i].first);
	}

	// rewrite the table physically ordered by the hottest predicate columns
	auto catalog = table.ParentCatalog().GetName();
	auto schema = table.ParentSchema().name;
	auto table_name = table.name;
	auto qualified = RawQualifiedTarget(bind_data.target);
	string tmp_name = "__rawduck_optimize_tmp";
	string tmp_qualified =
	    RawQuoteIdentifier(catalog) + "." + RawQuoteIdentifier(schema) + "." + RawQuoteIdentifier(tmp_name);

	Connection conn(*context.db);
	auto run = [&](const string &sql) {
		auto result = conn.Query(sql);
		if (result->HasError()) {
			result->ThrowError("RawDuck: ");
		}
		return result;
	};
	int64_t rows = 0;
	run("BEGIN TRANSACTION");
	try {
		auto created =
		    run("CREATE TABLE " + tmp_qualified + " AS SELECT * FROM " + qualified + " ORDER BY " + order_by);
		rows = created->GetValue(0, 0).GetValue<int64_t>();
		run("DROP TABLE " + qualified);
		run("ALTER TABLE " + tmp_qualified + " RENAME TO " + RawQuoteIdentifier(table_name));
		run("COMMIT");
	} catch (...) {
		conn.Query("ROLLBACK");
		throw;
	}

	output.SetValue(1, 0, Value(order_by));
	output.SetValue(2, 0, Value::BIGINT(rows));
	output.SetCardinality(1);
}

TableFunction GetRawOptimizeFunction() {
	return TableFunction("raw_optimize", {LogicalType::VARCHAR}, RawOptimizeFunction, RawOptimizeBind, RawOptimizeInit);
}

} // namespace duckdb
