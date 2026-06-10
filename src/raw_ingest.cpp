#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Type widening between an existing table column and incoming data
//===--------------------------------------------------------------------===//

static bool IsIntegerType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return true;
	default:
		return false;
	}
}

static bool IsPlainScalar(const LogicalType &type) {
	if (IsRawJSONType(type)) {
		return false;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::VARCHAR:
		return true;
	default:
		return IsIntegerType(type);
	}
}

// Returns the type the table column must have to hold both existing and
// incoming values. Falls back to the existing type (no ALTER) for exotic
// existing types: the INSERT cast will surface any true incompatibility.
static LogicalType JoinColumnTypes(const LogicalType &existing, const LogicalType &incoming) {
	if (existing == incoming) {
		return existing;
	}
	auto existing_json = IsRawJSONType(existing);
	auto incoming_json = IsRawJSONType(incoming);
	if (existing_json || incoming_json) {
		return LogicalType::JSON();
	}
	if (existing.id() == LogicalTypeId::LIST && incoming.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(JoinColumnTypes(ListType::GetChildType(existing), ListType::GetChildType(incoming)));
	}
	if (existing.id() == LogicalTypeId::LIST || incoming.id() == LogicalTypeId::LIST) {
		if (!IsPlainScalar(existing) && !IsPlainScalar(incoming)) {
			return existing;
		}
		return LogicalType::JSON();
	}
	if (!IsPlainScalar(existing)) {
		return existing;
	}
	if (IsIntegerType(existing) && IsIntegerType(incoming)) {
		// incoming integers are always inferred as BIGINT; only widen upwards
		return existing.id() == LogicalTypeId::HUGEINT ? existing : incoming;
	}
	auto is_numeric = [](const LogicalType &t) {
		return IsIntegerType(t) || t.id() == LogicalTypeId::FLOAT || t.id() == LogicalTypeId::DOUBLE;
	};
	auto is_temporal = [](const LogicalType &t) {
		return t.id() == LogicalTypeId::DATE || t.id() == LogicalTypeId::TIMESTAMP;
	};
	if (is_numeric(existing) && is_numeric(incoming)) {
		return LogicalType::DOUBLE;
	}
	if (is_temporal(existing) && is_temporal(incoming)) {
		return LogicalType::TIMESTAMP;
	}
	return LogicalType::VARCHAR;
}

//===--------------------------------------------------------------------===//
// SQL generation helpers
//===--------------------------------------------------------------------===//

string RawQuoteIdentifier(const string &name) {
	return KeywordHelper::WriteQuoted(name, '"');
}

static string QuoteLiteral(const string &value) {
	return KeywordHelper::WriteQuoted(value, '\'');
}

string RawQualifiedTarget(const string &target) {
	auto qualified = QualifiedName::Parse(target);
	string result;
	if (!qualified.catalog.empty()) {
		result += RawQuoteIdentifier(qualified.catalog) + ".";
	}
	if (!qualified.schema.empty()) {
		result += RawQuoteIdentifier(qualified.schema) + ".";
	}
	result += RawQuoteIdentifier(qualified.name);
	return result;
}

static unique_ptr<MaterializedQueryResult> RunQuery(Connection &conn, const string &sql) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->ThrowError("RawDuck: ");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// RawIngestor: schema evolution + insert for one target table
//===--------------------------------------------------------------------===//

// Drives one or more payload batches into a target table, evolving its schema
// as batches arrive. Tracks the table schema locally so streaming ingestion
// only probes the catalog once.
class RawIngestor {
public:
	RawIngestor(ClientContext &context, string target_p)
	    : conn(*context.db), target(std::move(target_p)), qualified(RawQualifiedTarget(target)) {
		Probe();
	}

	void Ingest(const string &payload_str) {
		RawPayload payload;
		payload.Parse(payload_str);
		auto root = payload.InferSchema();
		auto columns = FlattenSchema(*root, payload.scalar_rows);

		if (!exists && columns.empty()) {
			throw InvalidInputException("RawDuck: cannot create table %s from an empty payload", target);
		}
		if (columns.empty()) {
			return;
		}
		// Catalogs like DuckLake cannot rewrite a column with an expression
		// (ALTER ... USING), which JSON-widening needs; when that fails we
		// retry once with widening to JSON disabled, converting incoming
		// values to the existing column type instead.
		try {
			IngestBatch(payload, columns, payload_str, true);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (StringUtil::Contains(error.RawMessage(), "cannot be modified using an expression")) {
				IngestBatch(payload, columns, payload_str, false);
			} else {
				throw;
			}
		}
	}

	bool created = false;
	idx_t columns_added = 0;
	idx_t columns_widened = 0;
	idx_t rows = 0;

private:
	void Probe() {
		// a zero-row scan yields the exact column types, including
		// extension-registered aliases like JSON
		auto probe = conn.Query("SELECT * FROM " + qualified + " LIMIT 0");
		exists = !probe->HasError();
		if (!exists) {
			auto &error = probe->GetError();
			if (!StringUtil::Contains(error, "does not exist") && !StringUtil::Contains(error, "not found")) {
				throw InvalidInputException("RawDuck: cannot describe %s: %s", target, error);
			}
			return;
		}
		for (idx_t col = 0; col < probe->ColumnCount(); col++) {
			existing_types[probe->names[col]] = probe->types[col];
		}
	}

	void IngestBatch(RawPayload &payload, vector<RawColumn> &columns, const string &payload_str,
	                 bool allow_json_widening) {
		vector<string> statements;
		// final table type for every incoming column, to know which need to_json()
		vector<LogicalType> final_types;
		bool batch_creates = false;
		idx_t batch_added = 0;
		idx_t batch_widened = 0;

		if (!exists) {
			string create = "CREATE TABLE " + qualified + " (";
			for (idx_t i = 0; i < columns.size(); i++) {
				create += (i ? ", " : "") + RawQuoteIdentifier(columns[i].name) + " " + columns[i].type.ToString();
				final_types.push_back(columns[i].type);
			}
			create += ")";
			statements.push_back(create);
			batch_creates = true;
			batch_added = columns.size();
		} else {
			for (auto &column : columns) {
				auto entry = existing_types.find(column.name);
				if (entry == existing_types.end()) {
					statements.push_back("ALTER TABLE " + qualified + " ADD COLUMN " + RawQuoteIdentifier(column.name) +
					                     " " + column.type.ToString());
					final_types.push_back(column.type);
					batch_added++;
					continue;
				}
				auto target_type = JoinColumnTypes(entry->second, column.type);
				if (target_type == entry->second) {
					final_types.push_back(target_type);
					continue;
				}
				bool needs_rewrite = IsRawJSONType(target_type) && !IsRawJSONType(entry->second);
				if (needs_rewrite && !allow_json_widening) {
					// keep the existing column type; incoming values are
					// converted on insert instead
					final_types.push_back(entry->second);
					continue;
				}
				string alter = "ALTER TABLE " + qualified + " ALTER COLUMN " + RawQuoteIdentifier(column.name) +
				               " SET DATA TYPE " + target_type.ToString();
				if (needs_rewrite) {
					// a plain cast to JSON would reject bare strings
					alter += " USING to_json(" + RawQuoteIdentifier(column.name) + ")";
				}
				statements.push_back(alter);
				final_types.push_back(target_type);
				batch_widened++;
			}
		}

		idx_t batch_rows = 0;
		RunQuery(conn, "BEGIN TRANSACTION");
		try {
			for (auto &statement : statements) {
				RunQuery(conn, statement);
			}
			if (!payload.rows.empty()) {
				string insert = "INSERT INTO " + qualified + " (";
				string select_list;
				for (idx_t i = 0; i < columns.size(); i++) {
					insert += (i ? ", " : "") + RawQuoteIdentifier(columns[i].name);
					auto expr = RawQuoteIdentifier(columns[i].name);
					auto incoming_structural =
					    IsRawJSONType(columns[i].type) || columns[i].type.id() == LogicalTypeId::LIST;
					if (IsRawJSONType(final_types[i]) && !IsRawJSONType(columns[i].type)) {
						expr = "to_json(" + expr + ")";
					} else if (!IsRawJSONType(final_types[i]) && incoming_structural &&
					           final_types[i] != columns[i].type) {
						// degraded widening: render structural values as JSON
						// text and cast to the existing column type
						expr = "to_json(" + expr + ")::" + final_types[i].ToString();
					}
					select_list += (i ? ", " : "") + expr;
				}
				insert += ") SELECT " + select_list + " FROM raw_records(" + QuoteLiteral(payload_str) + ")";
				auto result = RunQuery(conn, insert);
				batch_rows = NumericCast<idx_t>(result->GetValue(0, 0).GetValue<int64_t>());
			}
			RunQuery(conn, "COMMIT");
		} catch (...) {
			conn.Query("ROLLBACK");
			throw;
		}

		// success: fold the batch into the running totals and schema cache
		created = created || batch_creates;
		columns_added += batch_added;
		columns_widened += batch_widened;
		rows += batch_rows;
		exists = true;
		for (idx_t i = 0; i < columns.size(); i++) {
			existing_types[columns[i].name] = final_types[i];
		}
	}

	Connection conn;
	string target;
	string qualified;
	bool exists = false;
	case_insensitive_map_t<LogicalType> existing_types;
};

//===--------------------------------------------------------------------===//
// raw_ingest(table, payload)
//===--------------------------------------------------------------------===//

struct RawIngestBindData : public TableFunctionData {
	string target;
	string payload;
	// raw_ingest_file
	string path;
	idx_t batch_size = 0;
};

struct RawIngestState : public GlobalTableFunctionState {
	bool done = false;
};

static void SetIngestSchema(vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BIGINT,
	                LogicalType::BIGINT};
	names = {"table", "created", "columns_added", "columns_widened", "rows"};
}

static unique_ptr<FunctionData> RawIngestBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawIngestBindData>();
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw InvalidInputException("RawDuck: raw_ingest(table, payload) arguments may not be NULL");
	}
	result->target = input.inputs[0].GetValue<string>();
	result->payload = input.inputs[1].GetValue<string>();
	SetIngestSchema(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawIngestInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawIngestState>();
}

static void EmitIngestRow(DataChunk &output, const string &target, const RawIngestor &ingestor) {
	output.SetValue(0, 0, Value(target));
	output.SetValue(1, 0, Value::BOOLEAN(ingestor.created));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(ingestor.columns_added)));
	output.SetValue(3, 0, Value::BIGINT(NumericCast<int64_t>(ingestor.columns_widened)));
	output.SetValue(4, 0, Value::BIGINT(NumericCast<int64_t>(ingestor.rows)));
	output.SetCardinality(1);
}

static void RawIngestFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawIngestBindData>();
	auto &state = data.global_state->Cast<RawIngestState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	RawIngestor ingestor(context, bind_data.target);
	ingestor.Ingest(bind_data.payload);
	EmitIngestRow(output, bind_data.target, ingestor);
}

TableFunction GetRawIngestFunction() {
	TableFunction function("raw_ingest", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RawIngestFunction, RawIngestBind,
	                       RawIngestInit);
	return function;
}

//===--------------------------------------------------------------------===//
// raw_ingest_file(table, path): streaming NDJSON ingestion
//===--------------------------------------------------------------------===//

static constexpr idx_t RAW_DEFAULT_BATCH_SIZE = 30000;
static constexpr idx_t RAW_READ_BUFFER_SIZE = 16ULL * 1024ULL * 1024ULL;

static unique_ptr<FunctionData> RawIngestFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawIngestBindData>();
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw InvalidInputException("RawDuck: raw_ingest_file(table, path) arguments may not be NULL");
	}
	result->target = input.inputs[0].GetValue<string>();
	result->path = input.inputs[1].GetValue<string>();
	result->batch_size = RAW_DEFAULT_BATCH_SIZE;
	auto batch_entry = input.named_parameters.find("batch_size");
	if (batch_entry != input.named_parameters.end()) {
		auto batch_size = batch_entry->second.GetValue<int64_t>();
		if (batch_size <= 0) {
			throw InvalidInputException("RawDuck: batch_size must be positive");
		}
		result->batch_size = NumericCast<idx_t>(batch_size);
	}
	SetIngestSchema(return_types, names);
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("batches");
	return std::move(result);
}

static void RawIngestFileFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawIngestBindData>();
	auto &state = data.global_state->Cast<RawIngestState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(bind_data.path, FileOpenFlags(FileFlags::FILE_FLAGS_READ) |
	                                              FileOpenFlags(FileCompressionType::AUTO_DETECT));

	RawIngestor ingestor(context, bind_data.target);
	idx_t batches = 0;
	auto buffer = make_unsafe_uniq_array_uninitialized<char>(RAW_READ_BUFFER_SIZE);
	string pending;
	idx_t pending_lines = 0;

	auto flush = [&](string batch) {
		if (batch.find_first_not_of(" \t\r\n") == string::npos) {
			return;
		}
		ingestor.Ingest(batch);
		batches++;
	};

	while (true) {
		auto read = handle->Read(buffer.get(), RAW_READ_BUFFER_SIZE);
		if (read <= 0) {
			break;
		}
		for (int64_t i = 0; i < read; i++) {
			if (buffer[i] == '\n') {
				pending_lines++;
			}
		}
		pending.append(buffer.get(), NumericCast<idx_t>(read));
		while (pending_lines >= bind_data.batch_size) {
			// split off the first batch_size lines
			idx_t split = 0;
			for (idx_t line = 0; line < bind_data.batch_size; line++) {
				split = pending.find('\n', split) + 1;
			}
			flush(pending.substr(0, split));
			pending.erase(0, split);
			pending_lines -= bind_data.batch_size;
		}
	}
	flush(std::move(pending));

	EmitIngestRow(output, bind_data.target, ingestor);
	output.SetValue(5, 0, Value::BIGINT(NumericCast<int64_t>(batches)));
}

TableFunction GetRawIngestFileFunction() {
	TableFunction function("raw_ingest_file", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RawIngestFileFunction,
	                       RawIngestFileBind, RawIngestInit);
	function.named_parameters["batch_size"] = LogicalType::BIGINT;
	return function;
}

} // namespace duckdb
