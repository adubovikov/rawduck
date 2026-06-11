#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// The virtual `ingest` schema: INSERT-syntax ingestion for RawDuck stores.
//
//   ATTACH 'rawduck:store.db' AS raw;
//   INSERT INTO raw.ingest.events VALUES ('{"id": 1, "action": "click"}');
//   INSERT INTO raw.ingest.events SELECT json FROM read_json(...);
//
// Tables under `ingest` are synthesized on lookup with a single `payload`
// column, so any INSERT binds; our PlanInsert intercepts and streams the
// payloads chunk-wise through one persistent native ingestor into the real
// table of the same name in `main` - auto-creation and schema evolution
// included. This is the high-throughput path: one statement, vector-at-a-
// time flow, no per-call SQL overhead.
//===--------------------------------------------------------------------===//

class RawIngestTableEntry : public TableCatalogEntry {
public:
	RawIngestTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
	    : TableCatalogEntry(catalog, schema, info) {
	}
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
		return nullptr;
	}
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override {
		throw NotImplementedException("RawDuck: ingest tables are write-only; query the real table instead");
	}
	TableStorageInfo GetStorageInfo(ClientContext &context) override {
		return TableStorageInfo();
	}
};

class RawIngestSchemaEntry : public SchemaCatalogEntry {
public:
	RawIngestSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
		internal = true;
	}

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction,
	                                       const EntryLookupInfo &lookup_info) override {
		if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
			return nullptr;
		}
		lock_guard<mutex> guard(lock);
		auto name = lookup_info.GetEntryName();
		auto entry = tables.find(name);
		if (entry == tables.end()) {
			CreateTableInfo info(catalog.GetName(), this->name, name);
			info.columns.AddColumn(ColumnDefinition("payload", LogicalType::VARCHAR));
			auto table = make_uniq<RawIngestTableEntry>(catalog, *this, info);
			table->internal = true;
			entry = tables.emplace(name, std::move(table)).first;
		}
		return entry->second.get();
	}

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
	}
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
	}
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override {
		throw NotImplementedException("RawDuck: the ingest schema cannot be copied");
	}
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	void DropEntry(ClientContext &context, DropInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}
	void Alter(CatalogTransaction transaction, AlterInfo &info) override {
		throw NotImplementedException("RawDuck: the ingest schema is virtual");
	}

private:
	mutex lock;
	unordered_map<string, unique_ptr<RawIngestTableEntry>> tables;
};

unique_ptr<SchemaCatalogEntry> RawCreateIngestSchema(Catalog &catalog) {
	auto info = make_uniq<CreateSchemaInfo>();
	info->schema = "ingest";
	info->internal = true;
	return make_uniq<RawIngestSchemaEntry>(catalog, *info);
}

bool RawIsIngestTable(const TableCatalogEntry &table) {
	return dynamic_cast<const RawIngestTableEntry *>(&table) != nullptr;
}

//===--------------------------------------------------------------------===//
// PhysicalRawIngest: streams payload chunks through one persistent ingestor
//===--------------------------------------------------------------------===//

class RawIngestSinkState : public GlobalSinkState {
public:
	unique_ptr<RawStreamIngestor> ingestor;
};

class RawIngestLocalSinkState : public LocalSinkState {
public:
	// consecutive single-object payloads coalesce into NDJSON batches
	string buffer;
	idx_t buffered = 0;
};

class PhysicalRawIngest : public PhysicalOperator {
public:
	static constexpr idx_t BATCH_BYTES = 8ULL * 1024ULL * 1024ULL;

	PhysicalRawIngest(PhysicalPlan &plan, string target_p, idx_t estimated_cardinality)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
	      target(std::move(target_p)) {
	}

	string GetName() const override {
		return "RAW_INGEST";
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		// each sink thread parses its own batches; the ingest handoff is
		// internally serialized and appends fan out through the pool
		return true;
	}
	bool IsSource() const override {
		return true;
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		auto state = make_uniq<RawIngestSinkState>();
		state->ingestor = RawCreateStreamIngestor(context, target, RawParseOptions());
		return std::move(state);
	}

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override {
		return make_uniq<RawIngestLocalSinkState>();
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &gstate = input.global_state.Cast<RawIngestSinkState>();
		auto &state = input.local_state.Cast<RawIngestLocalSinkState>();
		UnifiedVectorFormat payloads;
		chunk.data[0].ToUnifiedFormat(chunk.size(), payloads);
		auto strings = UnifiedVectorFormat::GetData<string_t>(payloads);
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto idx = payloads.sel->get_index(i);
			if (!payloads.validity.RowIsValid(idx)) {
				continue;
			}
			auto payload = strings[idx];
			auto data = payload.GetData();
			auto size = payload.GetSize();
			idx_t first = 0;
			while (first < size && StringUtil::CharacterIsSpace(data[first])) {
				first++;
			}
			if (first < size && data[first] == '{') {
				// single objects coalesce into one NDJSON batch
				state.buffer.append(data, size);
				state.buffer.push_back('\n');
				state.buffered++;
				if (state.buffer.size() >= BATCH_BYTES) {
					FlushBuffer(gstate, state);
				}
			} else {
				// arrays / NDJSON payloads ingest standalone
				FlushBuffer(gstate, state);
				gstate.ingestor->IngestConcurrent(string(data, size));
			}
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override {
		auto &gstate = input.global_state.Cast<RawIngestSinkState>();
		auto &state = input.local_state.Cast<RawIngestLocalSinkState>();
		FlushBuffer(gstate, state);
		return SinkCombineResultType::FINISHED;
	}

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		auto &gstate = input.global_state.Cast<RawIngestSinkState>();
		gstate.ingestor->Finish();
		return SinkFinalizeType::READY;
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		auto &state = sink_state->Cast<RawIngestSinkState>();
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(state.ingestor->Rows())));
		chunk.SetCardinality(1);
		return SourceResultType::FINISHED;
	}

private:
	static void FlushBuffer(RawIngestSinkState &gstate, RawIngestLocalSinkState &state) {
		if (state.buffered == 0) {
			return;
		}
		gstate.ingestor->IngestConcurrent(state.buffer);
		state.buffer.clear();
		state.buffered = 0;
	}

	string target;
};

PhysicalOperator &RawPlanIngestInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                      optional_ptr<PhysicalOperator> plan) {
	if (!plan) {
		throw NotImplementedException("RawDuck: INSERT INTO ingest tables requires a data source");
	}
	auto target = RawQuoteIdentifier(op.table.ParentCatalog().GetName()) + ".main." + RawQuoteIdentifier(op.table.name);
	auto &ingest = planner.Make<PhysicalRawIngest>(std::move(target), op.estimated_cardinality);
	ingest.children.push_back(*plan);
	return ingest;
}

} // namespace duckdb
