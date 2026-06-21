#define DUCKDB_EXTENSION_MAIN

#include "rawduck_extension.hpp"
#include "raw_functions.hpp"
#include "raw_write_settings.hpp"

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(GetRawRecordsFunction());
	loader.RegisterFunction(GetRawIngestFunction());
	loader.RegisterFunction(GetRawIngestFileFunction());
	loader.RegisterFunction(GetRawStatsFunction());
	loader.RegisterFunction(GetRawOptimizeFunction());
	loader.RegisterFunction(GetRawTypeFunction());
	loader.RegisterFunction(GetRawInferFunction());
	loader.RegisterFunction(GetRawTransformsFunction());
	loader.RegisterFunction(GetRawProjectionsFunction());
	loader.RegisterFunction(GetRawProjectFunction());
	loader.RegisterFunction(GetRawStatsSaveFunction());
	loader.RegisterFunction(GetRawStatsLoadFunction());
	loader.RegisterFunction(GetRawServeFunction());
	loader.RegisterFunction(GetRawServeStopFunction());
	loader.RegisterFunction(GetRawServeGrpcFunction());
	loader.RegisterFunction(GetRawServeGrpcStopFunction());
	loader.RegisterFunction(GetRawFlushFunction());
	loader.RegisterFunction(GetRawTransformDefineFunction());

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	// observe pushed-down predicates to drive raw_optimize()
	OptimizerExtension::Register(config, GetRawDuckOptimizerExtension());
	config.AddExtensionOption("rawduck_insert_transform",
	                          "Transform name or explode path applied by INSERTs into ingest tables",
	                          LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption("rawduck_async_insert", "Buffer ingestion calls and flush asynchronously",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("rawduck_async_max_data_size", "Async insert buffer flush threshold in bytes",
	                          LogicalType::BIGINT, Value::BIGINT(1024 * 1024));
	config.AddExtensionOption("rawduck_async_busy_timeout_ms", "Async insert buffer flush age threshold",
	                          LogicalType::BIGINT, Value::BIGINT(200));
	config.AddExtensionOption("rawduck_use_projections",
	                          "Rewrite eligible count(*) aggregations onto fresh materialized projections "
	                          "(append-only workloads)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("rawduck_overlap_flush",
	                          "Flush completed row groups during a large multi-threaded ingest while the schema is "
	                          "stable, overlapping parse with compression/IO. Faster on large stable-schema imports "
	                          "at the cost of higher peak memory; off by default (drain-free)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("rawduck_pool_min_rows",
	                          "Minimum rows per batch before engaging the multi-threaded append pool (fast pool uses "
	                          "one worker from 512 rows up to this threshold)",
	                          LogicalType::BIGINT, Value::BIGINT(RawWriteSettings::DEFAULT_POOL_MIN_ROWS));
	config.AddExtensionOption("rawduck_pool_threads",
	                          "Append pool worker threads (0 = auto: hw_concurrency/2 capped at 8 for large batches, "
	                          "1 for fast-pool batches)",
	                          LogicalType::BIGINT, Value::BIGINT(0));
	config.AddExtensionOption("rawduck_pipeline_threads",
	                          "Parse worker threads for raw_ingest_file (0 = auto, capped at 16)",
	                          LogicalType::BIGINT, Value::BIGINT(0));
	config.AddExtensionOption("rawduck_pipeline_depth",
	                          "Queue depth for raw_ingest_file reader/parser handoff and append pool backpressure",
	                          LogicalType::BIGINT, Value::BIGINT(RawWriteSettings::DEFAULT_PIPELINE_DEPTH));
	config.AddExtensionOption("rawduck_overlap_flush_auto",
	                          "Automatically enable overlap flush for stable absorbed schema shapes on large batches",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("rawduck_checkpoint_after_ingest",
	                          "Run CHECKPOINT after ingesting at least this many rows (0 = disabled)",
	                          LogicalType::BIGINT, Value::BIGINT(0));
	// ATTACH 'rawduck:store.db' AS raw
	StorageExtension::Register(config, "rawduck", GetRawDuckStorageExtension());
}

void RawduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RawduckExtension::Name() {
	return "rawduck";
}

std::string RawduckExtension::Version() const {
#ifdef EXT_VERSION_RAWDUCK
	return EXT_VERSION_RAWDUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rawduck, loader) {
	duckdb::LoadInternal(loader);
}
}
