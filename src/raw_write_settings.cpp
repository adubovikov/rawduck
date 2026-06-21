#include "raw_write_settings.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

#include <thread>

namespace duckdb {

RawWriteSettings RawWriteSettings::Get(ClientContext &context) {
	RawWriteSettings settings;
	Value value;
	if (context.TryGetCurrentSetting("rawduck_pool_min_rows", value) && !value.IsNull()) {
		auto min_rows = value.GetValue<int64_t>();
		if (min_rows > 0) {
			settings.pool_min_rows = NumericCast<idx_t>(min_rows);
		}
	}
	if (context.TryGetCurrentSetting("rawduck_pool_threads", value) && !value.IsNull()) {
		auto threads = value.GetValue<int64_t>();
		if (threads > 0) {
			settings.pool_threads = NumericCast<idx_t>(threads);
		}
	}
	if (context.TryGetCurrentSetting("rawduck_pipeline_threads", value) && !value.IsNull()) {
		auto threads = value.GetValue<int64_t>();
		if (threads > 0) {
			settings.pipeline_threads = NumericCast<idx_t>(threads);
		}
	}
	if (context.TryGetCurrentSetting("rawduck_pipeline_consumers", value) && !value.IsNull()) {
		auto consumers = value.GetValue<int64_t>();
		if (consumers > 0) {
			settings.pipeline_consumers = NumericCast<idx_t>(consumers);
		}
	}
	if (context.TryGetCurrentSetting("rawduck_pipeline_depth", value) && !value.IsNull()) {
		auto depth = value.GetValue<int64_t>();
		if (depth > 0) {
			settings.pipeline_depth = NumericCast<idx_t>(depth);
		}
	}
	if (context.TryGetCurrentSetting("rawduck_overlap_flush_auto", value) && !value.IsNull()) {
		settings.overlap_flush_auto = value.GetValue<bool>();
	}
	if (context.TryGetCurrentSetting("rawduck_checkpoint_after_ingest", value) && !value.IsNull()) {
		auto threshold = value.GetValue<int64_t>();
		if (threshold > 0) {
			settings.checkpoint_after_ingest = NumericCast<idx_t>(threshold);
		}
	}
	return settings;
}

idx_t RawWriteSettings::PoolThreadCount(idx_t batch_rows) const {
	if (pool_threads > 0) {
		return MinValue<idx_t>(pool_threads, MAX_POOL_THREADS);
	}
	if (batch_rows >= pool_min_rows) {
		return MaxValue<idx_t>(1, MinValue<idx_t>(std::thread::hardware_concurrency() / 2, AUTO_POOL_THREAD_CAP));
	}
	if (batch_rows >= FAST_POOL_MIN_ROWS) {
		return 1;
	}
	return 1;
}

idx_t RawWriteSettings::PipelineThreadCount() const {
	if (pipeline_threads > 0) {
		return MinValue<idx_t>(pipeline_threads, MAX_PIPELINE_THREADS);
	}
	return MaxValue<idx_t>(1, MinValue<idx_t>(std::thread::hardware_concurrency() * 2 / 3, AUTO_PIPELINE_THREAD_CAP));
}

idx_t RawWriteSettings::PipelineConsumerCount() const {
	if (pipeline_consumers > 0) {
		return MinValue<idx_t>(pipeline_consumers, MAX_PIPELINE_THREADS);
	}
	auto hw = std::thread::hardware_concurrency();
	if (hw <= 1) {
		return 1;
	}
	return MaxValue<idx_t>(2, MinValue<idx_t>(hw / 4, AUTO_PIPELINE_CONSUMER_CAP));
}

bool RawWriteSettings::OverlapFlushForBatch(ClientContext &context, bool shape_absorbed, idx_t batch_rows) const {
	Value enabled;
	if (context.TryGetCurrentSetting("rawduck_overlap_flush", enabled) && !enabled.IsNull() &&
	    enabled.GetValue<bool>()) {
		return true;
	}
	return overlap_flush_auto && shape_absorbed && batch_rows >= pool_min_rows;
}

void RawMaybeCheckpointAfterIngest(ClientContext &context, idx_t rows_ingested) {
	auto settings = RawWriteSettings::Get(context);
	if (settings.checkpoint_after_ingest == 0 || rows_ingested < settings.checkpoint_after_ingest) {
		return;
	}
	Connection conn(*context.db);
	auto result = conn.Query("CHECKPOINT");
	if (result->HasError()) {
		result->ThrowError("RawDuck checkpoint: ");
	}
}

} // namespace duckdb
