#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <chrono>
#include <thread>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Asynchronous inserts (opt-in), modeled on ClickHouse async_insert:
// with SET rawduck_async_insert = true, ingestion calls enqueue the payload
// into a per-table buffer and return immediately; a background flusher
// coalesces buffers and ingests them in one transaction per table when the
// buffer exceeds rawduck_async_max_data_size bytes or its oldest entry
// exceeds rawduck_async_busy_timeout_ms. raw_flush() drains synchronously.
//===--------------------------------------------------------------------===//

class RawAsyncBuffers : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rawduck_async_buffers";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}
	~RawAsyncBuffers() override {
		Shutdown();
	}

	struct Buffer {
		vector<pair<string, RawParseOptions>> payloads;
		idx_t bytes = 0;
		std::chrono::steady_clock::time_point oldest;
	};

	void Start(weak_ptr<DatabaseInstance> db_p) {
		std::unique_lock<mutex> guard(lock);
		db = std::move(db_p);
		if (!running) {
			running = true;
			flusher = std::thread([this] { FlushLoop(); });
		}
	}
	void Shutdown() {
		{
			std::unique_lock<mutex> guard(lock);
			if (!running) {
				return;
			}
			running = false;
			wake.notify_all();
		}
		flusher.join();
	}

	void Enqueue(const string &target, string payload, RawParseOptions options, idx_t max_bytes) {
		std::unique_lock<mutex> guard(lock);
		auto &buffer = buffers[target];
		if (buffer.payloads.empty()) {
			buffer.oldest = std::chrono::steady_clock::now();
		}
		buffer.bytes += payload.size();
		buffer.payloads.emplace_back(std::move(payload), std::move(options));
		if (buffer.bytes >= max_bytes) {
			wake.notify_all();
		}
	}

	// flush every buffer synchronously; returns (targets, rows)
	pair<idx_t, idx_t> FlushAll() {
		map<string, Buffer> taken;
		{
			std::unique_lock<mutex> guard(lock);
			taken.swap(buffers);
		}
		idx_t targets = 0;
		idx_t total_rows = 0;
		for (auto &entry : taken) {
			total_rows += FlushBuffer(entry.first, entry.second);
			targets++;
		}
		return {targets, total_rows};
	}

	idx_t busy_timeout_ms = 200;

private:
	idx_t FlushBuffer(const string &target, Buffer &buffer) {
		auto db_locked = db.lock();
		if (!db_locked) {
			return 0;
		}
		Connection conn(*db_locked);
		conn.BeginTransaction();
		idx_t flushed = 0;
		try {
			for (auto &payload : buffer.payloads) {
				flushed += RawIngestPayload(*conn.context, target, payload.first, payload.second).rows;
			}
			conn.Commit();
		} catch (...) {
			conn.Rollback();
			// fire-and-forget semantics: the batch is dropped, like
			// ClickHouse async inserts without wait_for_async_insert
			return 0;
		}
		return flushed;
	}

	void FlushLoop() {
		std::unique_lock<mutex> guard(lock);
		while (running) {
			wake.wait_for(guard, std::chrono::milliseconds(busy_timeout_ms / 2 + 1));
			if (!running) {
				break;
			}
			auto now = std::chrono::steady_clock::now();
			map<string, Buffer> due;
			for (auto it = buffers.begin(); it != buffers.end();) {
				auto age = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second.oldest).count();
				if (age >= NumericCast<int64_t>(busy_timeout_ms) || it->second.bytes >= due_bytes) {
					due.emplace(it->first, std::move(it->second));
					it = buffers.erase(it);
				} else {
					++it;
				}
			}
			if (!due.empty()) {
				guard.unlock();
				for (auto &entry : due) {
					FlushBuffer(entry.first, entry.second);
				}
				guard.lock();
			}
		}
		// drain on shutdown, best effort
		auto remaining = std::move(buffers);
		guard.unlock();
		for (auto &entry : remaining) {
			FlushBuffer(entry.first, entry.second);
		}
	}

	mutex lock;
	std::condition_variable wake;
	map<string, Buffer> buffers;
	weak_ptr<DatabaseInstance> db;
	std::thread flusher;
	bool running = false;

public:
	idx_t due_bytes = 1024 * 1024;
};

static RawAsyncBuffers &GetAsyncBuffers(ClientContext &context) {
	return *ObjectCache::GetObjectCache(context).GetOrCreate<RawAsyncBuffers>(RawAsyncBuffers::ObjectType());
}

bool RawAsyncEnabled(ClientContext &context) {
	Value enabled;
	return context.TryGetCurrentSetting("rawduck_async_insert", enabled) && enabled.GetValue<bool>();
}

void RawAsyncEnqueue(ClientContext &context, const string &target, string payload, RawParseOptions options) {
	auto &buffers = GetAsyncBuffers(context);
	Value setting;
	if (context.TryGetCurrentSetting("rawduck_async_max_data_size", setting)) {
		buffers.due_bytes = NumericCast<idx_t>(setting.GetValue<int64_t>());
	}
	if (context.TryGetCurrentSetting("rawduck_async_busy_timeout_ms", setting)) {
		buffers.busy_timeout_ms = NumericCast<idx_t>(setting.GetValue<int64_t>());
	}
	buffers.Start(context.db);
	buffers.Enqueue(target, std::move(payload), std::move(options), buffers.due_bytes);
}

//===--------------------------------------------------------------------===//
// raw_flush(): synchronously drain all async buffers
//===--------------------------------------------------------------------===//

struct RawFlushState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> RawFlushBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::BIGINT, LogicalType::BIGINT};
	names = {"targets", "rows"};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> RawFlushInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawFlushState>();
}

static void RawFlushFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RawFlushState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
	auto &buffers = GetAsyncBuffers(context);
	buffers.Start(context.db);
	auto flushed = buffers.FlushAll();
	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(flushed.first)));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(flushed.second)));
	output.SetCardinality(1);
}

TableFunction GetRawFlushFunction() {
	return TableFunction("raw_flush", {}, RawFlushFunction, RawFlushBind, RawFlushInit);
}

} // namespace duckdb
