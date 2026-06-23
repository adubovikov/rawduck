#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"

#include <cstring>

namespace duckdb {

//===--------------------------------------------------------------------===//
// RawDuck schema inference
//
// Implements the RawMergeTree-style "ingest first, schema later" model:
// every JSON payload is merged into a schema tree where scalar types widen
// along a lattice and structural conflicts degrade to JSON. Nested objects
// are flattened into dotted column paths so values land in real typed
// columns instead of opaque JSON strings.
//===--------------------------------------------------------------------===//

// Scalar widening lattice: UNSET < BOOLEAN | BIGINT < DOUBLE | DATE < TIMESTAMP,
// any other combination joins to VARCHAR (the universal scalar sink).
enum class RawScalarKind : uint8_t { UNSET = 0, BOOLEAN, BIGINT, DOUBLE, DATE, TIMESTAMP, VARCHAR };

enum class RawNodeClass : uint8_t {
	UNSET = 0, // only nulls seen so far
	SCALAR,    // scalar values, type tracked in `scalar`
	OBJECT,    // nested object, children flattened into dotted columns
	ARRAY,     // homogeneous array, element type tracked in `element`
	JSON       // structural conflict or unflattenable value: stored as JSON text
};

static constexpr idx_t RAW_NODE_NO_COLUMN = DConstants::INVALID_INDEX;

struct RawNode {
	RawNodeClass node_class = RawNodeClass::UNSET;
	RawScalarKind scalar = RawScalarKind::UNSET;
	// set by FlattenSchema: dense column index for leaf nodes, INVALID for
	// intermediate objects — eliminates hash lookups in the extraction hot path
	idx_t column_idx = RAW_NODE_NO_COLUMN;
	// object children in first-seen order, so inferred column order is stable
	vector<pair<string, unique_ptr<RawNode>>> children;
	unordered_map<string, idx_t> child_lookup;
	unique_ptr<RawNode> element;

	RawNode &GetOrCreateChild(const string &key);

	// zero-alloc child lookup by raw pointer+length (extraction hot path)
	idx_t FindChild(const char *key, size_t len) const {
		for (idx_t i = 0; i < children.size(); i++) {
			auto &name = children[i].first;
			if (name.size() == len && memcmp(name.data(), key, len) == 0) {
				return i;
			}
		}
		return DConstants::INVALID_INDEX;
	}
};

// A flattened output column: `name` is the dotted path, `path` the segments to
// walk in each JSON row, `node` points into the schema tree owned by the caller.
struct RawColumn {
	string name;
	vector<string> path;
	LogicalType type;
	const RawNode *node;
};

struct RawParseOptions {
	// skip and count unparseable NDJSON lines instead of failing
	bool ignore_errors = false;
	// ingest-time transform: explode the array at this dotted path into one
	// row per element, merging the envelope fields into each row
	vector<string> explode_path;
	// apply OTLP semantics after exploding: spread KeyValue attribute lists
	// into their parent, unwrap AnyValue, numeric *UnixNano fields
	bool otlp = false;
};

// A parsed payload: one or more JSON documents and the row roots within them.
struct RawPayload {
	vector<duckdb_yyjson::yyjson_doc *> docs;
	vector<duckdb_yyjson::yyjson_val *> rows;
	// true when rows are bare scalars/arrays: they map to a single "value" column
	bool scalar_rows = false;
	// NDJSON lines skipped because they failed to parse (ignore_errors only)
	idx_t parse_errors = 0;
	// apply OTLP semantic normalization during Explode
	bool otlp_semantics = false;

	RawPayload() = default;
	RawPayload(const RawPayload &) = delete;
	RawPayload &operator=(const RawPayload &) = delete;
	~RawPayload();

	// Accepts a JSON array of objects, a single object, or NDJSON; throws
	// InvalidInputException otherwise.
	void Parse(const string &payload, const RawParseOptions &options = RawParseOptions());
	unique_ptr<RawNode> InferSchema() const;
	void Explode(const vector<string> &path);
};

// Immutable schema snapshot for reuse across batches with the same shape.
// Shared (via shared_ptr) between RawSchemaCache and parser threads.
struct RawCachedSchema {
	shared_ptr<RawNode> root;
	vector<RawColumn> columns;
};

// A fully processed payload: parsed rows plus the inferred flattened schema.
// Parsed exactly once and shared by inference and extraction.
struct RawParsedPayload {
	RawPayload payload;
	unique_ptr<RawNode> root;
	// when using a cached schema, root_shared keeps it alive and root is unused
	shared_ptr<RawNode> root_shared;
	vector<RawColumn> columns;

	const RawNode &SchemaRoot() const {
		return root_shared ? *root_shared : *root;
	}

	static shared_ptr<RawParsedPayload> Process(const string &payload_text,
	                                            const RawParseOptions &options = RawParseOptions());
	// fast path: skip inference, reuse cached schema
	static shared_ptr<RawParsedPayload> ProcessWithSchema(const string &payload_text, const RawParseOptions &options,
	                                                      const shared_ptr<RawCachedSchema> &cached);
};

// Incremental payload assembly (zero-copy INSERT sink): parse documents one
// at a time straight from source memory, then finalize (uniformity check +
// inference + flattening) once per batch.
void RawPayloadAddDocument(RawParsedPayload &payload, const char *data, idx_t size, bool ignore_errors = false);
void RawPayloadFinalize(RawParsedPayload &payload, const RawParseOptions &options = RawParseOptions());

// FNV-1a hash over column names + type ids; used by the schema shape cache
uint64_t HashPayloadShape(const vector<RawColumn> &columns);

// Built-in ingest-time transforms (name -> dotted explode path); users can
// register additional ones via raw_transform_define().
const vector<pair<string, string>> &RawBuiltinTransforms();
bool ResolveBuiltinTransform(const string &name, string &path);
bool RawTransformIsOtlp(const string &name);
RawParseOptions RawExplodeOptions(const string &path);

RawScalarKind JoinScalarKinds(RawScalarKind a, RawScalarKind b);
RawScalarKind SniffScalarKind(duckdb_yyjson::yyjson_val *val);
void MergeValue(RawNode &node, duckdb_yyjson::yyjson_val *val);

LogicalType NodeToType(const RawNode &node);
vector<RawColumn> FlattenSchema(RawNode &root, bool scalar_rows);

bool IsRawJSONType(const LogicalType &type);

// Vectorized extraction. Rows are typically sparse compared to the unified
// schema, so extraction is row-major: each row's JSON tree is traversed once
// and values are routed to their column slot via the schema-tree nodes.
// Uses dense column_idx on RawNode (set by FlattenSchema) instead of hash maps.
class RawExtractor {
public:
	RawExtractor(const RawNode &root, const vector<RawColumn> &columns);

	// Routes one row's values into per-column slots at `row_idx`. Slots for
	// absent paths keep their nullptr from Reset().
	void AssignRow(duckdb_yyjson::yyjson_val *row, idx_t row_idx);
	void Reset(idx_t row_count);

	const vector<duckdb_yyjson::yyjson_val *> &ColumnValues(idx_t col) const {
		return values[col];
	}

private:
	void Traverse(duckdb_yyjson::yyjson_val *val, const RawNode &node, idx_t row_idx);

	const RawNode &root;
	bool root_is_column = false;
	idx_t root_column_idx = RAW_NODE_NO_COLUMN;
	idx_t column_count = 0;
	vector<vector<duckdb_yyjson::yyjson_val *>> values;
};

void FillVector(const vector<duckdb_yyjson::yyjson_val *> &vals, const LogicalType &type, Vector &result, idx_t offset);
// whether FillVector can write this type directly (otherwise extract in the
// inferred type and cast)
bool RawFillSupported(const LogicalType &type);

} // namespace duckdb
