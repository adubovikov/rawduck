#include "raw_json.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstdlib>

namespace duckdb {

using duckdb_yyjson::yyjson_arr_iter;
using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_obj_iter;
using duckdb_yyjson::yyjson_val;

// Objects nested deeper than this are kept as JSON columns instead of being
// flattened further.
static constexpr idx_t RAW_MAX_FLATTEN_DEPTH = 16;
// guard against pathological payloads exploding the table schema
static constexpr idx_t RAW_MAX_COLUMNS = 10000;
static constexpr auto RAW_READ_FLAGS = duckdb_yyjson::YYJSON_READ_ALLOW_INF_AND_NAN;

//===--------------------------------------------------------------------===//
// Payload parsing
//===--------------------------------------------------------------------===//

RawPayload::~RawPayload() {
	for (auto doc : docs) {
		duckdb_yyjson::yyjson_doc_free(doc);
	}
}

static void CollectRows(yyjson_val *root, vector<yyjson_val *> &rows) {
	if (duckdb_yyjson::yyjson_is_arr(root)) {
		yyjson_arr_iter iter;
		duckdb_yyjson::yyjson_arr_iter_init(root, &iter);
		while (auto element = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
			rows.push_back(element);
		}
	} else {
		rows.push_back(root);
	}
}

static void CheckRowUniformity(const vector<yyjson_val *> &rows, bool &scalar_rows) {
	// rows must be uniformly objects, or uniformly non-objects ("value" column)
	idx_t object_rows = 0;
	for (auto row : rows) {
		if (duckdb_yyjson::yyjson_is_obj(row)) {
			object_rows++;
		}
	}
	if (object_rows > 0 && object_rows != rows.size()) {
		throw InvalidInputException("RawDuck: payload mixes JSON objects with non-object values");
	}
	scalar_rows = !rows.empty() && object_rows == 0;
}

void RawPayload::Parse(const string &payload, const RawParseOptions &options) {
	auto doc = duckdb_yyjson::yyjson_read(payload.c_str(), payload.size(), RAW_READ_FLAGS);
	if (doc) {
		docs.push_back(doc);
		CollectRows(duckdb_yyjson::yyjson_doc_get_root(doc), rows);
	} else {
		// not a single JSON document: try newline-delimited JSON
		idx_t line_number = 0;
		size_t pos = 0;
		while (pos <= payload.size()) {
			auto end = payload.find('\n', pos);
			if (end == string::npos) {
				end = payload.size();
			}
			line_number++;
			auto line_begin = payload.data() + pos;
			auto line_len = end - pos;
			pos = end + 1;
			// skip blank lines
			bool blank = true;
			for (idx_t i = 0; i < line_len; i++) {
				if (!StringUtil::CharacterIsSpace(line_begin[i])) {
					blank = false;
					break;
				}
			}
			if (blank) {
				continue;
			}
			auto line_doc = duckdb_yyjson::yyjson_read(line_begin, line_len, RAW_READ_FLAGS);
			if (!line_doc) {
				if (options.ignore_errors) {
					parse_errors++;
					continue;
				}
				throw InvalidInputException("RawDuck: payload is not valid JSON or NDJSON (parse error on line %llu)",
				                            line_number);
			}
			docs.push_back(line_doc);
			rows.push_back(duckdb_yyjson::yyjson_doc_get_root(line_doc));
		}
	}
	CheckRowUniformity(rows, scalar_rows);
	if (!options.explode_path.empty()) {
		Explode(options.explode_path);
	}
}

// One envelope per traversed level: the object plus the key that was
// descended into (excluded when merging).
struct RawExplodeEnvelope {
	yyjson_val *object;
	const string *descended_key;
};

static void AddObjectFields(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *merged,
                            yyjson_val *object, const string *skip_key) {
	yyjson_obj_iter iter;
	duckdb_yyjson::yyjson_obj_iter_init(object, &iter);
	while (auto key = duckdb_yyjson::yyjson_obj_iter_next(&iter)) {
		auto key_str = duckdb_yyjson::yyjson_get_str(key);
		auto key_len = duckdb_yyjson::yyjson_get_len(key);
		if (skip_key && skip_key->size() == key_len && memcmp(skip_key->data(), key_str, key_len) == 0) {
			continue;
		}
		// first writer wins: deeper levels are added before shallower ones
		if (duckdb_yyjson::yyjson_mut_obj_getn(merged, key_str, key_len)) {
			continue;
		}
		auto key_copy = duckdb_yyjson::yyjson_mut_strncpy(doc, key_str, key_len);
		duckdb_yyjson::yyjson_mut_obj_add(
		    merged, key_copy, duckdb_yyjson::yyjson_val_mut_copy(doc, duckdb_yyjson::yyjson_obj_iter_get_val(key)));
	}
}

static void EmitMergedRow(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *out_rows, yyjson_val *leaf,
                          const vector<RawExplodeEnvelope> &envelopes) {
	if (!duckdb_yyjson::yyjson_is_obj(leaf)) {
		// scalar leaf elements cannot be merged: pass them through
		duckdb_yyjson::yyjson_mut_arr_append(out_rows, duckdb_yyjson::yyjson_val_mut_copy(doc, leaf));
		return;
	}
	auto merged = duckdb_yyjson::yyjson_mut_obj(doc);
	AddObjectFields(doc, merged, leaf, nullptr);
	// envelopes from deepest to shallowest: deeper fields win on collision
	for (idx_t i = envelopes.size(); i > 0; i--) {
		AddObjectFields(doc, merged, envelopes[i - 1].object, envelopes[i - 1].descended_key);
	}
	duckdb_yyjson::yyjson_mut_arr_append(out_rows, merged);
}

// Recursive multi-level explode: each path segment descends into an object
// key; arrays along the way fan out into one row per element. This handles
// both flat envelopes (cloudwatch-logs: logEvents) and nested ones
// (otlp-traces: resourceSpans.scopeSpans.spans).
static void ExplodeInto(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *out_rows, yyjson_val *node,
                        const vector<string> &path, idx_t depth, vector<RawExplodeEnvelope> &envelopes) {
	if (depth == path.size()) {
		EmitMergedRow(doc, out_rows, node, envelopes);
		return;
	}
	if (!duckdb_yyjson::yyjson_is_obj(node)) {
		EmitMergedRow(doc, out_rows, node, envelopes);
		return;
	}
	auto &segment = path[depth];
	auto child = duckdb_yyjson::yyjson_obj_getn(node, segment.c_str(), segment.size());
	if (!child) {
		// the hop is absent: pass the row through unchanged (merged with any
		// envelopes accumulated so far)
		EmitMergedRow(doc, out_rows, node, envelopes);
		return;
	}
	envelopes.push_back(RawExplodeEnvelope {node, &segment});
	if (duckdb_yyjson::yyjson_is_arr(child)) {
		duckdb_yyjson::yyjson_arr_iter elements;
		duckdb_yyjson::yyjson_arr_iter_init(child, &elements);
		while (auto element = duckdb_yyjson::yyjson_arr_iter_next(&elements)) {
			ExplodeInto(doc, out_rows, element, path, depth + 1, envelopes);
		}
	} else {
		ExplodeInto(doc, out_rows, child, path, depth + 1, envelopes);
	}
	envelopes.pop_back();
}

void RawPayload::Explode(const vector<string> &path) {
	auto mut_doc = duckdb_yyjson::yyjson_mut_doc_new(nullptr);
	auto out_rows = duckdb_yyjson::yyjson_mut_arr(mut_doc);
	duckdb_yyjson::yyjson_mut_doc_set_root(mut_doc, out_rows);

	vector<RawExplodeEnvelope> envelopes;
	for (auto row : rows) {
		ExplodeInto(mut_doc, out_rows, row, path, 0, envelopes);
	}

	auto exploded = duckdb_yyjson::yyjson_mut_doc_imut_copy(mut_doc, nullptr);
	duckdb_yyjson::yyjson_mut_doc_free(mut_doc);
	if (!exploded) {
		throw InternalException("RawDuck: failed to materialize exploded payload");
	}
	for (auto old_doc : docs) {
		duckdb_yyjson::yyjson_doc_free(old_doc);
	}
	docs.clear();
	rows.clear();
	docs.push_back(exploded);
	CollectRows(duckdb_yyjson::yyjson_doc_get_root(exploded), rows);
	CheckRowUniformity(rows, scalar_rows);
}

unique_ptr<RawNode> RawPayload::InferSchema() const {
	auto root = make_uniq<RawNode>();
	for (auto row : rows) {
		MergeValue(*root, row);
	}
	return root;
}

shared_ptr<RawParsedPayload> RawParsedPayload::Process(const string &payload_text, const RawParseOptions &options) {
	auto result = make_shared_ptr<RawParsedPayload>();
	result->payload.Parse(payload_text, options);
	result->root = result->payload.InferSchema();
	result->columns = FlattenSchema(*result->root, result->payload.scalar_rows);
	return result;
}

const vector<pair<string, string>> &RawBuiltinTransforms() {
	static const vector<pair<string, string>> builtins = {
	    {"cloudwatch-logs", "logEvents"},
	    {"cloudtrail", "Records"},
	    {"firehose", "records"},
	    {"otlp-traces", "resourceSpans.scopeSpans.spans"},
	    {"otlp-logs", "resourceLogs.scopeLogs.logRecords"},
	    {"otlp-metrics", "resourceMetrics.scopeMetrics.metrics"},
	};
	return builtins;
}

bool ResolveBuiltinTransform(const string &name, string &path) {
	auto lower = StringUtil::Lower(name);
	for (auto &builtin : RawBuiltinTransforms()) {
		if (builtin.first == lower) {
			path = builtin.second;
			return true;
		}
	}
	return false;
}

RawParseOptions RawExplodeOptions(const string &path) {
	RawParseOptions options;
	if (!path.empty()) {
		options.explode_path = StringUtil::Split(path, '.');
	}
	return options;
}

//===--------------------------------------------------------------------===//
// Inference
//===--------------------------------------------------------------------===//

RawNode &RawNode::GetOrCreateChild(const string &key) {
	auto entry = child_lookup.find(key);
	if (entry != child_lookup.end()) {
		return *children[entry->second].second;
	}
	child_lookup[key] = children.size();
	children.emplace_back(key, make_uniq<RawNode>());
	return *children.back().second;
}

RawScalarKind JoinScalarKinds(RawScalarKind a, RawScalarKind b) {
	if (a == RawScalarKind::UNSET) {
		return b;
	}
	if (b == RawScalarKind::UNSET || a == b) {
		return a;
	}
	auto is_numeric = [](RawScalarKind k) {
		return k == RawScalarKind::BIGINT || k == RawScalarKind::DOUBLE;
	};
	auto is_temporal = [](RawScalarKind k) {
		return k == RawScalarKind::DATE || k == RawScalarKind::TIMESTAMP;
	};
	if (is_numeric(a) && is_numeric(b)) {
		return RawScalarKind::DOUBLE;
	}
	if (is_temporal(a) && is_temporal(b)) {
		return RawScalarKind::TIMESTAMP;
	}
	return RawScalarKind::VARCHAR;
}

// Detect ISO dates/timestamps in strings so they land in temporal columns.
static RawScalarKind SniffString(const char *str, idx_t len) {
	// cheap pre-filter: temporal strings start with a digit and contain '-'
	if (len < 8 || len > 40 || !StringUtil::CharacterIsDigit(str[0])) {
		return RawScalarKind::VARCHAR;
	}
	idx_t pos = 0;
	date_t date_result;
	bool special = false;
	if (Date::TryConvertDate(str, len, pos, date_result, special, true) == DateCastResult::SUCCESS && pos == len &&
	    !special) {
		return RawScalarKind::DATE;
	}
	timestamp_t ts_result;
	if (Timestamp::TryConvertTimestamp(str, len, ts_result, true) == TimestampCastResult::SUCCESS) {
		return RawScalarKind::TIMESTAMP;
	}
	return RawScalarKind::VARCHAR;
}

RawScalarKind SniffScalarKind(yyjson_val *val) {
	switch (duckdb_yyjson::yyjson_get_type(val)) {
	case YYJSON_TYPE_BOOL:
		return RawScalarKind::BOOLEAN;
	case YYJSON_TYPE_NUM:
		if (duckdb_yyjson::yyjson_is_real(val)) {
			return RawScalarKind::DOUBLE;
		}
		if (duckdb_yyjson::yyjson_is_uint(val) &&
		    duckdb_yyjson::yyjson_get_uint(val) > static_cast<uint64_t>(NumericLimits<int64_t>::Maximum())) {
			// out of BIGINT range: degrade to DOUBLE rather than fail
			return RawScalarKind::DOUBLE;
		}
		return RawScalarKind::BIGINT;
	case YYJSON_TYPE_STR:
		return SniffString(duckdb_yyjson::yyjson_get_str(val), duckdb_yyjson::yyjson_get_len(val));
	default:
		return RawScalarKind::VARCHAR;
	}
}

static void DemoteToJSON(RawNode &node) {
	node.node_class = RawNodeClass::JSON;
	node.children.clear();
	node.child_lookup.clear();
	node.element.reset();
}

void MergeValue(RawNode &node, yyjson_val *val) {
	if (!val || duckdb_yyjson::yyjson_is_null(val)) {
		return;
	}
	if (node.node_class == RawNodeClass::JSON) {
		return;
	}
	if (duckdb_yyjson::yyjson_is_obj(val)) {
		if (node.node_class == RawNodeClass::UNSET) {
			node.node_class = RawNodeClass::OBJECT;
		}
		if (node.node_class != RawNodeClass::OBJECT) {
			DemoteToJSON(node);
			return;
		}
		yyjson_obj_iter iter;
		duckdb_yyjson::yyjson_obj_iter_init(val, &iter);
		while (auto key = duckdb_yyjson::yyjson_obj_iter_next(&iter)) {
			auto child_val = duckdb_yyjson::yyjson_obj_iter_get_val(key);
			auto key_str = string(duckdb_yyjson::yyjson_get_str(key), duckdb_yyjson::yyjson_get_len(key));
			MergeValue(node.GetOrCreateChild(key_str), child_val);
		}
		return;
	}
	if (duckdb_yyjson::yyjson_is_arr(val)) {
		if (node.node_class == RawNodeClass::UNSET) {
			node.node_class = RawNodeClass::ARRAY;
			node.element = make_uniq<RawNode>();
		}
		if (node.node_class != RawNodeClass::ARRAY) {
			DemoteToJSON(node);
			return;
		}
		yyjson_arr_iter iter;
		duckdb_yyjson::yyjson_arr_iter_init(val, &iter);
		while (auto element = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
			MergeValue(*node.element, element);
		}
		return;
	}
	// scalar
	auto kind = SniffScalarKind(val);
	if (node.node_class == RawNodeClass::UNSET) {
		node.node_class = RawNodeClass::SCALAR;
		node.scalar = kind;
		return;
	}
	if (node.node_class != RawNodeClass::SCALAR) {
		DemoteToJSON(node);
		return;
	}
	node.scalar = JoinScalarKinds(node.scalar, kind);
}

//===--------------------------------------------------------------------===//
// Schema flattening
//===--------------------------------------------------------------------===//

static LogicalType ScalarKindToType(RawScalarKind kind) {
	switch (kind) {
	case RawScalarKind::BOOLEAN:
		return LogicalType::BOOLEAN;
	case RawScalarKind::BIGINT:
		return LogicalType::BIGINT;
	case RawScalarKind::DOUBLE:
		return LogicalType::DOUBLE;
	case RawScalarKind::DATE:
		return LogicalType::DATE;
	case RawScalarKind::TIMESTAMP:
		return LogicalType::TIMESTAMP;
	default:
		return LogicalType::VARCHAR;
	}
}

LogicalType NodeToType(const RawNode &node) {
	switch (node.node_class) {
	case RawNodeClass::UNSET:
		return LogicalType::VARCHAR;
	case RawNodeClass::SCALAR:
		return ScalarKindToType(node.scalar);
	case RawNodeClass::ARRAY: {
		auto &element = *node.element;
		switch (element.node_class) {
		case RawNodeClass::UNSET:
			return LogicalType::LIST(LogicalType::VARCHAR);
		case RawNodeClass::SCALAR:
			return LogicalType::LIST(ScalarKindToType(element.scalar));
		case RawNodeClass::ARRAY:
			return LogicalType::LIST(NodeToType(element));
		default:
			// arrays of objects (or mixed) stay as a single JSON value
			return LogicalType::JSON();
		}
	}
	default:
		return LogicalType::JSON();
	}
}

static void FlattenInto(const RawNode &node, const string &prefix, vector<string> &path, idx_t depth,
                        vector<RawColumn> &result) {
	for (auto &entry : node.children) {
		auto &name = entry.first;
		auto &child = *entry.second;
		auto column_name = prefix.empty() ? name : prefix + "." + name;
		path.push_back(name);
		if (child.node_class == RawNodeClass::OBJECT && !child.children.empty() && depth < RAW_MAX_FLATTEN_DEPTH) {
			FlattenInto(child, column_name, path, depth + 1, result);
		} else {
			result.push_back(RawColumn {column_name, path, NodeToType(child), &child});
		}
		path.pop_back();
	}
}

vector<RawColumn> FlattenSchema(const RawNode &root, bool scalar_rows) {
	vector<RawColumn> result;
	if (scalar_rows || root.node_class != RawNodeClass::OBJECT) {
		if (root.node_class == RawNodeClass::UNSET) {
			return result;
		}
		result.push_back(RawColumn {"value", {}, NodeToType(root), &root});
		return result;
	}
	vector<string> path;
	FlattenInto(root, "", path, 0, result);
	if (result.size() > RAW_MAX_COLUMNS) {
		throw InvalidInputException(
		    "RawDuck: payload flattens to %llu columns, exceeding the limit of %llu; restructure the payload or "
		    "reduce key cardinality",
		    result.size(), RAW_MAX_COLUMNS);
	}
	// flattening can collide (e.g. key "a.b" vs nested a->b): disambiguate
	case_insensitive_set_t seen;
	for (auto &column : result) {
		auto base = column.name;
		idx_t suffix = 1;
		while (seen.count(column.name)) {
			column.name = base + "_" + to_string(suffix++);
		}
		seen.insert(column.name);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Value extraction
//===--------------------------------------------------------------------===//

bool IsRawJSONType(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARCHAR && type.GetAlias() == LogicalType::JSON_TYPE_NAME;
}

bool RawFillSupported(const LogicalType &type) {
	if (IsRawJSONType(type)) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::VARCHAR:
		return true;
	case LogicalTypeId::LIST:
		return RawFillSupported(ListType::GetChildType(type));
	default:
		return false;
	}
}

RawExtractor::RawExtractor(const RawNode &root_p, const vector<RawColumn> &columns) : root(root_p) {
	values.resize(columns.size());
	for (idx_t col = 0; col < columns.size(); col++) {
		if (columns[col].path.empty()) {
			// "value" column: the row itself is the value
			root_is_column = true;
		}
		column_of[columns[col].node] = col;
	}
}

void RawExtractor::Reset(idx_t row_count) {
	for (auto &column_values : values) {
		column_values.assign(row_count, nullptr);
	}
}

void RawExtractor::Traverse(yyjson_val *val, const RawNode &node, idx_t row_idx) {
	auto leaf = column_of.find(&node);
	if (leaf != column_of.end()) {
		values[leaf->second][row_idx] = val;
		return;
	}
	if (node.node_class != RawNodeClass::OBJECT || !duckdb_yyjson::yyjson_is_obj(val)) {
		// value at a path that is neither a column nor a flattened object
		// (e.g. a structural mismatch): nothing to route
		return;
	}
	yyjson_obj_iter iter;
	duckdb_yyjson::yyjson_obj_iter_init(val, &iter);
	while (auto key = duckdb_yyjson::yyjson_obj_iter_next(&iter)) {
		auto entry =
		    node.child_lookup.find(string(duckdb_yyjson::yyjson_get_str(key), duckdb_yyjson::yyjson_get_len(key)));
		if (entry == node.child_lookup.end()) {
			continue;
		}
		Traverse(duckdb_yyjson::yyjson_obj_iter_get_val(key), *node.children[entry->second].second, row_idx);
	}
}

void RawExtractor::AssignRow(yyjson_val *row, idx_t row_idx) {
	if (root_is_column) {
		values[column_of.at(&root)][row_idx] = row;
		return;
	}
	Traverse(row, root, row_idx);
}

static string_t WriteJSONString(yyjson_val *val, Vector &result) {
	size_t len = 0;
	auto data = duckdb_yyjson::yyjson_val_write(val, 0, &len);
	if (!data) {
		throw InternalException("RawDuck: failed to serialize JSON value");
	}
	auto str = StringVector::AddString(result, data, len);
	free(data);
	return str;
}

// Writes a batch of resolved JSON values into `result` starting at `offset`.
// Inference guarantees values fit the column type; anything that does not
// (or is null/missing) becomes NULL.
void FillVector(const vector<yyjson_val *> &vals, const LogicalType &type, Vector &result, idx_t offset) {
	auto &validity = FlatVector::Validity(result);
	auto set_null = [&](idx_t i) {
		validity.SetInvalid(offset + i);
	};
	if (IsRawJSONType(type)) {
		auto data = FlatVector::GetData<string_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (!val || duckdb_yyjson::yyjson_is_null(val)) {
				set_null(i);
				continue;
			}
			data[offset + i] = WriteJSONString(val, result);
		}
		return;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto data = FlatVector::GetData<bool>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (val && duckdb_yyjson::yyjson_is_bool(val)) {
				data[offset + i] = duckdb_yyjson::yyjson_get_bool(val);
			} else {
				set_null(i);
			}
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto data = FlatVector::GetData<int64_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (val && duckdb_yyjson::yyjson_is_sint(val)) {
				data[offset + i] = duckdb_yyjson::yyjson_get_sint(val);
			} else if (val && duckdb_yyjson::yyjson_is_uint(val)) {
				data[offset + i] = static_cast<int64_t>(duckdb_yyjson::yyjson_get_uint(val));
			} else {
				set_null(i);
			}
		}
		break;
	}
	case LogicalTypeId::DOUBLE: {
		auto data = FlatVector::GetData<double>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (val && duckdb_yyjson::yyjson_is_num(val)) {
				data[offset + i] = duckdb_yyjson::yyjson_get_num(val);
			} else {
				set_null(i);
			}
		}
		break;
	}
	case LogicalTypeId::DATE: {
		auto data = FlatVector::GetData<date_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			idx_t pos = 0;
			bool special = false;
			if (!val || !duckdb_yyjson::yyjson_is_str(val) ||
			    Date::TryConvertDate(duckdb_yyjson::yyjson_get_str(val), duckdb_yyjson::yyjson_get_len(val), pos,
			                         data[offset + i], special, true) != DateCastResult::SUCCESS) {
				set_null(i);
			}
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto data = FlatVector::GetData<timestamp_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (!val || !duckdb_yyjson::yyjson_is_str(val) ||
			    Timestamp::TryConvertTimestamp(duckdb_yyjson::yyjson_get_str(val), duckdb_yyjson::yyjson_get_len(val),
			                                   data[offset + i], true) != TimestampCastResult::SUCCESS) {
				set_null(i);
			}
		}
		break;
	}
	case LogicalTypeId::VARCHAR: {
		auto data = FlatVector::GetData<string_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (!val || duckdb_yyjson::yyjson_is_null(val)) {
				set_null(i);
			} else if (duckdb_yyjson::yyjson_is_str(val)) {
				data[offset + i] = StringVector::AddString(result, duckdb_yyjson::yyjson_get_str(val),
				                                           duckdb_yyjson::yyjson_get_len(val));
			} else {
				// mixed-type column: render non-strings as their JSON literal
				data[offset + i] = WriteJSONString(val, result);
			}
		}
		break;
	}
	case LogicalTypeId::LIST: {
		auto entries = FlatVector::GetData<list_entry_t>(result);
		auto child_offset = ListVector::GetListSize(result);
		vector<yyjson_val *> child_vals;
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (!val || !duckdb_yyjson::yyjson_is_arr(val)) {
				set_null(i);
				entries[offset + i] = list_entry_t {child_offset + child_vals.size(), 0};
				continue;
			}
			auto length = duckdb_yyjson::yyjson_arr_size(val);
			entries[offset + i] = list_entry_t {child_offset + child_vals.size(), length};
			yyjson_arr_iter iter;
			duckdb_yyjson::yyjson_arr_iter_init(val, &iter);
			while (auto element = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
				child_vals.push_back(element);
			}
		}
		ListVector::Reserve(result, child_offset + child_vals.size());
		ListVector::SetListSize(result, child_offset + child_vals.size());
		FillVector(child_vals, ListType::GetChildType(type), ListVector::GetEntry(result), child_offset);
		break;
	}
	default:
		for (idx_t i = 0; i < vals.size(); i++) {
			set_null(i);
		}
		break;
	}
}

} // namespace duckdb
