#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_val;

namespace {

struct ParsedDoc {
	yyjson_doc *doc = nullptr;
	yyjson_val *root = nullptr;

	explicit ParsedDoc(string_t input) {
		doc =
		    duckdb_yyjson::yyjson_read(input.GetData(), input.GetSize(), duckdb_yyjson::YYJSON_READ_ALLOW_INF_AND_NAN);
		if (doc) {
			root = duckdb_yyjson::yyjson_doc_get_root(doc);
		}
	}
	~ParsedDoc() {
		if (doc) {
			duckdb_yyjson::yyjson_doc_free(doc);
		}
	}
};

} // namespace

// raw_type: the RawTree dynamicType() equivalent, reports the concrete type
// of a JSON value. Unparseable input is treated as a plain string value.
static void RawTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		ParsedDoc parsed(input);
		const char *type_name = "String";
		if (parsed.root) {
			switch (duckdb_yyjson::yyjson_get_type(parsed.root)) {
			case YYJSON_TYPE_NULL:
				type_name = "Null";
				break;
			case YYJSON_TYPE_BOOL:
				type_name = "Bool";
				break;
			case YYJSON_TYPE_NUM:
				if (duckdb_yyjson::yyjson_is_real(parsed.root)) {
					type_name = "Double";
				} else if (duckdb_yyjson::yyjson_is_uint(parsed.root) &&
				           duckdb_yyjson::yyjson_get_uint(parsed.root) >
				               static_cast<uint64_t>(NumericLimits<int64_t>::Maximum())) {
					type_name = "UInt64";
				} else {
					type_name = "Int64";
				}
				break;
			case YYJSON_TYPE_STR:
				type_name = "String";
				break;
			case YYJSON_TYPE_ARR:
				type_name = "Array";
				break;
			case YYJSON_TYPE_OBJ:
				type_name = "Object";
				break;
			default:
				break;
			}
		}
		return StringVector::AddString(result, type_name);
	});
}

// raw_infer: the DuckDB type RawDuck inference assigns to a JSON value; for
// objects, the full flattened column layout.
static void RawInferFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		ParsedDoc parsed(input);
		string inferred;
		if (!parsed.root) {
			inferred = "VARCHAR";
		} else {
			RawNode node;
			MergeValue(node, parsed.root);
			if (node.node_class == RawNodeClass::OBJECT) {
				inferred = "OBJECT(";
				auto columns = FlattenSchema(node, false);
				for (idx_t i = 0; i < columns.size(); i++) {
					inferred += (i ? ", " : "") + columns[i].name + " " + columns[i].type.ToString();
				}
				inferred += ")";
			} else {
				inferred = NodeToType(node).ToString();
			}
		}
		return StringVector::AddString(result, inferred);
	});
}

ScalarFunction GetRawTypeFunction() {
	return ScalarFunction("raw_type", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RawTypeFunction);
}

ScalarFunction GetRawInferFunction() {
	return ScalarFunction("raw_infer", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RawInferFunction);
}

} // namespace duckdb
