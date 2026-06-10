#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct RawRecordsBindData : public TableFunctionData {
	// the schema tree must outlive the columns, which point into it
	unique_ptr<RawPayload> payload;
	unique_ptr<RawNode> root;
	vector<RawColumn> columns;
};

struct RawRecordsState : public GlobalTableFunctionState {
	explicit RawRecordsState(const RawRecordsBindData &bind_data) : extractor(*bind_data.root, bind_data.columns) {
	}

	idx_t next_row = 0;
	RawExtractor extractor;
};

static unique_ptr<FunctionData> RawRecordsBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawRecordsBindData>();
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("RawDuck: payload may not be NULL");
	}
	auto payload_str = input.inputs[0].GetValue<string>();
	result->payload = make_uniq<RawPayload>();
	result->payload->Parse(payload_str);
	result->root = result->payload->InferSchema();
	result->columns = FlattenSchema(*result->root, result->payload->scalar_rows);
	if (result->columns.empty()) {
		// empty payload: keep a valid (empty) result shape
		return_types.push_back(LogicalType::JSON());
		names.push_back("value");
		result->payload->rows.clear();
		return std::move(result);
	}
	for (auto &column : result->columns) {
		return_types.push_back(column.type);
		names.push_back(column.name);
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawRecordsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawRecordsState>(input.bind_data->Cast<RawRecordsBindData>());
}

static void RawRecordsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawRecordsBindData>();
	auto &state = data.global_state->Cast<RawRecordsState>();
	auto &rows = bind_data.payload->rows;
	auto count = MinValue<idx_t>(rows.size() - state.next_row, STANDARD_VECTOR_SIZE);
	state.extractor.Reset(count);
	for (idx_t i = 0; i < count; i++) {
		state.extractor.AssignRow(rows[state.next_row + i], i);
	}
	for (idx_t col = 0; col < bind_data.columns.size(); col++) {
		FillVector(state.extractor.ColumnValues(col), bind_data.columns[col].type, output.data[col], 0);
	}
	state.next_row += count;
	output.SetCardinality(count);
}

TableFunction GetRawRecordsFunction() {
	TableFunction function("raw_records", {LogicalType::VARCHAR}, RawRecordsFunction, RawRecordsBind, RawRecordsInit);
	return function;
}

} // namespace duckdb
