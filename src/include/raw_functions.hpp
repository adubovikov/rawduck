#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class OptimizerExtension;
class StorageExtension;
struct RawParseOptions;

TableFunction GetRawRecordsFunction();
TableFunction GetRawIngestFunction();
TableFunction GetRawIngestFileFunction();
TableFunction GetRawStatsFunction();
TableFunction GetRawOptimizeFunction();
TableFunction GetRawTransformsFunction();
TableFunction GetRawProjectionsFunction();
TableFunction GetRawProjectFunction();
ScalarFunction GetRawTypeFunction();
ScalarFunction GetRawInferFunction();
ScalarFunction GetRawTransformDefineFunction();
OptimizerExtension GetRawDuckOptimizerExtension();
shared_ptr<StorageExtension> GetRawDuckStorageExtension();

// shared SQL generation helpers
string RawQuoteIdentifier(const string &name);
string RawQualifiedTarget(const string &target);

// shared ingest parameter handling (transform / explode / ignore_errors)
string RawNamedStringParameter(const named_parameter_map_t &parameters, const string &name);
RawParseOptions RawBindParseOptions(ClientContext &context, const named_parameter_map_t &parameters);
RawParseOptions ResolveTransform(ClientContext &context, const string &transform, const string &explode);
void RawAddIngestParameters(TableFunction &function);

} // namespace duckdb
