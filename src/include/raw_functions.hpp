#pragma once

#include "duckdb.hpp"

namespace duckdb {

class OptimizerExtension;

TableFunction GetRawRecordsFunction();
TableFunction GetRawIngestFunction();
TableFunction GetRawIngestFileFunction();
TableFunction GetRawStatsFunction();
TableFunction GetRawOptimizeFunction();
ScalarFunction GetRawTypeFunction();
ScalarFunction GetRawInferFunction();
OptimizerExtension GetRawDuckOptimizerExtension();

// shared SQL generation helpers
string RawQuoteIdentifier(const string &name);
string RawQualifiedTarget(const string &target);

} // namespace duckdb
