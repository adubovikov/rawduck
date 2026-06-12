#include "raw_functions.hpp"

#ifdef RAWDUCK_WITH_OTLP_PROTOBUF
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
#include "opentelemetry/proto/collector/trace/v1/trace_service.pb.h"

#include <google/protobuf/util/json_util.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// OTLP/HTTP protobuf bodies (Content-Type: application/x-protobuf).
//
// Decoding reuses the exact same machinery as the gRPC server: parse the
// Export*ServiceRequest message, convert with MessageToJsonString, and feed
// the result through the regular OTLP/JSON ingestion path (where the otlp
// semantic normalization also converts base64 trace/span ids to hex).
// Compiled out (graceful 415) when protobuf is unavailable at build time.
//===--------------------------------------------------------------------===//

// serialized google.rpc.Status carrying only `string message = 2` - the
// OTLP/HTTP error body. Hand-encoded so error responses work in every build.
string RawOtlpStatusBytes(const string &message) {
	string result;
	result.push_back('\x12'); // field 2, length-delimited
	auto len = message.size();
	do {
		uint8_t byte = len & 0x7F;
		len >>= 7;
		result.push_back(static_cast<char>(len ? byte | 0x80 : byte));
	} while (len);
	result += message;
	return result;
}

#ifdef RAWDUCK_WITH_OTLP_PROTOBUF

bool RawOtlpProtobufSupported() {
	return true;
}

bool RawOtlpProtobufToJson(const string &signal, const string &body, string &json_out, string &error) {
	auto to_json = [&](google::protobuf::Message &request) {
		if (!request.ParseFromArray(body.data(), static_cast<int>(body.size()))) {
			error = "could not decode " + signal + " request body as OTLP protobuf";
			return false;
		}
		google::protobuf::util::JsonPrintOptions options;
		// the OTLP/JSON mapping requires enum fields (kind, severityNumber,
		// status.code, ...) as integers, matching what http/json SDKs send
		options.always_print_enums_as_ints = true;
		auto converted = google::protobuf::util::MessageToJsonString(request, &json_out, options);
		if (!converted.ok()) {
			error = "could not convert " + signal + " request: " + converted.ToString();
			return false;
		}
		return true;
	};
	if (signal == "traces") {
		opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest request;
		return to_json(request);
	}
	if (signal == "logs") {
		opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest request;
		return to_json(request);
	}
	opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest request;
	return to_json(request);
}

string RawOtlpProtobufResponse(const string &signal, uint64_t rejected, const string &error_message) {
	if (signal == "traces") {
		opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse response;
		if (rejected) {
			response.mutable_partial_success()->set_rejected_spans(static_cast<int64_t>(rejected));
			response.mutable_partial_success()->set_error_message(error_message);
		}
		return response.SerializeAsString();
	}
	if (signal == "logs") {
		opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse response;
		if (rejected) {
			response.mutable_partial_success()->set_rejected_log_records(static_cast<int64_t>(rejected));
			response.mutable_partial_success()->set_error_message(error_message);
		}
		return response.SerializeAsString();
	}
	opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse response;
	if (rejected) {
		response.mutable_partial_success()->set_rejected_data_points(static_cast<int64_t>(rejected));
		response.mutable_partial_success()->set_error_message(error_message);
	}
	return response.SerializeAsString();
}

#else

bool RawOtlpProtobufSupported() {
	return false;
}

bool RawOtlpProtobufToJson(const string &, const string &, string &, string &error) {
	error = "this build does not include OTLP/protobuf support";
	return false;
}

string RawOtlpProtobufResponse(const string &, uint64_t, const string &) {
	return string();
}

#endif

} // namespace duckdb
