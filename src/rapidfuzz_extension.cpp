#define DUCKDB_EXTENSION_MAIN

#include "rapidfuzz_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "rapidfuzz/fuzz.hpp"
namespace duckdb {

using rapidfuzz::fuzz::ratio;

inline void RapidFuzzRatioScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &a_vector = args.data[0];
	auto &b_vector = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, double>(
	    a_vector, b_vector, result, args.size(),
	    [&](string_t a, string_t b) { return rapidfuzz::fuzz::ratio(a.GetString(), b.GetString()); });
}

inline void RapidFuzzPartialRatioScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &a_vector = args.data[0];
	auto &b_vector = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, double>(
	    a_vector, b_vector, result, args.size(),
	    [&](string_t a, string_t b) { return rapidfuzz::fuzz::partial_ratio(a.GetString(), b.GetString()); });
}

inline void RapidFuzzTokenSortRatioScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &a_vector = args.data[0];
	auto &b_vector = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, double>(
	    a_vector, b_vector, result, args.size(),
	    [&](string_t a, string_t b) { return rapidfuzz::fuzz::token_sort_ratio(a.GetString(), b.GetString()); });
}

inline void RapidFuzzTokenSetRatioScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &a_vector = args.data[0];
	auto &b_vector = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, double>(
	    a_vector, b_vector, result, args.size(),
	    [&](string_t a, string_t b) { return rapidfuzz::fuzz::token_set_ratio(a.GetString(), b.GetString()); });
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto rapidfuzz_ratio_scalar_function = ScalarFunction(
	    "rapidfuzz_ratio", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, RapidFuzzRatioScalarFun);
	ExtensionUtil::RegisterFunction(instance, rapidfuzz_ratio_scalar_function);

	auto rapidfuzz_partial_ratio_scalar_function =
	    ScalarFunction("rapidfuzz_partial_ratio", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                   RapidFuzzPartialRatioScalarFun);
	ExtensionUtil::RegisterFunction(instance, rapidfuzz_partial_ratio_scalar_function);

	auto rapidfuzz_token_sort_ratio_scalar_function =
	    ScalarFunction("rapidfuzz_token_sort_ratio", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                   RapidFuzzTokenSortRatioScalarFun);
	ExtensionUtil::RegisterFunction(instance, rapidfuzz_token_sort_ratio_scalar_function);

	auto rapidfuzz_token_set_ratio_scalar_function =
	    ScalarFunction("rapidfuzz_token_set_ratio", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                   RapidFuzzTokenSetRatioScalarFun);
	ExtensionUtil::RegisterFunction(instance, rapidfuzz_token_set_ratio_scalar_function);
}

void RapidfuzzExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string RapidfuzzExtension::Name() {
	return "rapidfuz";
}

std::string RapidfuzzExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void rapidfuzz_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::RapidfuzzExtension>();
}

DUCKDB_EXTENSION_API const char *rapidfuzz_version() {
	return "0.1.0";
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
