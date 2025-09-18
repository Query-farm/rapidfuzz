#define DUCKDB_EXTENSION_MAIN

#include "rapidfuzz_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "rapidfuzz/fuzz.hpp"
#include "rapidfuzz/distance/JaroWinkler.hpp"
#include "rapidfuzz/distance/Hamming.hpp"
#include "rapidfuzz/distance/Indel.hpp"
#include "rapidfuzz/distance/OSA.hpp"
#include "rapidfuzz/distance/Prefix.hpp"
#include "rapidfuzz/distance/Postfix.hpp"
#include "rapidfuzz/distance/OSA.hpp"
#include "rapidfuzz/distance/LCSseq.hpp"
#include "query_farm_telemetry.hpp"

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

#define RAPIDFUZZ_SCALAR_FUN(name, fn)                                                                                 \
	inline void name(DataChunk &args, ExpressionState &state, Vector &result) {                                        \
		auto &a_vector = args.data[0];                                                                                 \
		auto &b_vector = args.data[1];                                                                                 \
		BinaryExecutor::Execute<string_t, string_t, double>(                                                           \
		    a_vector, b_vector, result, args.size(),                                                                   \
		    [&](string_t a, string_t b) { return fn(a.GetString(), b.GetString()); });                                 \
	}

RAPIDFUZZ_SCALAR_FUN(RapidFuzzJaroWinklerDistanceScalarFun, rapidfuzz::jaro_winkler_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzJaroWinklerSimilarityScalarFun, rapidfuzz::jaro_winkler_similarity)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzJaroWinklerNormalizedDistanceScalarFun, rapidfuzz::jaro_winkler_normalized_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzJaroWinklerNormalizedSimilarityScalarFun, rapidfuzz::jaro_winkler_normalized_similarity)

RAPIDFUZZ_SCALAR_FUN(RapidFuzzJaroDistanceScalarFun, rapidfuzz::jaro_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzJaroSimilarityScalarFun, rapidfuzz::jaro_similarity)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzJaroNormalizedDistanceScalarFun, rapidfuzz::jaro_normalized_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzJaroNormalizedSimilarityScalarFun, rapidfuzz::jaro_normalized_similarity)

RAPIDFUZZ_SCALAR_FUN(RapidFuzzLCSseqDistanceScalarFun, rapidfuzz::lcs_seq_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzLCSseqSimilarityScalarFun, rapidfuzz::lcs_seq_similarity)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzLCSseqNormalizedDistanceScalarFun, rapidfuzz::lcs_seq_normalized_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzLCSseqNormalizedSimilarityScalarFun, rapidfuzz::lcs_seq_normalized_similarity)

RAPIDFUZZ_SCALAR_FUN(RapidFuzzHammingDistanceScalarFun, rapidfuzz::hamming_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzHammingSimilarityScalarFun, rapidfuzz::hamming_similarity)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzHammingNormalizedDistanceScalarFun, rapidfuzz::hamming_normalized_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzHammingNormalizedSimilarityScalarFun, rapidfuzz::hamming_normalized_similarity)

RAPIDFUZZ_SCALAR_FUN(RapidFuzzIndelDistanceScalarFun, rapidfuzz::indel_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzIndelSimilarityScalarFun, rapidfuzz::indel_similarity)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzIndelNormalizedDistanceScalarFun, rapidfuzz::indel_normalized_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzIndelNormalizedSimilarityScalarFun, rapidfuzz::indel_normalized_similarity)

RAPIDFUZZ_SCALAR_FUN(RapidFuzzOSADistanceScalarFun, rapidfuzz::osa_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzOSASimilarityScalarFun, rapidfuzz::osa_similarity)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzOSANormalizedDistanceScalarFun, rapidfuzz::osa_normalized_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzOSANormalizedSimilarityScalarFun, rapidfuzz::osa_normalized_similarity)

RAPIDFUZZ_SCALAR_FUN(RapidFuzzPrefixDistanceScalarFun, rapidfuzz::prefix_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzPrefixSimilarityScalarFun, rapidfuzz::prefix_similarity)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzPrefixNormalizedDistanceScalarFun, rapidfuzz::prefix_normalized_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzPrefixNormalizedSimilarityScalarFun, rapidfuzz::prefix_normalized_similarity)

RAPIDFUZZ_SCALAR_FUN(RapidFuzzPostfixDistanceScalarFun, rapidfuzz::postfix_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzPostfixSimilarityScalarFun, rapidfuzz::postfix_similarity)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzPostfixNormalizedDistanceScalarFun, rapidfuzz::postfix_normalized_distance)
RAPIDFUZZ_SCALAR_FUN(RapidFuzzPostfixNormalizedSimilarityScalarFun, rapidfuzz::postfix_normalized_similarity)

#define REGISTER_FUN(name, fun)                                                                                        \
	loader.RegisterFunction(                                                                                           \
	    ScalarFunction(name, {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, fun));

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto rapidfuzz_ratio_scalar_function = ScalarFunction(
	    "rapidfuzz_ratio", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, RapidFuzzRatioScalarFun);
	loader.RegisterFunction(rapidfuzz_ratio_scalar_function);

	auto rapidfuzz_partial_ratio_scalar_function =
	    ScalarFunction("rapidfuzz_partial_ratio", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                   RapidFuzzPartialRatioScalarFun);
	loader.RegisterFunction(rapidfuzz_partial_ratio_scalar_function);

	auto rapidfuzz_token_sort_ratio_scalar_function =
	    ScalarFunction("rapidfuzz_token_sort_ratio", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                   RapidFuzzTokenSortRatioScalarFun);
	loader.RegisterFunction(rapidfuzz_token_sort_ratio_scalar_function);

	auto rapidfuzz_token_set_ratio_scalar_function =
	    ScalarFunction("rapidfuzz_token_set_ratio", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                   RapidFuzzTokenSetRatioScalarFun);
	loader.RegisterFunction(rapidfuzz_token_set_ratio_scalar_function);

	REGISTER_FUN("rapidfuzz_jaro_winkler_distance", RapidFuzzJaroWinklerDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_jaro_winkler_similarity", RapidFuzzJaroWinklerSimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_jaro_winkler_normalized_distance", RapidFuzzJaroWinklerNormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_jaro_winkler_normalized_similarity", RapidFuzzJaroWinklerNormalizedSimilarityScalarFun)

	REGISTER_FUN("rapidfuzz_jaro_distance", RapidFuzzJaroDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_jaro_similarity", RapidFuzzJaroSimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_jaro_normalized_distance", RapidFuzzJaroNormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_jaro_normalized_similarity", RapidFuzzJaroNormalizedSimilarityScalarFun)

	REGISTER_FUN("rapidfuzz_hamming_distance", RapidFuzzHammingDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_hamming_similarity", RapidFuzzHammingSimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_hamming_normalized_distance", RapidFuzzHammingNormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_hamming_normalized_similarity", RapidFuzzHammingNormalizedSimilarityScalarFun)

	REGISTER_FUN("rapidfuzz_lcs_seq_distance", RapidFuzzLCSseqDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_lcs_seq_similarity", RapidFuzzLCSseqSimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_lcs_seq_normalized_distance", RapidFuzzLCSseqNormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_lcs_seq_normalized_similarity", RapidFuzzLCSseqNormalizedSimilarityScalarFun)

	REGISTER_FUN("rapidfuzz_hamming_distance", RapidFuzzHammingDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_hamming_similarity", RapidFuzzHammingSimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_hamming_normalized_distance", RapidFuzzHammingNormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_hamming_normalized_similarity", RapidFuzzHammingNormalizedSimilarityScalarFun)

	REGISTER_FUN("rapidfuzz_indel_distance", RapidFuzzIndelDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_indel_similarity", RapidFuzzIndelSimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_indel_normalized_distance", RapidFuzzIndelNormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_indel_normalized_similarity", RapidFuzzIndelNormalizedSimilarityScalarFun)

	REGISTER_FUN("rapidfuzz_osa_distance", RapidFuzzOSADistanceScalarFun)
	REGISTER_FUN("rapidfuzz_osa_similarity", RapidFuzzOSASimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_osa_normalized_distance", RapidFuzzOSANormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_osa_normalized_similarity", RapidFuzzOSANormalizedSimilarityScalarFun)

	REGISTER_FUN("rapidfuzz_prefix_distance", RapidFuzzPrefixDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_prefix_similarity", RapidFuzzPrefixSimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_prefix_normalized_distance", RapidFuzzPrefixNormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_prefix_normalized_similarity", RapidFuzzPrefixNormalizedSimilarityScalarFun)

	REGISTER_FUN("rapidfuzz_postfix_distance", RapidFuzzPostfixDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_postfix_similarity", RapidFuzzPostfixSimilarityScalarFun)
	REGISTER_FUN("rapidfuzz_postfix_normalized_distance", RapidFuzzPostfixNormalizedDistanceScalarFun)
	REGISTER_FUN("rapidfuzz_postfix_normalized_similarity", RapidFuzzPostfixNormalizedSimilarityScalarFun)

	QueryFarmSendTelemetry(loader, "rapidfuzz", "2025091701");
}

void RapidfuzzExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RapidfuzzExtension::Name() {
	return "rapidfuzz";
}

std::string RapidfuzzExtension::Version() const {
	return "20205091701";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rapidfuzz, loader) {
	duckdb::LoadInternal(loader);
}
}
