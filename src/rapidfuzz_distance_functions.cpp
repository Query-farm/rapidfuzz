#include "rapidfuzz_scorer.hpp"

#include "rapidfuzz/distance/DamerauLevenshtein.hpp"
#include "rapidfuzz/distance/Hamming.hpp"
#include "rapidfuzz/distance/Indel.hpp"
#include "rapidfuzz/distance/Jaro.hpp"
#include "rapidfuzz/distance/JaroWinkler.hpp"
#include "rapidfuzz/distance/LCSseq.hpp"
#include "rapidfuzz/distance/Levenshtein.hpp"
#include "rapidfuzz/distance/OSA.hpp"
#include "rapidfuzz/distance/Postfix.hpp"
#include "rapidfuzz/distance/Prefix.hpp"

namespace duckdb {

namespace {

//! Upper bound of raw scores that are unbounded counts of characters
constexpr double UNBOUNDED_SCORE = std::numeric_limits<double>::infinity();

enum class ScoreVariant : uint8_t { DISTANCE, SIMILARITY, NORMALIZED_DISTANCE, NORMALIZED_SIMILARITY };

//! Adapts a metric family (distance, similarity and their normalized forms) to one scorer per variant
template <class FAMILY, ScoreVariant VARIANT>
struct DistanceScorer {
	static constexpr bool NORMALIZED =
	    VARIANT == ScoreVariant::NORMALIZED_DISTANCE || VARIANT == ScoreVariant::NORMALIZED_SIMILARITY;

	using OPTIONS = typename FAMILY::OPTIONS;
	using SCORE = std::conditional_t<NORMALIZED, double, typename FAMILY::RAW_SCORE>;
	static constexpr bool LOWER_IS_BETTER =
	    VARIANT == ScoreVariant::DISTANCE || VARIANT == ScoreVariant::NORMALIZED_DISTANCE;
	static constexpr double MAX_SCORE = NORMALIZED ? 1.0 : FAMILY::MAX_RAW_SCORE;

	template <class IT1, class IT2>
	static SCORE Score(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options, SCORE score_cutoff) {
		if constexpr (VARIANT == ScoreVariant::DISTANCE) {
			return FAMILY::Distance(first1, last1, first2, last2, options, score_cutoff);
		} else if constexpr (VARIANT == ScoreVariant::SIMILARITY) {
			return FAMILY::Similarity(first1, last1, first2, last2, options, score_cutoff);
		} else if constexpr (VARIANT == ScoreVariant::NORMALIZED_DISTANCE) {
			return FAMILY::NormalizedDistance(first1, last1, first2, last2, options, score_cutoff);
		} else {
			return FAMILY::NormalizedSimilarity(first1, last1, first2, last2, options, score_cutoff);
		}
	}

	template <class CHAR_T>
	static auto Cache(const CHAR_T *first, const CHAR_T *last, const OPTIONS &options) {
		return FAMILY::Cache(first, last, options);
	}

	template <class CACHED, class IT2>
	static SCORE ScoreCached(const CACHED &cached, IT2 first2, IT2 last2, SCORE score_cutoff) {
		if constexpr (VARIANT == ScoreVariant::DISTANCE) {
			return cached.distance(first2, last2, score_cutoff);
		} else if constexpr (VARIANT == ScoreVariant::SIMILARITY) {
			return cached.similarity(first2, last2, score_cutoff);
		} else if constexpr (VARIANT == ScoreVariant::NORMALIZED_DISTANCE) {
			return cached.normalized_distance(first2, last2, score_cutoff);
		} else {
			return cached.normalized_similarity(first2, last2, score_cutoff);
		}
	}
};

//! A metric family whose functions take no options besides score_cutoff
#define RAPIDFUZZ_DISTANCE_FAMILY(STRUCT, RAW, MAX_RAW, PREFIX, CACHED)                                                \
	struct STRUCT {                                                                                                    \
		using OPTIONS = NoOptions;                                                                                     \
		using RAW_SCORE = RAW;                                                                                         \
		static constexpr double MAX_RAW_SCORE = MAX_RAW;                                                               \
		template <class IT1, class IT2>                                                                                \
		static RAW Distance(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const NoOptions &, RAW cutoff) {             \
			return PREFIX##_distance(first1, last1, first2, last2, cutoff);                                            \
		}                                                                                                              \
		template <class IT1, class IT2>                                                                                \
		static RAW Similarity(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const NoOptions &, RAW cutoff) {           \
			return PREFIX##_similarity(first1, last1, first2, last2, cutoff);                                          \
		}                                                                                                              \
		template <class IT1, class IT2>                                                                                \
		static double NormalizedDistance(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const NoOptions &,              \
		                                 double cutoff) {                                                              \
			return PREFIX##_normalized_distance(first1, last1, first2, last2, cutoff);                                 \
		}                                                                                                              \
		template <class IT1, class IT2>                                                                                \
		static double NormalizedSimilarity(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const NoOptions &,            \
		                                   double cutoff) {                                                            \
			return PREFIX##_normalized_similarity(first1, last1, first2, last2, cutoff);                               \
		}                                                                                                              \
		template <class CHAR_T>                                                                                        \
		static CACHED<CHAR_T> Cache(const CHAR_T *first, const CHAR_T *last, const NoOptions &) {                      \
			return CACHED<CHAR_T>(first, last);                                                                        \
		}                                                                                                              \
	};

RAPIDFUZZ_DISTANCE_FAMILY(DamerauLevenshteinFamily, size_t, UNBOUNDED_SCORE,
                          rapidfuzz::experimental::damerau_levenshtein,
                          rapidfuzz::experimental::CachedDamerauLevenshtein)
RAPIDFUZZ_DISTANCE_FAMILY(IndelFamily, size_t, UNBOUNDED_SCORE, rapidfuzz::indel, rapidfuzz::CachedIndel)
RAPIDFUZZ_DISTANCE_FAMILY(LCSseqFamily, size_t, UNBOUNDED_SCORE, rapidfuzz::lcs_seq, rapidfuzz::CachedLCSseq)
RAPIDFUZZ_DISTANCE_FAMILY(OSAFamily, size_t, UNBOUNDED_SCORE, rapidfuzz::osa, rapidfuzz::CachedOSA)
RAPIDFUZZ_DISTANCE_FAMILY(PrefixFamily, size_t, UNBOUNDED_SCORE, rapidfuzz::prefix, rapidfuzz::CachedPrefix)
RAPIDFUZZ_DISTANCE_FAMILY(PostfixFamily, size_t, UNBOUNDED_SCORE, rapidfuzz::postfix, rapidfuzz::CachedPostfix)
RAPIDFUZZ_DISTANCE_FAMILY(JaroFamily, double, 1.0, rapidfuzz::jaro, rapidfuzz::CachedJaro)

#undef RAPIDFUZZ_DISTANCE_FAMILY

//===--------------------------------------------------------------------===//
// Levenshtein: insert_cost, delete_cost and replace_cost
//===--------------------------------------------------------------------===//
struct LevenshteinOptions {
	static constexpr bool CAN_THROW = false;
	rapidfuzz::LevenshteinWeightTable weights {1, 1, 1};

	static void AddParameters(vector<FunctionParameter> &parameters) {
		parameters.push_back(KeywordParameter("insert_cost", LogicalType::BIGINT, Value::BIGINT(1)));
		parameters.push_back(KeywordParameter("delete_cost", LogicalType::BIGINT, Value::BIGINT(1)));
		parameters.push_back(KeywordParameter("replace_cost", LogicalType::BIGINT, Value::BIGINT(1)));
	}
	static size_t BindCost(BindScalarFunctionInput &input, const char *name) {
		auto cost = input.GetNonNullConstant(Identifier(name)).GetValue<int64_t>();
		if (cost < 0) {
			throw InvalidInputException("%s: %s must not be negative, got %lld",
			                            input.GetBoundFunction().GetName().GetIdentifierName(), name, cost);
		}
		return size_t(cost);
	}
	static LevenshteinOptions Bind(BindScalarFunctionInput &input) {
		LevenshteinOptions result;
		result.weights.insert_cost = BindCost(input, "insert_cost");
		result.weights.delete_cost = BindCost(input, "delete_cost");
		result.weights.replace_cost = BindCost(input, "replace_cost");
		return result;
	}
	//! An insertion into s1 is a deletion from s2
	LevenshteinOptions Swapped() const {
		LevenshteinOptions result = *this;
		std::swap(result.weights.insert_cost, result.weights.delete_cost);
		return result;
	}
	void Validate(const string &function_name, idx_t length1, idx_t length2) const {
	}
	bool operator==(const LevenshteinOptions &other) const {
		return weights.insert_cost == other.weights.insert_cost && weights.delete_cost == other.weights.delete_cost &&
		       weights.replace_cost == other.weights.replace_cost;
	}
};

struct LevenshteinFamily {
	using OPTIONS = LevenshteinOptions;
	using RAW_SCORE = size_t;
	static constexpr double MAX_RAW_SCORE = UNBOUNDED_SCORE;

	template <class IT1, class IT2>
	static size_t Distance(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options, size_t cutoff) {
		return rapidfuzz::levenshtein_distance(first1, last1, first2, last2, options.weights, cutoff);
	}
	template <class IT1, class IT2>
	static size_t Similarity(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options, size_t cutoff) {
		return rapidfuzz::levenshtein_similarity(first1, last1, first2, last2, options.weights, cutoff);
	}
	template <class IT1, class IT2>
	static double NormalizedDistance(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options,
	                                 double cutoff) {
		return rapidfuzz::levenshtein_normalized_distance(first1, last1, first2, last2, options.weights, cutoff);
	}
	template <class IT1, class IT2>
	static double NormalizedSimilarity(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options,
	                                   double cutoff) {
		return rapidfuzz::levenshtein_normalized_similarity(first1, last1, first2, last2, options.weights, cutoff);
	}
	template <class CHAR_T>
	static rapidfuzz::CachedLevenshtein<CHAR_T> Cache(const CHAR_T *first, const CHAR_T *last, const OPTIONS &options) {
		return rapidfuzz::CachedLevenshtein<CHAR_T>(first, last, options.weights);
	}
};

//===--------------------------------------------------------------------===//
// Hamming: pad
//===--------------------------------------------------------------------===//
struct HammingOptions {
	static constexpr bool CAN_THROW = true;
	bool pad = true;

	static void AddParameters(vector<FunctionParameter> &parameters) {
		parameters.push_back(KeywordParameter("pad", LogicalType::BOOLEAN, Value::BOOLEAN(true)));
	}
	static HammingOptions Bind(BindScalarFunctionInput &input) {
		HammingOptions result;
		result.pad = BooleanValue::Get(input.GetNonNullConstant(Identifier("pad")));
		return result;
	}
	HammingOptions Swapped() const {
		return *this;
	}
	void Validate(const string &function_name, idx_t length1, idx_t length2) const {
		if (!pad && length1 != length2) {
			throw InvalidInputException("%s: with pad := false both strings must have the same length, got %llu "
			                            "and %llu characters",
			                            function_name, length1, length2);
		}
	}
	bool operator==(const HammingOptions &other) const {
		return pad == other.pad;
	}
};

struct HammingFamily {
	using OPTIONS = HammingOptions;
	using RAW_SCORE = size_t;
	static constexpr double MAX_RAW_SCORE = UNBOUNDED_SCORE;

	template <class IT1, class IT2>
	static size_t Distance(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options, size_t cutoff) {
		return rapidfuzz::hamming_distance(first1, last1, first2, last2, options.pad, cutoff);
	}
	template <class IT1, class IT2>
	static size_t Similarity(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options, size_t cutoff) {
		return rapidfuzz::hamming_similarity(first1, last1, first2, last2, options.pad, cutoff);
	}
	template <class IT1, class IT2>
	static double NormalizedDistance(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options,
	                                 double cutoff) {
		return rapidfuzz::hamming_normalized_distance(first1, last1, first2, last2, options.pad, cutoff);
	}
	template <class IT1, class IT2>
	static double NormalizedSimilarity(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options,
	                                   double cutoff) {
		return rapidfuzz::hamming_normalized_similarity(first1, last1, first2, last2, options.pad, cutoff);
	}
	template <class CHAR_T>
	static rapidfuzz::CachedHamming<CHAR_T> Cache(const CHAR_T *first, const CHAR_T *last, const OPTIONS &options) {
		return rapidfuzz::CachedHamming<CHAR_T>(first, last, options.pad);
	}
};

//===--------------------------------------------------------------------===//
// Jaro-Winkler: prefix_weight
//===--------------------------------------------------------------------===//
struct JaroWinklerOptions {
	static constexpr bool CAN_THROW = false;
	static constexpr double DEFAULT_PREFIX_WEIGHT = 0.1;
	static constexpr double MAX_PREFIX_WEIGHT = 0.25;
	double prefix_weight = DEFAULT_PREFIX_WEIGHT;

	static void AddParameters(vector<FunctionParameter> &parameters) {
		parameters.push_back(
		    KeywordParameter("prefix_weight", LogicalType::DOUBLE, Value::DOUBLE(DEFAULT_PREFIX_WEIGHT)));
	}
	static JaroWinklerOptions Bind(BindScalarFunctionInput &input) {
		JaroWinklerOptions result;
		result.prefix_weight = input.GetNonNullConstant(Identifier("prefix_weight")).GetValue<double>();
		// larger weights can push the similarity above 1
		if (!(result.prefix_weight >= 0 && result.prefix_weight <= MAX_PREFIX_WEIGHT)) {
			throw InvalidInputException("%s: prefix_weight must be between 0 and %g, got %g",
			                            input.GetBoundFunction().GetName().GetIdentifierName(), MAX_PREFIX_WEIGHT,
			                            result.prefix_weight);
		}
		return result;
	}
	JaroWinklerOptions Swapped() const {
		return *this;
	}
	void Validate(const string &function_name, idx_t length1, idx_t length2) const {
	}
	bool operator==(const JaroWinklerOptions &other) const {
		return prefix_weight == other.prefix_weight;
	}
};

struct JaroWinklerFamily {
	using OPTIONS = JaroWinklerOptions;
	using RAW_SCORE = double;
	static constexpr double MAX_RAW_SCORE = 1.0;

	template <class IT1, class IT2>
	static double Distance(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options, double cutoff) {
		return rapidfuzz::jaro_winkler_distance(first1, last1, first2, last2, options.prefix_weight, cutoff);
	}
	template <class IT1, class IT2>
	static double Similarity(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options, double cutoff) {
		return rapidfuzz::jaro_winkler_similarity(first1, last1, first2, last2, options.prefix_weight, cutoff);
	}
	template <class IT1, class IT2>
	static double NormalizedDistance(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options,
	                                 double cutoff) {
		return rapidfuzz::jaro_winkler_normalized_distance(first1, last1, first2, last2, options.prefix_weight, cutoff);
	}
	template <class IT1, class IT2>
	static double NormalizedSimilarity(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const OPTIONS &options,
	                                   double cutoff) {
		return rapidfuzz::jaro_winkler_normalized_similarity(first1, last1, first2, last2, options.prefix_weight,
		                                                     cutoff);
	}
	template <class CHAR_T>
	static rapidfuzz::CachedJaroWinkler<CHAR_T> Cache(const CHAR_T *first, const CHAR_T *last, const OPTIONS &options) {
		return rapidfuzz::CachedJaroWinkler<CHAR_T>(first, last, options.prefix_weight);
	}
};

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//
struct FamilyDocumentation {
	//! Function name prefix, e.g. "rapidfuzz_levenshtein"
	const char *prefix;
	//! What the metric measures
	const char *summary;
	//! Extra notes on the keyword options, if any
	const char *options;
	const char *example_s1;
	const char *example_s2;
};

template <class SCORER>
void RegisterVariant(ExtensionLoader &loader, const FamilyDocumentation &family, const char *suffix,
                     const char *variant_description, const char *cutoff_description) {
	auto name = StringUtil::Format("%s_%s", family.prefix, suffix);
	FunctionDocumentation documentation;
	documentation.description = StringUtil::Format("%s %s%s With `score_cutoff := n`, %s", family.summary,
	                                               variant_description, family.options, cutoff_description);
	documentation.examples = {StringUtil::Format("%s('%s', '%s')", name, family.example_s1, family.example_s2)};
	documentation.categories = {"text_similarity"};
	RegisterDocumentedFunction(loader, MakeScorerFunction<SCORER>(name), std::move(documentation));
}

template <class FAMILY>
void RegisterFamily(ExtensionLoader &loader, const FamilyDocumentation &family) {
	const bool integral = std::is_integral<typename FAMILY::RAW_SCORE>::value;
	RegisterVariant<DistanceScorer<FAMILY, ScoreVariant::DISTANCE>>(
	    loader, family, "distance",
	    integral ? "Returns the distance; 0 means identical." : "Returns the distance from 0 (identical) to 1.",
	    integral ? "distances above n are returned as n + 1." : "distances above n are returned as 1.");
	RegisterVariant<DistanceScorer<FAMILY, ScoreVariant::SIMILARITY>>(
	    loader, family, "similarity",
	    integral ? "Returns the similarity; higher is more similar."
	             : "Returns the similarity from 0 to 1 (identical).",
	    "similarities below n are returned as 0.");
	RegisterVariant<DistanceScorer<FAMILY, ScoreVariant::NORMALIZED_DISTANCE>>(
	    loader, family, "normalized_distance", "Returns the distance scaled from 0 (identical) to 1.",
	    "distances above n are returned as 1.");
	RegisterVariant<DistanceScorer<FAMILY, ScoreVariant::NORMALIZED_SIMILARITY>>(
	    loader, family, "normalized_similarity", "Returns the similarity scaled from 0 to 1 (identical).",
	    "similarities below n are returned as 0.");
}

template <class FAMILY>
void AddNormalizedSimilarity(ExtractScorerMap &scorers, const string &family) {
	scorers[family + "_normalized_similarity"] =
	    MakeExtractScorer<DistanceScorer<FAMILY, ScoreVariant::NORMALIZED_SIMILARITY>>();
}

} // namespace

void AddDistanceExtractScorers(ExtractScorerMap &scorers) {
	AddNormalizedSimilarity<LevenshteinFamily>(scorers, "levenshtein");
	AddNormalizedSimilarity<DamerauLevenshteinFamily>(scorers, "damerau_levenshtein");
	AddNormalizedSimilarity<OSAFamily>(scorers, "osa");
	AddNormalizedSimilarity<IndelFamily>(scorers, "indel");
	AddNormalizedSimilarity<LCSseqFamily>(scorers, "lcs_seq");
	AddNormalizedSimilarity<HammingFamily>(scorers, "hamming");
	AddNormalizedSimilarity<JaroFamily>(scorers, "jaro");
	AddNormalizedSimilarity<JaroWinklerFamily>(scorers, "jaro_winkler");
	AddNormalizedSimilarity<PrefixFamily>(scorers, "prefix");
	AddNormalizedSimilarity<PostfixFamily>(scorers, "postfix");
}

void RegisterDistanceFunctions(ExtensionLoader &loader) {
	RegisterFamily<LevenshteinFamily>(
	    loader, {"rapidfuzz_levenshtein",
	             "Levenshtein distance: the minimum number of single-character insertions, deletions and "
	             "substitutions needed to turn s1 into s2.",
	             " Each kind of edit can be weighted with `insert_cost`, `delete_cost` and `replace_cost` (default 1).",
	             "kitten", "sitting"});
	RegisterFamily<DamerauLevenshteinFamily>(
	    loader, {"rapidfuzz_damerau_levenshtein",
	             "Damerau-Levenshtein distance: like Levenshtein, but swapping two characters counts as one edit, "
	             "even when other edits occur between them.",
	             "", "ca", "abc"});
	RegisterFamily<OSAFamily>(loader, {"rapidfuzz_osa",
	                                   "Optimal String Alignment (restricted Damerau-Levenshtein) distance: like "
	                                   "Levenshtein, but swapping two adjacent characters counts as one edit.",
	                                   "", "CA", "AC"});
	RegisterFamily<IndelFamily>(loader, {"rapidfuzz_indel",
	                                     "Indel distance: the minimum number of insertions and deletions (no "
	                                     "substitutions) needed to turn s1 into s2.",
	                                     "", "lewenstein", "levenshtein"});
	RegisterFamily<LCSseqFamily>(loader, {"rapidfuzz_lcs_seq",
	                                      "Longest common subsequence: characters both strings share in the same "
	                                      "order, not necessarily adjacent.",
	                                      "", "this is a test", "this is a new test"});
	RegisterFamily<HammingFamily>(
	    loader, {"rapidfuzz_hamming", "Hamming distance: the number of positions at which the characters differ.",
	             " Strings of different lengths are compared as if the shorter were padded; with `pad := false` they "
	             "raise an error instead.",
	             "karolin", "kathrin"});
	RegisterFamily<JaroFamily>(loader, {"rapidfuzz_jaro",
	                                    "Jaro similarity: based on matching characters and transpositions. Works well "
	                                    "for short strings such as names.",
	                                    "", "martha", "marhta"});
	RegisterFamily<JaroWinklerFamily>(
	    loader, {"rapidfuzz_jaro_winkler",
	             "Jaro-Winkler similarity: Jaro with extra weight for a shared prefix of up to four characters. A "
	             "strong default for names.",
	             " The prefix bonus is set with `prefix_weight` (default 0.1, at most 0.25).", "martha", "marhta"});
	RegisterFamily<PrefixFamily>(loader, {"rapidfuzz_prefix",
	                                      "Prefix similarity: the length of the common prefix of both strings.", "",
	                                      "prefix_match", "prefix_test"});
	RegisterFamily<PostfixFamily>(loader, {"rapidfuzz_postfix",
	                                       "Postfix similarity: the length of the common suffix of both strings.", "",
	                                       "file_name.csv", "other_name.csv"});
}

} // namespace duckdb
