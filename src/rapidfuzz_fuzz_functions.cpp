#include "rapidfuzz_scorer.hpp"

#include "rapidfuzz/fuzz.hpp"

namespace duckdb {

namespace {

//! The rapidfuzz::fuzz scorers: similarities from 0 to 100, higher is better
struct RatioScorerBase {
	using OPTIONS = NoOptions;
	using SCORE = double;
	static constexpr bool LOWER_IS_BETTER = false;
	static constexpr double MAX_SCORE = 100;

	template <class CACHED, class IT2>
	static double ScoreCached(const CACHED &cached, IT2 first2, IT2 last2, double score_cutoff) {
		return cached.similarity(first2, last2, score_cutoff);
	}
};

#define RAPIDFUZZ_RATIO_SCORER(STRUCT, FUNCTION, CACHED)                                                               \
	struct STRUCT : public RatioScorerBase {                                                                           \
		template <class IT1, class IT2>                                                                                \
		static double Score(IT1 first1, IT1 last1, IT2 first2, IT2 last2, const NoOptions &, double score_cutoff) {    \
			return rapidfuzz::fuzz::FUNCTION(first1, last1, first2, last2, score_cutoff);                              \
		}                                                                                                              \
		template <class CHAR_T>                                                                                        \
		static rapidfuzz::fuzz::CACHED<CHAR_T> Cache(const CHAR_T *first, const CHAR_T *last, const NoOptions &) {     \
			return rapidfuzz::fuzz::CACHED<CHAR_T>(first, last);                                                       \
		}                                                                                                              \
	};

RAPIDFUZZ_RATIO_SCORER(RatioScorer, ratio, CachedRatio)
RAPIDFUZZ_RATIO_SCORER(PartialRatioScorer, partial_ratio, CachedPartialRatio)
RAPIDFUZZ_RATIO_SCORER(TokenSortRatioScorer, token_sort_ratio, CachedTokenSortRatio)
RAPIDFUZZ_RATIO_SCORER(PartialTokenSortRatioScorer, partial_token_sort_ratio, CachedPartialTokenSortRatio)
RAPIDFUZZ_RATIO_SCORER(TokenSetRatioScorer, token_set_ratio, CachedTokenSetRatio)
RAPIDFUZZ_RATIO_SCORER(PartialTokenSetRatioScorer, partial_token_set_ratio, CachedPartialTokenSetRatio)
RAPIDFUZZ_RATIO_SCORER(TokenRatioScorer, token_ratio, CachedTokenRatio)
RAPIDFUZZ_RATIO_SCORER(PartialTokenRatioScorer, partial_token_ratio, CachedPartialTokenRatio)
RAPIDFUZZ_RATIO_SCORER(WRatioScorer, WRatio, CachedWRatio)
RAPIDFUZZ_RATIO_SCORER(QRatioScorer, QRatio, CachedQRatio)

#undef RAPIDFUZZ_RATIO_SCORER

template <class SCORER>
void RegisterRatio(ExtensionLoader &loader, const string &name, const string &description, const string &example) {
	FunctionDocumentation documentation;
	documentation.description =
	    description + " Returns a score from 0 to 100. With `score_cutoff := n`, scores below n are returned as 0.";
	documentation.examples = {example};
	documentation.categories = {"text_similarity"};
	RegisterDocumentedFunction(loader, MakeScorerFunction<SCORER>(name), std::move(documentation));
}

} // namespace

void AddFuzzExtractScorers(ExtractScorerMap &scorers) {
	scorers["ratio"] = MakeExtractScorer<RatioScorer>();
	scorers["partial_ratio"] = MakeExtractScorer<PartialRatioScorer>();
	scorers["token_sort_ratio"] = MakeExtractScorer<TokenSortRatioScorer>();
	scorers["partial_token_sort_ratio"] = MakeExtractScorer<PartialTokenSortRatioScorer>();
	scorers["token_set_ratio"] = MakeExtractScorer<TokenSetRatioScorer>();
	scorers["partial_token_set_ratio"] = MakeExtractScorer<PartialTokenSetRatioScorer>();
	scorers["token_ratio"] = MakeExtractScorer<TokenRatioScorer>();
	scorers["partial_token_ratio"] = MakeExtractScorer<PartialTokenRatioScorer>();
	scorers["wratio"] = MakeExtractScorer<WRatioScorer>();
	scorers["qratio"] = MakeExtractScorer<QRatioScorer>();
}

void RegisterFuzzFunctions(ExtensionLoader &loader) {
	RegisterRatio<RatioScorer>(loader, "rapidfuzz_ratio",
	                           "Normalized Indel similarity of the two strings, like Python's `rapidfuzz.fuzz.ratio`.",
	                           "rapidfuzz_ratio('this is a test', 'this is a test!')");
	RegisterRatio<PartialRatioScorer>(
	    loader, "rapidfuzz_partial_ratio",
	    "Best `rapidfuzz_ratio` of the shorter string against any equally long substring of the longer one. Useful "
	    "when one string is a fragment of the other.",
	    "rapidfuzz_partial_ratio('this is a test', 'this is a test!')");
	RegisterRatio<TokenSortRatioScorer>(
	    loader, "rapidfuzz_token_sort_ratio",
	    "`rapidfuzz_ratio` after splitting both strings on whitespace and sorting the words, so word order is ignored.",
	    "rapidfuzz_token_sort_ratio('fuzzy wuzzy was a bear', 'wuzzy fuzzy was a bear')");
	RegisterRatio<PartialTokenSortRatioScorer>(
	    loader, "rapidfuzz_partial_token_sort_ratio",
	    "`rapidfuzz_partial_ratio` after sorting the words of both strings.",
	    "rapidfuzz_partial_token_sort_ratio('fuzzy was a bear', 'wuzzy fuzzy was a bear')");
	RegisterRatio<TokenSetRatioScorer>(
	    loader, "rapidfuzz_token_set_ratio",
	    "Compares the sets of words in both strings, so word order and repeated words are ignored. Scores 100 when "
	    "the words of one string are a subset of the other.",
	    "rapidfuzz_token_set_ratio('fuzzy was a bear', 'fuzzy fuzzy was a bear')");
	RegisterRatio<PartialTokenSetRatioScorer>(
	    loader, "rapidfuzz_partial_token_set_ratio",
	    "`rapidfuzz_partial_ratio` applied to the word sets of both strings. Scores 100 when the strings share any "
	    "word.",
	    "rapidfuzz_partial_token_set_ratio('fuzzy was a bear', 'fuzzy fuzzy was a bear')");
	RegisterRatio<TokenRatioScorer>(
	    loader, "rapidfuzz_token_ratio",
	    "The higher of `rapidfuzz_token_sort_ratio` and `rapidfuzz_token_set_ratio`, computed faster than calling "
	    "both.",
	    "rapidfuzz_token_ratio('fuzzy was a bear', 'wuzzy fuzzy was a bear')");
	RegisterRatio<PartialTokenRatioScorer>(
	    loader, "rapidfuzz_partial_token_ratio",
	    "The higher of `rapidfuzz_partial_token_sort_ratio` and `rapidfuzz_partial_token_set_ratio`, computed faster "
	    "than calling both.",
	    "rapidfuzz_partial_token_ratio('fuzzy was a bear', 'wuzzy fuzzy was a bear')");
	RegisterRatio<WRatioScorer>(
	    loader, "rapidfuzz_wratio",
	    "Weighted combination of the ratio, partial and token scorers chosen by the strings' length ratio, like "
	    "Python's `rapidfuzz.fuzz.WRatio`. A good general-purpose default.",
	    "rapidfuzz_wratio('this is a test', 'this is a new test!!!')");
	RegisterRatio<QRatioScorer>(
	    loader, "rapidfuzz_qratio",
	    "Like `rapidfuzz_ratio`, but returns 0 when either string is empty, like Python's `rapidfuzz.fuzz.QRatio`.",
	    "rapidfuzz_qratio('this is a test', 'this is a new test!!!')");
}

} // namespace duckdb
