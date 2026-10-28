#include "rapidfuzz_scorer.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"

#include <algorithm>

namespace duckdb {

namespace {

constexpr const char *DEFAULT_EXTRACT_SCORER = "wratio";
constexpr int64_t DEFAULT_EXTRACT_TOP_K = 5;

const ExtractScorerMap &GetExtractScorers() {
	static const ExtractScorerMap scorers = [] {
		ExtractScorerMap result;
		AddFuzzExtractScorers(result);
		AddDistanceExtractScorers(result);
		return result;
	}();
	return scorers;
}

string ExtractScorerNames() {
	vector<string> names;
	for (auto &entry : GetExtractScorers()) {
		names.push_back(entry.first);
	}
	std::sort(names.begin(), names.end());
	return StringUtil::Join(names, ", ");
}

struct ExtractBindData final : public FunctionData {
	ExtractBindData(string scorer_name_p, ExtractScorer scorer_p, optional_idx top_k_p, double score_cutoff_p)
	    : scorer_name(std::move(scorer_name_p)), scorer(scorer_p), top_k(top_k_p), score_cutoff(score_cutoff_p) {
	}

	string scorer_name;
	ExtractScorer scorer;
	//! The number of matches to return per row; invalid means all of them
	optional_idx top_k;
	double score_cutoff;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ExtractBindData>(scorer_name, scorer, top_k, score_cutoff);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ExtractBindData>();
		return scorer_name == other.scorer_name && top_k == other.top_k && score_cutoff == other.score_cutoff;
	}
};

template <bool WITH_TOP_K>
unique_ptr<FunctionData> BindExtract(BindScalarFunctionInput &input) {
	auto function_name = input.GetBoundFunction().GetName().GetIdentifierName();

	auto scorer_name = StringValue::Get(input.GetNonNullConstant(Identifier("scorer")));
	auto &scorers = GetExtractScorers();
	auto entry = scorers.find(scorer_name);
	if (entry == scorers.end()) {
		throw InvalidInputException("%s: unknown scorer '%s', expected one of: %s", function_name, scorer_name,
		                            ExtractScorerNames());
	}
	auto &scorer = entry->second;

	optional_idx top_k(1);
	if (WITH_TOP_K) {
		top_k = optional_idx();
		auto top_k_value = input.GetConstant(Identifier("top_k"));
		if (!top_k_value.IsNull()) {
			auto requested = top_k_value.GetValue<int64_t>();
			if (requested < 1) {
				throw InvalidInputException("%s: top_k must be at least 1, got %lld", function_name, requested);
			}
			top_k = optional_idx(idx_t(requested));
		}
	}

	double score_cutoff = 0;
	auto cutoff_value = input.GetConstant(Identifier(SCORE_CUTOFF_PARAMETER));
	if (!cutoff_value.IsNull()) {
		score_cutoff = cutoff_value.GetValue<double>();
		if (!(score_cutoff >= 0 && score_cutoff <= scorer.max_score)) {
			throw InvalidInputException("%s: score_cutoff for scorer '%s' must be between 0 and %g, got %s",
			                            function_name, scorer_name, scorer.max_score, cutoff_value.ToString());
		}
	}
	return make_uniq<ExtractBindData>(std::move(scorer_name), scorer, top_k, score_cutoff);
}

//! Best match first; ties keep the order of the choices list
bool BetterMatch(const ExtractMatch &a, const ExtractMatch &b) {
	if (a.score != b.score) {
		return a.score > b.score;
	}
	return a.index < b.index;
}

//! Ranks the choices of every row and calls write(writer, choices, matches), or writes NULL when the query or the
//! choices list is NULL. Matches are sorted best first and truncated to at most `top_k` entries.
template <class RESULT_TYPE, class WRITE>
void ExecuteExtract(DataChunk &args, ExpressionState &state, Vector &result, WRITE &&write) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<ExtractBindData>();
	auto top_k = bind_data.top_k;
	auto count = args.size();
	auto &choices_vector = args.data[1];
	if (choices_vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		choices_vector.Flatten();
	}
	auto queries = args.data[0].Values<string_t>();
	auto choice_lists = choices_vector.Values<VectorListType<string_t>>();
	auto writer = FlatVector::Writer<RESULT_TYPE>(result, count);

	vector<ExtractChoice> choices;
	vector<ExtractMatch> matches;
	for (idx_t row = 0; row < count; row++) {
		auto query = queries[row];
		auto choice_list = choice_lists[row];
		if (!query.IsValid() || !choice_list.IsValid()) {
			writer.WriteNull();
			continue;
		}
		choices.clear();
		idx_t position = 0;
		for (auto choice : choice_list.GetChildValues()) {
			if (choice.IsValid()) {
				choices.emplace_back(position, choice.GetValue());
			}
			position++;
		}
		matches.clear();
		bind_data.scorer.score_choices(query.GetValue(), choices, bind_data.score_cutoff, matches);

		auto keep = matches.size();
		if (top_k.IsValid() && top_k.GetIndex() < keep) {
			keep = top_k.GetIndex();
			std::partial_sort(matches.begin(), matches.begin() + NumericCast<int64_t>(keep), matches.end(),
			                  BetterMatch);
			matches.resize(keep);
		} else {
			std::sort(matches.begin(), matches.end(), BetterMatch);
		}
		write(writer, choice_list, matches);
	}
}

using ExtractList = VectorListType<VectorStructType<string_t, double, int64_t>>;
using ExtractStruct = VectorStructType<string_t, double, int64_t>;

template <class WRITER, class CHOICE_LIST>
void WriteMatch(WRITER &writer, const CHOICE_LIST &choice_list, const ExtractMatch &match) {
	writer.WriteValue([&](auto &choice, auto &score, auto &choice_index) {
		choice.WriteValue(choice_list.GetChildValue(match.index).GetValue());
		score.WriteValue(match.score);
		// list positions are 1-based in SQL
		choice_index.WriteValue(int64_t(match.index + 1));
	});
}

void ExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteExtract<ExtractList>(
	    args, state, result,
	    [](VectorWriter<ExtractList> &writer, const auto &choice_list, const vector<ExtractMatch> &matches) {
		    idx_t i = 0;
		    for (auto &match_writer : writer.WriteList(matches.size())) {
			    WriteMatch(match_writer, choice_list, matches[i++]);
		    }
	    });
}

void ExtractOneFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteExtract<ExtractStruct>(
	    args, state, result,
	    [](VectorWriter<ExtractStruct> &writer, const auto &choice_list, const vector<ExtractMatch> &matches) {
		    if (matches.empty()) {
			    writer.WriteNull();
			    return;
		    }
		    WriteMatch(writer, choice_list, matches[0]);
	    });
}

LogicalType ExtractMatchType() {
	return LogicalType::STRUCT(
	    {{"choice", LogicalType::VARCHAR}, {"score", LogicalType::DOUBLE}, {"choice_index", LogicalType::BIGINT}});
}

ScalarFunction MakeExtractFunction(const char *name, LogicalType return_type, scalar_function_t function,
                                   bool with_top_k) {
	vector<FunctionParameter> parameters {
	    PositionalParameter("query", LogicalType::VARCHAR),
	    PositionalParameter("choices", LogicalType::LIST(LogicalType::VARCHAR)),
	    KeywordParameter("scorer", LogicalType::VARCHAR, Value(DEFAULT_EXTRACT_SCORER))};
	if (with_top_k) {
		parameters.push_back(KeywordParameter("top_k", LogicalType::BIGINT, Value::BIGINT(DEFAULT_EXTRACT_TOP_K)));
	}
	parameters.push_back(KeywordParameter(SCORE_CUTOFF_PARAMETER, LogicalType::DOUBLE, Value(LogicalType::DOUBLE)));
	ScalarFunction result(Identifier(name), FunctionSignature(std::move(parameters), std::move(return_type)),
	                      std::move(function));
	result.SetBindCallback(with_top_k ? BindExtract<true> : BindExtract<false>);
	result.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return result;
}

} // namespace

void RegisterExtractFunctions(ExtensionLoader &loader) {
	const string scorers_note =
	    " `scorer` names the similarity to rank by (default 'wratio'): any rapidfuzz ratio (e.g. 'ratio', "
	    "'token_set_ratio') or a normalized similarity (e.g. 'levenshtein_normalized_similarity', "
	    "'jaro_winkler_normalized_similarity'). Choices scoring below `score_cutoff` are skipped, as are NULL "
	    "choices; ties keep list order.";

	FunctionDocumentation extract;
	extract.description =
	    "The best matches for query among choices, best first, like Python's "
	    "`rapidfuzz.process.extract`. Returns at most `top_k` matches (default 5; NULL for all), each "
	    "with the choice, its score and its 1-based index in choices." +
	    scorers_note;
	extract.examples = {"rapidfuzz_extract('new york jets', ['New York Jets', 'New York Giants', 'Dallas Cowboys'], "
	                    "scorer := 'token_sort_ratio', top_k := 2)"};
	extract.categories = {"text_similarity"};
	RegisterDocumentedFunction(
	    loader, MakeExtractFunction("rapidfuzz_extract", LogicalType::LIST(ExtractMatchType()), ExtractFunction, true),
	    std::move(extract));

	FunctionDocumentation extract_one;
	extract_one.description = "The best match for query among choices, like Python's "
	                          "`rapidfuzz.process.extractOne`: the choice, its score and its 1-based index in "
	                          "choices, or NULL when no choice qualifies." +
	                          scorers_note;
	extract_one.examples = {"rapidfuzz_extract_one('cowboys', ['New York Jets', 'New York Giants', 'Dallas Cowboys'])"};
	extract_one.categories = {"text_similarity"};
	auto extract_one_function =
	    MakeExtractFunction("rapidfuzz_extract_one", ExtractMatchType(), ExtractOneFunction, false);
	RegisterDocumentedFunction(loader, std::move(extract_one_function), std::move(extract_one));
}

} // namespace duckdb
