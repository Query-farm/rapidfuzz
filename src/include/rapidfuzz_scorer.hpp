#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "rapidfuzz_characters.hpp"

#include <cmath>
#include <limits>
#include <type_traits>

namespace duckdb {

//! Name of the keyword-only parameter every scorer accepts to stop early on poor matches
static constexpr const char *SCORE_CUTOFF_PARAMETER = "score_cutoff";

//! A named parameter that can only be passed by position
inline FunctionParameter PositionalParameter(const char *name, LogicalType type) {
	return FunctionParameter(name, std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

//! A named parameter that can only be passed as `name := value`
inline FunctionParameter KeywordParameter(const char *name, LogicalType type, Value default_value) {
	return FunctionParameter(name, std::move(type), std::move(default_value), FunctionParameterKind::KEYWORD_ONLY);
}

//! The SQL representation of a RapidFuzz score: integral scores become BIGINT, the rest DOUBLE
template <class SCORE>
struct SqlScore;

template <>
struct SqlScore<size_t> {
	using type = int64_t;
	static LogicalType Type() {
		return LogicalType::BIGINT;
	}
};

template <>
struct SqlScore<double> {
	using type = double;
	static LogicalType Type() {
		return LogicalType::DOUBLE;
	}
};

//! Options for scorers that take none
struct NoOptions {
	//! Whether Validate() can reject a pair of strings at runtime
	static constexpr bool CAN_THROW = false;

	static void AddParameters(vector<FunctionParameter> &parameters) {
	}
	static NoOptions Bind(BindScalarFunctionInput &input) {
		return NoOptions();
	}
	//! The options to use when the two strings are passed in the opposite order
	NoOptions Swapped() const {
		return *this;
	}
	//! Throws if the scorer cannot compare strings of these lengths (in characters)
	void Validate(const string &function_name, idx_t length1, idx_t length2) const {
	}
	bool operator==(const NoOptions &other) const {
		return true;
	}
};

//! Bound keyword arguments of a scorer
template <class SCORER>
struct ScorerBindData final : public FunctionData {
	using OPTIONS = typename SCORER::OPTIONS;
	using SCORE = typename SCORER::SCORE;

	ScorerBindData(string function_name_p, OPTIONS options_p, SCORE score_cutoff_p)
	    : function_name(std::move(function_name_p)), options(std::move(options_p)), score_cutoff(score_cutoff_p) {
	}

	string function_name;
	OPTIONS options;
	SCORE score_cutoff;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ScorerBindData>(function_name, options, score_cutoff);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ScorerBindData>();
		return options == other.options && score_cutoff == other.score_cutoff;
	}
};

//! The score_cutoff to use when the caller does not pass one: it never stops scoring early
template <class SCORER>
constexpr typename SCORER::SCORE DefaultScoreCutoff() {
	using SCORE = typename SCORER::SCORE;
	if constexpr (!SCORER::LOWER_IS_BETTER) {
		return SCORE(0);
	} else if constexpr (std::is_integral<SCORE>::value) {
		return std::numeric_limits<SCORE>::max();
	} else {
		return SCORE(SCORER::MAX_SCORE);
	}
}

template <class SCORER>
typename SCORER::SCORE BindScoreCutoff(const string &function_name, BindScalarFunctionInput &input) {
	using SCORE = typename SCORER::SCORE;
	auto value = input.GetConstant(Identifier(SCORE_CUTOFF_PARAMETER));
	if (value.IsNull()) {
		return DefaultScoreCutoff<SCORER>();
	}
	if constexpr (std::is_integral<SCORE>::value) {
		auto cutoff = value.GetValue<int64_t>();
		if (cutoff < 0) {
			throw InvalidInputException("%s: score_cutoff must not be negative, got %lld", function_name, cutoff);
		}
		return SCORE(cutoff);
	} else {
		auto cutoff = value.GetValue<double>();
		if (!(cutoff >= 0 && cutoff <= SCORER::MAX_SCORE)) {
			throw InvalidInputException("%s: score_cutoff must be between 0 and %g, got %s", function_name,
			                            SCORER::MAX_SCORE, value.ToString());
		}
		return SCORE(cutoff);
	}
}

template <class SCORER>
unique_ptr<FunctionData> BindScorer(BindScalarFunctionInput &input) {
	auto function_name = input.GetBoundFunction().GetName().GetIdentifierName();
	auto options = SCORER::OPTIONS::Bind(input);
	auto score_cutoff = BindScoreCutoff<SCORER>(function_name, input);
	return make_uniq<ScorerBindData<SCORER>>(std::move(function_name), std::move(options), score_cutoff);
}

inline bool IsNonNullConstant(const Vector &vector) {
	return vector.GetVectorType() == VectorType::CONSTANT_VECTOR && !ConstantVector::IsNull(vector);
}

//! Scores every row of s1 against s2. When exactly one side is constant (the common `f(column, 'literal')` shape)
//! that string is preprocessed once with RapidFuzz's cached scorer and reused for every row of the other side.
template <class SCORER>
void ExecuteScorer(DataChunk &args, ExpressionState &state, Vector &result) {
	using RESULT = typename SqlScore<typename SCORER::SCORE>::type;
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<ScorerBindData<SCORER>>();
	auto &function_name = bind_data.function_name;
	auto score_cutoff = bind_data.score_cutoff;
	auto &s1 = args.data[0];
	auto &s2 = args.data[1];
	auto count = args.size();

	CharacterBuffer buffer1;
	CharacterBuffer buffer2;
	const bool s1_constant = IsNonNullConstant(s1);
	const bool s2_constant = IsNonNullConstant(s2);
	if (s1_constant != s2_constant) {
		auto &pattern_vector = s1_constant ? s1 : s2;
		auto &rows = s1_constant ? s2 : s1;
		// every scorer is symmetric once its options are mirrored, so the pattern can take either position
		auto options = s1_constant ? bind_data.options : bind_data.options.Swapped();
		auto pattern = ConstantVector::GetData<string_t>(pattern_vector)[0];
		// scorers that cannot throw let the executor score each distinct value of a dictionary vector only once
		constexpr auto errors =
		    SCORER::OPTIONS::CAN_THROW ? FunctionErrors::CAN_THROW_RUNTIME_ERROR : FunctionErrors::CANNOT_ERROR;
		buffer1.Visit(pattern, [&](auto first1, auto last1) {
			using CHAR_T = std::remove_const_t<std::remove_pointer_t<decltype(first1)>>;
			const auto pattern_length = idx_t(last1 - first1);
			auto cached = SCORER::template Cache<CHAR_T>(first1, last1, options);
			UnaryExecutor::Execute<string_t, RESULT>(
			    rows, result, count,
			    [&](string_t row) {
				    return buffer2.Visit(row, [&](auto first2, auto last2) {
					    options.Validate(function_name, pattern_length, idx_t(last2 - first2));
					    return RESULT(SCORER::ScoreCached(cached, first2, last2, score_cutoff));
				    });
			    },
			    errors);
		});
		return;
	}

	auto &options = bind_data.options;
	BinaryExecutor::Execute<string_t, string_t, RESULT>(s1, s2, result, count, [&](string_t a, string_t b) {
		return VisitCharacterPair(buffer1, buffer2, a, b, [&](auto first1, auto last1, auto first2, auto last2) {
			options.Validate(function_name, idx_t(last1 - first1), idx_t(last2 - first2));
			return RESULT(SCORER::Score(first1, last1, first2, last2, options, score_cutoff));
		});
	});
}

//! Documentation surfaced through duckdb_functions()
struct FunctionDocumentation {
	string description;
	vector<string> examples;
	vector<string> categories;
};

//! Builds the scalar function `name(s1, s2, [options...,] score_cutoff := NULL)` for a scorer
template <class SCORER>
ScalarFunction MakeScorerFunction(const string &name) {
	auto score_type = SqlScore<typename SCORER::SCORE>::Type();
	vector<FunctionParameter> parameters {PositionalParameter("s1", LogicalType::VARCHAR),
	                                      PositionalParameter("s2", LogicalType::VARCHAR)};
	SCORER::OPTIONS::AddParameters(parameters);
	parameters.push_back(KeywordParameter(SCORE_CUTOFF_PARAMETER, score_type, Value(score_type)));
	ScalarFunction function(Identifier(name), FunctionSignature(std::move(parameters), score_type),
	                        ExecuteScorer<SCORER>);
	function.SetBindCallback(BindScorer<SCORER>);
	// a NULL score_cutoff means "no cutoff"; NULL strings still produce NULL through the executors
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	if (SCORER::OPTIONS::CAN_THROW) {
		function.SetFallible();
	}
	return function;
}

//! A choice that rapidfuzz_extract matched: its position in the choices list and its score
struct ExtractMatch {
	idx_t index;
	double score;
};

//! A non-NULL entry of the choices list and its position in that list
using ExtractChoice = std::pair<idx_t, string_t>;

//! Appends to matches every choice that scores at least score_cutoff against query
typedef void (*score_choices_t)(const string_t &query, const vector<ExtractChoice> &choices, double score_cutoff,
                                vector<ExtractMatch> &matches);

//! Preprocesses the query once with RapidFuzz's cached scorer and scores every choice against it
template <class SCORER>
void ScoreChoices(const string_t &query, const vector<ExtractChoice> &choices, double score_cutoff,
                  vector<ExtractMatch> &matches) {
	static_assert(std::is_same<typename SCORER::SCORE, double>::value && !SCORER::LOWER_IS_BETTER,
	              "rapidfuzz_extract ranks by similarity scores only");
	CharacterBuffer query_buffer;
	CharacterBuffer choice_buffer;
	query_buffer.Visit(query, [&](auto first1, auto last1) {
		using CHAR_T = std::remove_const_t<std::remove_pointer_t<decltype(first1)>>;
		auto cached = SCORER::template Cache<CHAR_T>(first1, last1, typename SCORER::OPTIONS());
		for (auto &choice : choices) {
			auto score = choice_buffer.Visit(choice.second, [&](auto first2, auto last2) {
				return SCORER::ScoreCached(cached, first2, last2, score_cutoff);
			});
			if (score >= score_cutoff) {
				matches.push_back(ExtractMatch {choice.first, score});
			}
		}
	});
}

//! A scorer that rapidfuzz_extract can rank choices with
struct ExtractScorer {
	score_choices_t score_choices;
	double max_score;
};

using ExtractScorerMap = case_insensitive_map_t<ExtractScorer>;

template <class SCORER>
ExtractScorer MakeExtractScorer() {
	return ExtractScorer {ScoreChoices<SCORER>, SCORER::MAX_SCORE};
}

void AddFuzzExtractScorers(ExtractScorerMap &scorers);
void AddDistanceExtractScorers(ExtractScorerMap &scorers);

void RegisterDocumentedFunction(ExtensionLoader &loader, ScalarFunction function, FunctionDocumentation documentation);

void RegisterFuzzFunctions(ExtensionLoader &loader);
void RegisterDistanceFunctions(ExtensionLoader &loader);
void RegisterAlignmentFunctions(ExtensionLoader &loader);
void RegisterExtractFunctions(ExtensionLoader &loader);

} // namespace duckdb
