#include "rapidfuzz_scorer.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "rapidfuzz/distance/Indel.hpp"
#include "rapidfuzz/distance/Levenshtein.hpp"
#include "rapidfuzz/fuzz.hpp"

namespace duckdb {

namespace {

string_t EditTypeName(rapidfuzz::EditType type) {
	switch (type) {
	case rapidfuzz::EditType::None:
		return string_t("equal");
	case rapidfuzz::EditType::Replace:
		return string_t("replace");
	case rapidfuzz::EditType::Insert:
		return string_t("insert");
	case rapidfuzz::EditType::Delete:
		return string_t("delete");
	}
	throw InternalException("Unknown rapidfuzz edit type %d", static_cast<int>(type));
}

LogicalType EditopsType() {
	return LogicalType::LIST(LogicalType::STRUCT(
	    {{"operation", LogicalType::VARCHAR}, {"s1_pos", LogicalType::BIGINT}, {"s2_pos", LogicalType::BIGINT}}));
}

LogicalType OpcodesType() {
	return LogicalType::LIST(LogicalType::STRUCT({{"operation", LogicalType::VARCHAR},
	                                              {"s1_start", LogicalType::BIGINT},
	                                              {"s1_end", LogicalType::BIGINT},
	                                              {"s2_start", LogicalType::BIGINT},
	                                              {"s2_end", LogicalType::BIGINT}}));
}

LogicalType AlignmentType() {
	return LogicalType::STRUCT({{"score", LogicalType::DOUBLE},
	                            {"s1_start", LogicalType::BIGINT},
	                            {"s1_end", LogicalType::BIGINT},
	                            {"s2_start", LogicalType::BIGINT},
	                            {"s2_end", LogicalType::BIGINT}});
}

//! Calls write(writer, s1, s2) for every row where both strings are non-NULL, and writes NULL otherwise
template <class RESULT_TYPE, class WRITE>
void ExecuteRowWise(DataChunk &args, Vector &result, WRITE &&write) {
	auto count = args.size();
	auto s1_values = args.data[0].Values<string_t>();
	auto s2_values = args.data[1].Values<string_t>();
	auto writer = FlatVector::Writer<RESULT_TYPE>(result, count);
	for (idx_t row = 0; row < count; row++) {
		auto s1 = s1_values[row];
		auto s2 = s2_values[row];
		if (!s1.IsValid() || !s2.IsValid()) {
			writer.WriteNull();
			continue;
		}
		write(writer, s1.GetValue(), s2.GetValue());
	}
}

using EditopsList = VectorListType<VectorStructType<string_t, int64_t, int64_t>>;
using OpcodesList = VectorListType<VectorStructType<string_t, int64_t, int64_t, int64_t, int64_t>>;
using AlignmentStruct = VectorStructType<double, int64_t, int64_t, int64_t, int64_t>;

template <class COMPUTE_EDITOPS>
void ExecuteEditops(DataChunk &args, Vector &result, COMPUTE_EDITOPS &&compute) {
	CharacterBuffer buffer1;
	CharacterBuffer buffer2;
	ExecuteRowWise<EditopsList>(args, result, [&](VectorWriter<EditopsList> &writer, string_t s1, string_t s2) {
		auto editops = VisitCharacterPair(buffer1, buffer2, s1, s2, compute);
		idx_t i = 0;
		for (auto &op_writer : writer.WriteList(editops.size())) {
			const auto &op = editops[i++];
			op_writer.WriteValue([&](auto &operation, auto &s1_pos, auto &s2_pos) {
				operation.WriteValue(EditTypeName(op.type));
				s1_pos.WriteValue(int64_t(op.src_pos));
				s2_pos.WriteValue(int64_t(op.dest_pos));
			});
		}
	});
}

template <class COMPUTE_EDITOPS>
void ExecuteOpcodes(DataChunk &args, Vector &result, COMPUTE_EDITOPS &&compute) {
	CharacterBuffer buffer1;
	CharacterBuffer buffer2;
	ExecuteRowWise<OpcodesList>(args, result, [&](VectorWriter<OpcodesList> &writer, string_t s1, string_t s2) {
		auto opcodes = rapidfuzz::Opcodes(VisitCharacterPair(buffer1, buffer2, s1, s2, compute));
		idx_t i = 0;
		for (auto &op_writer : writer.WriteList(opcodes.size())) {
			const auto &op = opcodes[i++];
			op_writer.WriteValue([&](auto &operation, auto &s1_start, auto &s1_end, auto &s2_start, auto &s2_end) {
				operation.WriteValue(EditTypeName(op.type));
				s1_start.WriteValue(int64_t(op.src_begin));
				s1_end.WriteValue(int64_t(op.src_end));
				s2_start.WriteValue(int64_t(op.dest_begin));
				s2_end.WriteValue(int64_t(op.dest_end));
			});
		}
	});
}

struct ComputeLevenshteinEditops {
	template <class IT1, class IT2>
	rapidfuzz::Editops operator()(IT1 first1, IT1 last1, IT2 first2, IT2 last2) const {
		return rapidfuzz::levenshtein_editops(first1, last1, first2, last2);
	}
};

struct ComputeIndelEditops {
	template <class IT1, class IT2>
	rapidfuzz::Editops operator()(IT1 first1, IT1 last1, IT2 first2, IT2 last2) const {
		return rapidfuzz::indel_editops(first1, last1, first2, last2);
	}
};

void LevenshteinEditopsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteEditops(args, result, ComputeLevenshteinEditops());
}

void LevenshteinOpcodesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteOpcodes(args, result, ComputeLevenshteinEditops());
}

void IndelEditopsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteEditops(args, result, ComputeIndelEditops());
}

void IndelOpcodesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ExecuteOpcodes(args, result, ComputeIndelEditops());
}

void PartialRatioAlignmentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	CharacterBuffer buffer1;
	CharacterBuffer buffer2;
	ExecuteRowWise<AlignmentStruct>(args, result, [&](VectorWriter<AlignmentStruct> &writer, string_t s1, string_t s2) {
		auto alignment =
		    VisitCharacterPair(buffer1, buffer2, s1, s2, [](auto first1, auto last1, auto first2, auto last2) {
			    return rapidfuzz::fuzz::partial_ratio_alignment(first1, last1, first2, last2);
		    });
		writer.WriteValue([&](auto &score, auto &s1_start, auto &s1_end, auto &s2_start, auto &s2_end) {
			score.WriteValue(alignment.score);
			s1_start.WriteValue(int64_t(alignment.src_start));
			s1_end.WriteValue(int64_t(alignment.src_end));
			s2_start.WriteValue(int64_t(alignment.dest_start));
			s2_end.WriteValue(int64_t(alignment.dest_end));
		});
	});
}

void RegisterAlignmentFunction(ExtensionLoader &loader, const char *name, LogicalType return_type,
                               scalar_function_t function, const string &description, const string &example) {
	ScalarFunction scalar_function(Identifier(name),
	                               FunctionSignature({PositionalParameter("s1", LogicalType::VARCHAR),
	                                                  PositionalParameter("s2", LogicalType::VARCHAR)},
	                                                 std::move(return_type)),
	                               std::move(function));
	FunctionDocumentation documentation;
	documentation.description = description;
	documentation.examples = {example};
	documentation.categories = {"text_similarity"};
	RegisterDocumentedFunction(loader, std::move(scalar_function), std::move(documentation));
}

constexpr const char *POSITIONS_NOTE = " Positions are 0-based character offsets; ranges are half-open.";

} // namespace

void RegisterAlignmentFunctions(ExtensionLoader &loader) {
	RegisterAlignmentFunction(
	    loader, "rapidfuzz_levenshtein_editops", EditopsType(), LevenshteinEditopsFunction,
	    string("The Levenshtein edit operations ('insert', 'delete' or 'replace') that turn s1 into s2.") +
	        POSITIONS_NOTE,
	    "rapidfuzz_levenshtein_editops('kitten', 'sitting')");
	RegisterAlignmentFunction(
	    loader, "rapidfuzz_levenshtein_opcodes", OpcodesType(), LevenshteinOpcodesFunction,
	    string("Describes how to turn s1 into s2 as a sequence of 'equal', 'insert', 'delete' and 'replace' blocks "
	           "covering both strings, based on Levenshtein edits.") +
	        POSITIONS_NOTE,
	    "rapidfuzz_levenshtein_opcodes('kitten', 'sitting')");
	RegisterAlignmentFunction(loader, "rapidfuzz_indel_editops", EditopsType(), IndelEditopsFunction,
	                          string("The Indel edit operations ('insert' or 'delete') that turn s1 into s2.") +
	                              POSITIONS_NOTE,
	                          "rapidfuzz_indel_editops('kitten', 'sitting')");
	RegisterAlignmentFunction(
	    loader, "rapidfuzz_indel_opcodes", OpcodesType(), IndelOpcodesFunction,
	    string("Describes how to turn s1 into s2 as a sequence of 'equal', 'insert' and 'delete' blocks covering both "
	           "strings, based on Indel edits.") +
	        POSITIONS_NOTE,
	    "rapidfuzz_indel_opcodes('kitten', 'sitting')");
	RegisterAlignmentFunction(
	    loader, "rapidfuzz_partial_ratio_alignment", AlignmentType(), PartialRatioAlignmentFunction,
	    string("The `rapidfuzz_partial_ratio` score together with the substrings of s1 and s2 that produced it.") +
	        POSITIONS_NOTE,
	    "rapidfuzz_partial_ratio_alignment('a certain string', 'cetain')");
}

} // namespace duckdb
