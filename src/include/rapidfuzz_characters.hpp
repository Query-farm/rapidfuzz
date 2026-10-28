#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Presents DuckDB strings to RapidFuzz as sequences of characters rather than UTF-8 bytes. Pure ASCII strings are
//! passed through as bytes (the fast path); anything else is decoded into Unicode code points, so that scores and
//! positions count characters.
class CharacterBuffer {
public:
	//! Calls fun(first, last) with an iterator pair over the characters of str, either `const uint8_t *` (ASCII)
	//! or `const uint32_t *` (code points). The pointers stay valid until the next call on this buffer.
	template <class FUNC>
	auto Visit(const string_t &str, FUNC &&fun) {
		auto bytes = const_data_ptr_cast(str.GetData());
		auto size = str.GetSize();
		if (IsAscii(bytes, size)) {
			return fun(bytes, bytes + size);
		}
		Decode(bytes, size);
		return fun(CodepointsBegin(), CodepointsEnd());
	}

	//! Decodes str into code points, regardless of whether it is ASCII
	void Decode(const string_t &str) {
		Decode(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	const uint32_t *CodepointsBegin() const {
		return codepoints.data();
	}
	const uint32_t *CodepointsEnd() const {
		return codepoints.data() + codepoints.size();
	}

	static bool IsAscii(const string_t &str) {
		return IsAscii(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	static bool IsAscii(const_data_ptr_t data, idx_t size);

private:
	void Decode(const_data_ptr_t data, idx_t size);

private:
	vector<uint32_t> codepoints;
};

//! Calls fun(first1, last1, first2, last2) over the characters of a and b. Both sides use bytes when both strings are
//! ASCII and code points otherwise, which keeps the number of template instantiations per scorer at two.
template <class FUNC>
auto VisitCharacterPair(CharacterBuffer &buffer_a, CharacterBuffer &buffer_b, const string_t &a, const string_t &b,
                        FUNC &&fun) {
	if (CharacterBuffer::IsAscii(a) && CharacterBuffer::IsAscii(b)) {
		auto a_bytes = const_data_ptr_cast(a.GetData());
		auto b_bytes = const_data_ptr_cast(b.GetData());
		return fun(a_bytes, a_bytes + a.GetSize(), b_bytes, b_bytes + b.GetSize());
	}
	buffer_a.Decode(a);
	buffer_b.Decode(b);
	return fun(buffer_a.CodepointsBegin(), buffer_a.CodepointsEnd(), buffer_b.CodepointsBegin(),
	           buffer_b.CodepointsEnd());
}

} // namespace duckdb
