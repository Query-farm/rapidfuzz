#include "rapidfuzz_characters.hpp"

#include <cstring>

namespace duckdb {

bool CharacterBuffer::IsAscii(const_data_ptr_t data, idx_t size) {
	static constexpr uint64_t HIGH_BITS = 0x8080808080808080ULL;
	idx_t i = 0;
	for (; i + sizeof(uint64_t) <= size; i += sizeof(uint64_t)) {
		uint64_t block;
		memcpy(&block, data + i, sizeof(block));
		if (block & HIGH_BITS) {
			return false;
		}
	}
	for (; i < size; i++) {
		if (data[i] & 0x80) {
			return false;
		}
	}
	return true;
}

void CharacterBuffer::Decode(const_data_ptr_t data, idx_t size) {
	codepoints.clear();
	codepoints.reserve(size);
	idx_t pos = 0;
	while (pos < size) {
		const uint8_t lead = data[pos];
		idx_t length;
		uint32_t codepoint;
		if (lead < 0x80) {
			length = 1;
			codepoint = lead;
		} else if ((lead & 0xE0) == 0xC0) {
			length = 2;
			codepoint = lead & 0x1F;
		} else if ((lead & 0xF0) == 0xE0) {
			length = 3;
			codepoint = lead & 0x0F;
		} else if ((lead & 0xF8) == 0xF0) {
			length = 4;
			codepoint = lead & 0x07;
		} else {
			length = 1;
			codepoint = lead;
		}
		// VARCHAR is valid UTF-8, but never read past the end or across a malformed sequence regardless
		if (pos + length > size) {
			length = 1;
			codepoint = lead;
		}
		for (idx_t i = 1; i < length; i++) {
			const uint8_t continuation = data[pos + i];
			if ((continuation & 0xC0) != 0x80) {
				length = 1;
				codepoint = lead;
				break;
			}
			codepoint = (codepoint << 6) | (continuation & 0x3F);
		}
		codepoints.push_back(codepoint);
		pos += length;
	}
}

} // namespace duckdb
