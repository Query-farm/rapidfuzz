# Changelog

## 2.0 (DuckDB v2.0)

### Breaking changes

- **Scores count characters, not bytes.** Non-ASCII strings are compared by Unicode code point, so
  `rapidfuzz_levenshtein_distance('café', 'cafe')` is now `1` (it was `2`), and normalized scores, Hamming lengths and
  token splitting behave as they do in Python's `rapidfuzz`. Results for pure ASCII strings are unchanged.
- **Integer metrics return `BIGINT`.** `*_distance` and `*_similarity` of Levenshtein, Damerau-Levenshtein, OSA, Indel,
  LCSseq, Hamming, Prefix and Postfix count characters and now return `BIGINT` instead of `DOUBLE`. Normalized scores,
  Jaro, Jaro-Winkler and the `fuzz` ratios still return `DOUBLE`.

### New functions

- Ratios: `rapidfuzz_wratio`, `rapidfuzz_qratio`, `rapidfuzz_token_ratio`, `rapidfuzz_partial_token_ratio`,
  `rapidfuzz_partial_token_sort_ratio`.
- Metric families (`_distance`, `_similarity`, `_normalized_distance`, `_normalized_similarity`):
  `rapidfuzz_levenshtein_*` (with configurable edit costs) and `rapidfuzz_damerau_levenshtein_*`.
- Best matches: `rapidfuzz_extract(query, choices)` returns the best `top_k` matches from a list of choices, and
  `rapidfuzz_extract_one(query, choices)` the single best one, like Python's `rapidfuzz.process.extract` and
  `extractOne`. Each match has the choice, its score and its 1-based `choice_index`. They rank by `scorer :=` any ratio
  (default `'wratio'`) or any `<metric>_normalized_similarity`, and accept `score_cutoff`.
- Alignment: `rapidfuzz_levenshtein_editops`, `rapidfuzz_levenshtein_opcodes`, `rapidfuzz_indel_editops`,
  `rapidfuzz_indel_opcodes` and `rapidfuzz_partial_ratio_alignment`. Positions are 0-based character offsets with
  half-open ranges, as in Python's `rapidfuzz`.

### New options

All options are keyword-only (`name := value`) and must be constant.

| Option | Functions | Default |
| --- | --- | --- |
| `score_cutoff` | every scorer, `rapidfuzz_extract`, `rapidfuzz_extract_one` | no cutoff |
| `scorer`, `top_k` | `rapidfuzz_extract` (`scorer` also on `rapidfuzz_extract_one`) | `'wratio'`, `5` (`NULL` for all) |
| `insert_cost`, `delete_cost`, `replace_cost` | `rapidfuzz_levenshtein_*` | `1` |
| `pad` | `rapidfuzz_hamming_*` | `true` |
| `prefix_weight` | `rapidfuzz_jaro_winkler_*` | `0.1` (at most `0.25`) |

`score_cutoff` follows RapidFuzz: distances above the cutoff are returned as `cutoff + 1` (`1.0` when normalized),
similarities below it as `0`. A NULL `score_cutoff` means no cutoff. Besides filtering, it lets RapidFuzz stop early on poor matches.

```sql
SELECT name FROM customers
WHERE rapidfuzz_levenshtein_distance(name, 'Jonathan', score_cutoff := 2) <= 2;
```

### Performance

- When one argument is constant (`f(column, 'literal')`, in either position), it is preprocessed once per vector with
  RapidFuzz's cached scorers. This is about 1.2–2.7× faster than 2.0's first release, depending on the scorer.
- Strings are no longer copied into `std::string` for every row.

### Other

- Every function has a description, an example and the `text_similarity` category in `duckdb_functions()`.
- RapidFuzz is pinned to rapidfuzz-cpp 3.3.4 through the overlay port in `vcpkg_ports/rapidfuzz-cpp`, instead of
  following the deprecated `rapidfuzz` vcpkg port. 3.3.4 includes the upstream fixes for `WRatio` at a length ratio of
  exactly 8 and for an out-of-bounds access in edit-operation handling.
