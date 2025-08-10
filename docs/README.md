
# RapidFuzz Extension for DuckDB

This `rapidfuzz` extension adds high-performance fuzzy string matching functions to DuckDB, powered by the RapidFuzz C++ library.

## Installation

**`rapidfuzz` is a [DuckDB Community Extension](https://github.com/duckdb/community-extensions).**

You can use it in DuckDB SQL:

```sql
install rapidfuzz from community;
load rapidfuzz;
```

## What is Fuzzy String Matching?

Fuzzy string matching allows you to compare strings and measure their similarity, even when they are not exactly the same. This is useful for:

- Data cleaning and deduplication
- Record linkage
- Search and autocomplete
- Spell checking

RapidFuzz provides fast, high-quality algorithms for string similarity and matching.

## Available Functions

This extension exposes several core RapidFuzz algorithms as DuckDB scalar functions:

### `rapidfuzz_ratio(a, b)`
- **Returns**: `DOUBLE` (similarity score between 0 and 100)
- **Description**: Computes the similarity ratio between two strings.

```sql
SELECT rapidfuzz_ratio('hello world', 'helo wrld');
┌─────────────────────────────────────────────┐
│ rapidfuzz_ratio('hello world', 'helo wrld') │
│                   double                    │
├─────────────────────────────────────────────┤
│                    90.0                     │
└─────────────────────────────────────────────┘
```

### `rapidfuzz_partial_ratio(a, b)`
- **Returns**: `DOUBLE`
- **Description**: Computes the best partial similarity score between substrings of the two inputs.

```sql
SELECT rapidfuzz_partial_ratio('hello world', 'world');
┌─────────────────────────────────────────────────┐
│ rapidfuzz_partial_ratio('hello world', 'world') │
│                     double                      │
├─────────────────────────────────────────────────┤
│                      100.0                      │
└─────────────────────────────────────────────────┘
```

### `rapidfuzz_token_sort_ratio(a, b)`
- **Returns**: `DOUBLE`
- **Description**: Compares strings after sorting their tokens (words), useful for matching strings with reordered words.

```sql
SELECT rapidfuzz_token_sort_ratio('world hello', 'hello world');
┌──────────────────────────────────────────────────────────┐
│ rapidfuzz_token_sort_ratio('world hello', 'hello world') │
│                          double                          │
├──────────────────────────────────────────────────────────┤
│                          100.0                           │
└──────────────────────────────────────────────────────────┘
```

### `rapidfuzz_token_set_ratio(a, b)`
- **Returns**: `DOUBLE`
- **Description**: A similarity metric that compares sets of tokens between two strings, ignoring duplicated words and word order.

```sql
SELECT rapidfuzz_token_set_ratio('new york new york city', 'new york city');
┌──────────────────────────────────────────────────────────────────────┐
│ rapidfuzz_token_set_ratio('new york new york city', 'new york city') │
│                                double                                │
├──────────────────────────────────────────────────────────────────────┤
│                                100.0                                 │
└──────────────────────────────────────────────────────────────────────┘
```


## Supported Data Types

All functions support DuckDB `VARCHAR` type. For best results, use with textual data.

## Usage Examples

### Basic Similarity

```sql
SELECT rapidfuzz_ratio('database', 'databse');
SELECT rapidfuzz_partial_ratio('duckdb extension', 'extension');
SELECT rapidfuzz_token_sort_ratio('fuzzy string match', 'string fuzzy match');
SELECT rapidfuzz_token_set_ratio('fuzzy string match', 'string fuzzy match');
```

### Data Deduplication

```sql
SELECT name, rapidfuzz_ratio(name, 'Jon Smith') AS similarity
FROM users
WHERE rapidfuzz_ratio(name, 'Jon Smith') > 80;
```

### Record Linkage

```sql
SELECT a.id, b.id, rapidfuzz_ratio(a.name, b.name) AS score
FROM table_a a
JOIN table_b b ON rapidfuzz_ratio(a.name, b.name) > 85;
```

### Search and Autocomplete

```sql
SELECT query, candidate, rapidfuzz_partial_ratio(query, candidate) AS score
FROM search_candidates
ORDER BY score DESC
LIMIT 10;
```

## Algorithm Selection Guide

- **General similarity**: Use `rapidfuzz_ratio` for overall similarity.
- **Partial matches**: Use `rapidfuzz_partial_ratio` for substring matches.
- **Reordered words**: Use `rapidfuzz_token_sort_ratio` for strings with the same words in different orders.

## Performance Tips

1. RapidFuzz algorithms are highly optimized for speed and accuracy.
2. For large datasets, use WHERE clauses to filter by similarity threshold.
3. Preprocess your data (e.g., lowercase, trim) for best results.

## License

MIT Licensed
