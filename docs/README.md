
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

### `ratio(a, b)`
- **Returns**: `DOUBLE` (similarity score between 0 and 100)
- **Description**: Computes the similarity ratio between two strings.

```sql
SELECT ratio('hello world', 'helo wrld');
-- Returns: 90.91
```

### `partial_ratio(a, b)`
- **Returns**: `DOUBLE`
- **Description**: Computes the best partial similarity score between substrings of the two inputs.

```sql
SELECT partial_ratio('hello world', 'world');
-- Returns: 100.0
```

### `token_sort_ratio(a, b)`
- **Returns**: `DOUBLE`
- **Description**: Compares strings after sorting their tokens (words), useful for matching strings with reordered words.

```sql
SELECT token_sort_ratio('world hello', 'hello world');
-- Returns: 100.0
```

## Supported Data Types

All functions support DuckDB `VARCHAR` type. For best results, use with textual data.

## Usage Examples

### Basic Similarity

```sql
SELECT ratio('database', 'databse');
SELECT partial_ratio('duckdb extension', 'extension');
SELECT token_sort_ratio('fuzzy string match', 'string fuzzy match');
```

### Data Deduplication

```sql
SELECT name, ratio(name, 'Jon Smith') AS similarity
FROM users
WHERE ratio(name, 'Jon Smith') > 80;
```

### Record Linkage

```sql
SELECT a.id, b.id, ratio(a.name, b.name) AS score
FROM table_a a
JOIN table_b b ON ratio(a.name, b.name) > 85;
```

### Search and Autocomplete

```sql
SELECT query, candidate, partial_ratio(query, candidate) AS score
FROM search_candidates
ORDER BY score DESC
LIMIT 10;
```

## Algorithm Selection Guide

- **General similarity**: Use `ratio` for overall similarity.
- **Partial matches**: Use `partial_ratio` for substring matches.
- **Reordered words**: Use `token_sort_ratio` for strings with the same words in different orders.

## Performance Tips

1. RapidFuzz algorithms are highly optimized for speed and accuracy.
2. For large datasets, use WHERE clauses to filter by similarity threshold.
3. Preprocess your data (e.g., lowercase, trim) for best results.

## License

MIT Licensed
