<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# RapidFuzz Extension for DuckDB

[![DuckDB](https://img.shields.io/badge/DuckDB-extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://query.farm/products/extensions/rapidfuzz/)
[![v2.0 build](https://github.com/Query-farm/rapidfuzz/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v2.0)](https://github.com/Query-farm/rapidfuzz/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av2.0)

Compare strings with fuzzy matching and edit-distance functions in DuckDB using RapidFuzz.

```sql
INSTALL rapidfuzz FROM community;
LOAD rapidfuzz;

SELECT rapidfuzz_wratio('this is a test', 'this is a new test!!!');            -- 85.5
SELECT rapidfuzz_levenshtein_distance('café', 'cafe');                        -- 1 (scores count characters)
SELECT rapidfuzz_extract_one('cowboys', ['New York Jets', 'New York Giants', 'Dallas Cowboys']);
-- {'choice': Dallas Cowboys, 'score': 83.07692307692308, 'choice_index': 3}
SELECT name FROM customers
WHERE rapidfuzz_jaro_winkler_similarity(name, 'Jonathan', score_cutoff := 0.9) >= 0.9;
```

See [CHANGELOG.md](../CHANGELOG.md) for what changed in 2.0, including breaking changes.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/rapidfuzz/](https://query.farm/products/extensions/rapidfuzz/)**
