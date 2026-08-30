<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# RapidFuzz Extension for DuckDB

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/rapidfuzz.html)
[![v1.5 build](https://github.com/Query-farm/rapidfuzz/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/rapidfuzz/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

This `rapidfuzz` extension adds high-performance fuzzy string matching and string edit distance functions to DuckDB, powered by the RapidFuzz C++ library.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/rapidfuzz](https://query.farm/products/extensions/rapidfuzz)**

## Installation

```sql
install rapidfuzz from community;
load rapidfuzz;
```
