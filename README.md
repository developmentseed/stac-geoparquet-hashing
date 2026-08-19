# stac-geoparquet-hashing

WIP

## DuckDB GeoParquet Benchmarks

Benchmark local DuckDB queries against the Microsoft GeoParquet layout, the hashed 128-file layout, and a generated hashed 12-file layout:

```sh
scripts/sync-benchmark-data.sh
scripts/generate-file-count-matched-geoparquet.sh
uv sync --group notebook
uv run --group notebook jupyter lab notebooks/duckdb-geoparquet-benchmarks.ipynb
```

The benchmark data is written under `data/`, which is ignored by git.
