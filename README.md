# stac-geoparquet-hashing

WIP

## DuckDB GeoParquet Benchmarks

Benchmark DuckDB queries against the Microsoft GeoParquet layout, the hashed 128-file layout, and a generated hashed 12-file layout:

```sh
scripts/sync-benchmark-data.sh
uv sync --group notebook
uv run --group notebook jupyter lab notebooks/duckdb-geoparquet-benchmarks.ipynb
```

Run `scripts/generate-file-count-matched-geoparquet.sh` if you want to rebuild the 12-file hashed layout locally.

The benchmark data is written under `data/`, which is ignored by git.

The notebook also includes a remote Source Cooperative S3 benchmark matrix for real-world object-store timings.
