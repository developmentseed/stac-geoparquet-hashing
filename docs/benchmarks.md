# DuckDB GeoParquet Benchmarks

This benchmark workflow compares direct DuckDB queries over three GeoParquet layouts, both from local files and directly from Source Cooperative S3:

| Dataset | Description |
| --- | --- |
| `microsoft` | Microsoft Planetary Computer GeoParquet layout from Source Cooperative. |
| `hashed_128_files` | Hashed GeoParquet layout from Source Cooperative. |
| `hashed_12_files` | Hashed GeoParquet layout rewritten into 12 files, matching the Microsoft file count. |

The first benchmark matrix is local-first so query timings do not include network latency. The notebook also includes a remote object-store matrix that uses the same layouts over S3 so timings include object-store listing, network latency, and HTTP range reads.

## Setup

```sh
scripts/sync-benchmark-data.sh
uv sync --group notebook
uv run --group notebook jupyter lab notebooks/duckdb-geoparquet-benchmarks.ipynb
```

Downloaded and generated data lives under `data/`, which is ignored by git. The sync script rebuilds the 12-file hashed layout from the synced hashed source so the dataset row counts stay equivalent. Run `scripts/generate-file-count-matched-geoparquet.sh` directly if you want to rebuild it with custom inputs.

## Queries

The notebook measures:

- full dataset count
- time range count
- bbox count
- STAC-style collection + time + bbox count
- hash range search for hashed datasets
- search page materialization ordered by datetime
- search page materialization ordered by hash for hashed datasets
- cloud-cover attribute filter using `"eo:cloud_cover"`
- exact geometry intersects
- grouped monthly aggregation
- latest items for a collection
- specific id lookup and scoped id lookup

## Remote Object-Store Run

The notebook defines `REMOTE_DATASETS` with `s3://` paths under `us-west-2.opendata.source.coop/developmentseed/stac-geoparquet`. DuckDB uses the `httpfs` extension and an unsigned public S3 secret for those reads.

Keep the local and remote result tables separate when interpreting performance:

- local results isolate Parquet layout and sort effects from network behavior
- remote results show the end-to-end behavior a client sees against object storage
- compare `remote_microsoft` against `remote_hashed_12_files` for the closest real-world file-count match

## Interpretation

The key comparison is `microsoft` vs `hashed_12_files`, because both layouts use 12 files. That helps isolate layout and sort effects from file-count effects.

Compare `hashed_128_files` vs `hashed_12_files` to estimate the cost of many smaller files versus fewer larger files for the same hashed data.

Use `EXPLAIN ANALYZE` for important query/dataset pairs and inspect:

- total runtime
- filters pushed into `READ_PARQUET`
- total files read
- rows emitted from the table scan

A faster query is most compelling when DuckDB reads fewer files or row groups, not just when it happens to finish faster in a warm local cache.
