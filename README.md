# stac-geoparquet-hashing

Given one or more **stac-geoparquet** files, re-write them into spatio-temporal-optimized **stac-geoparquet** by sorting them by their [**stac-hash**](https://www.gadom.ski/stac-hash/).
Optionally, prefix item ids by their hash value, drastically improving the performance of single-id searches against the **stac-geoparquet**.

## Usage

Install dependencies and run the `cosgp` CLI via [uv](https://docs.astral.sh/uv/):

```sh
uv sync
uv run cosgp INFILE DATETIME OUTDIR
```

- `INFILE`: a stac-geoparquet file, or a directory of `.parquet` files
- `DATETIME`: the datetime range to hash against — a year (`2024`), year-month (`2024-06`), or year-month-day (`2024-06-01`)
- `OUTDIR`: where the cloud-optimized output files are written

For example:

```sh
uv run cosgp items.parquet 2024 optimized/
```

Some useful options:

- `--bbox MINX MINY MAXX MAXY`: restrict the hash's spatial extent (defaults to the whole globe)
- `--prefix-id`: prefix each item's `id` with its hash, speeding up single-id lookups
- `--bucket-size BYTES`: target size per output file (default 2 GB uncompressed)
- `--no-progress`: log progress instead of showing a progress bar

Run `uv run cosgp --help` for the full list of options.

## Benchmarks

We've done some benchmarking against files retrieved from the [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/).
To see the results, check out [notebooks/duckdb-geoparquet-benchmarks.ipynb](notebooks/duckdb-geoparquet-benchmarks.ipynb).
Since they involve many remote queries the benchmarks take a while to run, but if you'd like to run them yourself:

```sh
scripts/sync-benchmark-data.sh
uv sync --group notebooks
uv run --group notebooks jupyter lab notebooks/duckdb-geoparquet-benchmarks.ipynb
```

## License

[MIT](LICENSE)
