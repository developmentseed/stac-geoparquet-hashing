# stac-geoparquet-hashing

Given one or more **stac-geoparquet** files, re-write them into spatio-temporal-optimized **stac-geoparquet** by sorting them by their [**stac-hash**](https://www.gadom.ski/stac-hash/).
Optionally, prefix item ids by their hash value, drastically improving the performance of single-id searches against the **stac-geoparquet**.

## Usage

TODO

## Benchmarks

We've done some benchmarking against files retrieved from the [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/).
To see the results, check out [notebooks/duckdb-geoparquet-benchmarks.ipynb](notebooks/duckdb-geoparquet-benchmarks.ipynb).
Since they involve many remote queries the benchmarks take a while to run, but if you'd like to run them yourself:

```sh
scripts/sync-benchmark-data.sh
uv sync --group notebook
uv run --group notebook jupyter lab notebooks/duckdb-geoparquet-benchmarks.ipynb
```

## License

[MIT](LICENSE)
