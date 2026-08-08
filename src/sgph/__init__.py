import datetime
import shutil
import tempfile
from pathlib import Path

import pandas
import pyarrow
import typer
from pyarrow.parquet import ParquetFile, ParquetWriter
from rich.progress import Progress
from stac_hash import Hasher

from sgph import sort as sort_module
from sgph.progress import progress_bar

app = typer.Typer()

TIMESTAMP = pyarrow.timestamp("us", tz="UTC")
SOURCE_COLUMNS = ("bbox", "datetime", "start_datetime", "end_datetime")
HASH_COLUMNS = ("hash:start_datetime", "hash:end_datetime", "hash:hash")


@app.command()
def hash(year: int, infile: Path, outfile: Path) -> None:
    """Add a `hash:hash` column to the stac-geoparquet at `infile`.

    Hashes are calculated over a whole-world bounding box and the temporal
    extent of `year`, so hashes from different files are comparable.
    """
    with progress_bar() as progress:
        hash_file(year, infile, outfile, progress)


@app.command()
def hash_dir(year: int, indir: Path, outdir: Path) -> None:
    """Hash every `*.parquet` file in `indir` into `outdir`, one for one.

    Output files keep their input name. Hashes are calculated over the
    temporal extent of `year`, so they are comparable across every file.
    """
    infiles = sorted(indir.glob("*.parquet"))
    if not infiles:
        raise ValueError(f"no parquet files in {indir}")
    with progress_bar() as progress:
        task = progress.add_task("all files", total=len(infiles))
        for infile in infiles:
            hash_file(year, infile, outdir / infile.name, progress)
            progress.advance(task)


@app.command(name="sort")
def sort_command(
    indir: Path,
    outdir: Path,
    buckets: int = 128,
    staging_dir: Path | None = None,
    force: bool = False,
) -> None:
    """Sort every `*.parquet` file in `indir` by `hash:hash` into `outdir`.

    Rows are range-partitioned into `buckets` ranges, each of which is sorted
    in memory, so nothing ever spills to disk. Reading the output files in
    filename order yields a globally sorted dataset.

    Raises `ValueError` if `outdir` already contains `part-*.parquet` files,
    since a re-run with a different `buckets` value can leave stale files
    behind that silently corrupt the output. Pass `force=True` to delete
    those files before sorting.
    """
    infiles = sorted(indir.glob("*.parquet"))
    if not infiles:
        raise ValueError(f"no parquet files in {indir}")
    existing = sorted(outdir.glob("part-*.parquet")) if outdir.exists() else []
    if existing:
        if not force:
            raise ValueError(
                f"{outdir} already contains {len(existing)} part-*.parquet file(s); "
                "remove them or pass --force"
            )
        for path in existing:
            path.unlink()
    outdir.mkdir(parents=True, exist_ok=True)
    schema = sort_module.input_schema(infiles)
    with progress_bar() as progress:
        boundaries = sort_module.bucket_boundaries(infiles, buckets)
        with tempfile.TemporaryDirectory(dir=staging_dir or outdir.parent) as name:
            staging = Path(name)
            sort_module.stage_buckets(infiles, boundaries, staging, progress)
            bucket_dirs = sort_module.staged_bucket_dirs(staging)
            task = progress.add_task("sorting", total=len(bucket_dirs))
            for index, bucket_dir in enumerate(bucket_dirs):
                sort_module.sort_bucket(
                    bucket_dir,
                    outdir / sort_module.outfile_name(index, len(bucket_dirs)),
                    schema,
                )
                shutil.rmtree(bucket_dir)
                progress.advance(task)
            progress.remove_task(task)


def hash_file(year: int, infile: Path, outfile: Path, progress: Progress) -> None:
    outfile.parent.mkdir(parents=True, exist_ok=True)
    start_datetime = datetime.datetime(year, 1, 1, tzinfo=datetime.UTC)
    end_datetime = datetime.datetime(year + 1, 1, 1, tzinfo=datetime.UTC)
    hasher = Hasher(start_datetime, end_datetime)
    parquet_file = ParquetFile(infile)
    schema = hashed_schema(parquet_file.schema_arrow)
    task = progress.add_task(infile.name, total=parquet_file.metadata.num_row_groups)
    with ParquetWriter(outfile, schema) as writer:
        for row_group in range(parquet_file.metadata.num_row_groups):
            table = parquet_file.read_row_group(row_group)
            writer.write_table(
                hashed_table(table, hasher, start_datetime, end_datetime)
            )
            progress.advance(task)
    progress.remove_task(task)


def hashed_schema(schema: pyarrow.Schema) -> pyarrow.Schema:
    return pyarrow.schema(
        [field for field in schema if field.name not in HASH_COLUMNS]
        + [
            pyarrow.field("hash:start_datetime", TIMESTAMP),
            pyarrow.field("hash:end_datetime", TIMESTAMP),
            pyarrow.field("hash:hash", pyarrow.int64()),
        ],
        metadata=schema.metadata,
    )


def hashed_table(
    table: pyarrow.Table,
    hasher: Hasher,
    start_datetime: datetime.datetime,
    end_datetime: datetime.datetime,
) -> pyarrow.Table:
    columns = [name for name in SOURCE_COLUMNS if name in table.column_names]
    data_frame = table.select(columns).to_pandas()
    longitudes, latitudes = bbox_centers(data_frame)
    hashes = hasher.hash_all_clamped(datetimes(data_frame), longitudes, latitudes)
    starts = pyarrow.array([start_datetime] * table.num_rows, TIMESTAMP)
    ends = pyarrow.array([end_datetime] * table.num_rows, TIMESTAMP)
    table = table.drop_columns(
        [name for name in HASH_COLUMNS if name in table.column_names]
    )
    return (
        table.append_column(pyarrow.field("hash:start_datetime", TIMESTAMP), starts)
        .append_column(pyarrow.field("hash:end_datetime", TIMESTAMP), ends)
        .append_column(
            pyarrow.field("hash:hash", pyarrow.int64()),
            pyarrow.array(hashes, pyarrow.int64()),
        )
    )


def bbox_centers(data_frame: pandas.DataFrame) -> tuple[list[float], list[float]]:
    bbox = pandas.DataFrame(data_frame["bbox"].tolist(), index=data_frame.index)
    longitudes = (bbox["xmin"] + bbox["xmax"]) / 2
    latitudes = (bbox["ymin"] + bbox["ymax"]) / 2
    return longitudes.tolist(), latitudes.tolist()


def datetimes(data_frame: pandas.DataFrame) -> list[datetime.datetime]:
    values = data_frame["datetime"]
    if "start_datetime" in data_frame and "end_datetime" in data_frame:
        start = data_frame["start_datetime"]
        end = data_frame["end_datetime"]
        values = values.fillna(start + (end - start) / 2)
    missing = int(values.isna().sum())
    if missing:
        raise ValueError(f"{missing} items have no datetime or start/end datetimes")
    return values.dt.to_pydatetime().tolist()
