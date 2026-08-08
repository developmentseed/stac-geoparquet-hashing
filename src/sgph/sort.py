from collections.abc import Iterator
from pathlib import Path

import numpy
import pyarrow
import pyarrow.dataset
from pyarrow.parquet import ParquetFile, ParquetWriter, read_table
from rich.progress import Progress, TaskID

HASH_COLUMN = "hash:hash"
BATCH_SIZE = 20_000


def read_keys(infile: Path) -> numpy.ndarray:
    """Returns every `hash:hash` value in `infile` as a numpy array."""
    parquet_file = ParquetFile(infile)
    if HASH_COLUMN not in parquet_file.schema_arrow.names:
        raise ValueError(f"{infile} has no {HASH_COLUMN} column")
    return parquet_file.read(columns=[HASH_COLUMN]).column(HASH_COLUMN).to_numpy()


def input_schema(infiles: list[Path]) -> pyarrow.Schema:
    """Returns the schema shared by every file in `infiles`.

    Raises `ValueError` if any file's schema differs from the first, compared
    ignoring metadata. `pyarrow.dataset.write_dataset` accepts mismatched
    batches silently, so this check is what prevents a corrupt mixed output.
    """
    schema = ParquetFile(infiles[0]).schema_arrow
    for infile in infiles[1:]:
        other = ParquetFile(infile).schema_arrow
        if not other.equals(schema, check_metadata=False):
            raise ValueError(f"{infile} schema does not match {infiles[0]}")
    return schema


def bucket_boundaries(infiles: list[Path], buckets: int) -> numpy.ndarray:
    """Returns strictly increasing hash values splitting `infiles` into even buckets.

    Boundaries are exact, taken from the sorted keys at even index positions,
    then deduplicated. A repeated hash value collapses boundaries together, so
    fewer than `buckets` ranges may come back.
    """
    if buckets < 1:
        raise ValueError(f"buckets must be >= 1, got {buckets}")
    keys = numpy.concatenate([read_keys(infile) for infile in infiles])
    keys.sort()
    count = len(keys)
    if count == 0:
        raise ValueError("no rows to sort")
    return numpy.unique(keys[[(i * count) // buckets for i in range(1, buckets)]])


BUCKET_FIELD = pyarrow.field("bucket", pyarrow.int32())


def with_bucket(table: pyarrow.Table, boundaries: numpy.ndarray) -> pyarrow.Table:
    """Returns `table` with an int32 `bucket` column appended.

    Uses `side="right"` so rows sharing a hash value always land in the same
    bucket, which is what makes concatenated per-bucket sorts a total order.
    """
    hashes = table.column(HASH_COLUMN).to_numpy()
    buckets = numpy.searchsorted(boundaries, hashes, side="right").astype("int32")
    return table.append_column(BUCKET_FIELD, pyarrow.array(buckets, pyarrow.int32()))


def stage_buckets(
    infiles: list[Path],
    boundaries: numpy.ndarray,
    staging_dir: Path,
    progress: Progress,
) -> None:
    """Writes every row of `infiles` into hive-partitioned files under `staging_dir`."""
    schema = ParquetFile(infiles[0]).schema_arrow.append(BUCKET_FIELD)
    total = sum(ParquetFile(infile).metadata.num_rows for infile in infiles)
    task = progress.add_task("bucketing", total=total)
    pyarrow.dataset.write_dataset(
        bucketed_batches(infiles, boundaries, progress, task),
        staging_dir,
        schema=schema,
        format="parquet",
        partitioning=pyarrow.dataset.partitioning(
            pyarrow.schema([BUCKET_FIELD]), flavor="hive"
        ),
        existing_data_behavior="overwrite_or_ignore",
    )
    progress.remove_task(task)


def bucketed_batches(
    infiles: list[Path],
    boundaries: numpy.ndarray,
    progress: Progress,
    task: TaskID,
) -> Iterator[pyarrow.RecordBatch]:
    for infile in infiles:
        parquet_file = ParquetFile(infile)
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            table = with_bucket(pyarrow.Table.from_batches([batch]), boundaries)
            yield from table.to_batches()
            progress.advance(task, advance=batch.num_rows)


def staged_bucket_dirs(staging_dir: Path) -> list[Path]:
    """Returns the `bucket=N` directories under `staging_dir` in numeric order.

    The indices are sparse: an empty range produces no directory, so the first
    directory is not necessarily `bucket=0`.
    """
    return sorted(
        staging_dir.glob("bucket=*"), key=lambda path: int(path.name.split("=")[1])
    )


ROW_GROUP_SIZE = 50_000


def outfile_name(index: int, count: int) -> str:
    """Returns the output file name for bucket `index` of `count`.

    Padded so that filename order always matches numeric order.
    """
    return f"part-{index:0{max(3, len(str(count - 1)))}d}.parquet"


def sort_bucket(bucket_dir: Path, outfile: Path, schema: pyarrow.Schema) -> None:
    """Sorts one staged bucket in memory and writes it to `outfile`.

    Casting to `schema` restores the original field types and the `geo` and
    `stac-geoparquet` metadata, which the staging round trip does not carry.
    """
    table = read_table(bucket_dir).select(schema.names).cast(schema)
    with ParquetWriter(outfile, schema, compression="zstd") as writer:
        writer.write_table(table.sort_by(HASH_COLUMN), row_group_size=ROW_GROUP_SIZE)
