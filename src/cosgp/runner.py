from __future__ import annotations

import datetime
import logging
import math
import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import numpy
import pyarrow
import pyarrow.dataset
import pyarrow.parquet
from pandas import DataFrame
from pyarrow import RecordBatch, Schema, Table
from pyarrow.parquet import ParquetFile, ParquetWriter
from rich.progress import Progress, TaskID
from stac_hash import Hasher

from .progress import progress_bar

logger = logging.getLogger(__name__)

BBox = tuple[float, float, float, float]

TIMESTAMP = pyarrow.timestamp("us", tz="UTC")
BBOX = pyarrow.struct(
    [
        pyarrow.field("minx", pyarrow.float64()),
        pyarrow.field("miny", pyarrow.float64()),
        pyarrow.field("maxx", pyarrow.float64()),
        pyarrow.field("maxy", pyarrow.float64()),
    ]
)
BUCKET = pyarrow.field("bucket", pyarrow.int32())
DEFAULT_ROW_GROUP_SIZE = 150_000


@dataclass
class HashResult:
    hashes: list[int]
    total_bytes: int
    outfiles: list[Path]
    schema: Schema


class Runner:
    """Runs the cloud-optimized stac-geoparquet conversion."""

    def __init__(
        self,
        bucket_size: int,
        temporary_directory: Path | None = None,
        progress: bool = True,
        compression: str = "zstd",
        row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    ) -> None:
        self.bucket_size = bucket_size
        self.temporary_directory = temporary_directory or Path(tempfile.mkdtemp())
        self.progress: Progress | None
        self.compression = compression
        self.row_group_size = row_group_size
        if progress:
            self.progress = progress_bar()
        else:
            self.progress = None

    def run(
        self,
        *,
        infiles: list[Path],
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
        outdir: Path,
        bbox: BBox | None = None,
    ) -> None:
        if self.progress:
            self.progress.start()

        hash_result = self.hash(
            infiles, start_datetime=start_datetime, end_datetime=end_datetime, bbox=bbox
        )
        directories = self.stage(
            infiles=hash_result.outfiles,
            hashes=hash_result.hashes,
            total_bytes=hash_result.total_bytes,
        )
        self.sort(directories, hash_result.schema, outdir)

        if self.progress:
            self.progress.stop()

    def hash(
        self,
        infiles: list[Path],
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
        bbox: BBox | None,
    ) -> HashResult:
        if not infiles:
            raise ValueError("infiles must not be empty")

        task: TaskID | None = None
        if self.progress:
            task = self.progress.add_task(
                f"hashing {len(infiles)} files", total=len(infiles)
            )
        else:
            logger.info("hashing %d files", len(infiles))
        hash_directory = self.temporary_directory / "hashing"
        shutil.rmtree(hash_directory, ignore_errors=True)
        hash_directory.mkdir(parents=True)
        hasher = Hasher(
            start_datetime,
            end_datetime,
            bbox=bbox,
        )
        outfiles = []
        hashes = []
        total_bytes = 0
        schema: Schema | None = None
        for infile in infiles:
            parquet_infile = ParquetFile(infile)
            schema = hashed_schema(parquet_infile.schema_arrow, bbox is not None)
            outfile = hash_directory / infile.name
            outfiles.append(outfile)
            file_task: TaskID | None = None
            if self.progress:
                file_task = self.progress.add_task(
                    infile.name, total=parquet_infile.metadata.num_row_groups
                )
            with ParquetWriter(outfile, schema) as writer:
                for row_group in range(parquet_infile.metadata.num_row_groups):
                    total_bytes += parquet_infile.metadata.row_group(
                        row_group
                    ).total_byte_size
                    table = parquet_infile.read_row_group(row_group)
                    table, table_hashes = hashed_table(
                        table, hasher, start_datetime, end_datetime, bbox
                    )
                    hashes.extend(table_hashes)
                    writer.write_table(table)
                    if self.progress and file_task is not None:
                        self.progress.advance(file_task)

            if self.progress and file_task is not None:
                self.progress.remove_task(file_task)
            if self.progress and task is not None:
                self.progress.advance(task)
            if not self.progress:
                logger.info("hashed %s", infile.name)
        if self.progress and task is not None:
            self.progress.remove_task(task)

        assert schema
        return HashResult(hashes, total_bytes, outfiles, schema)

    def stage(
        self, infiles: list[Path], hashes: list[int], total_bytes: int
    ) -> list[Path]:
        keys = numpy.array(hashes, dtype=numpy.uint64)
        keys.sort()
        num_buckets = max(1, math.ceil(total_bytes / self.bucket_size))
        boundaries = numpy.unique(
            keys[[(i * len(keys) // num_buckets) for i in range(1, num_buckets)]]
        )
        schema = ParquetFile(infiles[0]).schema_arrow
        staging_schema = schema.append(BUCKET)
        staging_directory = self.temporary_directory / "staging"
        shutil.rmtree(staging_directory, ignore_errors=True)
        task: TaskID | None = None
        if self.progress:
            task = self.progress.add_task(
                f"staging {len(hashes)} items", total=len(hashes)
            )
        else:
            logger.info("staging %d items into %d buckets", len(hashes), num_buckets)
        pyarrow.dataset.write_dataset(
            staging_buckets(infiles, boundaries, self.progress, task),
            staging_directory,
            schema=staging_schema,
            format="parquet",
            partitioning=pyarrow.dataset.partitioning(
                pyarrow.schema([BUCKET]), flavor="hive"
            ),
            existing_data_behavior="overwrite_or_ignore",
        )
        directories = sorted(
            staging_directory.glob("bucket=*"), key=lambda d: int(d.name.split("=")[1])
        )
        if self.progress and task is not None:
            self.progress.remove_task(task)
        else:
            logger.info("staged %d buckets", len(directories))
        return directories

    def sort(self, directories: list[Path], schema: Schema, outdir: Path) -> None:
        task: TaskID | None = None
        if self.progress:
            task = self.progress.add_task(
                f"sorting {len(directories)} files", total=len(directories)
            )
        else:
            logger.info("sorting %d files", len(directories))
        for index, bucket_directory in enumerate(directories):
            table = (
                pyarrow.parquet.read_table(bucket_directory)
                .select(schema.names)
                .cast(schema)
            )
            file_name = (
                f"part-{index:0{max(3, len(str(len(directories) - 1)))}d}.parquet"
            )
            with ParquetWriter(
                outdir / file_name, schema, compression=self.compression
            ) as writer:
                writer.write_table(
                    table.sort_by("hash:hash"), row_group_size=self.row_group_size
                )
            # TODO write
            shutil.rmtree(bucket_directory)
            if self.progress and task is not None:
                self.progress.advance(task)
            else:
                logger.info("sorted %s (%d/%d)", file_name, index + 1, len(directories))


def hashed_schema(schema: Schema, has_bbox: bool) -> Schema:
    hash_columns = ["hash:hash", "hash:start_datetime", "hash:end_datetime"]
    hash_fields = [
        pyarrow.field("hash:hash", pyarrow.uint64()),
        pyarrow.field("hash:start_datetime", TIMESTAMP),
        pyarrow.field("hash:end_datetime", TIMESTAMP),
    ]
    if has_bbox:
        hash_columns.append("hash:bbox")
        hash_fields.append(pyarrow.field("hash:bbox", BBOX))
    return pyarrow.schema(
        [field for field in schema if field.name not in hash_columns] + hash_fields,
        metadata=schema.metadata,
    )


def hashed_table(
    table: Table,
    hasher: Hasher,
    start_datetime: datetime.datetime,
    end_datetime: datetime.datetime,
    bbox: BBox | None,
) -> tuple[Table, list[int]]:
    datetimes = table.select(["datetime"]).to_pandas()
    if "start_datetime" in table.column_names and "end_datetime" in table.column_names:
        datetime_data_frame = table.select(
            ["start_datetime", "end_datetime"]
        ).to_pandas()
        start_datetimes = datetime_data_frame["start_datetime"]
        end_datetimes = datetime_data_frame["end_datetime"]
        datetimes = datetimes.fillna(
            start_datetimes + (end_datetimes - start_datetimes) / 2
        )
    missing_datetimes = int(datetimes.isna().sum().item())
    if missing_datetimes > 0:
        raise ValueError(f"Found {missing_datetimes} missing datetime values")
    bboxes = table.select(["bbox"]).to_pandas()["bbox"]
    bboxes = DataFrame(bboxes.tolist(), index=bboxes.index)
    longitudes = (bboxes["xmin"] + bboxes["xmax"]) / 2
    latitudes = (bboxes["ymin"] + bboxes["ymax"]) / 2
    hashes = hasher.hash_all_clamped(
        datetimes["datetime"].tolist(), longitudes.tolist(), latitudes.tolist()
    )
    # FIXME handle bbox
    return (
        table.append_column(
            pyarrow.field("hash:hash", pyarrow.uint64()),
            pyarrow.array(hashes, pyarrow.uint64()),
        )
        .append_column(
            pyarrow.field("hash:start_datetime", TIMESTAMP),
            pyarrow.array(pyarrow.array([start_datetime] * table.num_rows), TIMESTAMP),
        )
        .append_column(
            pyarrow.field("hash:end_datetime", TIMESTAMP),
            pyarrow.array(pyarrow.array([end_datetime] * table.num_rows), TIMESTAMP),
        ),
        hashes,
    )


def staging_buckets(
    infiles: list[Path],
    boundaries: numpy.ndarray,
    progress: Progress | None,
    task: TaskID | None,
) -> Iterator[RecordBatch]:
    for infile in infiles:
        for batch in ParquetFile(infile).iter_batches():
            table = Table.from_batches([batch])
            hashes = table.column("hash:hash").to_numpy()
            buckets = numpy.searchsorted(boundaries, hashes, side="right").astype(
                "int32"
            )
            table = table.append_column(
                BUCKET,
                pyarrow.array(buckets, pyarrow.int32()),
            )
            yield from table.to_batches()
            if progress and task is not None:
                progress.advance(task, advance=batch.num_rows)
