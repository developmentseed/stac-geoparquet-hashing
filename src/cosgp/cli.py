from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Annotated

from typer import Argument, Option, Typer

from .runner import BBox, Runner

app = Typer()


DEFAULT_BUCKET_SIZE = 2_000_000_000  # 2 GB of uncompressed data per output file


class Datetime:
    @classmethod
    def parse(cls, value: str) -> Datetime:
        parts = value.split("/")
        if len(parts) == 2:
            raise NotImplementedError
        elif len(parts) == 1:
            sub_parts = parts[0].split("-")
            if len(sub_parts) == 1:
                return Datetime.year(int(sub_parts[0]))
            elif len(sub_parts) == 2:
                return Datetime.month(int(sub_parts[0]), int(sub_parts[1]))
            elif len(sub_parts) == 3:
                return Datetime.day(
                    int(sub_parts[0]), int(sub_parts[1]), int(sub_parts[2])
                )
            else:
                raise ValueError(f"invalid datetime: {value}")
        else:
            raise ValueError(f"invalid datetime: {value}")

    @classmethod
    def year(cls, year: int) -> Datetime:
        start = dt.date(year, 1, 1)
        end = dt.date(year + 1, 1, 1)
        return Datetime(
            dt.datetime.combine(start, dt.time.min, tzinfo=dt.UTC),
            dt.datetime.combine(end, dt.time.min, tzinfo=dt.UTC),
        )

    @classmethod
    def month(cls, year: int, month: int) -> Datetime:
        start = dt.date(year, month, 1)
        end = dt.date(year, month + 1, 1)
        return Datetime(
            dt.datetime.combine(start, dt.time.min, tzinfo=dt.UTC),
            dt.datetime.combine(end, dt.time.min, tzinfo=dt.UTC),
        )

    @classmethod
    def day(cls, year: int, month: int, day: int) -> Datetime:
        start = dt.date(year, month, day)
        end = dt.date(year, month, day + 1)
        return Datetime(
            dt.datetime.combine(start, dt.time.min, tzinfo=dt.UTC),
            dt.datetime.combine(end, dt.time.min, tzinfo=dt.UTC),
        )

    def __init__(self, start: dt.datetime, end: dt.datetime):
        self.start = start
        self.end = end


@app.command()
def create(
    infile: Annotated[
        Path,
        Argument(
            help="The input stac-geoparquet file, or directory holding stac-geoparquet files (these files must have a .parquet suffix)"
        ),
    ],
    datetime: Annotated[
        Datetime,
        Argument(
            parser=Datetime.parse,
            help="The datetime range to use for the hash bounds. Can be specified as a start/end interval or via a simple year, a year-month, or a year-month-day",
        ),
    ],
    outdir: Annotated[
        Path,
        Argument(
            help="The output directory for the cloud-optimized stac-geoparquet files"
        ),
    ],
    bbox: Annotated[
        BBox | None,
        Option(
            help="The bounding box to use for the hash bounds. If not provided, defaults to the global bbox"
        ),
    ] = None,
    prefix_id: Annotated[
        bool,
        Option(
            help="Prefix the ids of each item with their hash value, greatly speeding up search-by-id queries. Might not be necessary if the item ids are already prefixed with datetimes in YMD format."
        ),
    ] = False,
    temporary_directory: Annotated[
        Path | None,
        Option(
            help="The temporary directory to use for intermediate files. If None, the system default will be used"
        ),
    ] = None,
    bucket_size: Annotated[
        int,
        Option(
            help="Target uncompressed size, in bytes, for each sorted output file. The number of hash buckets is estimated from the total input data volume so that each bucket is roughly this size."
        ),
    ] = DEFAULT_BUCKET_SIZE,
    progress: Annotated[
        bool,
        Option(
            help="Show a progress bar. If disabled, progress is logged at INFO level instead."
        ),
    ] = True,
) -> None:
    """Creates one or more cloud-optimized stac-geoparquet files.

    This is a three-step process:

        1. Copy each item to new stac-geoparquet files, adding a stac-hash and (optionally) prefixing the id with that hash
        2. Stage each item into "bucketed" datasets, partitioned by hash value
        3. Internally sort each partitioned dataset, then write the output files
    """
    if not progress:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
    infiles = [infile] if infile.is_file() else list(infile.glob("*.parquet"))
    if not infiles:
        raise ValueError(f"no parquet files in {infile}")
    Runner(
        temporary_directory=temporary_directory,
        bucket_size=bucket_size,
        progress=progress,
    ).run(
        infiles=infiles,
        start_datetime=datetime.start,
        end_datetime=datetime.end,
        bbox=bbox,
        outdir=outdir,
    )
