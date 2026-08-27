import datetime as dt
from pathlib import Path

import pyarrow
import pytest
from pyarrow.parquet import ParquetWriter, read_table
from pyarrowSchema import Schema

from cosgp.runner import Runner

BBOX_TYPE = pyarrow.struct(
    [
        pyarrow.field("xmin", pyarrow.float64()),
        pyarrow.field("ymin", pyarrow.float64()),
        pyarrow.field("xmax", pyarrow.float64()),
        pyarrow.field("ymax", pyarrow.float64()),
    ]
)

INPUT_SCHEMA = pyarrow.schema(
    [
        pyarrow.field("id", pyarrow.string()),
        pyarrow.field("datetime", pyarrow.timestamp("us", tz="UTC")),
        pyarrow.field("bbox", BBOX_TYPE),
    ]
)


def write_input_file(path: Path, num_rows: int, id_prefix: str, schema: Schema) -> None:
    table = pyarrow.table(
        {
            "id": [f"{id_prefix}-{i}" for i in range(num_rows)],
            "datetime": pyarrow.array(
                [1_700_000_000 + i for i in range(num_rows)],
                pyarrow.timestamp("us", tz="UTC"),
            ),
            "bbox": [
                {"xmin": -10.0, "ymin": -10.0, "xmax": 10.0, "ymax": 10.0}
                for _ in range(num_rows)
            ],
        },
        schema=schema,
    )
    with ParquetWriter(path, schema) as writer:
        writer.write_table(table)


def test_runner_hashes_stages_and_sorts(tmp_path: Path) -> None:
    num_rows = 200
    infile = tmp_path / "in.parquet"
    write_input_file(infile, num_rows, "item", INPUT_SCHEMA)

    outdir = tmp_path / "out"
    outdir.mkdir()
    Runner(bucket_size=1_000, progress=False).run(
        infiles=[infile],
        start_datetime=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
        end_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
        outdir=outdir,
    )

    outfiles = sorted(outdir.glob("*.parquet"))
    assert outfiles

    hashes = []
    for outfile in outfiles:
        hashes.extend(read_table(outfile).column("hash:hash").to_pylist())

    assert len(hashes) == num_rows
    assert hashes == sorted(hashes)


def test_runner_preserves_matching_file_level_metadata(tmp_path: Path) -> None:
    schema = INPUT_SCHEMA.with_metadata({b"geo": b'{"tag":"shared"}'})
    infile_a = tmp_path / "a.parquet"
    infile_b = tmp_path / "b.parquet"
    write_input_file(infile_a, 10, "a", schema)
    write_input_file(infile_b, 10, "b", schema)

    outdir = tmp_path / "out"
    outdir.mkdir()
    Runner(bucket_size=1_000, progress=False).run(
        infiles=[infile_a, infile_b],
        start_datetime=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
        end_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
        outdir=outdir,
    )

    outfiles = sorted(outdir.glob("*.parquet"))
    assert outfiles
    for outfile in outfiles:
        assert read_table(outfile).schema.metadata == schema.metadata


def test_runner_raises_on_mismatched_file_level_metadata(tmp_path: Path) -> None:
    schema_a = INPUT_SCHEMA.with_metadata({b"geo": b'{"tag":"a"}'})
    schema_b = INPUT_SCHEMA.with_metadata({b"geo": b'{"tag":"b"}'})
    infile_a = tmp_path / "a.parquet"
    infile_b = tmp_path / "b.parquet"
    write_input_file(infile_a, 10, "a", schema_a)
    write_input_file(infile_b, 10, "b", schema_b)

    outdir = tmp_path / "out"
    outdir.mkdir()
    with pytest.raises(ValueError):
        Runner(bucket_size=1_000, progress=False).run(
            infiles=[infile_a, infile_b],
            start_datetime=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
            end_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
            outdir=outdir,
        )
