import datetime as dt
import json
from pathlib import Path

import pyarrow
import pytest
from pyarrow import Schema
from pyarrow.parquet import ParquetFile, ParquetWriter, read_table

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


def write_input_file(
    path: Path,
    num_rows: int,
    id_prefix: str,
    schema: Schema,
    bbox: tuple[float, float, float, float] = (-10.0, -10.0, 10.0, 10.0),
) -> None:
    xmin, ymin, xmax, ymax = bbox
    table = pyarrow.table(
        {
            "id": [f"{id_prefix}-{i}" for i in range(num_rows)],
            "datetime": pyarrow.array(
                [1_700_000_000 + i for i in range(num_rows)],
                pyarrow.timestamp("us", tz="UTC"),
            ),
            "bbox": [
                {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}
                for _ in range(num_rows)
            ],
        },
        schema=schema,
    )
    with ParquetWriter(path, schema) as writer:
        writer.write_table(table)


def geo_metadata(bbox: tuple[float, float, float, float]) -> bytes:
    return (
        b'{"version":"1.1.0","primary_column":"geometry","columns":'
        b'{"geometry":{"encoding":"WKB","geometry_types":["MultiPolygon"],'
        b'"bbox":' + str(list(bbox)).encode() + b"}}}"
    )


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


def test_runner_preserves_geo_metadata_written_outside_arrow_schema(
    tmp_path: Path,
) -> None:
    schema = INPUT_SCHEMA.with_metadata({b"stac:geoparquet_version": b"1.0.0"})
    infile = tmp_path / "in.parquet"
    table = pyarrow.table(
        {
            "id": ["item-0"],
            "datetime": pyarrow.array(
                [1_700_000_000], pyarrow.timestamp("us", tz="UTC")
            ),
            "bbox": [{"xmin": -10.0, "ymin": -10.0, "xmax": 10.0, "ymax": 10.0}],
        },
        schema=schema,
    )
    with ParquetWriter(infile, schema) as writer:
        writer.write_table(table)
        writer.add_key_value_metadata({"geo": '{"fake":true}'})

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
    for outfile in outfiles:
        assert ParquetFile(outfile).metadata.metadata[b"geo"] == b'{"fake":true}'


def test_runner_recomputes_geo_bbox_per_output_file(tmp_path: Path) -> None:
    schema_a = INPUT_SCHEMA.with_metadata(
        {b"geo": geo_metadata((-999.0, -999.0, 999.0, 999.0))}
    )
    schema_b = INPUT_SCHEMA.with_metadata(
        {b"geo": geo_metadata((-1.0, -1.0, 1.0, 1.0))}
    )
    infile_a = tmp_path / "a.parquet"
    infile_b = tmp_path / "b.parquet"
    write_input_file(infile_a, 5, "a", schema_a, bbox=(-20.0, -20.0, -10.0, -10.0))
    write_input_file(infile_b, 5, "b", schema_b, bbox=(10.0, 10.0, 20.0, 20.0))

    outdir = tmp_path / "out"
    outdir.mkdir()
    Runner(bucket_size=1_000_000, progress=False).run(
        infiles=[infile_a, infile_b],
        start_datetime=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
        end_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
        outdir=outdir,
    )

    outfiles = sorted(outdir.glob("*.parquet"))
    assert len(outfiles) == 1
    metadata = ParquetFile(outfiles[0]).metadata.metadata
    geo = json.loads(metadata[b"geo"])
    assert geo["columns"]["geometry"]["bbox"] == [-20.0, -20.0, 20.0, 20.0]


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


def test_runner_match_file_count_produces_one_file_per_input(tmp_path: Path) -> None:
    infile_a = tmp_path / "a.parquet"
    infile_b = tmp_path / "b.parquet"
    write_input_file(
        infile_a, 100, "a", INPUT_SCHEMA, bbox=(-170.0, -80.0, -160.0, -70.0)
    )
    write_input_file(infile_b, 100, "b", INPUT_SCHEMA, bbox=(160.0, 70.0, 170.0, 80.0))

    outdir = tmp_path / "out"
    outdir.mkdir()
    Runner(bucket_size=1, progress=False, match_file_count=True).run(
        infiles=[infile_a, infile_b],
        start_datetime=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
        end_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
        outdir=outdir,
    )

    outfiles = sorted(outdir.glob("*.parquet"))
    assert len(outfiles) == 2
