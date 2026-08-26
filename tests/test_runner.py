import datetime as dt
from pathlib import Path

import pyarrow
from pyarrow.parquet import ParquetWriter, read_table

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


def test_runner_hashes_stages_and_sorts(tmp_path: Path) -> None:
    num_rows = 200
    table = pyarrow.table(
        {
            "id": [f"item-{i}" for i in range(num_rows)],
            "datetime": pyarrow.array(
                [1_700_000_000 + i for i in range(num_rows)],
                pyarrow.timestamp("us", tz="UTC"),
            ),
            "bbox": [
                {"xmin": -10.0, "ymin": -10.0, "xmax": 10.0, "ymax": 10.0}
                for _ in range(num_rows)
            ],
        },
        schema=INPUT_SCHEMA,
    )
    infile = tmp_path / "in.parquet"
    with ParquetWriter(infile, INPUT_SCHEMA) as writer:
        writer.write_table(table)

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
