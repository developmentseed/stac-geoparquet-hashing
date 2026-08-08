from collections.abc import Callable
from pathlib import Path

import numpy
import pyarrow
import pytest
from pyarrow.parquet import ParquetWriter

METADATA = {
    b"geo": b'{"version": "1.1.0"}',
    b"stac-geoparquet": b'{"version": "1.0.0"}',
}


def hashed_schema() -> pyarrow.Schema:
    return pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.string()),
            pyarrow.field("datetime", pyarrow.timestamp("us", tz="UTC")),
            pyarrow.field("hash:hash", pyarrow.int64()),
        ],
        metadata=METADATA,
    )


def _write_hashed(directory: Path, name: str, hashes: numpy.ndarray) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    table = pyarrow.table(
        {
            "id": [f"{name}-{i}" for i in range(len(hashes))],
            "datetime": pyarrow.array(
                [0] * len(hashes), pyarrow.timestamp("us", tz="UTC")
            ),
            "hash:hash": pyarrow.array(hashes, pyarrow.int64()),
        },
        schema=hashed_schema(),
    )
    with ParquetWriter(path, hashed_schema()) as writer:
        writer.write_table(table, row_group_size=500)
    return path


@pytest.fixture
def write_hashed() -> Callable[[Path, str, numpy.ndarray], Path]:
    """Returns a callable that writes a synthetic hashed stac-geoparquet file."""
    return _write_hashed


@pytest.fixture
def hashed_dir(tmp_path: Path) -> Path:
    generator = numpy.random.default_rng(0)
    directory = tmp_path / "hashed"
    for index in range(3):
        _write_hashed(
            directory, f"part-{index}.parquet", generator.integers(0, 1000, size=5000)
        )
    return directory
