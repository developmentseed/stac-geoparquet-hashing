from collections.abc import Callable
from pathlib import Path

import numpy
import pytest
from pyarrow.parquet import ParquetFile

from sgph import sort as sgph_sort

WriteHashed = Callable[[Path, str, numpy.ndarray], Path]


def test_hashed_dir_fixture_has_three_files(hashed_dir: Path) -> None:
    files = sorted(hashed_dir.glob("*.parquet"))
    assert len(files) == 3
    schema = ParquetFile(files[0]).schema_arrow
    assert schema.metadata is not None
    assert b"stac-geoparquet" in schema.metadata


def test_bucket_boundaries_are_strictly_increasing(hashed_dir: Path) -> None:
    infiles = sorted(hashed_dir.glob("*.parquet"))
    boundaries = sgph_sort.bucket_boundaries(infiles, 8)
    assert len(boundaries) <= 7
    assert numpy.all(boundaries[:-1] < boundaries[1:])


def test_bucket_boundaries_split_evenly(hashed_dir: Path) -> None:
    infiles = sorted(hashed_dir.glob("*.parquet"))
    boundaries = sgph_sort.bucket_boundaries(infiles, 8)
    keys = numpy.concatenate([sgph_sort.read_keys(f) for f in infiles])
    indices = numpy.searchsorted(boundaries, keys, side="right")
    counts = numpy.bincount(indices, minlength=len(boundaries) + 1)
    assert counts.max() < len(keys) / 6


def test_bucket_boundaries_collapse_for_identical_hashes(
    tmp_path: Path, write_hashed: WriteHashed
) -> None:
    path = write_hashed(tmp_path / "d", "a.parquet", numpy.full(1000, 5))
    boundaries = sgph_sort.bucket_boundaries([path], 8)
    assert len(boundaries) == 1


def test_bucket_boundaries_empty_for_single_bucket(hashed_dir: Path) -> None:
    infiles = sorted(hashed_dir.glob("*.parquet"))
    assert len(sgph_sort.bucket_boundaries(infiles, 1)) == 0


def test_bucket_boundaries_raises_for_non_positive_buckets(hashed_dir: Path) -> None:
    infiles = sorted(hashed_dir.glob("*.parquet"))
    with pytest.raises(ValueError):
        sgph_sort.bucket_boundaries(infiles, 0)


def test_read_keys_raises_without_hash_column(tmp_path: Path) -> None:
    import pyarrow
    from pyarrow.parquet import ParquetWriter

    path = tmp_path / "no-hash.parquet"
    table = pyarrow.table({"id": ["a"]})
    with ParquetWriter(path, table.schema) as writer:
        writer.write_table(table)
    with pytest.raises(ValueError):
        sgph_sort.read_keys(path)


def test_input_schema_returns_metadata(hashed_dir: Path) -> None:
    schema = sgph_sort.input_schema(sorted(hashed_dir.glob("*.parquet")))
    assert schema.metadata is not None
    assert b"stac-geoparquet" in schema.metadata


def test_input_schema_raises_on_mismatch(hashed_dir: Path, tmp_path: Path) -> None:
    import pyarrow
    from pyarrow.parquet import ParquetWriter

    odd = hashed_dir / "part-9.parquet"
    table = pyarrow.table(
        {
            "id": ["a"],
            "datetime": pyarrow.array([0], pyarrow.timestamp("us", tz="UTC")),
            "hash:hash": pyarrow.array([1], pyarrow.int64()),
            "extra": [1.0],
        }
    )
    with ParquetWriter(odd, table.schema) as writer:
        writer.write_table(table)
    with pytest.raises(ValueError):
        sgph_sort.input_schema(sorted(hashed_dir.glob("*.parquet")))


def test_with_bucket_keeps_equal_hashes_together(hashed_dir: Path) -> None:
    from pyarrow.parquet import read_table

    infiles = sorted(hashed_dir.glob("*.parquet"))
    boundaries = sgph_sort.bucket_boundaries(infiles, 8)
    table = sgph_sort.with_bucket(read_table(infiles[0]), boundaries)
    hashes = table.column(sgph_sort.HASH_COLUMN).to_numpy()
    buckets = table.column("bucket").to_numpy()
    for value in numpy.unique(hashes):
        assert len(numpy.unique(buckets[hashes == value])) == 1


def test_stage_buckets_preserves_row_count(hashed_dir: Path, tmp_path: Path) -> None:
    from pyarrow.parquet import read_table

    from sgph.progress import progress_bar

    infiles = sorted(hashed_dir.glob("*.parquet"))
    boundaries = sgph_sort.bucket_boundaries(infiles, 8)
    staging = tmp_path / "staging"
    with progress_bar() as progress:
        sgph_sort.stage_buckets(infiles, boundaries, staging, progress)
    dirs = sgph_sort.staged_bucket_dirs(staging)
    staged = sum(read_table(d).num_rows for d in dirs)
    assert staged == 15000


def test_staged_bucket_dirs_are_numerically_ordered(
    hashed_dir: Path, tmp_path: Path
) -> None:
    from sgph.progress import progress_bar

    infiles = sorted(hashed_dir.glob("*.parquet"))
    boundaries = sgph_sort.bucket_boundaries(infiles, 16)
    staging = tmp_path / "staging"
    with progress_bar() as progress:
        sgph_sort.stage_buckets(infiles, boundaries, staging, progress)
    indices = [int(d.name.split("=")[1]) for d in sgph_sort.staged_bucket_dirs(staging)]
    assert indices == sorted(indices)


def test_stage_buckets_is_sparse_for_identical_hashes(
    tmp_path: Path, write_hashed: WriteHashed
) -> None:
    from sgph.progress import progress_bar

    path = write_hashed(tmp_path / "d", "a.parquet", numpy.full(1000, 5))
    boundaries = sgph_sort.bucket_boundaries([path], 8)
    staging = tmp_path / "staging"
    with progress_bar() as progress:
        sgph_sort.stage_buckets([path], boundaries, staging, progress)
    indices = [int(d.name.split("=")[1]) for d in sgph_sort.staged_bucket_dirs(staging)]
    assert indices == [1]


def test_outfile_name_pads_to_three_digits() -> None:
    assert sgph_sort.outfile_name(0, 128) == "part-000.parquet"
    assert sgph_sort.outfile_name(127, 128) == "part-127.parquet"


def test_outfile_name_widens_beyond_a_thousand() -> None:
    assert sgph_sort.outfile_name(0, 2000) == "part-0000.parquet"
    assert sgph_sort.outfile_name(1999, 2000) == "part-1999.parquet"


def test_sort_bucket_sorts_and_keeps_metadata(hashed_dir: Path, tmp_path: Path) -> None:
    from pyarrow.parquet import read_table

    from sgph.progress import progress_bar

    infiles = sorted(hashed_dir.glob("*.parquet"))
    schema = ParquetFile(infiles[0]).schema_arrow
    boundaries = sgph_sort.bucket_boundaries(infiles, 8)
    staging = tmp_path / "staging"
    with progress_bar() as progress:
        sgph_sort.stage_buckets(infiles, boundaries, staging, progress)
    outfile = tmp_path / "part-000.parquet"
    sgph_sort.sort_bucket(sgph_sort.staged_bucket_dirs(staging)[0], outfile, schema)

    table = read_table(outfile)
    hashes = table.column(sgph_sort.HASH_COLUMN).to_numpy()
    assert numpy.all(hashes[:-1] <= hashes[1:])
    assert table.schema.metadata is not None
    assert b"stac-geoparquet" in table.schema.metadata
    assert b"geo" in table.schema.metadata
    assert "bucket" not in table.column_names
    assert table.schema.equals(schema, check_metadata=True)


def test_sort_bucket_enforces_column_order(hashed_dir: Path, tmp_path: Path) -> None:
    import pyarrow
    from pyarrow.parquet import ParquetWriter, read_table

    from sgph.progress import progress_bar

    infiles = sorted(hashed_dir.glob("*.parquet"))
    schema = ParquetFile(infiles[0]).schema_arrow
    boundaries = sgph_sort.bucket_boundaries(infiles, 8)
    staging = tmp_path / "staging"
    with progress_bar() as progress:
        sgph_sort.stage_buckets(infiles, boundaries, staging, progress)

    bucket_dirs = sgph_sort.staged_bucket_dirs(staging)
    assert bucket_dirs
    bucket_dir = bucket_dirs[0]
    staged_files = sorted(bucket_dir.glob("*.parquet"))
    assert staged_files
    staged_table = read_table(staged_files[0])
    reordered_cols = list(reversed(staged_table.column_names))
    reordered_table = staged_table.select(reordered_cols)
    reordered_path = staged_files[0]
    with ParquetWriter(reordered_path, reordered_table.schema) as writer:
        writer.write_table(reordered_table)

    outfile = tmp_path / "part-000.parquet"
    sgph_sort.sort_bucket(bucket_dir, outfile, schema)

    table = read_table(outfile)
    assert table.column_names == schema.names


def read_all_hashes(outdir: Path) -> numpy.ndarray:
    from pyarrow.parquet import read_table

    return numpy.concatenate(
        [
            read_table(f).column("hash:hash").to_numpy()
            for f in sorted(outdir.glob("*.parquet"))
        ]
    )


def test_sort_produces_globally_sorted_output(hashed_dir: Path, tmp_path: Path) -> None:
    import sgph

    outdir = tmp_path / "sorted"
    sgph.sort_command(hashed_dir, outdir, buckets=8)
    hashes = read_all_hashes(outdir)
    assert len(hashes) == 15000
    assert numpy.all(hashes[:-1] <= hashes[1:])


def test_sort_loses_no_ids(hashed_dir: Path, tmp_path: Path) -> None:
    import sgph
    from pyarrow.parquet import read_table

    outdir = tmp_path / "sorted"
    sgph.sort_command(hashed_dir, outdir, buckets=8)
    out_ids = set()
    for f in sorted(outdir.glob("*.parquet")):
        out_ids.update(read_table(f).column("id").to_pylist())
    in_ids = set()
    for f in sorted(hashed_dir.glob("*.parquet")):
        in_ids.update(read_table(f).column("id").to_pylist())
    assert out_ids == in_ids


def test_sort_names_outputs_contiguously_despite_sparse_buckets(
    tmp_path: Path, write_hashed: WriteHashed
) -> None:
    import sgph

    write_hashed(tmp_path / "d", "a.parquet", numpy.full(1000, 5))
    outdir = tmp_path / "sorted"
    sgph.sort_command(tmp_path / "d", outdir, buckets=8)
    assert [f.name for f in sorted(outdir.glob("*.parquet"))] == ["part-000.parquet"]


def test_sort_removes_staging_dir_on_success(hashed_dir: Path, tmp_path: Path) -> None:
    import sgph

    staging = tmp_path / "staging"
    staging.mkdir()
    sgph.sort_command(hashed_dir, tmp_path / "sorted", buckets=8, staging_dir=staging)
    assert list(staging.iterdir()) == []


def test_sort_removes_staging_dir_on_failure(
    hashed_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sgph

    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(sgph_sort, "sort_bucket", boom)
    staging = tmp_path / "staging"
    staging.mkdir()
    with pytest.raises(RuntimeError):
        sgph.sort_command(
            hashed_dir, tmp_path / "sorted", buckets=8, staging_dir=staging
        )
    assert list(staging.iterdir()) == []


def test_sort_raises_on_empty_indir(tmp_path: Path) -> None:
    import sgph

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError):
        sgph.sort_command(empty, tmp_path / "sorted")


def test_sort_raises_when_hash_column_missing(tmp_path: Path) -> None:
    import pyarrow
    import sgph
    from pyarrow.parquet import ParquetWriter

    indir = tmp_path / "d"
    indir.mkdir()
    table = pyarrow.table({"id": ["a"]})
    with ParquetWriter(indir / "a.parquet", table.schema) as writer:
        writer.write_table(table)
    with pytest.raises(ValueError):
        sgph.sort_command(indir, tmp_path / "sorted")


def test_sort_raises_on_stale_output_files(hashed_dir: Path, tmp_path: Path) -> None:
    import sgph

    outdir = tmp_path / "sorted"
    sgph.sort_command(hashed_dir, outdir, buckets=16)
    before = sorted(outdir.glob("part-*.parquet"))
    assert len(before) > 4
    with pytest.raises(ValueError):
        sgph.sort_command(hashed_dir, outdir, buckets=4)
    after = sorted(outdir.glob("part-*.parquet"))
    assert after == before


def test_sort_force_deletes_stale_output_files(hashed_dir: Path, tmp_path: Path) -> None:
    import sgph

    outdir = tmp_path / "sorted"
    sgph.sort_command(hashed_dir, outdir, buckets=16)
    sgph.sort_command(hashed_dir, outdir, buckets=4, force=True)
    hashes = read_all_hashes(outdir)
    assert len(sorted(outdir.glob("part-*.parquet"))) <= 4
    assert len(hashes) == 15000
    assert numpy.all(hashes[:-1] <= hashes[1:])


def test_sort_command_raises_for_non_positive_buckets(
    hashed_dir: Path, tmp_path: Path
) -> None:
    import sgph

    with pytest.raises(ValueError):
        sgph.sort_command(hashed_dir, tmp_path / "sorted", buckets=0)


def test_sort_handles_extreme_int64_hashes(
    tmp_path: Path, write_hashed: WriteHashed
) -> None:
    import sgph

    hashes = numpy.array(
        [
            -(2**63),
            -(2**63) + 1,
            -1,
            0,
            1,
            2**63 - 2,
            2**63 - 1,
        ]
        * 200,
        dtype=numpy.int64,
    )
    write_hashed(tmp_path / "d", "a.parquet", hashes)
    outdir = tmp_path / "sorted"
    sgph.sort_command(tmp_path / "d", outdir, buckets=8)
    out_hashes = read_all_hashes(outdir)
    assert len(out_hashes) == len(hashes)
    assert numpy.all(out_hashes[:-1] <= out_hashes[1:])
    assert numpy.array_equal(numpy.sort(out_hashes), numpy.sort(hashes))
