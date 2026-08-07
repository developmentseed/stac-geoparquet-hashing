import datetime
from pathlib import Path

import pandas
import typer
from stac_hash import Hasher

app = typer.Typer()


@app.command()
def hash(year: int, infile: Path, outfile: Path) -> None:
    """Add a `hash:hash` column to the stac-geoparquet at `infile`.

    Hashes are calculated over a whole-world bounding box and the temporal
    extent of `year`, so hashes from different files are comparable.
    """
    data_frame = pandas.read_parquet(infile)
    hasher = Hasher(
        datetime.datetime(year, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(year + 1, 1, 1, tzinfo=datetime.UTC),
    )
    longitudes, latitudes = bbox_centers(data_frame)
    data_frame["hash:hash"] = hasher.hash_all(
        datetimes(data_frame),
        longitudes,
        latitudes,
    )
    data_frame.to_parquet(outfile)


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
