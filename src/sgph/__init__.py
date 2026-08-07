from io import BytesIO
from pathlib import Path

import httpx2
import typer
from obstore.auth.planetary_computer import PlanetaryComputerCredentialProvider
from obstore.store import AzureStore
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

app = typer.Typer()


@app.command()
def main(year: int, outdir: Path) -> None:
    collection = (
        httpx2.get(
            "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a"
        )
        .raise_for_status()
        .json()
    )
    geoparquet_items = collection["assets"]["geoparquet-items"]
    url = geoparquet_items["href"]
    account_name = geoparquet_items["table:storage_options"]["account_name"]
    store = AzureStore.from_url(
        url,
        account_name=account_name,
        credential_provider=PlanetaryComputerCredentialProvider(
            url, account_name=account_name
        ),
    )
    paths = []
    typer.echo(f"Collecting paths for {year}...", nl=False)
    for list_result in store.list():
        for object_meta in list_result:
            if contains_year(object_meta["path"], year):
                paths.append(object_meta["path"])
    typer.echo(f"{len(paths)} found")

    outdir.mkdir(exist_ok=True, parents=True)
    for path in paths:
        get_result = store.get(path)
        with open(outdir / path, "wb") as f, Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(path, total=get_result.meta["size"])
            for chunk in get_result:
                f.write(chunk)
                progress.update(task, advance=len(chunk))


def contains_year(path: str, year: int) -> bool:
    parts = path.split("_")
    return parts[1].startswith(str(year)) or parts[2].startswith(str(year))
