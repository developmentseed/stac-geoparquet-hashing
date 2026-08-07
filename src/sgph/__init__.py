import typer

app = typer.Typer()


@app.command()
def main(src: str, dst: str) -> None:
    """Hash and sort `src` into stac-geoparquet at `dst`."""
    typer.echo(f"Would hash and sort {src} into {dst}")
