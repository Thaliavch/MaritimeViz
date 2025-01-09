"""Console script for maritimeviz."""
import maritimeviz

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def main():
    """Console script for maritimeviz."""
    console.print("Replace this message by putting your code into "
               "maritimeviz.cli.main")
    console.print("See Typer documentation at https://typer.tiangolo.com/")
    


if __name__ == "__main__":
    app()
