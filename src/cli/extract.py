from typing import Type

import typer

from sqlalchemy import create_engine
from src.extractor import extract_sql_to_parquet
from src.models import ExtractResult

app = typer.Typer()


@app.command()
def extract(
        sqlalchemy_url: str,
        query: str,
        file_path: str
) -> Type[ExtractResult]:
    """
    Export SQL query to parquet.

    Make sure the engine in your SQLALCHEMY_URL is installed on your system.
    """

    engine = create_engine(sqlalchemy_url)

    result = extract_sql_to_parquet(
        sqlalchemy_engine=engine,
        query=query,
        file_path=file_path
    )

    if result:
        typer.echo(f'Extraction of "{query}" to {file_path} completed in {result.job_duration}')


if __name__ == "__main__":
    app()