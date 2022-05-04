import json
from typing import Type

import typer

from sqlalchemy import create_engine
from src.extractor import extract_sql_to_parquet
from src.models import ExtractResult
from src.utils import file_system_from_uri
from src.schema import schema_to_pa_schema

app = typer.Typer()


@app.command()
def extract(
        sqlalchemy_url: str,
        query: str,
        file_path: str,
        file_system_uri: str = None,
        rows_per_partition: int = None,
        schema: str = None
) -> Type[ExtractResult]:
    """
    Export SQL query to parquet.

    Make sure the engine in your SQLALCHEMY_URL is installed on your system.
    """

    engine = create_engine(sqlalchemy_url)

    if file_system_uri:
        fs = file_system_from_uri(file_system_uri)
    else:
        fs = None

    if schema:
        schema = json.loads(schema)
        pa_schema = schema_to_pa_schema(schema)

    if not rows_per_partition:
        rows_per_partition = 1000000

    result = extract_sql_to_parquet(
        sqlalchemy_engine=engine,
        query=query,
        file_path=file_path,
        filesystem=fs,
        rows_per_partition=rows_per_partition,
        pa_schema=pa_schema
    )

    if result:
        typer.echo(f'Extraction of "{query}" to {file_path} completed in {result.job_duration}')


if __name__ == "__main__":
    app()