import json
from typing import Type

import typer

from sqlalchemy import create_engine
from src.snowflake_utils import sf_load_from_storage_integration
from src.schema import schema_to_pa_schema

app = typer.Typer()


@app.command()
def load(
        sqlalchemy_url: str,
        sql_schema: str,
        sql_table: str,
        storage_integration: str,
        account_name: str,
        container: str,
        file_path: str,
        schema: str = None,

):
    """
    Import parquet data to database.

    Make sure the engine in your SQLALCHEMY_URL is installed on your system.
    """

    engine = create_engine(sqlalchemy_url)

    if schema:
        schema = json.loads(schema)
        pa_schema = schema_to_pa_schema(schema)
    else:
        pa_schema = None

    result = sf_load_from_storage_integration(
        sqlalchemy_engine=engine,
        query=query,
        file_path=file_path,
        filesystem=fs,
        rows_per_partition=rows_per_partition,
        pa_schema=pa_schema
    )


    typer.echo(f'Extraction of "{query}" to {file_path} completed in {result.job_duration}')


if __name__ == "__main__":
    app()