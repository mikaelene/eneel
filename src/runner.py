import concurrent.futures
from typing import Type, Any, List
from pydantic import BaseModel

from sqlalchemy import create_engine
from pyarrow import Schema

from src.extractor import extract_sql_to_parquet
from src.snowflake_utils import sf_load_from_storage_integration


class ExtractTask(BaseModel):
    sqlalchemy_url: str
    query: str
    file_path: str
    filesystem: Any = None
    rows_per_partition: int = 1000000
    pa_schema: Type[Schema] = None


class LoadSnowflakeTask(BaseModel):
    sqlalchemy_url: str
    sql_db: str
    sql_schema: str
    sql_table: str
    arrow_schema: Type[Schema] = None
    storage_integration: str
    account_name: str
    container: str = None
    file_path: str = None
    stage_name: str = None
    provider: str = 'Azure'


class ExtractLoadSnowflakeTask(BaseModel):
    extract_task: ExtractTask
    load_task: LoadSnowflakeTask


def run_extract_task(task: ExtractTask):
    #print(f'{multiprocessing.current_process().name[5:]} starting to extract {task.query}')
    engine = create_engine(task.sqlalchemy_url)
    res = extract_sql_to_parquet(
        sqlalchemy_engine=engine,
        query=task.query,
        file_path=task.file_path,
        filesystem=task.filesystem,
        rows_per_partition=task.rows_per_partition,
        pa_schema=task.pa_schema
    )

    return res


def run_load_snowflake_task(task: LoadSnowflakeTask):
    #print(f'{multiprocessing.current_process().name[5:]} starting to extract {task.query}')
    engine = create_engine(task.sqlalchemy_url)
    sf_load_from_storage_integration(
        sqlalchemy_engine=engine,
        sql_db=task.sql_db,
        sql_schema=task.sql_schema,
        sql_table=task.sql_table,
        arrow_schema=task.arrow_schema,
        storage_integration=task.storage_integration,
        account_name=task.account_name,
        container=task.container,
        file_path=task.file_path,
        stage_name=task.stage_name,
        provider=task.provider
    )


def run_extract_load_snowflake_task(task: ExtractLoadSnowflakeTask):
    extract_result = run_extract_task(task.extract_task)

    task.load_task.arrow_schema = extract_result.arrow_schema

    run_load_snowflake_task(task.load_task)


def runner_extract_load_snowflake_task(tasks: List[ExtractLoadSnowflakeTask], max_workers: int = None):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(run_extract_load_snowflake_task, tasks):
            print('1 Flow finished')


def runner_extract_task(tasks: List[ExtractTask], max_workers: int = None):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(run_extract_task, tasks):
            print(result.job_start)
