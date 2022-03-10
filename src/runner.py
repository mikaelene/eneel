import concurrent.futures
from typing import Type, Any, List, Optional
from pydantic import BaseModel, validator
import multiprocessing

from sqlalchemy import create_engine
from pyarrow import Schema

from src.extractor import extract_sql_to_parquet, ExtractResult
from src.snowflake_utils import sf_load_from_storage_integration

import logging

logging.basicConfig(format="%(levelname)s - %(asctime)s - %(message)s", )
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

process_name = multiprocessing.current_process().name


class ExtractTask(BaseModel):
    task_name: Optional[str] = None
    sqlalchemy_url: str
    query: str
    file_path: str
    filesystem: Any = None
    rows_per_partition: int = 1000000
    pa_schema: Type[Schema] = None
    status: str = 'not started'
    extract_result: ExtractResult = None


class LoadSnowflakeTask(BaseModel):
    task_name: Optional[str] = None
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
    status: str = 'not started'


class ExtractLoadSnowflakeTask(BaseModel):
    task_name: Optional[str] = None
    extract_task: ExtractTask
    load_task: LoadSnowflakeTask
    status: str = 'not started'


def run_extract_task(task: ExtractTask):
    logger.info(f'{process_name} - Extraction of "{task.task_name}" starting')
    engine = create_engine(task.sqlalchemy_url)
    result = extract_sql_to_parquet(
        sqlalchemy_engine=engine,
        query=task.query,
        file_path=task.file_path,
        filesystem=task.filesystem,
        rows_per_partition=task.rows_per_partition,
        pa_schema=task.pa_schema
    )

    if result:
        task.extract_result = result
        task.status = 'completed'
        logger.info(f'{process_name} - Extraction of "{task.task_name}" to {task.extract_result.output_file_system_type} in path {task.extract_result.output_file_path} {task.status} in {task.extract_result.job_duration}')

    return task


def run_load_snowflake_task(task: LoadSnowflakeTask):
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

    task.status = 'completed'

    return task


def run_extract_load_snowflake_task(task: ExtractLoadSnowflakeTask):
    # Extract data to filesystem or blob
    task.extract_task = run_extract_task(task.extract_task)

    # Set the load arrow schema as the extract arrow schema
    task.load_task.arrow_schema = task.extract_task.extract_result.arrow_schema

    # Load data to snowflake
    task.load_task = run_load_snowflake_task(task.load_task)

    if task.extract_task.status == 'completed' and task.load_task.status == 'completed':
        task.status = 'completed'

    return task


def runner_extract_load_snowflake_task(tasks: List[ExtractLoadSnowflakeTask], max_workers: int = None):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for extract_load_snowflake_task in executor.map(run_extract_load_snowflake_task, tasks):
            logger.info(
                f'{process_name} - {extract_load_snowflake_task.task_name} {extract_load_snowflake_task.status}')


def runner_extract_task(tasks: List[ExtractTask], max_workers: int = None):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(run_extract_task, tasks):
            print(result.job_start)
