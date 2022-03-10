import logging
import datetime
import time
from pydantic import BaseModel
from typing import Type, List, Optional

import multiprocessing
import pyarrow as pa
from pyarrow import Schema
from pyarrow.filesystem import FileSystem
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

from src.schema import enlarge_pa_schema

#%(process)s
logging.basicConfig(format="%(levelname)s - %(asctime)s - %(message)s",)
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


class Partition(BaseModel):
    id: int
    file_path: Optional[str]
    records: Optional[int]
    size_in_mb: Optional[float]
    partition_start: Optional[datetime.datetime]
    partition_end: Optional[datetime.datetime]
    partition_duration: Optional[datetime.timedelta]
    extract_duration: Optional[datetime.timedelta]
    transform_duration: Optional[datetime.timedelta]
    save_duration: Optional[datetime.timedelta]


class ExtractResult(BaseModel):
    db_engine_type: str
    query: str
    output_file_system_type: str
    output_file_path: str
    rows_per_partition: int
    job_start: datetime.datetime
    job_end: Optional[datetime.datetime]
    job_duration: Optional[datetime.timedelta]
    total_extract_duration: Optional[datetime.timedelta]
    total_transform_duration: Optional[datetime.timedelta]
    total_save_duration: Optional[datetime.timedelta]
    arrow_schema: Optional[Type[Schema]]
    number_of_partitions: Optional[int]
    partitions: Optional[List[Partition]]


def extract_sql_to_parquet(
        sqlalchemy_engine: create_engine,
        query: str,
        file_path: str,
        filesystem: Type[FileSystem] = None,
        rows_per_partition: int = 1000000,
        pa_schema: Type[Schema] = None
) -> Type[ExtractResult]:
    """
    :param sqlalchemy_engine: A valid SQLAlchemy Engine
    :param query: The SELECT query to run for the extract
    :param file_path: A filepath as abstract path, /-separated, even on Windows,
    and shouldn’t include special path components such as . and ...
    Symbolic links, if supported by the underlying storage, are automatically dereferenced.
    :param filesystem: [Optional] A fsspec-compliant filesystem. If not provided, local file system is used.
    :param pa_schema: [Optional] A arrow schema used for the resulting parquet file.
    :param rows_per_partition: [Optional]
    :return: ExtactResult
    """

    process_name = multiprocessing.current_process().name
    # if process_name == 'MainProcess':
    #     process_name = ''
    # else:
    #     process_name = f'Process_{process_name.split("-")[1]}:'

    job_start = datetime.datetime.now()

    result = ExtractResult(
        db_engine_type= sqlalchemy_engine.dialect.name,
        query= query,
        output_file_system_type= type(filesystem).__name__,
        output_file_path= file_path,
        rows_per_partition= rows_per_partition,
        job_start= job_start
    )

    logger.info(f'{process_name} - Extraction of "{result.query}" to {result.output_file_system_type} in path {result.output_file_path} starting')

    if not filesystem:
        filesystem = pa.fs.LocalFileSystem()

    if isinstance(filesystem, type(pa.fs.LocalFileSystem())):
        # create folder if not exist
        filesystem.create_dir(file_path, recursive=True)

    conn = sqlalchemy_engine.raw_connection()
    cursor = conn.cursor()

    cursor.arraysize = 10000
    #cursor.prefetchrows = 10000

    execute_query = cursor.execute(query)

    column_names = [col[0] for col in cursor.description]

    partitions = []

    start_time = time.perf_counter()

    i = 0
    while True:
        partition = cursor.fetchmany(rows_per_partition)
        if not partition:
            break
        i += 1
        partition_id = i
        partition_info = Partition(id=partition_id)
        partition_file_path = f'{file_path}/data_{str(partition_id)}.parquet'
        partition_info.file_path = partition_file_path
        partition_info.extract_duration = time.perf_counter() - start_time
        #logger.info(f"Partition {str(partition_id)} extracted in {time.perf_counter() - start_time} sec")
        start_time = time.perf_counter()

        if not pa_schema:
            pa_table = pa.table(list(zip(*partition)), names=column_names)
            pa_schema = enlarge_pa_schema(pa_schema=pa_table.schema)

        pa_table = pa.table(list(zip(*partition)), schema=pa_schema)

        partition_info.transform_duration = time.perf_counter() - start_time
        partition_info.records = pa_table.num_rows
        partition_info.size_in_mb = pa_table.nbytes / 1000000
        #logger.info(f"Partition {str(partition_id)} transformed to Arrow table in {time.perf_counter() - start_time} sec")
        start_time = time.perf_counter()

        pq.write_table(pa_table, partition_file_path, filesystem=filesystem)

        partition_info.save_duration = time.perf_counter() - start_time

        partitions.append(partition_info)

        #logger.info(f"Partition {str(partition_id)} saved as parquet file in {time.perf_counter() - start_time} sec")
        start_time = time.perf_counter()

    job_end = datetime.datetime.now()
    job_duration = job_end - job_start
    result.job_end = job_end
    result.job_duration = job_duration

    result.total_extract_duration = datetime.timedelta(seconds=sum([i.extract_duration for i in partitions]))
    result.total_transform_duration = datetime.timedelta(seconds=sum([i.transform_duration for i in partitions]))
    result.total_save_duration = datetime.timedelta(seconds=sum([i.save_duration for i in partitions]))

    result.number_of_partitions = len(partitions)
    result.arrow_schema = pa_schema
    result.partitions = partitions

    logger.info(f'{process_name} - Extraction of "{result.query}" finished in {result.job_duration}')

    conn.close()

    return result
