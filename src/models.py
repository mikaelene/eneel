import datetime
from typing import List, Optional, Type, Any
from pydantic import BaseModel

from pyarrow import Schema


# Extrator classes
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


# Runner classes
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


# job config classes
class FileSystem(BaseModel):
    type: Optional[str]
    account_name: Optional[str]
    account_key: Optional[str]
    container_name: Optional[str]
    path: Optional[str]
    snowflake_storage_integration: Optional[str]


class Table(BaseModel):
    table_name: str


class Schema(BaseModel):
    source_schema: str
    target_schema:str
    tables: List[Table]


class Job(BaseModel):
    job_name: Optional[str] = None
    parallel_loads: Optional[int]
    source_uri: str
    target_uri: Optional[str]
    file_system: FileSystem
    schemas: List[Schema]
