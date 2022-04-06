from adlfs import AzureBlobFileSystem

from src.models import ExtractTask, LoadSnowflakeTask, ExtractLoadSnowflakeTask


def job_to_extract_load_tasks(job: dict) -> ExtractLoadSnowflakeTask:
    el_jobs = []

    abfs = AzureBlobFileSystem(
        account_name=job.file_system.account_name,
        account_key=job.file_system.account_key,
        container_name=job.file_system.container_name)

    for schema in job.schemas:
        for table in schema.tables:
            file_path = f'{job.file_system.container_name}/{schema.source_schema}_{table.table_name}'
            query = f'select * from {schema.source_schema}.{table.table_name}'

            extract_task = ExtractTask(
                task_name=query,
                sqlalchemy_url=job.source_uri,
                query=query,
                file_path=file_path,
                filesystem=abfs,
                #rows_per_partition=10000
            )

            load_snowflake_task = LoadSnowflakeTask(
                task_name=f'{schema.target_schema}.{table.table_name}',
                sqlalchemy_url=job.target_uri,
                sql_schema=schema.target_schema,
                sql_table=table.table_name,
                storage_integration=job.file_system.snowflake_storage_integration,
                account_name=job.file_system.account_name,
                container=job.file_system.container_name,
                file_path=file_path,
            )

            extract_load_task = ExtractLoadSnowflakeTask(
                task_name=f'Job: {schema.source_schema}.{table.table_name} -> {schema.target_schema}.{table.table_name}',
                extract_task=extract_task,
                load_task=load_snowflake_task
            )

            el_jobs.append(extract_load_task)

    return el_jobs