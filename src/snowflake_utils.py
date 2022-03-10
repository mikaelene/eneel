from typing import Type
from pyarrow import Schema
from sqlalchemy import create_engine, text

from src.schema import pa_schema_to_extended_arrow_schema, ExtendedArrowSchema

import logging

logging.basicConfig(format="%(levelname)s - %(asctime)s - %(filename)s - %(message)s", )
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


def sf_create_table(sql_db: str, sql_schema: str, sql_table: str, arrow_schema: Type[ExtendedArrowSchema]) -> str:
    sql = f'''create or replace table {sql_db}.{sql_schema}.{sql_table} (
        '''

    for i, col in enumerate(arrow_schema.columns):
        if col.col_group == 'decimal':
            data_type = f'decimal({col.precision},{col.scale})'
        else:
            data_type = col.col_group

        sql += f'''{col.col_name} {data_type}'''

        if i + 1 < len(arrow_schema.columns):
            sql += f''', 
        '''

    sql += '''
    )'''

    return sql


def sf_create_stage(storage_integration: str, account_name: str, folder: str, stage_name: str,
                    provider: str = 'Azure'):
    if provider == 'Azure':
        base_url = f'azure://{account_name}.blob.core.windows.net/'
    else:
        logger.error('provider must be Azure')

    sql = f"""create or replace stage {stage_name}
    storage_integration = {storage_integration}
    url = '{base_url}{folder}'
    file_format = (TYPE = PARQUET)"""

    return sql


def sf_create_select(arrow_schema: Type[ExtendedArrowSchema], stage_name: str):
    sql = f"""select
    """
    for i, col in enumerate(arrow_schema.columns):
        sql += f'''$1:{col.col_name}'''
        if i + 1 < len(arrow_schema.columns):
            sql += f''', 
    '''
    sql += f'''
from
    @{stage_name}/ (PATTERN => '.*parquet')'''

    return sql


def sf_create_copy(sql_db: str, sql_schema: str, sql_table: str, sql_select: str):
    sql = f'''copy into {sql_db}.{sql_schema}.{sql_table}
    from (
{sql_select}
    )
  file_format = (TYPE = 'PARQUET')
        '''

    return sql


def sf_load_from_storage_integration(
        sqlalchemy_engine: create_engine,
        sql_db: str,
        sql_schema: str,
        sql_table: str,
        arrow_schema: Type[Schema],
        storage_integration: str,
        account_name: str,
        container: str,
        file_path: str,
        stage_name: str = None,
        provider: str = 'Azure',
):

    logger.info(f'Start loading {sql_db}.{sql_schema}.{sql_table} from {storage_integration} {container}/{file_path}')

    if not stage_name:
        stage_name = f'{sql_db}.{sql_schema}.{sql_table}_stage'

    extended_arrow_schema = pa_schema_to_extended_arrow_schema(arrow_schema)

    sql_create_table = sf_create_table(
        sql_db=sql_db,
        sql_schema=sql_schema,
        sql_table=sql_table,
        arrow_schema=extended_arrow_schema
    )

    sql_create_stage = sf_create_stage(
        storage_integration=storage_integration,
        account_name=account_name,
        folder=file_path,
        stage_name=stage_name,
        provider=provider
    )

    sql_select = sf_create_select(
        arrow_schema=extended_arrow_schema,
        stage_name=stage_name
    )

    sql_copy = sf_create_copy(
        sql_db=sql_db,
        sql_schema=sql_schema,
        sql_table=sql_table,
        sql_select=sql_select
    )

    # logger.debug(sql_create_table)
    # logger.debug(sql_create_stage)
    # logger.debug(sql_select)
    # logger.debug(sql_copy)

    sf_conn = sqlalchemy_engine.connect()

    sf_conn.execute(text(sql_create_table))

    sf_conn.execute(text(sql_create_stage))

    sf_conn.execute(text(sql_copy))

    sf_conn.close()

    logger.info(f'Finished loading {sql_db}.{sql_schema}.{sql_table}')
