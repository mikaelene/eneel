import eneel.utils as utils
from concurrent.futures import ProcessPoolExecutor as Executor
import os
import eneel.logger as logger
logger = logger.get_logger(__name__)


def run_project(connections_path, project_path):
    connections_config = utils.get_connections(connections_path)
    project_config = utils.get_project(project_path)

    source_conninfo = connections_config[project_config['source']]
    target_conninfo = connections_config[project_config['target']]

    project = project_config.copy()
    del project['schemas']

    temp_path = project.get('temp_path')
    utils.create_path(temp_path)

    source_conninfos = []
    target_conninfos = []
    projects = []
    schemas = []
    tables = []

    for schema_config in project_config['schemas']:
        schema = schema_config.copy()
        del schema['tables']
        for table in schema_config['tables']:
            source_conninfo_item = source_conninfo
            target_conninfo_item = target_conninfo
            project_item = project
            schema_item = schema
            table_item = table

            source_conninfos.append(source_conninfo_item)
            target_conninfos.append(target_conninfo_item)
            projects.append(project_item)
            schemas.append(schema_item)
            tables.append(table_item)

            #load = [source, target, project, schema, table]
            #loads.append(load)

            #run_load(source, target, project, schema, table)

    workers = project.get('parallel_loads', 1)
    num_tables_to_load = len(tables)
    if num_tables_to_load < workers:
        workers = num_tables_to_load
    logger.info("Start loading " + str(num_tables_to_load) + " tables with " + str(workers) + " parallel workers")

    with Executor(max_workers=workers) as executor:
        for _ in executor.map(run_load, source_conninfos, target_conninfos, projects, schemas, tables):
            pass

    utils.delete_path(temp_path)
    logger.info("Finished loading tables")


def run_load(source_conninfo, target_conninfo, project, schema, table):
    source = utils.connection_from_config(source_conninfo)
    target = utils.connection_from_config(target_conninfo)

    # Temp_path
    temp_path = project.get('temp_path')

    # Delimiter
    csv_delimiter = project.get('csv_delimiter')

    # Schemas
    source_schema = schema.get('source_schema')
    target_schema = schema.get('target_schema')

    # Tables
    source_table = table.get('table_name')
    full_source_table = source_schema + '.' + source_table
    target_table = schema.get('table_prefix', "") + table.get('table_name') + schema.get('table_suffix', "")
    full_target_table = target_schema + '.' + target_table

    # Temp table
    target_table_tmp = target_table + '_tmp'

    # Temp path for specific load
    temp_path_schema = os.path.join(temp_path, source_schema)
    temp_path_load = os.path.join(temp_path_schema, source_table)
    utils.create_path(temp_path_load)

    # Source column types to exclude
    source_columntypes_to_exclude = project.get('source_columntypes_to_exclude')
    if source_columntypes_to_exclude:
        source_columntypes_to_exclude = source_columntypes_to_exclude.lower().replace(" ", "").split(",")

    # Columns to load
    try:
        columns = source.table_columns(source_schema, source_table)
        if source_columntypes_to_exclude:
            columns_to_load = columns.copy()
            for col in columns:
                data_type = col[2].lower()
                if data_type in source_columntypes_to_exclude:
                    columns_to_load.remove(col)
            columns = columns_to_load
    except:
        logger.error("Could not determine columns to load")

    # Load type
    replication_method = table.get('replication_method')

    if replication_method == "FULL_TABLE":
        logger.info("Start loading: " + full_source_table + " using FULL_TABLE replication")
        # Export table
        file, delimiter = source.export_table(source_schema, source_table, columns, temp_path_load,
                                              csv_delimiter)

        # Create temp table
        target.create_table_from_columns(target_schema, target_table_tmp, columns)

        # Import into temp table
        target.import_table(target_schema, target_table_tmp, file, delimiter)

        # Switch tables
        target.switch_tables(target_schema, target_table, target_table_tmp)

        #if target.check_table_exist(full_target_table):

            #target.truncate_table(full_target_table)

        #else:
            # Recreate table
            #columns = source.table_columns(source_schema, source_table)
            #target.create_table_from_columns(target_schema, target_table, columns)

        # Import table
        #target.import_table(target_schema, target_table, file, delimiter)

    elif replication_method == "INCREMENTAL":
        logger.info("Start loading: " + full_source_table + " using INCREMENTAL replication")
        replication_key = table.get('replication_key')

        if target.check_table_exist(full_target_table) and replication_key:
            max_replication_key = target.get_max_column_value(full_target_table, replication_key)
            # Export new rows
            file, delimiter = source.export_table(source_schema, source_table, columns, temp_path_load, csv_delimiter,
                                                  replication_key, max_replication_key)
            # Create temp table
            target.create_table_from_columns(target_schema, target_table_tmp, columns)
            # Import into temp table
            target.import_table(target_schema, target_table_tmp, file, delimiter)
            # Insert into and drop
            target.insert_from_table_and_drop(target_schema, target_table, target_table_tmp)
            logger.info(full_source_table + " updated")

        else:
            # Full export
            file, delimiter = source.export_table(source_schema, source_table, columns, temp_path_load, csv_delimiter)
            # Create temp table
            target.create_table_from_columns(target_schema, target_table_tmp, columns)
            # Import into temp table
            target.import_table(target_schema, target_table_tmp, file, delimiter)
            # Switch tables
            target.switch_tables(target_schema, target_table, target_table_tmp)
            logger.info(full_source_table + " imported")

        # Import table



    else:
        logger.error("replication_method not valid")

    # delete temp folder
    utils.delete_path(temp_path_load)
    logger.info("Finished loading: " + full_source_table)
