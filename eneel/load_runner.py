import eneel.utils as utils
from concurrent.futures import ProcessPoolExecutor as Executor
import os


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

    #print(loads[''])

    workers = project.get('parallel_loads',1)
    print("Start loading tables with " + str(workers) + " parallel workers")

    with Executor(max_workers=workers) as executor:
        for _ in executor.map(run_load, source_conninfos, target_conninfos, projects, schemas, tables):
            pass

    utils.delete_path(temp_path)
    print("Finished loading tables")


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

    # Temp path for specific load
    temp_path_schema = os.path.join(temp_path, source_schema)
    #utils.create_path(temp_path_schema)
    temp_path_load = os.path.join(temp_path_schema, source_table)
    utils.create_path(temp_path_load)

    # Load type
    replication_method = table.get('replication_method')
    #print(replication_method)
    if replication_method == "FULL_TABLE":
        print("Start loading: " + full_source_table + " using FULL_TABLE replication")
        # Export table
        file, delimiter = source.export_table(source_schema, source_table, temp_path_load, csv_delimiter)

        if target.check_table_exist(full_target_table):
            #print('truncate')
            target.truncate_table(full_target_table)

        else:
            # Recreate table
            columns = source.table_columns(source_schema, source_table)
            target.create_table_from_columns(target_schema, target_table, columns)

        # Import table
        target.import_table(target_schema, target_table, file, delimiter)

    elif replication_method == "INCREMENTAL":
        print("Start loading: " + full_source_table + " using INCREMENTAL replication")
        replication_key = table.get('replication_key')

        if target.check_table_exist(full_target_table):
            max_replication_key = target.get_max_column_value(full_target_table, replication_key)
            # Export new rows
            file, delimiter = source.export_table(source_schema, source_table, temp_path_load, csv_delimiter,
                                                  replication_key, max_replication_key)

        else:
            # Full export
            file, delimiter = source.export_table(source_schema, source_table, temp_path_load, csv_delimiter)
            # Recreate table
            columns = source.table_columns(source_schema, source_table)
            target.create_table_from_columns(target_schema, target_table, columns)

        # Import table
        target.import_table(target_schema, target_table, file, delimiter)

    else:
        print("replication_method not valid")

    # delete temp folder
    utils.delete_path(temp_path_load)
    print("Finished loading: " + full_source_table)
