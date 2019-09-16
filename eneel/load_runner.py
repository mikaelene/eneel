import eneel.utils as utils


def run_project(connections_path, project_path):
    connections_config = utils.get_connections(connections_path)
    project_config = utils.get_project(project_path)

    source_conninfo = connections_config[project_config['source']]
    target_conninfo = connections_config[project_config['target']]
    #print(source_conninfo)

    source = utils.connection_from_config(source_conninfo)
    target = utils.connection_from_config(target_conninfo)

    project = project_config.copy()
    del project['schemas']
    #print(project_info)

    temp_path = project.get('temp_path')
    utils.create_path(temp_path)
    #print(temp_path)

    for schema_config in project_config['schemas']:
        schema = schema_config.copy()
        del schema['tables']
        #    print(schema)
        #    print(schema_info)
        for table in schema_config['tables']:
            run_load(source, target, project, schema, table)
            #        print(schema['source_schema'])
            #        print(schema['target_schema'])
            #print(project)
            #print(schema)
            #print(table)


def run_load(source, target, project, schema, table):

    # Temp_path
    temp_path = project.get('temp_path')
    csv_delimiter = project.get('csv_delimiter')

    # Schemas
    source_schema = schema.get('source_schema')
    target_schema = schema.get('target_schema')

    # Table
    source_table = table.get('table_name')
    target_table = schema.get('table_prefix', "") + table.get('table_name') + schema.get('table_suffix', "")
    full_target_table = target_schema + '.' + target_table

    # Load type
    replication_method = table.get('replication_method')
    #print(replication_method)
    if replication_method == "FULL_TABLE":
        print("FULL_TABLE")
        # Export table
        file, delimiter = source.export_table(source_schema, source_table, temp_path, csv_delimiter)

        if target.check_table_exist(full_target_table):
            print('truncate')
            target.truncate_table(full_target_table)

        else:
            # Recreate table
            columns = source.table_columns(source_schema, source_table)
            target.create_table_from_columns(target_schema, target_table, columns)

        # Import table
        target.import_table(target_schema, target_table, file, delimiter)

        utils.copy_table(source, target, temp_path, source_schema, source_table, target_schema, target_table)

        # delete csv-file
        utils.delete_file(file)

    elif replication_method == "INCREMENTAL":
        replication_key = table.get('replication_key')

        if target.check_table_exist(full_target_table):
            max_replication_key = target.get_max_column_value(full_target_table, replication_key)
            # Export new rows
            file, delimiter = source.export_table(source_schema, source_table, temp_path, csv_delimiter, replication_key, max_replication_key)

        else:
            # Full export
            file, delimiter = source.export_table(source_schema, source_table, temp_path, csv_delimiter)
            # Recreate table
            columns = source.table_columns(source_schema, source_table)
            target.create_table_from_columns(target_schema, target_table, columns)

        # Import table
        target.import_table(target_schema, target_table, file, delimiter)

        # delete csv-file
        utils.delete_file(file)

    else:
        print("replication_method not valid")


