import os
import subprocess
import shutil
import yaml
import eneel.adapters.postgres as postgres
import eneel.adapters.oracle as oracle
import eneel.adapters.sqlserver as sqlserver
import logging
logger = logging.getLogger('main_logger')


def create_relative_path(path_name):
    if not os.path.exists(path_name):
        os.makedirs(path_name)


def create_path(path_name):
    # Create path
    if not os.path.exists(path_name):
        os.makedirs(path_name)

    # Absolute path
    abs_temp_file_dir = os.path.abspath(path_name)
    return abs_temp_file_dir


def delete_path(path_name):
    if os.path.exists(path_name):
        try:
            shutil.rmtree(path_name)
        except:
            print("Could not delete directory")
            pass


def delete_file(file):
    if os.path.exists(file):
        os.remove(file)


def copy_table(source, target, temp_file_dir, source_schema, source_table, target_schema, target_table):
    # Create tempdir
    create_relative_path(temp_file_dir)

    file, delimiter = source.export_table(source_schema, source_table, temp_file_dir)

    # Recreate table
    columns = source.table_columns(source_schema, source_table)
    #print(columns)
    target.create_table_from_columns(target_schema, target_table, columns)

    # Import table
    target.import_table(target_schema, target_table, file, delimiter)

    # delete csv-file
    delete_file(file)

    #print("table copied")


def copy_tables(source, target, temp_file_dir, target_schema, tables, target_prefix=None, target_suffix=None, limit=None):
    print(tables)
    for table in tables:
        source_schema = table.split(".")[0]
        source_table = table.split(".")[1]

        target_table = target_prefix if target_prefix else ""
        target_table += source_table
        target_table += target_suffix if target_suffix else ""
        print(target_table)

        copy_table(source, target, temp_file_dir, source_schema, source_table, target_schema, target_table, limit)


def load_yaml_from_path(path):
    with open(path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def load_yaml(stream):
    try:
        return yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


def load_file_contents(path, strip=True):
    if not os.path.exists(path):
        print(path, ' not found')

    with open(path, 'rb') as handle:
        to_return = handle.read().decode('utf-8')

    if strip:
        to_return = to_return.strip()

    return to_return


def get_connections(connections_path=None):
    if not connections_path:
        connections_path = os.path.join(os.path.expanduser('~'), '.eneel/connections.yml')
    #connections_path = os.path.join(os.getcwd(), 'connections.yml')
    try:
        connections_file_contents = load_file_contents(connections_path, strip=False)
        connections = load_yaml(connections_file_contents)

        connections_dict = {}
        for conn in connections:
            name = conn
            type = connections[name]['type']
            target = connections[name]['target']
            credentials = connections[name]['outputs'][target]
            connection = {'name': conn, 'type': type, 'target': target, 'credentials': credentials}

            connections_dict[name] = connection
        return connections_dict
    except:
        logger.error("Could not load connections.yml")


def get_project(project):
    project_file_contents = load_file_contents(project + '.yml', strip=False)
    project = load_yaml(project_file_contents)

    return project


def run_cmd(cmd):
    res = subprocess.run(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, universal_newlines=True)
    if res.returncode == 0:
        return res.returncode, res.stdout
    else:
        return res.returncode, res.stderr


def connection_from_config(connection_info):
    #print(connection_info)
    database = connection_info['credentials'].get('database')
    user = connection_info['credentials'].get('user')
    password = connection_info['credentials'].get('password')
    server = connection_info['credentials'].get('host')
    limit_rows = connection_info.get('credentials').get('limit_rows')
    table_where_clause = connection_info.get('credentials').get('table_where_clause')
    if connection_info.get('type') == 'oracle':
        #print('oracle')
        server = connection_info['credentials'].get('host') + ':' + str(connection_info['credentials'].get('port'))
        return oracle.Database(server, user, password, database, limit_rows, table_where_clause)
    elif connection_info.get('type') == 'sqlserver':
        #print('sqlserver')
        odbc_driver = connection_info['credentials'].get('driver')
        trusted_connection = connection_info['credentials'].get('trusted_connection')
        as_columnstore = connection_info.get('credentials').get('as_columnstore')
        return sqlserver.Database(odbc_driver, server, database, limit_rows, user, password, trusted_connection, as_columnstore)
    elif connection_info.get('type') == 'postgres':
        #print('postgres')
        return postgres.Database(server, user, password, database, limit_rows)
    else:
        print('source type not found')


def run_load(load_strategy, source, target, temp_file_dir, source_schema, source_table, target_schema, target_table, limit=None):
    if load_strategy == "FULL_TABLE":
        # Create tempdir
        create_relative_path(temp_file_dir)

        # Export table
        if limit:
            file, delimiter = source.export_table(source_schema, source_table, temp_file_dir, "|", limit)
        else:
            file, delimiter = source.export_table(source_schema, source_table, temp_file_dir)

        # Recreate table
        columns = source.table_columns(source_schema, source_table)
        print(columns)
        target.create_table_from_columns(target_schema, target_table, columns)

        # Import table
        target.import_table(target_schema, target_table, file, delimiter)

        # delete csv-file
        delete_file(file)

        print("table copied")
    elif load_strategy == "INCREMENTAL":
        pass
