import os
import sys
import eneel.utils as utils
import eneel.adapters.postgres as postgres
import eneel.adapters.oracle as oracle
import eneel.adapters.sqlserver as sqlserver

import logging
logger = logging.getLogger('main_logger')


def get_connections(connections_path=None, target=None):
    if not connections_path:
        connections_path = os.path.join(os.path.expanduser('~'), '.eneel/connections.yml')
    try:
        connections_file_contents = utils.load_file_contents(connections_path, strip=False)
        connections = utils.load_yaml(connections_file_contents)

        connections_dict = {}
        for conn in connections:
            name = conn
            type = connections[name]['type']
            read_only = connections[name].get('read_only')
            if not target:
                target = connections[name]['target']
            credentials = connections[name]['outputs'][target]
            connection = {'name': conn, 'type': type, 'read_only': read_only, 'target': target, 'credentials': credentials}

            connections_dict[name] = connection
        return connections_dict
    except:
        logger.error("Could not load connections.yml")


def get_project(project):
    try:
        project_file_contents = utils.load_file_contents(project + '.yml', strip=False)
        project = utils.load_yaml(project_file_contents)

        return project
    except:
        logger.error(project + ".yml not found")
        sys.exit(-1)


def connection_from_config(connection_info):
    database = connection_info['credentials'].get('database')
    user = connection_info['credentials'].get('user')
    password = connection_info['credentials'].get('password')
    server = connection_info['credentials'].get('host')
    limit_rows = connection_info.get('credentials').get('limit_rows')
    table_where_clause = connection_info.get('credentials').get('table_where_clause')
    read_only = connection_info.get('read_only')
    type = connection_info.get('type')
    if connection_info.get('type') == 'oracle':
        server = connection_info['credentials'].get('host') + ':' + str(connection_info['credentials'].get('port'))
        return oracle.Database(server, user, password, database, limit_rows, table_where_clause, read_only)
    elif connection_info.get('type') == 'sqlserver':
        odbc_driver = connection_info['credentials'].get('driver')
        trusted_connection = connection_info['credentials'].get('trusted_connection')
        as_columnstore = connection_info.get('credentials').get('as_columnstore')
        return sqlserver.Database(odbc_driver, server, database, limit_rows, user, password, trusted_connection,
                                  as_columnstore, read_only)
    elif connection_info.get('type') == 'postgres':
        return postgres.Database(server, user, password, database, limit_rows, read_only)
    else:
        logger.error('source type not found')


class Project:
    def __init__(self, project_name, connections_path=None):
        self._project_name = project_name
        self._connections_path = connections_path
        # Get configurations
        self._connections_config = get_connections(connections_path)
        self._project_config = get_project(project_name)

        self._source_name = self.project_config['source']
        self._target_name = self.project_config['target']

        self._source_conninfo = connections_config[source_name]
        self._target_conninfo = connections_config[target_name]

        self._project = self.project_config.copy()
        del self._project['schemas']

        # Create temp dir
        self._temp_path = self._project.get('temp_path', 'temp')
        self._temp_path = self._temp_path + '/' + project_name

    def __enter__(self):
        return self

    def loads(self):
        # Lists of load settings
        load_orders = []
        project_names = []
        source_conninfos = []
        target_conninfos = []
        projects = []
        schemas = []
        tables = []
        temp_paths = []

        # Populate load settings
        for schema_config in self._project_config['schemas']:
            schema = schema_config.copy()
            del schema['tables']
            order_num = 1
            for table in schema_config['tables']:
                source_conninfo_item = self._source_conninfo
                target_conninfo_item = self._target_conninfo
                project_item = self._project
                schema_item = schema
                table_item = table

                load_orders.append(order_num)
                order_num += 1
                project_names.append(self._project_name)
                source_conninfos.append(source_conninfo_item)
                target_conninfos.append(target_conninfo_item)
                projects.append(project_item)
                schemas.append(schema_item)
                tables.append(table_item)
                temp_paths.append(self._temp_path)

        loads = [{'load_order': load_order,
                  'project_name': project_name,
                  'source_conninfo': source_conninfo,
                  'target_conninfo': target_conninfo,
                  'project': project,
                  'schema': schema,
                  'table': table,
                  'temp_path': temp_path
                  }
                 for load_order,
                     project_name,
                     source_conninfo,
                     target_conninfo,
                     project,
                     schema,
                     table,
                     temp_path in zip(load_orders,
                     project_names,
                     source_conninfos,
                     target_conninfos,
                     projects,
                     schemas,
                     tables,
                     temp_paths)]

        ### NEEDS TO BE ADDED TO THE LOADS
        # Parallel load settings
        num_tables_to_load = len(tables)

        num_tables_to_loads = []
        for i in range(num_tables_to_load):
            num_tables_to_loads.append(num_tables_to_load)
        ######

        return loads



    workers = project.get('parallel_loads', 1)