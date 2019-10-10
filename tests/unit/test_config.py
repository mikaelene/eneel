from eneel.config import *
import os
import pytest

from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())

@pytest.fixture
def test_project_yml(tmp_path):
    test_project_yml_path = tmp_path / 'test_project.yml'
    test_project_yml_path.write_text("""# Project info
id: project id                            # OPTIONAL and currently not used
name: project name                        # OPTIONAL and currently not used
owner: somebody@yourcompany.com           # OPTIONAL and currently not used
temp_path: /tempfiles                     # The directory to use for temp csv files during load (OPTIONAL: default=run_path/temp )
csv_delimiter: "|"                        # The delimiter to use in the csv files (OPTIONAL: default=| )

# Connection details
source: postgres1                         # A Connection name in connections.yml
target: sqlserver1                        # A Connection name in connections.yml

# Source to Destination Schema mapping
schemas:
  - source_schema: "public"               # You can replicate from multiple schemas
    target_schema: "public_tgt"           # Target schema
    table_prefix: "pre_"                  # Prefix for all created tables name (OPTIONAL)
    table_suffix: "suf_"                  # Suffix for all created tables name (OPTIONAL)
    tables:                               # List Tables to replicate
      - table_name: "customer"            # Source table name
        replication_method: FULL_TABLE    # FULL_TABLE replication. Will recreate the table on each load
      - table_name: "payment"
        replication_method: INCREMENTAL   # INCREMENTAL replication. Will add new rows to the table
        replication_key: "payment_date"   # Incremental load needs replication key""")

    yield str(test_project_yml_path)


@pytest.fixture
def test_config_yml(tmp_path):
    test_config_yml_path = tmp_path / 'test_connections.yml'
    test_config_yml_path.write_text("""# Connection details to an Postgres database
postgres1:
  type: postgres
  read_only: False                        # Will disable SQL and DML modifications
  outputs:
    dev:
      host: localhost
      port: 5432
      user: user_name
      password: secret_password
      database: my_db
      limit_rows: 100                         # Will limit all exports to 100 rows
    prod:
      host: prodserver_host
      port: 5432
      user: user_name
      password: secret_password
      database: my_db
  target: dev                                 # The profile that will be used when running the load

# Connection details to an SQL Server database
sqlserver1:
  type: sqlserver
  outputs:
    dev:
      driver: ODBC Driver 17 for SQL Server   # Your ODBC driver
      host: localhost
      port: 1433
      trusted_connection: True                # Use logged in user for credentials
      as_columnstore: True                    # Create tables as Clustered Columnstore Index (SQL Server only)
      database: my_db
      limit_rows: 100
    prod:
      driver: ODBC Driver 17 for SQL Server
      host: prodserver_host
      port: 1433
      user: user_name
      password: secret_password
      database: my_db
  target: dev                                 # The profile that will be used when running the load

# Connection details to an Oracle database
oracle1:
  type: oracle
  outputs:
    dev:
      host: localhost
      port: 1521
      user: user_name
      password: secret_password
      database: my_db
      limit_rows: 100                         # Will limit all exports to 100 rows
    prod:
      host: prodserver_host
      port: 1521
      user: user_name
      password: secret_password
      database: my_db
  target: dev                                 # The profile that will be used when running the load""")

    yield str(test_config_yml_path)

@pytest.fixture
def test_config_expanduser(tmp_path):
    home_path = tmp_path / '.eneel'
    home_path.mkdir()
    test_config_yml_path = home_path / 'connections.yml'
    test_config_yml_path.write_text("""# Connection details to an Postgres database
postgres1:
  type: postgres
  read_only: False                        # Will disable SQL and DML modifications
  outputs:
    dev:
      host: localhost
      port: 5432
      user: user_name
      password: secret_password
      database: my_db
      limit_rows: 100                         # Will limit all exports to 100 rows
    prod:
      host: prodserver_host
      port: 5432
      user: user_name
      password: secret_password
      database: my_db
  target: dev                                 # The profile that will be used when running the load

# Connection details to an SQL Server database
sqlserver1:
  type: sqlserver
  outputs:
    dev:
      driver: ODBC Driver 17 for SQL Server   # Your ODBC driver
      host: localhost
      port: 1433
      trusted_connection: True                # Use logged in user for credentials
      as_columnstore: True                    # Create tables as Clustered Columnstore Index (SQL Server only)
      database: my_db
      limit_rows: 100
    prod:
      driver: ODBC Driver 17 for SQL Server
      host: prodserver_host
      port: 1433
      user: user_name
      password: secret_password
      database: my_db
  target: dev                                 # The profile that will be used when running the load

# Connection details to an Oracle database
oracle1:
  type: oracle
  outputs:
    dev:
      host: localhost
      port: 1521
      user: user_name
      password: secret_password
      database: my_db
      limit_rows: 100                         # Will limit all exports to 100 rows
    prod:
      host: prodserver_host
      port: 1521
      user: user_name
      password: secret_password
      database: my_db
  target: dev                                 # The profile that will be used when running the load""")

    yield str(tmp_path)


def substitute_os_path_expanduser(test_config_expanduser):
    return test_config_expanduser


class TestGetProject:

    def test_get_project_from_path(self, test_project_yml):
        project_config = get_project(test_project_yml)
        assert type(project_config) == dict

    def test_get_project(self, test_project_yml):
        project_yml = test_project_yml[:-4]
        project_config = get_project(project_yml)
        assert type(project_config) == dict


class TestConnectionFromConfig:

    def test_connection_from_config_postgres(self):
        credentials = {'host': os.getenv('POSTGRES_TEST_HOST'),
                       'port': os.getenv('POSTGRES_TEST_PORT'),
                       'user': os.getenv('POSTGRES_TEST_USER'),
                       'password': os.getenv('POSTGRES_TEST_PASS'),
                       'database': os.getenv('POSTGRES_TEST_DBNAME')}
        connection_info = {'name': 'postgres1', 'type': 'postgres', 'read_only': False, 'target': 'prod',
                              'credentials': credentials}

        connection = connection_from_config(connection_info)

        assert connection._dialect == 'postgres'


class TestConnections:
    @pytest.mark.skip(reason="must get monkeypatch and fixture working")
    def test_Connections(self, monkeypatch):
        monkeypatch.setattr(os.path, "expanduser", substitute_os_path_expanduser )
        connections = Connections()

        assert connections.connections['postgres1']['type'] == 'postgres'

    @pytest.mark.skip(reason="must get monkeypatch and fixture working")
    def test_Connections_target(self, monkeypatch):
        monkeypatch.setattr(os.path, "expanduser", substitute_os_path_expanduser)
        connections = Connections(target='prod')

        assert connections.connections['postgres1']['type'] == 'postgres'

    def test_Connections_from_path(self, test_config_yml):
        connections_path = os.path.abspath(test_config_yml)
        connections = Connections(connections_path=connections_path)

        assert connections.connections['postgres1']['type'] == 'postgres'

    def test_Connections_from_path_target(self, test_config_yml):
        connections_path = os.path.abspath(test_config_yml)
        connections = Connections(connections_path=connections_path, target='prod')

        assert connections.connections['postgres1']['type'] == 'postgres'


def test_Project(test_project_yml, test_config_yml):
    connections = Connections(connections_path=test_config_yml)
    project = Project(test_project_yml, connections.connections)

    assert project.source_name == 'postgres1'

