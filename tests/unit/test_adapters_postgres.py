from eneel.adapters.postgres import *

server = 'localhost'
port = 5432
user = 'mikaelene'
password = 'password-1234'
database = 'dvd2'

db = Database(server, user, password, database)


class TestDatabasePg:

    def test_init(self):

        assert db._dialect == 'postgres'

    def test_schemas(self):
        schemas = db.schemas()
        assert type(schemas) == list
        assert len(schemas) > 0

    def test_tables(self):
        tables = db.tables()
        assert type(tables) == list
        assert len(tables) > 0

    def test_table_columns(self):
        schema = 'public'
        table = 'city'
        table_columns = db.table_columns(schema,table)
        assert type(table_columns) == list
        assert len(table_columns) > 0

    def test_check_table_exist(self):
        schema = 'public'
        table = 'city'
        table_name = schema + '.' + table
        assert db.check_table_exist(table_name) is True

    