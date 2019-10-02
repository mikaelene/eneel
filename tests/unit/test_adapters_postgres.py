from eneel.adapters.postgres import *
import pytest

server = 'localhost'
port = 5432
user = 'mikaelene'
password = 'password-1234'
database = 'dvd2'

#db = Database(server, user, password, database)

@pytest.fixture
def db():
    db = Database('localhost', 'mikaelene', 'password-1234', 'dvd2')
    yield db
    db.close()


class TestDatabasePg:

    def test_init(self,db):

        assert db._dialect == 'postgres'

    def test_schemas(self,db):
        schemas = db.schemas()
        assert type(schemas) == list
        assert len(schemas) > 0

    def test_tables(self,db):
        tables = db.tables()
        assert type(tables) == list
        assert len(tables) > 0

    def test_table_columns(self,db):
        schema = 'public'
        table = 'city'
        table_columns = db.table_columns(schema,table)
        assert type(table_columns) == list
        assert len(table_columns) > 0

    def test_check_table_exist(self,db):
        schema = 'public'
        table = 'city'
        table_name = schema + '.' + table
        assert db.check_table_exist(table_name) is True

    def test_truncate_table(self,db):
        if db.check_table_exist('test_truncate'):
            db.execute("drop table test_truncate")

        ddl = """create table test_truncate(
        test    int)"""
        db.execute(ddl)

        sql = "insert into test_truncate (test) values (1)"
        db.execute(sql)

        db.truncate_table('test_truncate')

        counts = db.query("select count(*) from test_truncate")

        assert counts[0][0] == 0

    def test_create_schema(self,db):
        schema = 'test_schema'

        if schema in db.schemas():
            db.execute("drop schmea " + schema)

        db.create_schema(schema)

        assert schema in db.schemas()

    def test_get_max_column_value(self,db):
        schema = 'public'
        table = 'actor_tgt'
        table_name = schema + '.' + table

        assert db.get_max_column_value(table_name,'actor_id') == '200'

    def test_export_table(self,tmpdir,db):
        schema = 'public'
        table = 'actor_tgt'
        columns = [(1, 'actor_id', 'integer', None, 32, 0)]
        path = tmpdir
        file_path, delimiter, row_count = db.export_table(schema, table, columns, path)

        assert row_count == 200
        assert os.path.exists(file_path) == 1

    def test_import_table(self,tmpdir,db):
        schema = 'public'
        table = 'actor_tgt'
        columns = [(1, 'actor_id', 'integer', None, 32, 0)]
        path = tmpdir
        file_path, delimiter, row_count = db.export_table(schema, table, columns, path)

        t_schema = 'public'
        t_table = 'actor_tgt_import'

        db.create_table_from_columns(t_schema, t_table, columns)

        return_code, row_count = db.import_table(t_schema, t_table, file_path)

        assert return_code == 'DONE'
        assert row_count == '200'

    def test_generate_create_table_ddl(self,db):
        columns = [(1, 'actor_id', 'integer', None, 32, 0)]
        schema = 'public'
        table = 'actor_tgt_import'

        ddl = db.generate_create_table_ddl(schema, table, columns)

        assert ddl == """CREATE TABLE public.actor_tgt_import(
actor_id integer)"""