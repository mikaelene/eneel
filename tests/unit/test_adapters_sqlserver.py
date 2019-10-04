from eneel.adapters.sqlserver import *
import pytest
import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


@pytest.fixture
def db():
    db = Database(
        os.getenv('SQLSERVER_TEST_DRIVER'),
        os.getenv('SQLSERVER_TEST_HOST'),
        os.getenv('SQLSERVER_TEST_DBNAME'),
        user=os.getenv('SQLSERVER_TEST_USER'),
        password=os.getenv('SQLSERVER_TEST_PASS'),
        port=os.getenv('SQLSERVER_TEST_PORT'),
        trusted_connection=os.getenv('SQLSERVER_TEST_TRUSTED_CONNECTION'))

    setup_sql1 = """
    drop table if exists test.test1;
    drop schema if exists test;"""
    db.execute(setup_sql1)

    db.execute('create schema test;')

    setup_sql2 = """
    create table test.test1(
    id_col 			int,
    name_col		varchar(64),
    datetime_col	datetime2
    ); 
    
    insert into test.test1 values(1, 'First', '2019-10-01 11:00:00');
    insert into test.test1 values(2, 'Second', '2019-10-02 12:00:00');
    insert into test.test1 values(3, 'Third', '2019-10-03 13:00:00');
    """
    db.execute(setup_sql2)

    yield db

    teardown_sql = """
    drop table test.test1;
    drop schema test;
    """
    db.execute(teardown_sql)

    db.close()


class TestDatabaseSqlserver:

    def test_init(self, db):
        assert db._dialect == 'sqlserver'

    def test_schemas(self, db):
        schemas = db.schemas()

        assert type(schemas) == list
        assert len(schemas) > 0

    def test_tables(self, db):
        tables = db.tables()

        assert type(tables) == list
        assert len(tables) > 0

    def test_table_columns(self, db):
        table_columns = db.table_columns('test', 'test1')

        assert type(table_columns) == list
        assert len(table_columns) > 0

    def test_check_table_exist(self, db):

        assert db.check_table_exist('test.test1') is True
        assert db.check_table_exist('test.test_does_not_exist') is False

    def test_truncate_table(self, db):
        db.truncate_table('test.test1')
        counts = db.query("select count(*) from test.test1")

        assert counts[0][0] == 0

    def test_create_schema(self, db):
        db.execute('drop schema if exists test_create_schema')
        db.create_schema('test_create_schema')

        assert 'test_create_schema' in db.schemas()

    def test_get_max_column_value(self, db):

        assert db.get_max_column_value('test.test1', 'id_col') == '3'

    def test_export_table(self, tmpdir, db):
        columns = [(1, 'id_col', 'integer', None, 32, 0)]
        path = tmpdir
        file_path, delimiter, row_count = db.export_table('test', 'test1', columns, path)

        assert row_count == 3
        assert os.path.exists(file_path) == 1

    def test_import_table(self, tmpdir, db):
        columns = [(1, 'id_col', 'integer', None, 32, 0)]
        path = tmpdir
        file_path, delimiter, row_count = db.export_table('test', 'test1', columns, path)

        db.create_table_from_columns('test_target', 'test1_target', columns)

        return_code, row_count = db.import_table('test_target', 'test1_target', file_path)

        assert return_code == 'DONE'
        assert row_count == 3

    def test_generate_create_table_ddl(self, db):
        columns = [(1, 'id_col', 'integer', None, 32, 0)]

        ddl = db.generate_create_table_ddl('test', 'test1', columns)
        assert ddl == """CREATE TABLE test.test1(
[id_col] integer)"""

    def test_create_log_table(self, db):
        db.execute('drop table if exists log_schema.log_table')
        db.create_log_table('log_schema', 'log_table')

        assert db.check_table_exist('log_schema.log_table')

    def test_log(self, db):
        db.execute('drop table if exists log_schema.log_table')
        db.create_log_table('log_schema', 'log_table')
        db.log('log_schema', 'log_table', project='project')

        assert db.query('select count(*) from log_schema.log_table')[0][0] == 1


