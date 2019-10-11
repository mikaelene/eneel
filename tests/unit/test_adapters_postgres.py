from eneel.adapters.postgres import *
import pytest
import os

from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())


@pytest.fixture
def db():
    db = Database(
        os.getenv('POSTGRES_TEST_HOST'),
        os.getenv('POSTGRES_TEST_USER'),
        os.getenv('POSTGRES_TEST_PASS'),
        os.getenv('POSTGRES_TEST_DBNAME'),
        os.getenv('POSTGRES_TEST_PORT'))

    setup_sql = """
    drop schema if exists test cascade;
    
    create schema test;
    
    create table test.test1(
    id_col 			int,
    name_col		varchar(64),
    datetime_col	timestamp
    ); 
    
    insert into test.test1 values(1, 'First', '2019-10-01 11:00:00');
    insert into test.test1 values(2, 'Second', '2019-10-02 12:00:00');
    insert into test.test1 values(3, 'Third', '2019-10-03 13:00:00');
    """
    db.execute(setup_sql)

    yield db

    teardown_sql = """
    drop table test.test1;
    drop schema test;
    """
    db.execute(teardown_sql)

    db.close()


class TestDatabasePg:

    def test_init(self, db):
        assert db._dialect == 'postgres'

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

    def test_get_min_max_column_value(self, db):
        min, max = db.get_min_max_column_value('test.test1', 'id_col')
        assert min == 1
        assert max == 3

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

        assert return_code == 'RUN'
        assert row_count == 3

    def test_generate_create_table_ddl(self, db):
        columns = [(1, 'id_col', 'integer', None, 32, 0)]
        ddl = db.generate_create_table_ddl('test', 'test1', columns)

        assert ddl == """CREATE TABLE test.test1(
id_col integer)"""

    def test_create_log_table(self, db):
        db.execute('drop table if exists log_schema.log_table')
        db.create_log_table('log_schema', 'log_table')

        assert db.check_table_exist('log_schema.log_table')

    def test_log(self, db):
        db.execute('drop table if exists log_schema.log_table')
        db.create_log_table('log_schema', 'log_table')
        db.log('log_schema', 'log_table', project='project')

        assert db.query('select count(*) from log_schema.log_table')[0][0] == 1


