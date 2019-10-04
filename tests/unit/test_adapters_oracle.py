from eneel.adapters.oracle import *
import pytest
import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


@pytest.fixture
def db():
    db = Database(
        os.getenv('ORACLE_TEST_HOST'),
        os.getenv('ORACLE_TEST_USER'),
        os.getenv('ORACLE_TEST_PASS'),
        os.getenv('ORACLE_TEST_DBNAME'),
        os.getenv('ORACLE_TEST_PORT'))

    yield db

    db.close()


class TestDatabasePg:

    def test_init(self, db):
        assert db._dialect == 'oracle'

    def test_schemas(self, db):
        schemas = db.schemas()

        assert type(schemas) == list
        assert len(schemas) > 0

    def test_tables(self, db):
        tables = db.tables()

        assert type(tables) == list
        assert len(tables) > 0

    def test_table_columns(self, db):
        table_columns = db.table_columns(os.getenv('ORACLE_TEST_SCHEMA'), os.getenv('ORACLE_TEST_TABLE'))

        assert type(table_columns) == list
        assert len(table_columns) > 0

    def test_check_table_exist(self, db):
        assert db.check_table_exist(os.getenv('ORACLE_TEST_SCHEMA') + '.' + os.getenv('ORACLE_TEST_TABLE')) is True
        assert db.check_table_exist('test.test_does_not_exist') is False

    def test_truncate_table(self, db):
        assert db.truncate_table('test.test_does_not_exist') == 'Not implemented for this adapter'

    def test_create_schema(self, db):
        assert db.create_schema('test') == 'Not implemented for this adapter'

    def test_get_max_column_value(self, db):
        assert db.get_max_column_value('test.test1', 'id_col') == 'Not implemented for this adapter'

    def test_export_table(self, tmpdir, db):
        columns = os.environ['ORACLE_TEST_TABLE_COLUMN'].split()
        path = tmpdir
        file_path, delimiter, row_count = db.export_table(os.getenv('ORACLE_TEST_SCHEMA'), os.getenv('ORACLE_TEST_TABLE'), columns, path)

        #assert row_count > 0
        assert os.path.exists(file_path) == 1
        assert os.stat(file_path).st_size > 0

    def test_import_table(self, tmpdir, db):
        assert db.import_table('test_target', 'test1_target', 'path') == 'Not implemented for this adapter'

    def test_generate_create_table_ddl(self, db):
        assert db.generate_create_table_ddl('test', 'test1', 'columns') == 'Not implemented for this adapter'

    def test_create_log_table(self, db):
        assert db.create_log_table('log_schema', 'log_table') == 'Not implemented for this adapter'

    def test_log(self, db):
        assert db.log('log_schema', 'log_table', project='project') == 'Not implemented for this adapter'

