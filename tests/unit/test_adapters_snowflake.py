from eneel.adapters.snowflake import *
import pytest
import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


@pytest.fixture
def db():
    db = Database(
        os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
        os.getenv("SNOWFLAKE_TEST_USER"),
        os.getenv("SNOWFLAKE_TEST_PASS"),
        os.getenv("SNOWFLAKE_TEST_DBNAME"),
        os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
        os.getenv("SNOWFLAKE_TEST_SCHEMA"),
    )

    setup_sql = """
    drop schema if exists TEST cascade;

    create schema TEST;

    create table TEST.TEST1(
    ID_COL 			int,
    NAME_COL		varchar(64),
    DATETIME_COL	timestamp
    ); 

    insert into TEST.TEST1 values(1, 'First', '2019-10-01 11:00:00');
    insert into TEST.TEST1 values(2, 'Second', '2019-10-02 12:00:00');
    insert into TEST.TEST1 values(3, 'Third', '2019-10-03 13:00:00');
    """
    db.execute(setup_sql)

    yield db

    teardown_sql = """
    drop table TEST.TEST1;
    drop schema TEST;
    """
    db.execute(teardown_sql)

    db.close()


class TestDatabaseSf:
    def test_init(self, db):
        assert db._dialect == "snowflake"

    def test_schemas(self, db):
        schemas = db.schemas()

        assert type(schemas) == list
        assert len(schemas) > 0

    def test_tables(self, db):
        tables = db.tables()

        assert type(tables) == list
        assert len(tables) > 0

    def test_table_columns(self, db):
        table_columns = db.table_columns("TEST", "TEST1")

        assert type(table_columns) == list
        assert len(table_columns) > 0

    def test_check_table_exist(self, db):

        assert db.check_table_exist("TEST.TEST1") is True
        assert db.check_table_exist("TEST.TEST_DOES_NOT_EXIST") is False

    def test_create_schema(self, db):
        db.execute("drop schema if exists TEST_CREATE_SCHEMA")
        db.create_schema("TEST_CREATE_SCHEMA")

        assert "TEST_CREATE_SCHEMA" in db.schemas()

    def test_get_max_column_value(self, db):

        assert db.get_max_column_value("TEST.TEST1", "ID_COL") == "3"

    def test_get_min_max_column_value(self, db):
        min, max = db.get_min_max_column_value("TEST.TEST1", "ID_COL")
        assert min == 1
        assert max == 3

    @pytest.mark.skip(reason="fails when max is supersmaller then _table_parallel_batch_size")
    def test_get_min_max_batch(self, db):
        min, max, batch = db.get_min_max_batch("TEST.TEST1", "ID_COL")
        assert min == 1
        assert max == 3
        assert batch == 1

    def test_truncate_table(self, db):
        db.truncate_table("TEST.TEST1")
        counts = db.query("select count(*) from TEST.TEST1")

        assert counts[0][0] == 0

