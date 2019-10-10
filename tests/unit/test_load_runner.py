from eneel.load_runner import *
from eneel.adapters.postgres import *
import pytest

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
    drop schema if exists load_runner cascade;

    create schema load_runner;

    create table load_runner.test1(
    id_col 			int,
    name_col		varchar(64),
    datetime_col	timestamp
    ); 

    insert into load_runner.test1 values(1, 'First', '2019-10-01 11:00:00');
    insert into load_runner.test1 values(2, 'Second', '2019-10-02 12:00:00');
    insert into load_runner.test1 values(3, 'Third', '2019-10-03 13:00:00');
    """
    db.execute(setup_sql)

    yield db

    teardown_sql = """
    drop table load_runner.test1;
    drop schema load_runner;
    """
    db.execute(teardown_sql)

    db.close()


def test_export_table(db, tmp_path):
    table_columns = db.table_columns('load_runner', 'test1')
    return_code, temp_path_load, delimiter, export_row_count = export_table("ERROR", 1, 1, db, 'load_runner', 'test1',
                                                                            table_columns, tmp_path, '|',
                                                                            replication_key=None,
                                                                            max_replication_key=None,
                                                                            parallelization_key=None)
    assert return_code == 'RUN'
    assert temp_path_load == tmp_path
    assert delimiter == '|'
    assert export_row_count == 3

def test_create_temp_table(db):
    table_columns = db.table_columns('load_runner', 'test1')
    return_code = create_temp_table("ERROR", 1, 1, db, 'load_runner', 'test1_tmp', table_columns, 'load_runner.test1')

    assert return_code == 'RUN'