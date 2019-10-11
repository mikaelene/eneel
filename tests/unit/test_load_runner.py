from eneel.load_runner import *
from eneel.adapters.postgres import *
import pytest

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


@pytest.fixture
def db(tmp_path):
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
    
    create table load_runner.test1_tmp_test(
    id_col 			int,
    name_col		varchar(64),
    datetime_col	timestamp
    ); 

    insert into load_runner.test1_tmp_test values(1, 'First', '2019-10-01 11:00:00');
    insert into load_runner.test1_tmp_test values(2, 'Second', '2019-10-02 12:00:00');
    insert into load_runner.test1_tmp_test values(3, 'Third', '2019-10-03 13:00:00');
    
    create table load_runner.test1_tmp_empty(
    id_col 			int,
    name_col		varchar(64),
    datetime_col	timestamp
    ); 

    """
    db.execute(setup_sql)

    # export table
    table_columns = db.table_columns('load_runner', 'test1')
    _, _, _, _ = export_table("ERROR", 1, 1, db, 'load_runner', 'test1',
                              table_columns, tmp_path, '|',
                              replication_key=None,
                              max_replication_key=None,
                              parallelization_key=None)

    yield db

    teardown_sql = """
    drop schema load_runner CASCADE;
    """
    db.execute(teardown_sql)

    db.close()


# I NEED TO TEST FAILING FOR THESE AS WELL


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
    return_code = create_temp_table("ERROR", 1, 1, db, 'load_runner', 'test1_tmp_tmp', table_columns, 'load_runner.test1')

    assert return_code == 'RUN'
    assert db.check_table_exist('load_runner.test1') is True


def test_import_into_temp_table(db, tmp_path):
    # import into tmp table
    return_code, import_row_count = import_into_temp_table("ERROR", 1, 1, db, 'load_runner', 'test1_tmp_empty', tmp_path, '|',
                                                           'load_runner.test1_tmp')

    assert return_code == 'RUN'
    assert import_row_count == 3


def test_switch_table(db, tmp_path):
    return_code = switch_table("ERROR", 1, 1, db, 'load_runner', 'test1', 'test1_tmp_test', 'load_runner.test1')

    assert return_code == 'RUN'


def test_insert_from_table_and_drop_tmp(db, tmp_path):
    return_code = insert_from_table_and_drop_tmp("ERROR", 1, 1, db, 'load_runner', 'test1', 'test1_tmp_test',
                                                 'load_runner.test1')

    assert return_code == 'RUN'


def test_strategy_full_table_load(db, tmp_path):
    table_columns = db.table_columns('load_runner', 'test1')
    return_code, export_row_count, import_row_count = strategy_full_table_load("ERROR", 1, 1, db, 'load_runner',
                                                                               'test1', table_columns, tmp_path, '|',
                                                                               db, 'load_runner', 'test1_target',
                                                                               'id_col')

    assert return_code == 'DONE'
    assert export_row_count == 3
    assert import_row_count == 3
