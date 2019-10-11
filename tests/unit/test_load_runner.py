from eneel.load_runner import *
from eneel.adapters.postgres import *
import datetime
import pytest

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

@pytest.fixture
def project_load(tmp_path):
    db = Database(
        os.getenv('POSTGRES_TEST_HOST'),
        os.getenv('POSTGRES_TEST_USER'),
        os.getenv('POSTGRES_TEST_PASS'),
        os.getenv('POSTGRES_TEST_DBNAME'),
        os.getenv('POSTGRES_TEST_PORT'))

    setup_sql = """
    drop schema if exists test_run_load cascade;

    create schema test_run_load;

    create table test_run_load.test1(
    id_col 			int,
    name_col		varchar(64),
    datetime_col	timestamp
    ); 

    insert into test_run_load.test1 values(1, 'First', '2019-10-01 11:00:00');
    insert into test_run_load.test1 values(2, 'Second', '2019-10-02 12:00:00');
    insert into test_run_load.test1 values(3, 'Third', '2019-10-03 13:00:00');
    """

    db.execute(setup_sql)

    db.create_log_table('test_logging', 'run_log')

    project_load = {'load_order': 1, 'num_tables_to_load': 2, 'project_name': 'test_project', 'source_conninfo': {'name': 'ource', 'type': 'postgres', 'read_only': None, 'target': 'dev', 'credentials': {'host': os.getenv('POSTGRES_TEST_HOST'), 'port': os.getenv('POSTGRES_TEST_PORT'), 'user': os.getenv('POSTGRES_TEST_USER'), 'password': os.getenv('POSTGRES_TEST_PASS'), 'database': os.getenv('POSTGRES_TEST_DBNAME'), 'table_parallel_loads': 10, 'table_parallel_batch_size': 500}}, 'target_conninfo': {'name': 'target', 'type': 'postgres', 'read_only': None, 'target': 'dev', 'credentials': {'host': os.getenv('POSTGRES_TEST_HOST'), 'port': os.getenv('POSTGRES_TEST_PORT'), 'user': os.getenv('POSTGRES_TEST_USER'), 'password': os.getenv('POSTGRES_TEST_PASS'), 'database': os.getenv('POSTGRES_TEST_DBNAME')}}, 'logdb': {'conninfo': {'name': 'logging', 'type': 'postgres', 'read_only': None, 'target': 'dev', 'credentials': {'host': os.getenv('POSTGRES_TEST_HOST'), 'port': os.getenv('POSTGRES_TEST_PORT'), 'user': os.getenv('POSTGRES_TEST_USER'), 'password': os.getenv('POSTGRES_TEST_PASS'), 'database': os.getenv('POSTGRES_TEST_DBNAME')}}, 'schema': 'test_logging', 'table': 'run_log'}, 'project': {'id': 'project id', 'name': 'Postgres to postgres', 'owner': 'somebody@yourcompany.com', 'temp_path': '/Users/mikaelene/Dev/eneel_projects/tempfiles', 'keep_tempfiles': False, 'csv_delimiter': '|', 'parallel_loads': 2, 'source': 'pg_source', 'source_columntypes_to_exclude': 'timestamp, boolean', 'target': 'pg_target', 'logdb': 'pg_logging', 'logtable': 'run_log'}, 'schema': {'source_schema': 'test_run_load', 'target_schema': 'test_run_load_target'}, 'table': {'table_name': 'test1', 'replication_method': 'FULL_TABLE', 'parallelization_key': 'id_col'}, 'temp_path': tmp_path, 'project_started_at': datetime.datetime(2019, 10, 11, 19, 31, 15, 809923)}

    yield project_load

    teardown_sql = """
        drop schema test_run_load CASCADE;
        drop schema test_logging CASCADE;
        """
    db.execute(teardown_sql)

    db.close()


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
    
    create table load_runner.test1_inc_test(
    id_col 			int,
    name_col		varchar(64),
    datetime_col	timestamp
    ); 

    insert into load_runner.test1_inc_test values(1, 'First', '2019-10-01 11:00:00');
    insert into load_runner.test1_inc_test values(2, 'Second', '2019-10-02 12:00:00');
    insert into load_runner.test1_inc_test values(3, 'Third', '2019-10-03 13:00:00');
    insert into load_runner.test1_inc_test values(4, 'Forth', '2019-10-04 12:00:00');
    insert into load_runner.test1_inc_test values(5, 'Fifth', '2019-10-05 13:00:00');

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
    full_load_path = tmp_path /'full_load'
    full_load_path.mkdir()
    return_code, export_row_count, import_row_count = strategy_full_table_load("ERROR", 1, 1, db, 'load_runner',
                                                                               'test1', table_columns, full_load_path, '|',
                                                                               db, 'load_runner', 'test1_target',
                                                                               'id_col')

    assert return_code == 'DONE'
    assert export_row_count == 3
    assert import_row_count == 3


def test_strategy_incremental(db, tmp_path):
    table_columns = db.table_columns('load_runner', 'test1_inc_test')
    inc_load_path = tmp_path / 'inc_load'
    inc_load_path.mkdir()
    return_code, export_row_count, import_row_count = strategy_incremental("ERROR", 1, 1, db, 'load_runner',
                                                                               'test1_inc_test', table_columns,
                                                                               inc_load_path, '|', db, 'load_runner',
                                                                               'test1', 'id_col', 'id_col')
    assert return_code == 'DONE'
    assert export_row_count == 2
    assert import_row_count == 2


def test_run_load(project_load):
    return_code = run_load(project_load)

    assert return_code == 'DONE'