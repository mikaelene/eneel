import cx_Oracle
import sys
import eneel.utils as utils
import decimal
import os
import re

import logging

logger = logging.getLogger("main_logger")


def run_export_cmd(cmd_commands):
    envs = [['NLS_LANG', 'SWEDISH_SWEDEN.AL32UTF8']]
    logger.debug(cmd_commands)
    cmd_code, cmd_message = utils.run_cmd(cmd_commands, envs)
    if cmd_code == 0:
        logger.debug(cmd_commands[2] + " exported")
        return 0
    else:
        logger.error(
            "Error exportng " + cmd_commands[2] + " : cmd_code: " + str(cmd_code) + " cmd_message: " + cmd_message)
        return 0


def generate_spool_query(query, delimiter):
    # Generate SQL statement for extract

    # From
    query_from = query.split(' FROM ', 1)[1]

    # Columns
    query_cols = query.split('SELECT ', 1)[1].split(' FROM ')[0]
    columns = re.split('(?<!\(.),(?!.\))', query_cols)

    for index, col in enumerate(columns):
        col_parts = col.strip().split(' ')
        if len(col_parts) > 1:
            columns[index] = ' '.join(col_parts[:-1])
        # print(columns[index])

    select_stmt = "SELECT "
    for col in columns[:-1]:
        # print(col)
        column_name = col
        select_stmt += "REPLACE(" + column_name + ",chr(0),'')" + " || '" + delimiter + "' || \n"
    last_column_name = "REPLACE(" + columns[-1:][0] + ",chr(0),'')"
    select_stmt += last_column_name
    select_stmt += " FROM " + query_from
    select_stmt += ";\n"

    logger.debug(select_stmt)

    return select_stmt


def generate_spool_cmd(file_path, select_stmt):
    spool_cmd = """
alter session set NLS_NUMERIC_CHARACTERS = '. ';
alter session set NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF';
alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
set markup csv on quote off
set term off
set echo off
set trimspool on 
set trimout on
set feedback off
Set serveroutput off
set heading off
set arraysize 5000
SET LONG 32767 
spool """

    spool_cmd += file_path + '\n'
    spool_cmd += select_stmt
    spool_cmd += "spool off\n"
    spool_cmd += "exit"
    logger.debug(spool_cmd)
    logger.debug(spool_cmd)
    return spool_cmd


def generate_cmd_command(server, user, password, database, port, sql_file):
    server_db = "{}:{}/{}".format(server, port, database)
    cmd = 'sqlplus'
    ora_conn = user + "/" + password + "@//" + server_db
    sqlfile = '@' + sql_file
    cmd_to_run = [cmd, ora_conn, sqlfile]
    return cmd_to_run


def run_export_query(
    server, user, password, database, port, query, file_path, delimiter
):
    # Generate SQL statement for extract
    select_stmt = generate_spool_query(query, delimiter)

    spool_cmd = generate_spool_cmd(file_path, select_stmt)

    sql_file = file_path.replace('.csv', '.sql')
    with open(sql_file, "w") as text_file:
        text_file.write(spool_cmd)

    cmd_commands = generate_cmd_command(server, user, password, database, port, sql_file)

    total_row_count = run_export_cmd(cmd_commands)

    return total_row_count


def output_type_handler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.NUMBER:
        return cursor.var(str, 100, cursor.arraysize, outconverter=decimal.Decimal)
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)


class Database:
    def __init__(
        self,
        server,
        user,
        password,
        database,
        port=None,
        limit_rows=None,
        table_where_clause=None,
        read_only=False,
        table_parallel_loads=10,
        table_parallel_batch_size=1000000,
    ):
        try:
            server_db = "{}:{}/{}".format(server, port, database)
            self._server = server
            self._user = user
            self._password = password
            self._database = database
            self._port = port
            self._server_db = server_db
            self._dialect = "oracle"
            self._limit_rows = limit_rows
            self._table_where_clause = table_where_clause
            self._read_only = read_only
            self._table_parallel_loads = table_parallel_loads
            self._table_parallel_batch_size = table_parallel_batch_size

            os.environ["NLS_LANG"] = "SWEDISH_SWEDEN.AL32UTF8"
            self._conn = cx_Oracle.connect(user, password, server_db)

            self._conn.outputtypehandler = output_type_handler

            self._cursor = self._conn.cursor()
            logger.debug("Connection to oracle successful")
        except cx_Oracle.Error as e:
            logger.error(e)
            sys.exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self._conn.close()

    def close(self):
        self._conn.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        try:
            return self.cursor.execute(sql, params or ())
        except cx_Oracle.Error as e:
            logger.error(e)

    def execute_many(self, sql, values):
        try:
            return self.cursor.executemany(sql, values)
        except cx_Oracle.Error as e:
            logger.error(e)

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except cx_Oracle.Error as e:
            logger.error(e)

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        except cx_Oracle.Error as e:
            logger.error(e)

    def fetchmany(self, rows):
        try:
            return self.cursor.fetchmany(rows)
        except cx_Oracle.Error as e:
            logger.error(e)

    def query(self, sql, params=None):
        try:
            self.cursor.execute(sql, params or ())
            return self.fetchall()
        except cx_Oracle.Error as e:
            logger.error(e)

    def schemas(self):
        try:
            q = "SELECT DISTINCT OWNER FROM ALL_TABLES"
            schemas = self.query(q)
            return schemas
        except:
            logger.error("Failed getting schemas")

    def tables(self):
        try:
            q = "select OWNER || '.' || TABLE_NAME from ALL_TABLES"
            tables = self.query(q)
            return tables
        except:
            logger.error("Failed getting tables")

    def table_columns(self, schema, table):
        query = "SELECT * FROM " + schema + "." + table
        columns = self.query_columns(query)
        return columns

    def query_columns(self, query):
        try:
            query = "SELECT * FROM (" + query + ") q WHERE ROWNUM <= 1"
            cursor_columns = self.execute(query).description
        except:
            logger.error("Failed getting query columns")
            return
        try:
            columns = []
            for column in cursor_columns:
                ordinal_position = cursor_columns.index(column)
                column_name = column[0]
                data_type = re.findall(r"'(.+?)'", str(column[1]))[0]
                character_maximum_length = None
                numeric_precision = None
                numeric_scale = None
                if data_type in (
                    "cx_Oracle.BLOB",
                    "cx_Oracle.OBJECT",
                    "cx_Oracle.BFILE",
                    "cx_Oracle.NCLOB",
                ):
                    data_type = "bytes"
                elif data_type in ("cx_Oracle.DATETIME", "cx_Oracle.TIMESTAMP"):
                    data_type = "datetime.datetime"
                elif data_type == "cx_Oracle.STRING":
                    data_type = "str"
                    character_maximum_length = column[3]
                elif data_type == "cx_Oracle.CLOB":
                    data_type = "str"
                    character_maximum_length = -1
                elif data_type in ("cx_Oracle.NUMBER"):
                    if column[5] == 0:
                        data_type = "int"
                    elif column[5] < 0:
                        data_type = "float"
                    else:
                        data_type = "decimal.Decimal"
                        numeric_precision = column[4]
                        numeric_scale = column[5]
                # data_type = python_type_to_db_type(data_type)

                column = (
                    ordinal_position + 1,
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                )
                columns.append(column)
            return columns
        except Exception as e:
            logger.error(e)
            logger.error("Failed generating db types from cursor description")

    def remove_unsupported_columns(self, columns):
        columns_to_keep = columns.copy()
        for column in columns:
            data_type = column[2]
            character_maximum_length = column[3]
            if data_type == "str" and character_maximum_length > 4000 \
                    or data_type == "str" and character_maximum_length == -1:
                columns_to_keep.remove(column)
            if data_type in ("bytearray"):
                columns_to_keep.remove(column)
        return columns_to_keep

    def check_table_exist(self, table_name):
        try:
            check_statement = (
                """
            SELECT 1
           FROM   ALL_TABLES 
           WHERE  OWNER || '.' || TABLE_NAME = '"""
                + table_name
                + "'"
            )
            exists = self.query(check_statement)
            if exists:
                return True
            else:
                return False
        except:
            logger.error("Failed checking table exist")

    def truncate_table(self, table_name):
        return "Not implemented for this adapter"

    def create_schema(self, schema):
        return "Not implemented for this adapter"

    def get_max_column_value(self, table_name, column):
        return "Not implemented for this adapter"

    def get_min_max_column_value(self, table_name, column):
        try:
            sql = "SELECT MIN(" + column + "), MAX(" + column + ") FROM " + table_name
            res = self.query(sql)
            min_value = int(res[0][0])
            max_value = int(res[0][1])
            return min_value, max_value
        except:
            logger.debug("Failed getting min and max column value")

    def get_min_max_batch(self, table_name, column):
        try:
            sql = "SELECT MIN(" + column + "), MAX(" + column
            sql += "), ceil((max( " + column + ") - min("
            sql += (
                column
                + ")) / (count(*)/"
                + str(self._table_parallel_batch_size)
                + ".0)) FROM "
                + table_name
            )
            res = self.query(sql)
            min_value = int(res[0][0])
            max_value = int(res[0][1])
            batch_size_key = int(res[0][2])
            return min_value, max_value, batch_size_key
        except:
            logger.debug("Failed getting min, max and batch column value")

    def generate_export_query(
        self,
        columns,
        schema,
        table,
        replication_key=None,
        max_replication_key=None,
        parallelization_where=None,
    ):
        # Generate SQL statement for extract
        select_stmt = "SELECT "
        # Add columns
        for col in columns:
            column_name = col[1]
            select_stmt += column_name + ", "
        select_stmt = select_stmt[:-2]

        select_stmt += " FROM " + schema + "." + table

        # Where-claues for incremental replication
        if replication_key:
            replication_where = (
                replication_key + " > " + "'" + max_replication_key + "'"
            )
        else:
            replication_where = None

        wheres = replication_where, self._table_where_clause, parallelization_where
        wheres = [x for x in wheres if x is not None]
        if len(wheres) > 0:
            select_stmt += " WHERE " + wheres[0]
            for where in wheres[1:]:
                select_stmt += " AND " + where

        if self._limit_rows:
            select_stmt += " FETCH FIRST " + str(self._limit_rows) + " ROW ONLY"

        return select_stmt

    def export_query(self, query, file_path, delimiter):
        rowcounts = 0
        #rowcounts = run_export_query(
        run_export_query(
            self._server,
            self._user,
            self._password,
            self._database,
            self._port,
            query,
            file_path,
            delimiter,
        )
        return rowcounts

    def insert_from_table_and_drop(self, schema, to_table, from_table):
        return "Not implemented for this adapter"

    def switch_tables(self, schema, old_table, new_table):
        return "Not implemented for this adapter"

    def import_table(self, schema, table, file, delimiter=","):
        return "Not implemented for this adapter"

    def generate_create_table_ddl(self, schema, table, columns):
        return "Not implemented for this adapter"

    def create_table_from_columns(self, schema, table, columns):
        return "Not implemented for this adapter"

    def create_log_table(self, schema, table):
        return "Not implemented for this adapter"

    def log(
        self,
        schema,
        table,
        project=None,
        project_started_at=None,
        source_table=None,
        target_table=None,
        started_at=None,
        ended_at=None,
        status=None,
        exported_rows=None,
        imported_rows=None,
    ):
        return "Not implemented for this adapter"
