import os
import cx_Oracle
import sys
import eneel.utils as utils
from concurrent.futures import ThreadPoolExecutor as Executor

import logging
logger = logging.getLogger('main_logger')


def run_export_cmd(cmd_file):
    cmd_code, cmd_message = utils.run_cmd(cmd_file)
    if cmd_code == 0:
        logger.debug(cmd_file + " exported")
        return 0
    else:
        logger.error(
            "Error exportng " + cmd_file + " : cmd_code: " + str(cmd_code) + " cmd_message: " + cmd_message)
        return 0


class Database:
    def __init__(self, server, user, password, database, port=None, limit_rows=None, table_where_clause=None, read_only=False):
        try:
            server_db = '{}:{}/{}'.format(server, port, database)
            self._server = server
            self._user = user
            self._password = password
            self._database = database
            self._server_db = server_db
            self._dialect = "oracle"
            self._limit_rows = limit_rows
            self._table_where_clause = table_where_clause
            self._read_only = read_only

            self._conn = cx_Oracle.connect(user, password, server_db)
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

    def fetchmany(self,rows):
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
            q = 'SELECT DISTINCT OWNER FROM ALL_TABLES'
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
        try:
            q = """
                SELECT 
                      COLUMN_ID AS ordinal_position,
                      COLUMN_NAME AS column_name,
                      DATA_TYPE AS data_type,
                      DATA_LENGTH AS character_maximum_length,
                      DATA_PRECISION AS numeric_precision,
                      DATA_SCALE AS numeric_scale
                FROM all_tab_cols
                WHERE 
                    owner = :s
                    and table_name = :t
                    AND COLUMN_ID IS NOT NULL
                    order by COLUMN_ID
                    """
            columns = self.query(q, [schema, table])
            return columns
        except:
            logger.error("Failed getting columns")

    def check_table_exist(self, table_name):
        try:
            check_statement = """
            SELECT 1
           FROM   ALL_TABLES 
           WHERE  OWNER || '.' || TABLE_NAME = '""" + table_name + "'"
            exists = self.query(check_statement)
            if exists:
                return True
            else:
                return False
        except:
            logger.error("Failed checking table exist")

    def truncate_table(self, table_name):
        return 'Not implemented for this adapter'

    def create_schema(self, schema):
        return 'Not implemented for this adapter'

    def get_max_column_value(self, table_name, column):
        return 'Not implemented for this adapter'

    def get_min_max_column_value(self, table_name, column):
        try:
            sql = "SELECT MIN(" + column + "), MAX(" + column + ") FROM " + table_name
            res = self.query(sql)
            min_value = int(res[0][0])
            max_value = int(res[0][1])
            return min_value, max_value
        except:
            logger.debug("Failed getting min and max column value")

    def generate_cmd_file(self, sql_file):
        cmd = "SET NLS_LANG=SWEDISH_SWEDEN.WE8ISO8859P1\n"
        cmd += "set NLS_NUMERIC_CHARACTERS=. \n"
        cmd += "set NLS_TIMESTAMP_TZ_FORMAT=YYYY-MM-DD HH24:MI:SS.FF\n"
        cmd += "sqlplus " + self._user + "/" + self._password + "@//" + self._server_db + " @" + sql_file
        logger.debug(cmd)
        return cmd

    def generate_spool_cmd(self, file_path, select_stmt):
        spool_cmd = """set markup csv on quote off
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
        return spool_cmd

    def generate_spool_query(self, columns, delimiter, schema, table, replication_key=None, max_replication_key=None, parallelization_where=None):
        # Generate SQL statement for extract
        select_stmt = "SELECT "
        for col in columns[:-1]:
            column_name = col[1]
            select_stmt += "REPLACE(" + column_name + ",chr(0),'')" + " || '" + delimiter + "' || \n"
        last_column_name = "REPLACE(" + columns[-1:][0][1] + ",chr(0),'')"
        select_stmt += last_column_name
        select_stmt += ' FROM ' + schema + "." + table

        # Where-claues for incremental replication
        if replication_key:
            replication_where = replication_key + " > " + "'" + max_replication_key + "'"
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

        select_stmt += ";\n"

        return select_stmt

    def export_table(self,
                     schema,
                     table,
                     columns,
                     path,
                     delimiter=',',
                     replication_key=None,
                     max_replication_key=None,
                     parallelization_key=None):
        try:
            # Add logic for parallelization_key
            if parallelization_key:
                min_parallelization_key, max_parallelization_key = self.get_min_max_column_value(schema + '.' + table,
                                                                                                 parallelization_key)
                batch_size = 1000000
                batch_id = 1
                batch_start = min_parallelization_key
                total_row_count = 0
                #file_paths = []
                #batch_stmts = []
                #delimiters = []
                #batches = []
                cmds_files = []
                while batch_start < max_parallelization_key:
                    file_name = self._database + "_" + schema + "_" + table + "_" + str(batch_id) + ".csv"
                    file_path = os.path.join(path, file_name)
                    parallelization_where = parallelization_key + ' between ' + str(batch_start) + ' and ' + str(batch_start + batch_size - 1)
                    batch_stmt = self.generate_spool_query(columns, delimiter, schema, table, replication_key, max_replication_key, parallelization_where)
                    spool_cmd = self.generate_spool_cmd(file_path, batch_stmt)

                    sql_file = os.path.join(path, self._database + "_" + schema + "_" + table + "_" + str(batch_id) + ".sql")
                    with open(sql_file, "w") as text_file:
                        text_file.write(spool_cmd)

                    cmd = self.generate_cmd_file(sql_file)
                    cmd_file = os.path.join(path, self._database + "_" + schema + "_" + table + "_" + str(batch_id) + ".cmd")
                    with open(cmd_file, "w") as text_file:
                        text_file.write(cmd)

                    #file_paths.append(file_path)
                    #batch_stmts.append(batch_stmt)
                    #delimiters.append(delimiter)
                    #batch = (batch_stmt, file_path)
                    #batches.append(batch)
                    cmds_files.append(cmd_file)
                    batch_start += batch_size
                    batch_id += 1

                try:
                    with Executor(max_workers=10) as executor:
                        for row_count in executor.map(utils.run_cmd, cmds_files):
                            total_row_count += row_count
                except Exception as exc:
                    logger.error(exc)

            else:
                # Generate SQL statement for extract
                select_stmt = self.generate_spool_query(columns, delimiter, schema, table, replication_key, max_replication_key)

                # Generate file name
                file_name = self._database + "_" + schema + "_" + table + ".csv"
                file_path = os.path.join(path, file_name)

                spool_cmd = self.generate_spool_cmd(file_path, select_stmt)

                sql_file = os.path.join(path, self._database + "_" + schema + "_" + table + ".sql")
                with open(sql_file, "w") as text_file:
                    text_file.write(spool_cmd)

                cmd = self.generate_cmd_file(sql_file)
                cmd_file = os.path.join(path, self._database + "_" + schema + "_" + table + ".cmd")
                with open(cmd_file, "w") as text_file:
                    text_file.write(cmd)

                total_row_count = run_export_cmd(cmd_file)

            return path, delimiter, None
        except:
            logger.error("Failed exporting table: " + schema + '.' + table)

    def insert_from_table_and_drop(self, schema, to_table, from_table):
        return 'Not implemented for this adapter'

    def switch_tables(self, schema, old_table, new_table):
        return 'Not implemented for this adapter'

    def import_table(self, schema, table, file, delimiter=','):
        return 'Not implemented for this adapter'

    def generate_create_table_ddl(self, schema, table, columns):
        return 'Not implemented for this adapter'

    def create_table_from_columns(self, schema, table, columns):
        return 'Not implemented for this adapter'

    def create_log_table(self, schema, table):
        return 'Not implemented for this adapter'

    def log(self, schema, table,
            project=None,
            project_started_at=None,
            source_table=None,
            target_table=None,
            started_at=None,
            ended_at=None,
            status=None,
            exported_rows=None,
            imported_rows=None):
        return 'Not implemented for this adapter'
