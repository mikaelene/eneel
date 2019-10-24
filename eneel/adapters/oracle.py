import cx_Oracle
import sys
import eneel.utils as utils

import logging
logger = logging.getLogger('main_logger')


def run_export_query(server, user, password, database, port, query, file_path, delimiter, rows=5000):
    try:
        db = Database(server, user, password, database, port)
        export = db.cursor.execute(query)
        rowcounts = 0
        while rows:
            try:
                rows = export.fetchmany(rows)
            except:
                return rowcounts
            rowcount = utils.export_csv(rows, file_path, delimiter)  # Method appends the rows in a file
            rowcounts = rowcounts + rowcount
        return rowcounts
        db.close()
    except Exception as e:
        logger.error(e)


class Database:
    def __init__(self, server, user, password, database, port=None, limit_rows=None, table_where_clause=None,
                 read_only=False, table_parallel_loads=10, table_parallel_batch_size=1000000):
        try:
            server_db = '{}:{}/{}'.format(server, port, database)
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

    def get_min_max_batch(self, table_name, column):
        try:
            sql = "SELECT MIN(" + column + "), MAX(" + column
            sql += "), ceil((max( " + column + ") - min("
            sql += column + ")) / (count(*)/" + str(self._table_parallel_batch_size) + ".0)) FROM " + table_name
            res = self.query(sql)
            min_value = int(res[0][0])
            max_value = int(res[0][1])
            batch_size_key = int(res[0][2])
            return min_value, max_value, batch_size_key
        except:
            logger.debug("Failed getting min, max and batch column value")

    def generate_export_query(self, columns, schema, table, replication_key=None, max_replication_key=None,
                              parallelization_where=None):
        # Generate SQL statement for extract
        select_stmt = "SELECT "
        # Add columns
        for col in columns:
            column_name = col[1]
            select_stmt += column_name + ", "
        select_stmt = select_stmt[:-2]

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

        #select_stmt += ";"

        return select_stmt


    def export_query(self, query, file_path, delimiter, rows=5000):
        rowcounts = run_export_query(self._server, self._user, self._password, self._database, self._port, query, file_path,
                         delimiter, rows=5000)
        return rowcounts

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
