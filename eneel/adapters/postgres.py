import sys
import psycopg2
import psycopg2.extras
from time import time
from datetime import datetime
import eneel.utils as utils
import re

import logging

logger = logging.getLogger("main_logger")


def run_import_file(
    server, user, password, database, port, schema_table, file_path, delimiter
):
    db = Database(server, user, password, database, port)
    # Create and run the cmd
    sql = "COPY %s FROM STDIN WITH DELIMITER AS '%s'"
    file = open(file_path, "r")
    try:
        db.cursor.copy_expert(sql=sql % (schema_table, delimiter), file=file)
        row_count = db.cursor.rowcount
        return row_count
    except psycopg2.Error as e:
        logger.error(e)
    finally:
        db.close()


def run_export_query(
    server, user, password, database, port, query, file_path, delimiter, rows=5000
):
    try:
        db = Database(server, user, password, database, port)
        db.cursor.execute(query)
        rowcounts = 0
        while True:
            try:
                fetched_rows = db.cursor.fetchmany(rows)
                rowcount = utils.export_csv(fetched_rows, file_path, delimiter)
                if not fetched_rows:
                    db.close()
                    return rowcounts
                rowcounts = rowcounts + rowcount
            except Exception as e:
                logger.error(e)
                db.close()
                return rowcounts
    except Exception as e:
        logger.error(e)


def python_type_to_db_type(python_type):
    if python_type in ("str", "unicode"):
        return "varchar"
    elif python_type in ("bytes", "bytearray", "memoryview", "buffer"):
        return "bytea"
    elif python_type == "bool":
        return "bool"
    elif python_type == "datetime.date":
        return "date"
    elif python_type == "datetime.time":
        return "time"
    elif python_type == "datetime.datetime":
        return "timestamp"
    elif python_type in ("int", "long"):
        return "int"
    elif python_type == "float":
        return "real"
    elif python_type in "decimal.Decimal":
        return "numeric"
    elif python_type == "UUID.uuid":
        return "uuid"
    elif python_type == "timedelta":
        return "interval"
    else:
        return python_type


class Database:
    def __init__(
        self,
        server,
        user,
        password,
        database,
        port=5432,
        limit_rows=None,
        table_where_clause=None,
        read_only=False,
        table_parallel_loads=10,
        table_parallel_batch_size=10000000,
    ):
        try:
            conn_string = (
                "host="
                + server
                + " dbname="
                + database
                + " user="
                + user
                + " password="
                + password
            )
            self._server = server
            self._user = user
            self._password = password
            self._database = database
            self._port = port
            self._dialect = "postgres"
            self._limit_rows = limit_rows
            self._table_where_clause = table_where_clause
            self._read_only = read_only
            self._table_parallel_loads = table_parallel_loads
            self._table_parallel_batch_size = table_parallel_batch_size

            self._conn = psycopg2.connect(conn_string)
            self._conn.autocommit = True
            self._cursor = self._conn.cursor()
            logger.debug("Connection to postgres successful")
        except psycopg2.Error as e:
            logger.error(e)
            sys.exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self._conn.close()

    def close(self):
        self._conn.close()
        logger.debug("Connection closed")

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
        except psycopg2.Error as e:
            logger.error(e)

    def execute_many(self, sql, values):
        try:
            return self.cursor.executemany(sql, values)
        except psycopg2.Error as e:
            logger.error(e)

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except psycopg2.Error as e:
            logger.error(e)

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        except psycopg2.Error as e:
            logger.error(e)

    def fetchmany(self, rows):
        try:
            return self.cursor.fetchmany(rows)
        except psycopg2.Error as e:
            logger.error(e)

    def query(self, sql, params=None):
        try:
            self.cursor.execute(sql, params or ())
            return self.fetchall()
        except psycopg2.Error as e:
            logger.error(e)

    def schemas(self):
        try:
            q = "SELECT schema_name FROM information_schema.schemata"
            schemas = self.query(q)
            return [row[0] for row in schemas]
        except:
            logger.error("Failed getting schemas")

    def tables(self):
        try:
            q = "select table_schema || '.' || table_name from information_schema.tables"
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
            query = "SELECT * FROM (" + query + ") q fetch first 1 row only"
            self.execute(query)
            data = self.fetchone()
            cursor_columns = self.cursor.description

        except:
            logger.error("Failed getting query columns")
            return
        try:
            columns = []
            for i in range(len(cursor_columns)):
                ordinal_position = i
                column_name = cursor_columns[i][0]
                data_type = re.findall(r"'(.+?)'", str(type(data[i])))[0]
                if data_type == "str":
                    character_maximum_length = cursor_columns[i][3]
                #    if character_maximum_length == -1:
                #        data_type = "text"
                else:
                    character_maximum_length = None
                if data_type in ("decimal.Decimal", "decimal", "int"):
                    numeric_precision = cursor_columns[i][4]
                else:
                    numeric_precision = None
                if data_type in ("decimal.Decimal", "decimal", "int"):
                    numeric_scale = cursor_columns[i][5]
                else:
                    numeric_scale = None
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
        except:
            logger.error("Failed generating db types from cursor description")

    def check_table_exist(self, table_name):
        try:
            check_statement = """
            SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema || '.' || table_name = '"""
            check_statement += table_name.lower() + "')"
            exists = self.query(check_statement)
            return exists[0][0]
        except:
            logger.error("Failed checking table exist")

    def truncate_table(self, table_name):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            sql = "TRUNCATE TABLE " + table_name
            self.execute(sql)
            logger.debug("Table " + table_name + " truncated")
        except:
            logger.error("Failed truncating table")

    def create_schema(self, schema):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            if schema in self.schemas():
                logger.debug("Schema exists")
            else:
                create_statement = "CREATE SCHEMA " + schema
                self.execute(create_statement)
                logger.debug("Schema" + schema + " created")
        except:
            logger.error("Failed creating schema")

    def get_max_column_value(self, table_name, column):
        try:
            sql = "SELECT MAX(" + column + ")::text FROM " + table_name
            max_value = self.query(sql)
            return max_value[0][0]
        except:
            logger.debug("Failed getting max column value")

    def get_min_max_column_value(self, table_name, column):
        try:
            sql = "SELECT MIN(" + column + "), MAX(" + column + ") FROM " + table_name
            res = self.query(sql)
            min_value = res[0][0]
            max_value = res[0][1]
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
            min_value = res[0][0]
            max_value = res[0][1]
            batch_size_key = res[0][2]
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

    def export_query(self, query, file_path, delimiter, rows=5000):
        rowcounts = run_export_query(
            self._server,
            self._user,
            self._password,
            self._database,
            self._port,
            query,
            file_path,
            delimiter,
            rows=rows,
        )
        return rowcounts

    def insert_from_table_and_drop(self, schema, to_table, from_table):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        to_schema_table = schema + "." + to_table
        from_schema_table = schema + "." + from_table
        try:
            self.execute(
                "INSERT INTO "
                + to_schema_table
                + " SELECT * FROM  "
                + from_schema_table
            )
            self.execute("DROP TABLE " + from_schema_table)
            return_code = "RUN"
        except:
            logger.error("Failed to insert_from_table_and_drop")
            return_code = "ERROR"
        finally:
            return return_code

    def switch_tables(self, schema, old_table, new_table):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:

            old_schema_table = schema + "." + old_table
            new_schema_table = schema + "." + new_table
            delete_table = old_table + "_delete"
            delete_schema_table = schema + "." + delete_table

            if self.check_table_exist(old_schema_table):
                self.execute(
                    "ALTER TABLE " + old_schema_table + " RENAME TO " + delete_table
                )
                self.execute(
                    "ALTER TABLE " + new_schema_table + " RENAME TO " + old_table
                )
                self.execute("DROP TABLE " + delete_schema_table)
                logger.debug("Switched tables")
            else:
                self.execute(
                    "ALTER TABLE " + new_schema_table + " RENAME TO " + old_table
                )
                logger.debug("Renamed temp table")
            return_code = "RUN"
        except:
            logger.error("Failed to switch tables")
            return_code = "ERROR"
        finally:
            return return_code

    def import_file(self, schema, table, path, delimiter=","):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        schema_table = schema + "." + table
        row_count = run_import_file(
            self._server,
            self._user,
            self._password,
            self._database,
            self._port,
            schema_table,
            path,
            delimiter,
        )
        return row_count

    def generate_create_table_ddl(self, schema, table, columns):
        try:
            create_table_sql = "CREATE TABLE " + schema + "." + table + "(\n"
            for col in columns:

                ordinal_position = col[0]
                column_name = col[1]
                data_type = col[2]
                data_type = python_type_to_db_type(data_type)
                character_maximum_length = col[3]
                numeric_precision = col[4]
                numeric_scale = col[5]

                if data_type == "varchar":
                    if character_maximum_length == -1:
                        column = column_name + " text"
                    else:
                        column = (
                            column_name
                            + " varchar"
                            + "("
                            + str(character_maximum_length)
                            + ")"
                        )
                elif data_type == "numeric":
                    column = (
                        column_name
                        + " numeric("
                        + str(numeric_precision)
                        + ","
                        + str(numeric_scale)
                        + ")"
                    )
                else:
                    column = column_name + " " + data_type

                create_table_sql += column + ", \n"
            create_table_sql = create_table_sql[:-3]
            create_table_sql += ")"

            print(create_table_sql)

            return create_table_sql
        except Exception as e:
            print(e)
            logger.error("Failed generating create table script")

    def create_table_from_columns(self, schema, table, columns):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            table_exists = self.query(
                "SELECT * FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = %s AND "
                "TABLE_NAME = %s",
                [schema, table],
            )

            if table_exists:
                self.execute("DROP TABLE " + schema + "." + table)

            self.create_schema(schema)
            create_table_sql = self.generate_create_table_ddl(schema, table, columns)
            self.execute(create_table_sql)
            logger.debug("table created")
        except:
            logger.error("Failed create table from columns")

    def create_log_table(self, schema, table):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")

        full_table = schema + "." + table

        if self.check_table_exist(full_table):
            logger.debug("Log table exist")
            return

        ddl = "create table "
        ddl += full_table
        ddl += """(
        log_time    timestamp,
        project	varchar(128),
        project_started_at	timestamp,
        source_table	varchar(128),
        target_table	varchar(128),
        started_at	timestamp,
        ended_at	timestamp,
        status		varchar(128),
        exported_rows	int,
        imported_rows	int
        );"""

        self.create_schema(schema)
        self.execute(ddl)
        logger.debug(full_table + " created")

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

        full_table = schema + "." + table
        log_time = datetime.fromtimestamp(time())
        row = [
            log_time,
            project,
            project_started_at,
            source_table,
            target_table,
            started_at,
            ended_at,
            status,
            exported_rows,
            imported_rows,
        ]

        sql = "INSERT INTO " + full_table
        sql += " (log_time, project, project_started_at, source_table, target_table, started_at, ended_at, status, exported_rows, imported_rows)"
        sql += " VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        self.execute(sql, row)
