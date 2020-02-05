import sys
import pyodbc
from time import time
from datetime import datetime
import eneel.utils as utils
import re

import logging

logger = logging.getLogger("main_logger")


def run_import_file(
    server,
    database,
    user,
    password,
    trusted_connection,
    codepage,
    schema,
    table,
    file_path,
    delimiter,
):

    try:
        # Import data
        bcp_in = ["bcp"]
        bcp_in.append(f"[{database}].[{schema}].[{table}]")
        bcp_in.append("in")
        bcp_in.append(file_path)
        bcp_in.append(f"-t{delimiter}")
        bcp_in.append("-c")
        bcp_in.append(f"-C{codepage}")
        bcp_in.append("-b100000")
        bcp_in.append(f"-S{server}")
        if trusted_connection:
            bcp_in.append("-T")
        else:
            bcp_in.append(f"-U{user}")
            bcp_in.append(f"-P{password}")

        logger.debug(bcp_in)
        cmd_code, cmd_message = utils.run_cmd(bcp_in)
        return_code = "ERROR"
        row_count = 0

        if cmd_code == 0:
            try:
                errors = cmd_message.count("Error")
                if errors > 0:
                    logger.error(
                        f"Importing in {schema}.{table} completed with errors"
                    )
                return_message = cmd_message.splitlines()
                try:
                    row_count = int(return_message[-3].split()[0])
                    return_code = "RUN"
                except:
                    if return_message[2].split()[0] == "SQLState":
                        logger.debug(cmd_message)
                        return_code = "WARN"
            except:
                logger.warning(f"{table}: Failed to parse sucessfull import cmd")
        else:
            logger.error(f"Error importing {schema}.{table}")
            logger.debug(cmd_message)
    except:
        logger.error("Failed importing table")
    return return_code, row_count


def run_export_query(
        server,
        user,
        password,
        trusted_connection,
        query,
        file_path,
        delimiter,
        codepage='65001',
):
    # Export data
    # Generate bcp command
    bcp_out = ['bcp']
    bcp_out.append(query)
    bcp_out.append('queryout')
    bcp_out.append(file_path)
    bcp_out.append('-t' + delimiter)
    bcp_out.append('-c')
    bcp_out.append('-C' + codepage)
    bcp_out.append('-S' + server)
    if trusted_connection:
        bcp_out.append('-T')
    else:
        bcp_out.append('-U' + user)
        bcp_out.append('-P' + password)

    logger.debug(bcp_out)

    cmd_code, cmd_message = utils.run_cmd(bcp_out)
    if cmd_code == 0:
        try:
            return_message = cmd_message.splitlines()
            row_count = int(return_message[-3].split()[0])
            timing = str(return_message[-1].split()[5])
            if row_count > 0:
                average = str(return_message[-1].split()[8][1:-3])
            else:
                average = '0'
            logger.debug(f"{query}: {str(row_count)} rows exported, in {timing} ms. at an average of {average} rows per sec")
            return row_count
        except:
            logger.warning(f"{query} : Failed to parse sucessfull export cmd for")
        logger.debug(f"{query} exported")
    else:
        logger.error(f"Error exportng {query}: {cmd_message}")


def python_type_to_db_type(python_type):
    if python_type == "str":
        return "nvarchar"
    elif python_type in ("bytes", "bytearray", "memoryview", "buffer"):
        return "binary"
    elif python_type == "bool":
        return "bit"
    elif python_type == "datetime.date":
        return "date"
    elif python_type in ("datetime.time", "timedelta"):
        return "time"
    elif python_type == "datetime.datetime":
        return "datetime2"
    elif python_type in ("int", "long"):
        return "bigint"
    elif python_type == "float":
        return "float"
    elif python_type == "decimal.Decimal":
        return "numeric"
    elif python_type == "UUID.uuid":
        return "UNIQUEIDENTIFIER"
    else:
        return python_type


class Database:
    def __init__(
        self,
        driver,
        server,
        database,
        port=1433,
        limit_rows=None,
        user=None,
        password=None,
        trusted_connection=None,
        as_columnstore=False,
        read_only=False,
        codepage=None,
        table_parallel_loads=10,
        table_parallel_batch_size=10000000,
        table_where_clause=None,
    ):
        try:
            conn_string = (
                "DRIVER={"
                + driver
                + "};SERVER="
                + server
                + ";DATABASE="
                + database
                + ";PORT="
                + str(port)
            )
            if trusted_connection:
                conn_string += ";trusted_connection=yes"
            else:
                conn_string += f";UID={user};PWD={password}"
            self._driver = driver
            self._server = server
            self._user = user
            self._password = password
            self._database = database
            self._port = port
            self._dialect = "sqlserver"
            self._limit_rows = limit_rows
            self._as_columnstore = as_columnstore
            self._trusted_connection = trusted_connection
            self._read_only = read_only
            if codepage:
                self._codepage = codepage
            else:
                self._codepage = "65001"
            self._table_parallel_loads = table_parallel_loads
            self._table_parallel_batch_size = table_parallel_batch_size
            self._table_where_clause = table_where_clause

            self._conn = pyodbc.connect(conn_string, autocommit=True)
            self._cursor = self._conn.cursor()
            logger.debug(f"Connection to {self._server} successful")
        except pyodbc.Error as e:
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
        except pyodbc.Error as e:
            logger.error(e)

    def execute_many(self, sql, values):
        try:
            return self.cursor.executemany(sql, values)
        except pyodbc.Error as e:
            logger.error(e)

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except pyodbc.Error as e:
            logger.error(e)

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        except pyodbc.Error as e:
            logger.error(e)

    def fetchmany(self, rows):
        try:
            return self.cursor.fetchmany(rows)
        except pyodbc.Error as e:
            logger.error(e)

    def query(self, sql, params=None):
        try:
            self.cursor.execute(sql, params or ())
            return self.fetchall()
        except pyodbc.Error as e:
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
            q = "select table_schema + '.' + table_name from information_schema.tables"
            tables = self.query(q)
            return tables
        except:
            logger.error("Failed getting tables")

    def table_columns(self, schema, table):
        query = f"SELECT * FROM {schema}.{table}"
        columns = self.query_columns(query)
        return columns

    def query_columns(self, query):
        try:
            query = f"SELECT TOP 1 * FROM ({query}) q"
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
                if data_type == "str":
                    character_maximum_length = column[3]
                else:
                    character_maximum_length = None
                if data_type in ("decimal.Decimal", "decimal", "int"):
                    numeric_precision = column[4]
                else:
                    numeric_precision = None
                if data_type in ("decimal.Decimal", "decimal", "int"):
                    numeric_scale = column[5]
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

    def remove_unsupported_columns(self, columns):
        columns_to_keep = columns.copy()
        for column in columns:
            data_type = column[2]
            character_maximum_length = column[3]
            if data_type == 'str' and character_maximum_length > 4000:
                columns_to_keep.remove(column)
            if data_type in ("bytes", "bytearray", "memoryview", "buffer"):
                columns_to_keep.remove(column)
        return columns_to_keep

    def check_table_exist(self, table_name):
        try:
            check_statement = f"""SELECT 1
            FROM information_schema.tables 
            WHERE table_schema + '.' + table_name = '{table_name}'"""
            exists = self.query(check_statement)
            if exists:
                return True
            else:
                return False
        except:
            logger.error("Failed checking table exist")

    def truncate_table(self, table_name):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            sql = f"TRUNCATE TABLE {table_name}"
            self.execute(sql)
            logger.debug(f"Table {table_name} truncated")
        except:
            logger.error("Failed truncating table")

    def create_schema(self, schema):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            if schema in self.schemas():
                pass
            else:
                create_statement = f"CREATE SCHEMA {schema}"
                self.execute(create_statement)
                logger.debug(f"Schema {schema} created")
        except:
            logger.error("Failed creating schema")

    def get_max_column_value(self, table_name, column):
        try:
            sql = f"SELECT MAX({column}) FROM {table_name}"
            max_value = self.query(sql)
            type_max_value = type(max_value[0][0])
            if str(type_max_value) == "<class 'datetime.datetime'>":
                max_value_return = str(max_value[0][0])[:-3]
            else:
                max_value_return = str(max_value[0][0])
            logger.debug(f"Max {column} is {max_value_return}")
            return max_value_return
        except:
            logger.debug("Failed getting max column value")

    def get_min_max_column_value(self, table_name, column):
        try:
            sql = f"SELECT MIN({column}), MAX({column}) FROM {table_name}"
            res = self.query(sql)
            min_value = int(res[0][0])
            max_value = int(res[0][1])
            return min_value, max_value
        except:
            logger.debug("Failed getting min and max column value")

    def get_min_max_batch(self, table_name, column):
        try:
            sql = f"SELECT " \
                  f"MIN({column}), " \
                  f"MAX({column}), " \
                  f"CEILING((max({column}) - min({column})) / " \
                  f"(count(*)/{str(self._table_parallel_batch_size)}.0)) " \
                  f"FROM {table_name}"
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

        # Add limit
        if self._limit_rows:
            select_stmt += f"TOP {str(self._limit_rows)} "

        # Add columns
        for col in columns:
            column_name = f"[{col[1]}], "
            select_stmt += column_name
        select_stmt = select_stmt[:-2]

        select_stmt += f" FROM [{self._database}].[{schema}].[{table}] WITH (NOLOCK)"

        # Where-claues for incremental replication
        if replication_key:
            replication_where = f"{replication_key} > '{max_replication_key}'"
        else:
            replication_where = None

        wheres = replication_where, self._table_where_clause, parallelization_where
        wheres = [x for x in wheres if x is not None]
        if len(wheres) > 0:
            select_stmt += f" WHERE {wheres[0]}"
            for where in wheres[1:]:
                select_stmt += f" AND {where}"
        return select_stmt

    def export_query(self, query, file_path, delimiter):
        rowcounts = run_export_query(
            self._server,
            self._user,
            self._password,
            self._trusted_connection,
            query,
            file_path,
            delimiter,
        )
        return rowcounts

    def insert_from_table_and_drop(self, schema, to_table, from_table):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        to_schema_table = schema + "." + to_table
        from_schema_table = schema + "." + from_table
        try:
            self.execute(f"INSERT INTO {to_schema_table} "
                         f"SELECT * FROM {from_schema_table}"
            )
            self.execute(f"DROP TABLE {from_schema_table}")
            return_code = "RUN"
        except:
            logger.error("Failed to insert_from_table_and_drop")
            return_code = "ERROR"
        finally:
            return return_code

    def merge_from_table_and_drop(self, schema, to_table, from_table, primary_key):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")

        to_schema_table = f"{schema}.{to_table}"
        from_schema_table = f"{schema}.{from_table}"

        columns = self.table_columns(schema, to_table)
        columns = [row[1] for row in columns]
        columns = list(set(columns) - set(primary_key))

        on_stmt = "ON "
        for i, col in enumerate(primary_key):
            if i:
                on_stmt += " and "
            on_stmt += f"t.{col} = s.{col}"
        if on_stmt[-5:] == " and ":
            on_stmt = on_stmt[:-5]
        #print(on_stmt)

        update_stmt = "WHEN MATCHED " \
                      " THEN UPDATE SET\n"
        for col in columns:
            update_stmt += f"t.{col} = s.{col},\n"
        update_stmt = update_stmt[:-2]
        #print(update_stmt)

        insert_stmt = "WHEN NOT MATCHED BY TARGET " \
                      " THEN INSERT ("
        for col in columns:
            insert_stmt += f"{col}, "
        insert_stmt = insert_stmt[:-2] + ")\n VALUES("
        for col in columns:
            insert_stmt += f"{col}, "
        insert_stmt = insert_stmt[:-2] + ")"
        #print(insert_stmt)

        merge_stmt = f"MERGE {to_schema_table} t USING {from_schema_table} s\n{on_stmt}\n{update_stmt}\n{insert_stmt};"
        logger.debug(merge_stmt)

        try:
            self.execute(merge_stmt)
            self.execute(f"DROP TABLE {from_schema_table}")
            return_code = "RUN"
        except:
            logger.error("Failed to merge_from_table_and_drop")
            return_code = "ERROR"
        finally:
            return return_code

    def switch_tables(self, schema, old_table, new_table):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            old_schema_table = f"{schema}.{old_table}"
            new_schema_table = f"{schema}.{new_table}"
            delete_table = f"{old_table}_delete"
            delete_schema_table = f"{schema}.{delete_table}"

            if self.check_table_exist(old_schema_table):
                self.execute(f"EXEC sp_rename '{old_schema_table}', '{delete_table}'")
                self.execute(f"EXEC sp_rename '{new_schema_table}', '{old_table}'")
                self.execute(f"DROP TABLE {delete_schema_table}")
                logger.debug("Switched tables")
            else:
                self.execute(f"EXEC sp_rename '{new_schema_table}', '{old_table}'")
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
        return_code, row_count = run_import_file(
            self._server,
            self._database,
            self._user,
            self._password,
            self._trusted_connection,
            self._codepage,
            schema,
            table,
            path,
            delimiter,
        )
        return row_count

    def generate_create_table_ddl(self, schema, table, columns):
        try:
            create_table_sql = f"CREATE TABLE {schema}.{table} (\n"
            for col in columns:

                ordinal_position = col[0]
                column_name = f"[{col[1]}]"
                data_type = col[2]
                data_type = python_type_to_db_type(data_type)
                character_maximum_length = col[3]
                numeric_precision = col[4]
                numeric_scale = col[5]

                if data_type == "nvarchar":
                    if character_maximum_length <= 0 or character_maximum_length > 4000:
                        column = f"{column_name} nvarchar(MAX)"
                    else:
                        column = f"{column_name} nvarchar({str(character_maximum_length)})"

                elif data_type == "numeric":
                    column = f"{column_name} numeric({str(numeric_precision)},{str(numeric_scale)})"
                else:
                    column = f"{column_name} {data_type}"

                create_table_sql += f"{column}, \n"
            create_table_sql = f"{create_table_sql[:-3]})"

            return create_table_sql
        except Exception as e:
            logger.error(e)
            logger.error("Failed generating create table script")

    def create_table_from_columns(self, schema, table, columns):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            schema_table = schema + "." + table
            self.execute(
                "SELECT * FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = ? AND "
                "TABLE_NAME = ?",
                [schema, table],
            )
            table_exists = self.fetchone()

            if table_exists:
                self.execute(f"DROP TABLE {schema_table}")
                logger.debug(f"Table: {schema_table} droped")

            self.create_schema(schema)
            create_table_sql = self.generate_create_table_ddl(schema, table, columns)
            logger.debug(create_table_sql)
            self.execute(create_table_sql)
            logger.debug(f"Table: {schema_table} created")

            # Create Clustered Columnstore Index if set on connection
            if self._as_columnstore:
                try:
                    index_name = f"{schema}_{table}_cci"
                    self.execute(
                        f"DROP INDEX IF EXISTS  {index_name} ON {schema_table}"
                    )
                    self.execute(
                        f"CREATE CLUSTERED COLUMNSTORE INDEX {index_name} ON {schema_table}"
                    )
                    logger.debug(f"Index: {index_name} created")
                except:
                    logger.error("Failed create columnstoreindex")
        except:
            logger.error("Failed create table from columns")

    def create_log_table(self, schema, table):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")

        full_table = f"schema.{table}"

        if not self.check_table_exist(full_table):
            logger.debug("Log table exist")
            ddl = f"""create table {full_table} (
            log_time    datetime2(6),
            project	varchar(128),
            project_started_at	datetime2(6),
            source_table	varchar(128),
            target_table	varchar(128),
            started_at	datetime2(6),
            ended_at	datetime2(6),
            status		varchar(128),
            exported_rows	int,
            imported_rows	int
            );"""

            self.create_schema(schema)
            self.execute(ddl)
            logger.debug(f"{full_table} created")

        view1_ddl = f"""create or alter view {full_table}_summary as 
select
CONVERT(varchar(10),project_started_at,120) as project_started_date, 
project_started_at, 
project, 
case max( CASE "status"  WHEN 'end' THEN 1 ELSE 0 END ) when 1 then 'Finished' else 'Running or failed' end as status,
sum(case when source_table is not null and status = 'DONE'  then 1 else 0 end) as  completed_loads,
case max( CASE "status"  WHEN 'end' THEN 1 ELSE 0 END ) when 1 then max(ended_at) end as ended_at,
CONVERT(varchar, dateadd(ms,datediff(ms , min(project_started_at), max(ended_at)),0), 108) as duration,
sum(exported_rows) as exported_rows,
sum(imported_rows) as imported_rows,
sum(imported_rows) / datediff(second , min(project_started_at), max(ended_at)) as loaded_rows_per_sec
from  {full_table}
group BY
CONVERT(varchar(10),project_started_at,120), 
project_started_at, 
project"""
        self.execute(view1_ddl)

        view2_ddl = f"""create or alter view {full_table}_details as 
select
CONVERT(varchar(10),project_started_at,120) as project_started_date, 
project_started_at, 
project,
source_table,
target_table,
CONVERT(varchar(19), started_at, 120) as started_at,
CONVERT(varchar(19), ended_at, 120) as ended_at,
CONVERT(varchar, dateadd(ms,datediff(ms , started_at, ended_at),0), 108) as duration,
status, 
exported_rows as exported_rows,
imported_rows as imported_rows,
imported_rows / datediff(second , started_at, ended_at) as loaded_rows_per_sec
from {full_table} where source_table is not null"""
        self.execute(view2_ddl)

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

        sql = f"""INSERT INTO {full_table} 
(log_time, project, project_started_at, source_table, target_table, started_at, ended_at, status, exported_rows, imported_rows) 
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        self.execute(sql, row)
