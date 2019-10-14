import os
import sys
import pyodbc
import eneel.utils as utils
from time import time
from datetime import datetime
from glob import glob
from concurrent.futures import ThreadPoolExecutor as Executor

import logging
logger = logging.getLogger('main_logger')


class Database:
    def __init__(self, driver, server, database, port=1433, limit_rows=None, user=None, password=None,
                 trusted_connection=None, as_columnstore=False, read_only=False, codepage=None,
                 table_parallel_loads=10, table_parallel_batch_size=10000000):
        try:
            conn_string = "DRIVER={" + driver + "};SERVER=" + server + ";DATABASE=" + \
                          database + ";PORT=" + str(port)
            if trusted_connection:
                conn_string += ";trusted_connection=yes"
            else:
                conn_string += ";UID=" + user + ";PWD=" + password
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
                self._codepage = '1252'
            self._table_parallel_loads = table_parallel_loads
            self._table_parallel_batch_size = table_parallel_batch_size

            self._conn = pyodbc.connect(conn_string, autocommit=True)
            self._cursor = self._conn.cursor()
            logger.debug("Connection to sqlserver successful")
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
            q = 'SELECT schema_name FROM information_schema.schemata'
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
        try:
            q = """
                SELECT  
                      ordinal_position,
                      column_name,
                      data_type,
                      character_maximum_length,
                      numeric_precision,
                      numeric_scale
                FROM information_schema.columns
                WHERE 
                    --table_schema + '.' + table_name = ?
                    table_schema = ?
                    and table_name = ?
                    order by ordinal_position
            """
            columns = self.query(q, [schema, table])
            return columns
        except:
            logger.error("Failed getting columns")

    def check_table_exist(self, table_name):
        try:
            check_statement = """
            SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema + '.' + table_name = '"""
            check_statement += table_name + "'"
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
                pass
            else:
                create_statement = 'CREATE SCHEMA ' + schema
                self.execute(create_statement)
                logger.debug("Schema" + schema + " created")
        except:
            logger.error("Failed creating schema")

    def get_max_column_value(self, table_name, column):
        try:
            sql = "SELECT cast(MAX(" + column + ") as varchar(max)) FROM " + table_name
            max_value = self.query(sql)
            logger.debug("Max " + column + " is " + str(max_value[0][0]))
            return max_value[0][0]
        except:
            logger.debug("Failed getting max column value")

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
            sql += "), CEILING((max( " + column + ") - min("
            sql += column + ")) / (count(*)/" + str(self._table_parallel_batch_size) + ".0)) FROM " + table_name
            res = self.query(sql)
            min_value = int(res[0][0])
            max_value = int(res[0][1])
            batch_size_key = int(res[0][2])
            return min_value, max_value, batch_size_key
        except:
            logger.debug("Failed getting min, max and batch column value")

    def export_query(self, query, file_path, delimiter):
        # Export data
        # Generate bcp command
        bcp_out = ['bcp']
        bcp_out.append(query)
        bcp_out.append('queryout')
        bcp_out.append(file_path)
        bcp_out.append('-t' + delimiter)
        bcp_out.append('-c')
        bcp_out.append('-C' + self._codepage)
        bcp_out.append('-S' + self._server)
        if self._trusted_connection:
            bcp_out.append('-T')
        else:
            bcp_out.append('-U' + self._user)
            bcp_out.append('-P' + self._password)

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
                logger.debug(query + ": " + str(
                    row_count) + " rows exported, in " + timing + " ms. at an average of " + average + " rows per sec")
                return row_count
            except:
                logger.warning(query + ": " + "Failed to parse sucessfull export cmd for")
            logger.debug(query + " exported")
        else:
            logger.error("Error exportng " + query + " :" + cmd_message)

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
            # Generate SQL statement for extract
            select_stmt = 'SELECT '

            # Add limit
            if self._limit_rows:
                select_stmt += 'TOP ' + str(self._limit_rows) + ' '

            # Add columns
            for col in columns:
                column_name = "[" + col[1] + "]"
                select_stmt += column_name + ", "
            select_stmt = select_stmt[:-2]

            select_stmt += ' FROM [' + self._database + '].[' + schema + '].[' + table + ']' + ' WITH (NOLOCK)'

            # Add incremental where
            if replication_key:
                select_stmt += " WHERE " + replication_key + " > " + "'" + max_replication_key + "'"
            #select_stmt += '"'
            logger.debug(select_stmt)

            # Add logic for parallelization_key
            if parallelization_key:
                min_parallelization_key, max_parallelization_key, batch_size_key = self.get_min_max_batch(
                    schema + '.' + table,
                    parallelization_key)
                batch_id = 1
                batch_start = min_parallelization_key
                total_row_count = 0
                file_paths = []
                batch_stmts = []
                delimiters = []
                batches = []
                while batch_start < max_parallelization_key:
                    file_name = self._database + "_" + schema + "_" + table + "_" + str(batch_id) + ".csv"
                    file_path = os.path.join(path, file_name)
                    batch_stmt = '"SELECT * FROM (' + select_stmt[1:-1] + ") q WHERE " + parallelization_key + ' between ' + str(
                        batch_start) + ' and ' + str(batch_start + batch_size_key - 1) + '"'
                    file_paths.append(file_path)
                    batch_stmts.append(batch_stmt)
                    delimiters.append(delimiter)
                    batch = (batch_stmt, file_path)
                    batches.append(batch)
                    batch_start += batch_size_key
                    batch_id += 1

                table_workers = self._table_parallel_loads
                if len(batch_stmts) < table_workers:
                    table_workers = len(batch_stmts)

                try:
                    with Executor(max_workers=table_workers) as executor:
                        for row_count in executor.map(self.export_query, batch_stmts, file_paths, delimiters):
                            total_row_count += row_count
                except Exception as exc:
                    logger.error(exc)

            else:
                # Generate file name
                file_name = self._database + '_' + schema + '_' + table + '.csv'
                file_path = os.path.join(path, file_name)

                total_row_count = self.export_query(select_stmt, file_path, delimiter)

            return path, delimiter, total_row_count
        except:
            logger.error("Failed exporting table")

    def insert_from_table_and_drop(self, schema, to_table, from_table):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        to_schema_table = schema + "." + to_table
        from_schema_table = schema + "." + from_table
        try:
            self.execute("INSERT INTO " + to_schema_table + " SELECT * FROM  " + from_schema_table)
            self.execute("DROP TABLE " + from_schema_table)
            return_code = 'RUN'
        except:
            logger.error("Failed to insert_from_table_and_drop")
            return_code = 'ERROR'
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
                self.execute("EXEC sp_rename '" + old_schema_table + "', '" + delete_table + "'")
                self.execute("EXEC sp_rename '" + new_schema_table + "', '" + old_table + "'")
                self.execute("DROP TABLE " + delete_schema_table)
                logger.debug("Switched tables")
            else:
                self.execute("EXEC sp_rename '" + new_schema_table + "', '" + old_table + "'")
                logger.debug("Renamed temp table")
            return_code = 'RUN'
        except:
            logger.error("Failed to switch tables")
            return_code = 'ERROR'
        finally:
            return return_code

    def import_file(self, schema, table, file_path, delimiter, codepage):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            # Import data
            bcp_in = ['bcp']
            bcp_in.append('[' + self._database + '].[' + schema + '].[' + table + ']')
            bcp_in.append('in')
            bcp_in.append(file_path)
            bcp_in.append('-t' + delimiter)
            bcp_in.append('-c')
            bcp_in.append('-C' + self._codepage)
            bcp_in.append('-b100000')
            bcp_in.append('-S' + self._server)
            if self._trusted_connection:
                bcp_in.append('-T')
            else:
                bcp_in.append('-U' + self._user)
                bcp_in.append('-P' + self._password)

            logger.debug(bcp_in)
            cmd_code, cmd_message = utils.run_cmd(bcp_in)
            return_code = 'ERROR'
            row_count = 0

            if cmd_code == 0:
                try:
                    return_message = cmd_message.splitlines()
                    try:
                        row_count = int(return_message[-3].split()[0])
                        return_code = 'RUN'
                    except:
                        if return_message[2].split()[0] == 'SQLState':
                            logger.debug(cmd_message)
                            return_code = "WARN"
                except:
                    logger.warning(table + ": " + "Failed to parse sucessfull import cmd")
            else:
                logger.debug("Error importing " + schema + "." + table + " :" + cmd_message)
        except:
            logger.error("Failed importing table")
        return return_code, row_count

    def import_table(self, schema, table, path, delimiter=',', codepage='1252'):
        if self._read_only:
            sys.exit('This source is readonly. Terminating load run')
        try:
            schema_table = schema + '.' + table
            csv_files = glob(os.path.join(path, '*.csv'))
            servers = []
            users = []
            passwords = []
            databases = []
            ports = []
            file_paths = []
            schemas = []
            tables = []
            delimiters = []
            codepages = []

            for file_path in csv_files:
                servers.append(self._server)
                users.append(self._user)
                passwords.append(self._password)
                databases.append(self._database)
                ports.append(self._port)
                file_paths.append(file_path)
                schemas.append(schema)
                tables.append(table)
                delimiters.append(delimiter)
                codepages.append(codepage)

            table_workers = self._table_parallel_loads
            if len(file_paths) < table_workers:
                table_workers = len(file_paths)

            total_row_counts = []
            return_codes = []
            try:
                with Executor(max_workers=table_workers) as executor:
                    for return_code, row_count in executor.map(self.import_file,
                                                  schemas, tables,  file_paths, delimiters, codepages):
                        return_codes.append(return_code)
                        total_row_counts.append(row_count)
            except Exception as exc:
                    logger.debug(exc)

            if 'ERROR' in return_codes:
                return_code == 'ERROR'
            elif 'WARN' in return_codes:
                return_code == 'WARN'
            else:
                return_code == 'DONE'

            total_row_count = sum(total_row_counts)

            return return_code, total_row_count

        except:
            logger.error("Failed importing table")

    def generate_create_table_ddl(self, schema, table, columns):
        try:
            create_table_sql = "CREATE TABLE " + schema + "." + table + "(\n"
            for col in columns:

                ordinal_position = col[0]
                column_name = "[" + col[1] + "]"
                data_type = col[2].lower()
                character_maximum_length = col[3]
                numeric_precision = col[4]
                numeric_scale = col[5]

                if "char" in data_type:
                    db_data_type = "VARCHAR" + "("
                    if character_maximum_length == -1:
                        db_data_type += "max"
                    else:
                        db_data_type += str(character_maximum_length)
                    db_data_type += ")"
                elif "numeric" in data_type or "number" in data_type or "decimal" in data_type:
                    if numeric_scale:
                        db_data_type = "NUMERIC" + "(" + str(numeric_precision) + "," + str(numeric_scale) + ")"
                    elif numeric_precision:
                        db_data_type = "NUMERIC" + "(" + str(numeric_precision) + ")"
                    else:
                        db_data_type = "FLOAT"
                elif "time zone" in data_type:
                    db_data_type = "DATETIME2"
                elif "timestamp" in data_type:
                    db_data_type = "DATETIME2"
                elif "bool" in data_type:
                    db_data_type = "INT"
                elif "clob" in data_type:
                    db_data_type = "VARCHAR(MAX)"
                else:
                    db_data_type = data_type

                column = column_name + " " + db_data_type
                create_table_sql += column + ", \n"

            create_table_sql = create_table_sql[:-3]
            create_table_sql += ")"

            logger.debug(create_table_sql)

            return create_table_sql
        except:
            logger.error("Failed generating create table script")

    def create_table_from_columns(self, schema, table, columns):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            schema_table = schema + "." + table
            self.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES "
                                        "WHERE TABLE_SCHEMA = ? AND "
                                        "TABLE_NAME = ?", [schema, table])
            table_exists = self.fetchone()

            if table_exists:
                self.execute("DROP TABLE " + schema_table)
                logger.debug("Table: " + schema_table + " droped")

            self.create_schema(schema)
            create_table_sql = self.generate_create_table_ddl(schema, table, columns)
            self.execute(create_table_sql)
            logger.debug("Table: " + schema_table + " created")

            # Create Clustered Columnstore Index if set on connection
            if self._as_columnstore:
                try:
                    index_name = schema + "_" + table + "_cci"
                    self.execute("DROP INDEX IF EXISTS " + index_name + " ON " + schema_table)
                    self.execute("CREATE CLUSTERED COLUMNSTORE INDEX " + index_name + " ON " + schema_table)
                    logger.debug("Index: " + index_name + " created")
                except:
                    logger.error("Failed create columnstoreindex")
        except:
            logger.error("Failed create table from columns")

    def create_log_table(self, schema, table):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")

        full_table = schema + '.' + table

        if not self.check_table_exist(full_table):
            logger.debug('Log table exist')
            ddl = 'create table '
            ddl += full_table
            ddl += """(
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
            logger.debug(full_table + ' created')

        view1_ddl = 'create or alter view '
        view1_ddl += full_table + '_summary as '
        view1_ddl += """select
CONVERT(varchar(10),project_started_at,120) as project_started_date, 
project_started_at, 
project, 
case max( CASE "status"  WHEN 'end' THEN 1 ELSE 0 END ) when 1 then 'Finished' else 'Running or failed' end as status,
case max( CASE "status"  WHEN 'end' THEN 1 ELSE 0 END ) when 1 then max(ended_at) end as ended_at,
CONVERT(varchar, dateadd(ms,datediff(ms , min(project_started_at), max(ended_at)),0), 108) as duration,
sum(exported_rows) as exported_rows,
sum(imported_rows) as imported_rows,
sum(imported_rows) / datediff(second , min(project_started_at), max(ended_at)) as loaded_rows_per_sec
from  """
        view1_ddl += full_table
        view1_ddl += """
group BY
CONVERT(varchar(10),project_started_at,120), 
project_started_at, 
project"""
        self.execute(view1_ddl)

        view2_ddl = 'create or alter view '
        view2_ddl += full_table + '_details as '
        view2_ddl += """select
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
from  """
        view2_ddl += full_table
        view2_ddl += ' where source_table is not null'
        self.execute(view2_ddl)

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

        full_table = schema + '.' + table
        log_time = datetime.fromtimestamp(time())
        row = [log_time, project, project_started_at, source_table, target_table, started_at, ended_at, status, exported_rows, imported_rows]

        sql = 'INSERT INTO ' + full_table
        sql += ' (log_time, project, project_started_at, source_table, target_table, started_at, ended_at, status, exported_rows, imported_rows)'
        sql += ' VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

        self.execute(sql, row)
