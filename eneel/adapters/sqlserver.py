import os
import sys
import pyodbc
import eneel.utils as utils
import logging
logger = logging.getLogger('main_logger')


class Database:
    def __init__(self, driver, server, database, limit_rows=None, user=None, password=None,
                 trusted_connection=None, as_columnstore=False, read_only=False):
        try:
            conn_string = "DRIVER={" + driver + "};SERVER=" + server + ";DATABASE=" + \
                          database
            if trusted_connection:
                conn_string += ";trusted_connection=yes"
            else:
                conn_string += ";UID=" + user + ";PWD=" + password
            self._server = server
            self._user = user
            self._password = password
            self._database = database
            self._dialect = "sqlserver"
            self._limit_rows = limit_rows
            self._as_columnstore = as_columnstore
            self._trusted_connection = trusted_connection
            self._read_only = read_only

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

    def export_table(self, schema, table, columns, path, delimiter='|', replication_key=None, max_replication_key=None, codepage='1252'):
        try:
            # Generate SQL statement for extract
            select_stmt = '"SELECT '

            # Add limit
            if self._limit_rows:
                select_stmt += 'TOP ' + str(self._limit_rows) + ' '

            # Add columns
            for col in columns:
                column_name = col[1]
                select_stmt += column_name + ", "
            select_stmt = select_stmt[:-2]

            select_stmt += ' FROM [' + self._database + '].[' + schema + '].[' + table + ']' + ' WITH (NOLOCK)'

            # Add incremental where
            if replication_key:
                select_stmt += " WHERE " + replication_key + " > " + "'" + max_replication_key + "'"
            select_stmt += '"'
            logger.debug(select_stmt)

            # Generate file name
            file_name = self._database + '_' + schema + '_' + table + '.csv'
            file_path = os.path.join(path, file_name)

            # Generate bcp command
            bcp_out = "bcp " + select_stmt + " queryout " + \
                      file_path + " -t" + delimiter + " -c -C" + codepage + " -S" + self._server
            if self._trusted_connection:
                bcp_out += " -T"
            else:
                bcp_out += " -U" + self._user + " -P" + self._password

            logger.debug(bcp_out)

            cmd_code, cmd_message = utils.run_cmd(bcp_out)
            if cmd_code == 0:
                try:
                    return_message = cmd_message.splitlines()
                    num_rows = str(return_message[-3].split()[0])
                    timing = str(return_message[-1].split()[5])
                    average = str(return_message[-1].split()[8][1:-3])
                    logger.debug(table + ": " + num_rows + " rows exported, in " + timing + " ms. at an average of " + average + " rows per sec")
                except:
                    logger.warning(table + ": " + "Failed to parse sucessfull export cmd for")
                logger.debug(schema + '.' + table + " exported")
            else:
                logger.error("Error exportng " + schema + '.' + table + " :" + cmd_message)

            return file_path, delimiter
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
        except:
            logger.error("Failed to insert_from_table_and_drop")

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
        except:
            logger.error("Failed to switch tables")

    def import_table(self, schema, table, file, delimiter=',', codepage='1252'):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            # Import data
            bcp_in = "bcp [" + self._database + "].[" + schema + "].[" + table + "] in " + \
                     file + " -t" + delimiter + " -c -C" + codepage + " -U -b100000 -S" + self._server
            if self._trusted_connection:
                bcp_in += " -T"
            else:
                bcp_in += " -U" + self._user + " -P" + self._password

            logger.debug(bcp_in)
            cmd_code, cmd_message = utils.run_cmd(bcp_in)
            if cmd_code == 0:
                try:
                    return_message = cmd_message.splitlines()
                    row_count = str(return_message[-3].split()[0])
                    try:
                        if return_message[2].split()[0] == 'SQLState':
                            return "WARN", row_count
                    except:
                        pass

                    return "DONE", row_count
                except:
                    logger.warning(table + ": " + "Failed to parse sucessfull import cmd")
                    return "DONE", None
            else:
                logger.debug("Error importing " + schema + "." + table + " :" + cmd_message)
        except:
            logger.error("Failed importing table")

    def generate_create_table_ddl(self, schema, table, columns):
        try:
            create_table_sql = "CREATE TABLE " + schema + "." + table + "(\n"
            for col in columns:

                ordinal_position = col[0]
                column_name = col[1]
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

