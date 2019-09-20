import os
import sys
import psycopg2
import psycopg2.extras
import logging
logger = logging.getLogger('main_logger')


class Database:
    def __init__(self, server, user, password, database, limit_rows=None):
        try:
            conn_string = "host=" + server + " dbname=" + \
                          database + " user=" + user + " password=" + password
            self._server = server
            self._user = user
            self._password = password
            self._database = database
            self._dialect = "postgres"
            self._limit_rows = limit_rows

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

    def fetchmany(self,rows):
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
            q = 'SELECT schema_name FROM information_schema.schemata'
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
                    table_schema = %s
                    and table_name = %s
                    order by ordinal_position
            """
            columns = self.query(q, [schema, table])
            return columns
        except:
            logger.error("Failed getting columns")

    def create_table_script(self, table_name):
        columns = self.table_columns(table_name)
        statement = 'CREATE TABLE ' + table_name + '( '
        for col in columns:
            if col[2]:
                statement = statement + col[0] + ' ' + col[1] + '(' + str(col[2]) + '), '
            else:
                statement = statement + col[0] + ' ' + col[1] + ', '
        statement = statement[:-2] + ')'
        return statement

    def check_table_exist(self, table_name):
        try:
            check_statement = """
            SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema || '.' || table_name = '"""
            check_statement += table_name + "')"
            exists = self.query(check_statement)
            return exists[0][0]
        except:
            logger.error("Failed checking table exist")

    def truncate_table(self, table_name):
        try:
            sql = "TRUNCATE TABLE " + table_name
            self.execute(sql)
            logger.debug("Table " + table_name + " truncated")
        except:
            logger.error("Failed truncating table")

    def create_schema(self, schema):
        try:
            if schema in self.schemas():
                logger.debug("Schema exists")
            else:
                create_statement = 'CREATE SCHEMA ' + schema
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
            logger.error("Failed getting max column value")

    def export_table(self, schema, table, columns, path, delimiter=',', replication_key=None, max_replication_key=None):
        try:
            # Generate SQL statement for extract
            select_stmt = "SELECT "

            # Add columns
            for col in columns:
                column_name = col[1]
                select_stmt += column_name + ", "
            select_stmt = select_stmt[:-2]

            select_stmt += " FROM " + schema + '.' + table

            # Add incremental where
            if replication_key:
                select_stmt += " WHERE " + replication_key + " > " + "'" + max_replication_key + "'"

            # Add limit
            if self._limit_rows:
                select_stmt += " FETCH FIRST " + str(self._limit_rows) + " ROW ONLY"
            logger.debug(select_stmt)

            # Generate file name
            file_name = self._database + "_" + schema + "_" + table + ".csv"
            file_path = os.path.join(path, file_name)



            #schema_table = schema + '.' + table

            sql = "COPY (%s) TO STDIN WITH DELIMITER AS '%s'"
            file = open(file_path, "w")
            self.cursor.copy_expert(sql=sql % (select_stmt, delimiter), file=file)
            logger.info(str(self.cursor.rowcount) + " records exported")

            #cmd = "COPY (" + select_stmt + ") TO '" + file_path + "' With CSV DELIMITER '" + delimiter + "';"

            return file_path, delimiter
        except:
            logger.error("Failed exporting table")

    def insert_from_table_and_drop(self, schema, to_table, from_table):
        to_schema_table = schema + "." + to_table
        from_schema_table = schema + "." + from_table
        try:
            self.execute("INSERT INTO " + to_schema_table + " SELECT * FROM  " + from_schema_table)
            self.execute("DROP TABLE " + from_schema_table)
        except:
            logger.error("Failed to insert_from_table_and_drop")

    def switch_tables(self, schema, old_table, new_table):
        try:

            old_schema_table = schema + "." + old_table
            new_schema_table = schema + "." + new_table
            delete_table = old_table + "_delete"
            delete_schema_table = schema + "." + delete_table

            if self.check_table_exist(old_schema_table):
                self.execute("ALTER TABLE " + old_schema_table + " RENAME TO " + delete_table)
                self.execute("ALTER TABLE " + new_schema_table + " RENAME TO " + old_table)
                self.execute("DROP TABLE " + delete_schema_table)
                logger.debug("Switched tables")
            else:
                self.execute("ALTER TABLE " + new_schema_table + " RENAME TO " + old_table)
                logger.debug("Renamed temp table")
        except:
            logger.error("Failed to switch tables")

    def import_table(self, schema, table, file, delimiter=','):
        try:
            schema_table = schema + '.' + table

            sql = "COPY %s FROM STDIN WITH DELIMITER AS '%s'"
            file = open(file, "r")

            try:
                self.cursor.copy_expert(sql=sql % (schema_table, delimiter), file=file)
            except psycopg2.Error as e:
                logger.error(e)

            logger.info(str(self.cursor.rowcount) + " records imported")
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

                column = column_name + " " + data_type
                if "char" in data_type:
                    column += "("
                    if character_maximum_length == -1:
                        column += "max"
                    else:
                        column += str(character_maximum_length)
                    column += ")"
                elif "numeric" in data_type:
                    column += "(" + str(numeric_precision) + "," + str(numeric_scale) + ")"
                elif data_type == "USER-DEFINED":
                    column = column_name + " TEXT"
                elif data_type == "ARRAY":
                    column = column_name + " TEXT"
                create_table_sql += column + ", \n"
            create_table_sql = create_table_sql[:-3]
            create_table_sql += ")"

            return create_table_sql
        except:
            logger.error("Failed generating create table script")

    def create_table_from_columns(self, schema, table, columns):
        try:
            table_exists = self.query("SELECT * FROM INFORMATION_SCHEMA.TABLES "
                                        "WHERE TABLE_SCHEMA = %s AND "
                                        "TABLE_NAME = %s", [schema, table])
            #table_exists = self.fetchone()

            if table_exists:
                self.execute("DROP TABLE " + schema + "." + table)

            self.create_schema(schema)
            create_table_sql = self.generate_create_table_ddl(schema, table, columns)
            self.execute(create_table_sql)
            logger.debug("table created")
        except:
            logger.error("Failed create table from columns")
