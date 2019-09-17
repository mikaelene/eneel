import os
import pyodbc
import eneel.utils as utils


def column_from_matrix(matrix, i):
    return [row[i] for row in matrix]


class database:
    def __init__(self, driver, server, database, limit_rows=None, user=None, password=None, trusted_connection=None):
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
            self._trusted_connection = trusted_connection

            self._conn = pyodbc.connect(conn_string, autocommit=True)
            self._cursor = self._conn.cursor()
            #print("Connection to sqlserver successful")
        except:
            # logger.error("Connection error")
            print("Error while connecting to sqlserver")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self._conn.close()

    def close(self):
        self._conn.close()
        # logger.info("Connection closed")

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        return self.cursor.execute(sql, params or ())

    def execute_many(self, sql, values):
        return self.cursor.executemany(sql, values)

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchmany(self, rows):
        return self.cursor.fetchmany(rows)

    def query(self, sql, params=None):
        try:
            self.cursor.execute(sql, params or ())
            return self.fetchall()
        except:
            # logger.error("query error")
            print("query error")

    def schemas(self):
        q = 'SELECT schema_name FROM information_schema.schemata'
        schemas = self.query(q)
        #return column_from_matrix(schemas, 0)
        return [row[0] for row in schemas]

    def tables(self):
        q = "select table_schema + '.' + table_name from information_schema.tables"
        tables = self.query(q)
        return tables

    def table_columns(self, schema, table):
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

    def check_table_exist(self, table_name):
        check_statement = """
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_schema + '.' + table_name = '"""
        check_statement += table_name + "'"
        #print(check_statement)
        exists = self.query(check_statement)
        if exists:
            return True
        else:
            return False
        #return exists[0][0]

    def truncate_table(self, table_name):
        sql = "TRUNCATE TABLE " + table_name
        self.execute(sql)
        print(table_name + " truncated")

    def create_schema(self, schema):
        if schema in self.schemas():
            pass
        else:
            create_statement = 'CREATE SCHEMA ' + schema
            self.execute(create_statement)
            print("Schema created")

    def get_max_column_value(self, table_name, column):
        sql = "SELECT cast(MAX(" + column + ") as varchar(max)) FROM " + table_name
        max_value = self.query(sql)
        return max_value[0][0]

    def export_table(self, schema, table, path, delimiter='|', replication_key=None, max_replication_key=None):
        # Generate SQL statement for extract
        select_stmt = '"SELECT'
        if self._limit_rows:
            select_stmt += ' TOP ' + str(self._limit_rows)
        select_stmt += ' * FROM [' + self._database + '].[' + schema + '].[' + table + ']'
        if replication_key:
            select_stmt += " WHERE " + replication_key + " > " + "'" + max_replication_key + "'"
        select_stmt += '"'
        print(select_stmt)

        #print(select_stmt)

        # Generate file name
        file_name = self._database + '_' + schema + '_' + table + '.csv'
        file_path = os.path.join(path, file_name)

        # Generate bcp command
        bcp_out = "bcp " + select_stmt + " queryout " + \
                  file_path + " -t" + delimiter + " -c -S" + self._server
        if self._trusted_connection:
            bcp_out += " -T"
        else:
            bcp_out += " -U" + self._user + " -P" + self._password

        print(bcp_out)

        #print(bcp_out)
        cmd_code, cmd_message = utils.run_cmd(bcp_out)
        if cmd_code == 0:
            print(schema + '.' + table + " exported")
        else:
            print("Error exportng " + schema + '.' + table + " :" + cmd_message)

        return file_path, delimiter

    def import_table(self, schema, table, file, delimiter=',', codepage='1252'):

        # Import data
        bcp_in = "bcp [" + self._database + "].[" + schema + "].[" + table + "] in " + \
                 file + " -t" + delimiter + " -c -C" + codepage + " -U -S" + self._server
        if self._trusted_connection:
            bcp_in += " -T"
        else:
            bcp_in += " -U" + self._user + " -P" + self._password

        #print(bcp_in)
        cmd_code, cmd_message = utils.run_cmd(bcp_in)
        if cmd_code == 0:
            print(schema + '.' + table + " imported")
        else:
            print("Error importing " + schema + "." + table + " :" + cmd_message)

    def generate_create_table_ddl(self, schema, table, columns):
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
            else:
                db_data_type = data_type

            column = column_name + " " + db_data_type
            create_table_sql += column + ", \n"

        create_table_sql = create_table_sql[:-3]
        create_table_sql += ")"

        #print(create_table_sql)

        return create_table_sql

    def create_table_from_columns(self, schema, table, columns):
        self.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES "
                                    "WHERE TABLE_SCHEMA = ? AND "
                                    "TABLE_NAME = ?", [schema, table])
        table_exists = self.fetchone()

        if table_exists:
            self.execute("DROP TABLE " + schema + "." + table)

        self.create_schema(schema)
        create_table_sql = self.generate_create_table_ddl(schema, table, columns)
        self.execute(create_table_sql)
        print(schema + '.' + table + " created")



