import os
import subprocess
import psycopg2
import psycopg2.extras
import eneel.logger as logger
logger = logger.get_logger(__name__)


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
            #self._cursor = self._conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
            self._cursor = self._conn.cursor()
            logger.debug("Connection sucess")
        except:
            logger.error("Connection error")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self._conn.close()

    def close(self):
        self._conn.close()
        #logger.info("Connection closed")

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

    def fetchmany(self,rows):
        return self.cursor.fetchmany(rows)

    def query(self, sql, params=None):
        try:
            self.cursor.execute(sql, params or ())
            return self.fetchall()
        except psycopg2.DatabaseError as e:
            logger.error(e)
            #print("Postgres-Error-Code:", e.pgcode)
            #print("Postgres-Error-Message:", e.pgerror)


#    def schemas(self):
#        q = 'SELECT schema_name FROM information_schema.schemata'
#        schemas = []
#        schemas_list = self.query(q)
#        for schema in schemas_list:
#            s = Schema(self, schema)
#            schemas.append(s)
#        return schemas

    def schemas(self):
        q = 'SELECT schema_name FROM information_schema.schemata'
        schemas = self.query(q)
        return [row[0] for row in schemas]

    def tables(self):
        q = "select table_schema || '.' || table_name from information_schema.tables"
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
                table_schema = %s
                and table_name = %s
                order by ordinal_position
        """
        columns = self.query(q, [schema, table])

        return columns

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

    def select_table_script(self, table_name):
        columns = self.table_columns(table_name)
        statement = 'SELECT ' + table_name + '( '
        for col in columns:
            statement = statement + col[0] + ', '
        statement = statement[:-2] + ' FROM ' + table_name
        return statement

    def insert_table_script(self, table_name):
        columns = self.table_columns(table_name)
        statement = 'INSERT INTO ' + table_name + '( '
        for col in columns:
            statement = statement + col[0] + ', '
        statement = statement[:-2] + ') VALUES %s'
        return statement

    def check_table_exist(self, table_name):
        check_statement = """
        SELECT EXISTS (
       SELECT 1
       FROM   information_schema.tables 
       WHERE  table_schema || '.' || table_name = '"""
        check_statement += table_name + "')"
        exists = self.query(check_statement)
        return exists[0][0]

    def truncate_table(self, table_name):
        sql = "TRUNCATE TABLE " + table_name
        self.execute(sql)
        logger.debug(table_name + " truncated")

    def create_schema(self, schema):
        if schema in self.schemas():
            logger.debug("Schema exists")
        else:
            create_statement = 'CREATE SCHEMA ' + schema
            self.execute(create_statement)
            #self.commit()
            logger.debug("Schema created")

    def get_max_column_value(self, table_name, column):
        sql = "SELECT MAX(" + column + ")::text FROM " + table_name
        max_value = self.query(sql)
        return max_value[0][0]

    def run_cmd(self, cmd):

        res = subprocess.Popen(cmd.split(),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)

        stdout, stderr = res.communicate()

        return stdout, stderr

    def export_table(self, schema, table, path, delimiter=',', replication_key=None, max_replication_key=None):

        # Generate SQL statement for extract
        select_stmt = "SELECT * FROM " + schema + '.' + table
        if replication_key:
            select_stmt += " WHERE " + replication_key + " > " + "'" + max_replication_key + "'"
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

    def import_table(self, schema, table, file, delimiter=','):
        schema_table = schema + '.' + table

        sql = "COPY %s FROM STDIN WITH DELIMITER AS '%s'"
        file = open(file, "r")
        self.cursor.copy_expert(sql=sql % (schema_table, delimiter), file=file)
        logger.info(str(self.cursor.rowcount) + " records imported")

    def generate_create_table_ddl(self, schema, table, columns):
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

    def create_table_from_columns(self, schema, table, columns):
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
