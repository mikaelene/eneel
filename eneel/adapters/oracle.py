import os
import cx_Oracle
import eneel.utils as utils
import eneel.logger as logger
logger = logger.get_logger(__name__)


class Database:
    def __init__(self, server, user, password, database, limit_rows=None):
        try:
            server_db = server + "/" + database
            conn_string = user + ", " + password + ", " + server + "/" + database
            self._server = server
            self._user = user
            self._password = password
            self._database = database
            self._server_db = server_db
            self._dialect = "oracle"
            self._limit_rows = limit_rows

            #print(conn_string)
            self._conn = cx_Oracle.connect(user, password, server_db)

            self._cursor = self._conn.cursor()
#            self._cursor.rowfactory = makeNamedTupleFactory(self._cursor)
            logger.debug("Connection to oracle successful")
        except:
            logger.error("Error while connecting to oracle")

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
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            logger.error("Oracle-Error-Code:", error.code)
            logger.error("Oracle-Error-Message:", error.message)

    def schemas(self):
        q = 'SELECT DISTINCT OWNER FROM ALL_TABLES'
        schemas = self.query(q)
        return schemas

    def tables(self):
        q = "select OWNER || '.' || TABLE_NAME from ALL_TABLES"
        tables = self.query(q)
        return tables

    def table_columns(self, schema, table):
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
        SELECT 1
       FROM   ALL_TABLES 
       WHERE  OWNER || '.' || TABLE_NAME = '""" + table_name + "'"
        exists = self.query(check_statement)
        if exists:
            return True
        else:
            return False

    def export_table(self, schema, table, path, delimiter='|', replication_key=None, max_replication_key=None):
        columns = self.table_columns(schema, table)

        # Generate SQL statement for extract
        select_stmt = "SELECT "
        for col in columns[:-1]:
            column_name = col[1]
            select_stmt += column_name + " || '" + delimiter + "' || "
        last_column_name = columns[-1:][0][1]
        select_stmt += last_column_name
        select_stmt += ' FROM ' + schema + "." + table
        if replication_key:
            select_stmt += " WHERE " + replication_key + " > " + "'" + max_replication_key + "'"
        if self._limit_rows:
            select_stmt += " FETCH FIRST " + str(self._limit_rows) + " ROW ONLY"

        select_stmt += ";\n"

        # Generate file name
        file_name = self._database + "_" + schema + "_" + table + ".csv"
        file_path = os.path.join(path, file_name)

        spool_cmd = "set colsep '" + delimiter + "'"

        spool_cmd += """
set HEADING OFF
SET FEEDBACK OFF
set WRAP OFF
set COLSEP ,
SET LINESIZE 32767
set NEWPAGE none
set UNDERLINE OFF
set TRIMSPOOL ON
set PAGESIZE 50000
SET TERMOUT OFF
spool """

        spool_cmd += file_path + '\n'
        spool_cmd += select_stmt
        spool_cmd += "spool off\n"
        spool_cmd += "exit"
        logger.debug(spool_cmd)

        sql_file = os.path.join(path, self._database + "_" + schema + "_" + table + ".sql")

        with open(sql_file, "w") as text_file:
            text_file.write(spool_cmd)

        cmd = "SET NLS_LANG=SWEDISH_SWEDEN.WE8ISO8859P1\n"
        cmd += "set NLS_NUMERIC_CHARACTERS=. \n"
        cmd += "set NLS_TIMESTAMP_TZ_FORMAT=YYYY-MM-DD HH24:MI:SS.FF\n"
        cmd += "sqlplus " + self._user + "/" + self._password + "@//" + self._server_db + " @" + sql_file

        logger.debug(cmd)
        cmd_file = os.path.join(path, self._database + "_" + schema + "_" + table + ".cmd")

        with open(cmd_file, "w") as text_file:
            text_file.write(cmd)

        cmd_code, cmd_message = utils.run_cmd(cmd_file)
        if cmd_code == 0:
            logger.info(schema + '.' + table + " exported")
        else:
            logger.error("Error exportng " + schema + '.' + table + " : cmd_code: " + str(cmd_code) + " cmd_message: " + cmd_message)

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
            column = col.column_name + " " + col.data_type
            if "char" in col.data_type:
                column += "("
                if col.character_maximum_length == -1:
                    column += "max"
                else:
                    column += str(col.character_maximum_length)
                column += ")"
            elif "numeric" in col.data_type:
                column += "(" + str(col.numeric_precision) + "," + str(col.numeric_scale) + ")"
            elif col.data_type == "USER-DEFINED":
                column = col.column_name + " TEXT"
            elif col.data_type == "ARRAY":
                column = col.column_name + " TEXT"
            create_table_sql += column + ", \n"
        create_table_sql = create_table_sql[:-3]
        create_table_sql += ")"

        return create_table_sql

    def create_table_from_columns(self, schema, table, columns):
        table_exists = self.check_table_exist(table)

        if table_exists:
            self.execute("DROP TABLE " + schema + "." + table)

        self.commit()

        create_table_sql = self.generate_create_table_ddl(schema, table, columns)
        self.execute(create_table_sql)

