import os
import sys
import snowflake.connector
import eneel.utils as utils
from fsplit.filesplit import FileSplit

import logging

logger = logging.getLogger("main_logger")


def run_import_file(account,
                    user,
                    password,
                    database,
                    warehouse,
                    schema,
                    schema_table,
                    file_path,
                    delimiter=",",
                    ):
    return_code = "ERROR"
    row_count = 0

    db = Database(
        account,
        user,
        password,
        database,
        warehouse,
        schema,
    )
    try:
        schema_table = schema_table.upper()

        # Tablename underscored
        table_name_text = database + "_" + schema_table.replace('.', '_')

        # Fileformat
        table_format = table_name_text + "_format"
        create_format_sql = (
            "create or replace file format "
            + table_format
            + " type = 'CSV' field_delimiter = '"
            + delimiter
            + "' skip_header = 1; "
        )
        print(create_format_sql)
        db.execute(create_format_sql)

        # Stage
        table_stage = table_name_text + "_stage"
        create_stage_sql = (
            "create or replace stage "
            + table_stage
            + " file_format = "
            + table_format
            + ";"
        )
        print(create_stage_sql)
        db.execute(create_stage_sql)

        # Split files
        file_abs = file_path
        file_name_local = file_abs.split("/")[-1]
        file_dir = file_abs.replace(file_name_local, "")

        fs = FileSplit(file=file_abs, splitsize=50000000, output_dir=file_dir)
        fs.split()

        os.remove(file_abs)

        # put
        files = file_abs[:-4] + "*.csv"
        put_sql = (
            "PUT file://" + files + " @" + table_stage + " auto_compress=true;"
        )
        print(put_sql)
        db.execute(put_sql)
        file_name = file_path.split("/")[-1] + ".gz"
        files_stage = files.split("/")[-1]

        # copy
        copy_sql = (
            "COPY INTO "
            + schema_table
            + " FROM @"
            + table_stage
            + " file_format = (format_name = "
            + table_format
            + ") on_error = 'skip_file';"
        )
        print(copy_sql)
        sfqid = db.execute(copy_sql).sfqid
        print("copy table success")

        logger.debug('Snowflake copy query id: ' + sfqid)
        sfqid = "'" + sfqid + "'"

        qstring = 'SELECT "rows_parsed","rows_loaded" FROM  TABLE(RESULT_SCAN({}))'
        rows_parsed, rows_loaded = db.execute(qstring.format(sfqid)).fetchone()

        row_count = rows_loaded

        # remove stage
        drop_stage_sql = "DROP STAGE IF EXISTS " + table_stage
        print(drop_stage_sql)
        db.execute(drop_stage_sql)
        print("stage deleted")

        # remove fileformat
        drop_file_format_sql = "DROP FILE FORMAT IF EXISTS " + table_format
        print(drop_file_format_sql)
        db.execute(drop_file_format_sql)
        print("format deleted")

        return_code = "RUN"

        logger.info(str(row_count) + " records imported")

        return row_count

    except:
        logger.error("Failed importing table")
        return row_count


def python_type_to_db_type(python_type):
    if python_type == "str":
        return "VARCHAR"
    elif python_type in ("bytes", "bytearray", "memoryview", "buffer", "bytea"):
        return "BINARY"
    elif python_type == "bool":
        return "BOOLEAN"
    elif python_type == "datetime.date":
        return "DATE"
    elif python_type in ("datetime.time", "timedelta"):
        return "TIME"
    elif python_type == "datetime.datetime":
        return "TIMESTAMP_NTZ"
    elif python_type in ("int", "long"):
        return "BIGINT"
    elif python_type == "float":
        return "REAL"
    elif python_type == "decimal.Decimal":
        return "NUMBER"
    elif python_type == "UUID.uuid":
        return "TEXT"
    else:
        return python_type


class Database:
    def __init__(
        self,
        account,
        user,
        password,
        database,
        warehouse,
        schema,
        limit_rows=None,
        read_only=False,
        table_parallel_loads=10,
        table_parallel_batch_size=10000000,
    ):

        try:
            self._account = account
            self._user = user
            self._password = password
            self._database = database
            self._dialect = "snowflake"
            self._warehouse = warehouse
            self._schema = schema
            self._limit_rows = limit_rows
            self._read_only = read_only
            self._table_parallel_loads = table_parallel_loads
            self._table_parallel_batch_size = table_parallel_batch_size

            self._conn = snowflake.connector.connect(
                user=self._user,
                password=self._password,
                account=self._account,
                warehouse=self._warehouse,
                database=self._database,
                schema=self._schema,
            )

            self._conn.autocommit = True
            self._cursor = self._conn.cursor()
            logger.debug("Connection to snowflake successful")
        except snowflake.connector.Error as e:
            print(e)
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
        except snowflake.connector.Error as e:
            logger.error(e)

    def execute_many(self, sql, values):
        try:
            return self.cursor.executemany(sql, values)
        except snowflake.connector.Error as e:
            logger.error(e)

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except snowflake.connector.Error as e:
            logger.error(e)

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        except snowflake.connector.Error as e:
            logger.error(e)

    def fetchmany(self, rows):
        try:
            return self.cursor.fetchmany(rows)
        except snowflake.connector.Error as e:
            logger.error(e)

    def query(self, sql, params=None):
        try:
            self.cursor.execute(sql, params or ())
            return self.fetchall()
        except snowflake.connector.Error as e:
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
            tables_list = []
            for table in tables:
                tables_list.append(table[0])
            return tables_list
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
        statement = "CREATE TABLE " + table_name + "( "
        for col in columns:
            if col[2]:
                statement = (
                    statement + col[0] + " " + col[1] + "(" + str(col[2]) + "), "
                )
            else:
                statement = statement + col[0] + " " + col[1] + ", "
        statement = statement[:-2] + ")"
        return statement

    def check_table_exist(self, table_name):
        try:
            table_name = table_name.upper()
            if table_name in self.tables():
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
            schema = schema.upper()
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
            sql = 'SELECT MAX("' + column + '")::text FROM ' + table_name
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

    def export_table(
        self,
        schema,
        table,
        columns,
        path,
        delimiter=",",
        replication_key=None,
        max_replication_key=None,
    ):
        try:
            # Generate SQL statement for extract
            select_stmt = "SELECT "

            # Add columns
            for col in columns:
                column_name = col[1]
                select_stmt += column_name + ", "
            select_stmt = select_stmt[:-2]

            select_stmt += " FROM " + schema + "." + table

            # Add incremental where
            if replication_key:
                select_stmt += (
                    " WHERE "
                    + replication_key
                    + " > "
                    + "'"
                    + max_replication_key
                    + "'"
                )

            # Add limit
            if self._limit_rows:
                select_stmt += " FETCH FIRST " + str(self._limit_rows) + " ROW ONLY"
            logger.debug(select_stmt)

            # Generate file name
            file_name = self._database + "_" + schema + "_" + table + ".csv"
            file_path = os.path.join(path, file_name)

            # Create and run the cmd
            sql = "COPY (%s) TO STDIN WITH DELIMITER AS '%s'"
            file = open(file_path, "w")
            try:
                self.cursor.copy_expert(sql=sql % (select_stmt, delimiter), file=file)
            except snowflake.connector.Error as e:
                logger.error(e)

            row_count = self.cursor.rowcount

            logger.debug(str(row_count) + " records exported")

            return file_path, delimiter
        except:
            logger.error("Failed exporting table")

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
                self.execute(
                    "ALTER TABLE "
                    + old_schema_table
                    + " RENAME TO "
                    + delete_schema_table
                )
                self.execute(
                    "ALTER TABLE " + new_schema_table + " RENAME TO " + old_schema_table
                )
                self.execute("DROP TABLE " + delete_schema_table)
                logger.debug("Switched tables")
            else:
                self.execute(
                    "ALTER TABLE " + new_schema_table + " RENAME TO " + old_schema_table
                )
                logger.debug("Renamed temp table")
        except:
            logger.error("Failed to switch tables")

    def import_file(self, schema, table, path, delimiter=","):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        schema_table = schema + "." + table
        row_count = run_import_file(
            self._account,
            self._user,
            self._password,
            self._database,
            self._warehouse,
            self._schema,
            schema_table,
            path,
            delimiter,
        )
        return row_count

    def import_table(self, schema, table, file, delimiter=","):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")
        try:
            schema = schema.upper()
            table = table.upper()
            schema_table = schema + "." + table

            # Tablename underscored
            table_name_text = self._database + "_" + schema + "_" + table

            # Fileformat
            table_format = table_name_text + "_format"
            create_format_sql = (
                "create or replace file format "
                + table_format
                + " type = 'CSV' field_delimiter = '"
                + delimiter
                + "' skip_header = 1; "
            )
            print(create_format_sql)
            self.execute(create_format_sql)

            # Stage
            table_stage = table_name_text + "_stage"
            create_stage_sql = (
                "create or replace stage "
                + table_stage
                + " file_format = "
                + table_format
                + ";"
            )
            print(create_stage_sql)
            self.execute(create_stage_sql)

            # Split files
            file_abs = utils.abs_path(file)
            file_name_local = file_abs.split("/")[-1]
            file_dir = file_abs.replace(file_name_local, "")

            fs = FileSplit(file=file_abs, splitsize=50000000, output_dir=file_dir)
            fs.split()

            os.remove(file_abs)

            # put
            files = file_abs[:-4] + "*.csv"
            put_sql = (
                "PUT file://" + files + " @" + table_stage + " auto_compress=true;"
            )
            print(put_sql)
            self.execute(put_sql)
            file_name = file.split("/")[-1] + ".gz"
            files_stage = files.split("/")[-1]

            # copy
            copy_sql = (
                "COPY INTO "
                + schema_table
                + " FROM @"
                + table_stage
                + " file_format = (format_name = "
                + table_format
                + ") on_error = 'skip_file';"
            )
            print(copy_sql)
            self.execute(copy_sql)
            print("copy table success")

            # remove stage
            drop_stage_sql = "DROP STAGE IF EXISTS " + table_stage
            print(drop_stage_sql)
            self.execute(drop_stage_sql)
            print("stage deleted")

            # remove fileformat
            drop_file_format_sql = "DROP FILE FORMAT IF EXISTS " + table_format
            print(drop_file_format_sql)
            self.execute(drop_file_format_sql)
            print("format deleted")

            row_count = str(self.cursor.rowcount)

            logger.info(row_count + " records imported")

            return "DONE", row_count

        except:
            logger.error("Failed importing table")
            return "ERROR", e

    def generate_create_table_ddl(self, schema, table, columns):
        try:
            create_table_sql = "CREATE TABLE " + schema + "." + table + "(\n"
            for col in columns:

                ordinal_position = col[0]
                column_name = col[1]
                column_name = '"' + column_name + '"'
                data_type = col[2]
                data_type = python_type_to_db_type(data_type)
                character_maximum_length = col[3]
                numeric_precision = col[4]
                numeric_scale = col[5]

                if data_type == "VARCHAR":
                    if character_maximum_length <= 0:
                        column = column_name + " VARCHAR"
                    else:
                        column = (
                            column_name
                            + " VARCHAR"
                            + "("
                            + str(character_maximum_length)
                            + ")"
                        )
                elif data_type == "NUMBER":
                    column = (
                        column_name
                        + " NUMBER("
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

            return create_table_sql
        except Exception as e:
            logger.error(e)
            logger.error("Failed generating create table script")

    def create_table_from_columns(self, schema, table, columns):
        if self._read_only:
            sys.exit("This source is readonly. Terminating load run")

        schema = schema.upper()
        table = table.upper()
        full_table = schema + "." + table

        try:
            if self.check_table_exist(full_table):
                print("creating table")
                self.execute("DROP TABLE " + full_table)

            self.create_schema(schema)
            create_table_sql = self.generate_create_table_ddl(schema, table, columns)
            logger.debug(create_table_sql)
            self.execute(create_table_sql)
            logger.debug("table created")
        except:
            logger.error("Failed create table from columns")
