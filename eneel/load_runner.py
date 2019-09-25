import eneel.utils as utils
from concurrent.futures import ProcessPoolExecutor as Executor
import os
import sys
import eneel.printer as printer
import time

import logging
logger = logging.getLogger('main_logger')


def run_project(project_name, connections_path=None):
    # Get configurations
    connections_config = utils.get_connections(connections_path)
    project_config = utils.get_project(project_name)


    source_name = project_config['source']
    target_name = project_config['target']

    source_conninfo = connections_config[source_name]
    target_conninfo = connections_config[target_name]

    project = project_config.copy()
    del project['schemas']

    # Create temp dir
    temp_path = project.get('temp_path', 'temp')
    temp_path = temp_path + '/' + project_name
    temp_path = utils.create_path(temp_path)

    # Lists of load settings
    load_orders = []
    project_names = []
    source_conninfos = []
    target_conninfos = []
    projects = []
    schemas = []
    tables = []
    temp_paths = []

    # Populate load settings
    for schema_config in project_config['schemas']:
        schema = schema_config.copy()
        del schema['tables']
        order_num = 1
        for table in schema_config['tables']:
            source_conninfo_item = source_conninfo
            target_conninfo_item = target_conninfo
            project_item = project
            schema_item = schema
            table_item = table

            load_orders.append(order_num)
            order_num += 1
            project_names.append(project_name)
            source_conninfos.append(source_conninfo_item)
            target_conninfos.append(target_conninfo_item)
            projects.append(project_item)
            schemas.append(schema_item)
            tables.append(table_item)
            temp_paths.append(temp_path)

    # Parallel load settings
    num_tables_to_load = len(tables)

    num_tables_to_loads = []
    for i in range(num_tables_to_load):
        num_tables_to_loads.append(num_tables_to_load)

    workers = project.get('parallel_loads', 1)

    printer.print_msg('Running ' + project_name
                      + ' with ' + str(num_tables_to_load) + ' loadjobs from '
                      + source_name + ' to ' + target_name
                      )
    printer.print_msg('')

    if num_tables_to_load < workers:
        workers = num_tables_to_load
    start_msg = "Start loading " + str(num_tables_to_load) + " tables with " + str(workers) + " parallel workers"
    printer.print_output_line(start_msg)

    job_start_time = time.time()

    # Execute parallel load
    load_results = []
    with Executor(max_workers=workers) as executor:
        for result in executor.map(run_load, load_orders, num_tables_to_loads, project_names, source_conninfos, target_conninfos,
                              projects, temp_paths, schemas, tables):
            load_results.append(result)

    load_successes = 0
    load_warnings = 0
    load_errors = 0

    for load_result in load_results:
        if load_result == 'DONE':
            load_successes += 1
        if load_result == 'WARN':
            load_warnings += 1
        if load_result == 'ERROR':
            load_errors += 1

    # Clean up temp dir
    if not project.get('keep_tempfiles', False):
        utils.delete_path(temp_path)

    execution_time = time.time() - job_start_time

    status_time = " in {execution_time:0.2f}s".format(
        execution_time=execution_time)

    end_msg = "Finished loading " + str(num_tables_to_load) + " tables in " + status_time + ": " + \
                                                            str(load_successes) + " successfull, " + \
                                                            str(load_warnings) + " with warnings and " + \
                                                            str(load_errors) + " with errors"
    printer.print_output_line("")
    printer.print_output_line(end_msg)
    #logger.info("Finished loading " + str(num_tables_to_load) + " tables ")

    printer.print_msg("")
    if load_errors > 0:
        printer.print_msg("Completed with errors", "red")
    elif load_warnings > 0:
        printer.print_msg("Completed with warnings", "yellow")
    else:
        printer.print_msg("Completed successfully", "green")


def run_load(load_order, num_tables_to_load, project_name, source_conninfo, target_conninfo, project, temp_path, schema, table):
    return_code = 'ERROR'

    load_start_time = time.time()

    import eneel.logger as logger
    logger = logger.get_logger(project_name)

    # Remove duplicated handler if any
    for handler in logger.handlers[2:]:
        logger.removeHandler(handler)

    source = utils.connection_from_config(source_conninfo)
    target = utils.connection_from_config(target_conninfo)

    # Delimiter
    csv_delimiter = project.get('csv_delimiter')

    # Schemas
    source_schema = schema.get('source_schema')
    target_schema = schema.get('target_schema')

    # Tables
    source_table = table.get('table_name')
    full_source_table = source_schema + '.' + source_table
    target_table = schema.get('table_prefix', "") + table.get('table_name') + schema.get('table_suffix', "")
    full_target_table = target_schema + '.' + target_table
    index = load_order
    total = num_tables_to_load

    if not source.check_table_exist(full_source_table):
        printer.print_load_line(index, total, "ERROR", full_source_table, msg="does not exist in source")
        return return_code

    # Temp table
    target_table_tmp = target_table + '_tmp'

    # Temp path for specific load
    temp_path_schema = os.path.join(temp_path, source_schema)
    temp_path_load = os.path.join(temp_path_schema, source_table)
    utils.create_path(temp_path_load)

    # Source column types to exclude
    source_columntypes_to_exclude = project.get('source_columntypes_to_exclude')
    if source_columntypes_to_exclude:
        source_columntypes_to_exclude = source_columntypes_to_exclude.lower().replace(" ", "").split(",")

    # Columns to load
    try:
        columns = source.table_columns(source_schema, source_table)
        if source_columntypes_to_exclude:
            columns_to_load = columns.copy()
            for col in columns:
                data_type = col[2].lower()
                if data_type in source_columntypes_to_exclude:
                    columns_to_load.remove(col)
            columns = columns_to_load
    except:
        logger.error("Could not determine columns to load")
        return return_code

    # Load type
    replication_method = table.get('replication_method')

    if not replication_method or replication_method == "FULL_TABLE":
        index = load_order
        total = num_tables_to_load
        return_code = "START"
        table_msg = full_source_table + " (" + "FULL_TABLE" + ")"
        printer.print_load_line(index, total, return_code, table_msg)

        # Export table
        try:
            file, delimiter = source.export_table(source_schema, source_table, columns, temp_path_load,
                                              csv_delimiter)

            # Create temp table
            target.create_table_from_columns(target_schema, target_table_tmp, columns)

            # Import into temp table
            return_code, import_row_count = target.import_table(target_schema, target_table_tmp, file, delimiter)

            # Switch tables
            target.switch_tables(target_schema, target_table, target_table_tmp)

        except:
            printer.print_load_line(index, total, "ERROR", full_source_table, msg="load ")
            return return_code

    elif replication_method == "INCREMENTAL":
        try:
            index = load_order
            total = num_tables_to_load
            return_code = "START"
            table_msg = full_source_table + " (" + replication_method + ")"
            printer.print_load_line(index, total, return_code, table_msg)

            replication_key = table.get('replication_key')
            if not replication_key:
                printer.print_load_line(index, total, "ERROR", full_source_table, msg="replication key not defined")
                return return_code
            if replication_key not in [column[1] for column in columns]:
                printer.print_load_line(index, total, "ERROR", full_source_table, msg="replication key not found in table")
                return return_code

            max_replication_key = target.get_max_column_value(full_target_table, replication_key)
            if not max_replication_key:
                # Full export
                file, delimiter = source.export_table(source_schema, source_table, columns, temp_path_load,
                                                      csv_delimiter)
                # Create temp table
                target.create_table_from_columns(target_schema, target_table_tmp, columns)
                # Import into temp table
                return_code, import_row_count = target.import_table(target_schema, target_table_tmp, file, delimiter)
                # Switch tables
                target.switch_tables(target_schema, target_table, target_table_tmp)

            else:
                # Export new rows
                file, delimiter = source.export_table(source_schema, source_table, columns, temp_path_load, csv_delimiter,
                                                      replication_key, max_replication_key)
                # Create temp table
                target.create_table_from_columns(target_schema, target_table_tmp, columns)
                # Import into temp table
                return_code, import_row_count = target.import_table(target_schema, target_table_tmp, file, delimiter)
                # Insert into and drop
                target.insert_from_table_and_drop(target_schema, target_table, target_table_tmp)

        except:
            printer.print_load_line(index, total, "ERROR", full_source_table, msg="load failed")
            return return_code

    else:
        printer.print_load_line(index, total, "ERROR", full_source_table, msg="replication_method not valid")
        return return_code

    # delete temp folder
    if not project.get('keep_tempfiles', False):
        utils.delete_path(temp_path_load)

    # Close connections
    source.close()
    target.close()

    rows = import_row_count
    execution_time = time.time() - load_start_time
    printer.print_load_line(index, total, return_code, full_source_table, rows, execution_time)

    return return_code

