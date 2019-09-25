import eneel.utils as utils
from concurrent.futures import ProcessPoolExecutor as Executor
import os
import eneel.printer as printer
import time
import eneel.config as config

import logging
logger = logging.getLogger('main_logger')


def run_project(project_name, connections_path=None, target=None):
    # Connections
    connections = config.Connections(connections_path, target)

    # Project
    project = config.Project(project_name, connections.connections)

    printer.print_msg('Running ' + project.project_name
                      + ' with ' + str(project.num_tables_to_load) + ' loadjobs from '
                      + project.source_name + ' to ' + project.target_name
                      )
    printer.print_msg('')

    workers = project.workers

    if project.num_tables_to_load < workers:
        workers = project.num_tables_to_load
    start_msg = "Start loading " + str(project.num_tables_to_load) + " tables with " + str(workers) + " parallel workers"
    printer.print_output_line(start_msg)

    job_start_time = time.time()

    # Execute parallel load
    load_results = []
    with Executor(max_workers=workers) as executor:
        for result in executor.map(run_load, project.loads):
            load_results.append(result)

    load_errors = 0
    load_successes = 0

    for load_result in load_results:
        if load_result == 'error':
            load_errors += 1
        if load_result == 'success':
            load_successes += 1

    # Clean up temp dir
    if not project.keep_tempfiles:
        utils.delete_path(project.temp_path)

    execution_time = time.time() - job_start_time

    status_time = " in {execution_time:0.2f}s".format(
        execution_time=execution_time)

    end_msg = "Finished loading " + str(load_successes) + " of " + str(project.num_tables_to_load) + \
              " tables successfully in " + status_time
    printer.print_output_line("")
    printer.print_output_line(end_msg)
    #logger.info("Finished loading " + str(num_tables_to_load) + " tables ")

    printer.print_msg("")
    if load_errors > 0:
        printer.print_msg("Completed with errors", "red")
    else:
        printer.print_msg("Completed successfully", "green")


def run_load(project_load):
    load_order = project_load.get('load_order')
    num_tables_to_load = project_load.get('num_tables_to_load')
    project_name = project_load.get('project_name')
    source_conninfo = project_load.get('source_conninfo')
    target_conninfo = project_load.get('target_conninfo')
    project = project_load.get('project')
    temp_path = project_load.get('temp_path')
    schema = project_load.get('schema')
    table = project_load.get('table')

    return_code = 'error'

    load_start_time = time.time()

    import eneel.logger as logger
    logger = logger.get_logger(project_name)

    # Remove duplicated handler if any
    for handler in logger.handlers[2:]:
        logger.removeHandler(handler)

    source = config.connection_from_config(source_conninfo)
    target = config.connection_from_config(target_conninfo)

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
        status = "START"
        table_msg = full_source_table + " (" + "FULL_TABLE" + ")"
        printer.print_load_line(index, total, status, table_msg)

        # Export table
        try:
            file, delimiter = source.export_table(source_schema, source_table, columns, temp_path_load,
                                              csv_delimiter)

            # Create temp table
            target.create_table_from_columns(target_schema, target_table_tmp, columns)

            # Import into temp table
            import_status, import_row_count = target.import_table(target_schema, target_table_tmp, file, delimiter)

            # Switch tables
            target.switch_tables(target_schema, target_table, target_table_tmp)

        except:
            printer.print_load_line(index, total, "ERROR", full_source_table, msg="load ")
            return return_code

    elif replication_method == "INCREMENTAL":
        try:
            index = load_order
            total = num_tables_to_load
            status = "START"
            table_msg = full_source_table + " (" + replication_method + ")"
            printer.print_load_line(index, total, status, table_msg)

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
                import_status, import_row_count = target.import_table(target_schema, target_table_tmp, file, delimiter)
                # Switch tables
                target.switch_tables(target_schema, target_table, target_table_tmp)

            else:
                # Export new rows
                file, delimiter = source.export_table(source_schema, source_table, columns, temp_path_load, csv_delimiter,
                                                      replication_key, max_replication_key)
                # Create temp table
                target.create_table_from_columns(target_schema, target_table_tmp, columns)
                # Import into temp table
                import_status, import_row_count = target.import_table(target_schema, target_table_tmp, file, delimiter)
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

    status = import_status
    rows = import_row_count
    execution_time = time.time() - load_start_time
    printer.print_load_line(index, total, status, full_source_table, rows, execution_time)
    return_code = 'success'
    return return_code

