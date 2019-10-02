import eneel.utils as utils
from concurrent.futures import ProcessPoolExecutor as Executor
import os
import eneel.printer as printer
from time import time
from datetime import datetime
import eneel.config as config

import logging
logger = logging.getLogger('main_logger')


def run_project(project_name, connections_path=None, target=None):

    project_name = project_name.lower()
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

    job_start_time = time()
    project_started_at = datetime.fromtimestamp(job_start_time)

    if project.logdb:
        try:
            logdb = config.connection_from_config(project.logdb['conninfo'])
            logdb.create_log_table(project.logdb['schema'], project.logdb['table'])
            logdb.log(project.logdb['schema'], project.logdb['table'],
                      project=project_name,
                      project_started_at=project_started_at,
                      started_at=project_started_at,
                      status='START')
            for load in project.loads:
                load.update({"project_started_at": project_started_at})
        except:
            logger.debug('Failed creating database logger')
            project.logdb = None
            for load in project.loads:
                load['logdbs'] = None

    # Execute parallel load
    load_results = []
    with Executor(max_workers=workers) as executor:
        for result in executor.map(run_load, project.loads):
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
    if not project.keep_tempfiles:
        utils.delete_path(project.temp_path)

    job_end_time = time()
    project_ended_at = datetime.fromtimestamp(job_end_time)

    execution_time = job_end_time - job_start_time

    status_time = " in {execution_time:0.2f}s".format(
        execution_time=execution_time)

    end_msg = "Finished loading " + str(project.num_tables_to_load) + " tables in " + status_time + ": " + \
                                                            str(load_successes) + " successfull, " + \
                                                            str(load_warnings) + " with warnings and " + \
                                                            str(load_errors) + " with errors"
    printer.print_output_line("")
    printer.print_output_line(end_msg)

    printer.print_msg("")
    if load_errors > 0:
        printer.print_msg("Completed with errors", "red")
    elif load_warnings > 0:
        printer.print_msg("Completed with warnings", "yellow")
    else:
        printer.print_msg("Completed successfully", "green")

    # Close connections
    if project.logdb:
        logdb.log(project.logdb['schema'], project.logdb['table'],
                  project_started_at=project_started_at,
                  project=project_name,
                  ended_at=project_ended_at,
                  status='END')

        logdb.close()


def run_load(project_load):
    load_order = project_load.get('load_order')
    num_tables_to_load = project_load.get('num_tables_to_load')
    project_name = project_load.get('project_name')
    source_conninfo = project_load.get('source_conninfo')
    target_conninfo = project_load.get('target_conninfo')
    logdb_conninfo = project_load.get('logdb')['conninfo']
    logdb_schema = project_load.get('logdb')['schema']
    logdb_table = project_load.get('logdb')['table']
    project_started_at = project_load.get('project_started_at')
    project = project_load.get('project')
    temp_path = project_load.get('temp_path')
    schema = project_load.get('schema')
    table = project_load.get('table')

    return_code = 'ERROR'

    load_start_time = time()

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
        return_code = "START"
        table_msg = full_source_table + " (" + "FULL_TABLE" + ")"
        printer.print_load_line(index, total, return_code, table_msg)

        # Export table
        try:
            file, delimiter, export_row_count = source.export_table(source_schema, source_table, columns, temp_path_load,
                                              csv_delimiter)
        except:
            printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed to export")
            return return_code
        try:
            # Create temp table
            target.create_table_from_columns(target_schema, target_table_tmp, columns)
        except:
            printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed create temptable")
            return return_code

        try:
            # Import into temp table
            return_code, import_row_count = target.import_table(target_schema, target_table_tmp, file, delimiter)
        except:
            printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed import into temptable")
            return return_code

        try:
            # Switch tables
            target.switch_tables(target_schema, target_table, target_table_tmp)
        except:
            printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed switching temptable")
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

            if target.check_table_exist(full_target_table):
                max_replication_key = target.get_max_column_value(full_target_table, replication_key)
            else:
                max_replication_key = None
                printer.print_load_line(index, total, return_code, full_target_table,
                                        msg="does not exist in target. Starts FULL_TABLE load")

            if not max_replication_key:
                # Export table
                try:
                    file, delimiter, export_row_count = source.export_table(source_schema, source_table, columns, temp_path_load,
                                                          csv_delimiter)
                except:
                    printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed to export")
                    return return_code
                try:
                    # Create temp table
                    target.create_table_from_columns(target_schema, target_table_tmp, columns)
                except:
                    printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed create temptable")
                    return return_code

                try:
                    # Import into temp table
                    return_code, import_row_count = target.import_table(target_schema, target_table_tmp, file,
                                                                        delimiter)
                except:
                    printer.print_load_line(index, total, "ERROR", full_source_table,
                                            msg="failed import into temptable")
                    return return_code

                try:
                    # Switch tables
                    target.switch_tables(target_schema, target_table, target_table_tmp)
                except:
                    printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed switching temptable")
                    return return_code

            else:
                try:
                    # Export new rows
                    file, delimiter, export_row_count = source.export_table(source_schema, source_table, columns, temp_path_load, csv_delimiter,
                                                          replication_key, max_replication_key)
                except:
                    printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed to export")
                    return return_code

                try:
                    # Create temp table
                    target.create_table_from_columns(target_schema, target_table_tmp, columns)
                except:
                    printer.print_load_line(index, total, "ERROR", full_source_table, msg="failed create temptable")
                    return return_code

                try:
                    # Import into temp table
                    return_code, import_row_count = target.import_table(target_schema, target_table_tmp, file,
                                                                        delimiter)
                except:
                    printer.print_load_line(index, total, "ERROR", full_source_table,
                                            msg="failed import into temptable")
                    return return_code

                try:
                    # Insert into and drop
                    target.insert_from_table_and_drop(target_schema, target_table, target_table_tmp)

                except:
                    printer.print_load_line(index, total, "ERROR", full_source_table,
                                            msg="failed import from temptable")
                    return return_code


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

    end_time = time()
    execution_time = end_time - load_start_time

    printer.print_load_line(index, total, return_code, full_source_table, import_row_count, execution_time)

    if logdb_conninfo:
        logdb = config.connection_from_config(logdb_conninfo)

        load_started_at = datetime.fromtimestamp(load_start_time)
        load_ended_at = datetime.fromtimestamp(end_time)
        logdb.log(logdb_schema, logdb_table,
                  project=project_name,
                  project_started_at=project_started_at,
                  source_table=full_source_table,
                  target_table=full_target_table,
                  started_at=load_started_at,
                  ended_at=load_ended_at,
                  status=return_code,
                  exported_rows=export_row_count,
                  imported_rows=import_row_count)

    return return_code

