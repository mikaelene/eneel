import eneel.utils as utils
import eneel.load_strategies as load_strategies
from concurrent.futures import ProcessPoolExecutor as ProcessExecutor
import os
import eneel.printer as printer
from time import time
from datetime import datetime
import eneel.config as config


import logging

logger = logging.getLogger("main_logger")


def run_project(project_name, connections_path=None, target=None):

    project_name = project_name.lower()
    # Connections
    connections = config.Connections(connections_path, target)

    # Project
    project = config.Project(project_name, connections.connections)

    printer.print_msg(
        "Running "
        + project.project_name
        + " with "
        + str(project.num_tables_to_load)
        + " loadjobs from "
        + project.source_name
        + " to "
        + project.target_name
    )
    printer.print_msg("")

    workers = project.workers

    # Set number of workers
    if project.num_tables_to_load < workers:
        workers = project.num_tables_to_load
    start_msg = (
        "Start loading "
        + str(project.num_tables_to_load)
        + " tables with "
        + str(workers)
        + " parallel workers"
    )
    printer.print_output_line(start_msg)

    # Job start time variable
    job_start_time = time()
    project_started_at = datetime.fromtimestamp(job_start_time)

    # Set up logdb
    if project.logdb:
        try:
            logdb = config.connection_from_config(project.logdb["conninfo"])
            logdb.create_log_table(project.logdb["schema"], project.logdb["table"])
            logdb.log(
                project.logdb["schema"],
                project.logdb["table"],
                project=project_name,
                project_started_at=project_started_at,
                started_at=project_started_at,
                status="START",
            )
            for load in project.loads:
                load.update({"project_started_at": project_started_at})
        except:
            logger.debug("Failed creating database logger")
            project.logdb = None
            for load in project.loads:
                load["logdbs"] = None

    # Execute parallel load
    load_results = []
    with ProcessExecutor(max_workers=workers) as executor:
        for result in executor.map(run_load, project.loads):
            load_results.append(result)

    # Parse result from parallel loads
    load_successes = 0
    load_warnings = 0
    load_errors = 0
    for load_result in load_results:
        if load_result == "DONE":
            load_successes += 1
        if load_result == "WARN":
            load_warnings += 1
        if load_result == "ERROR":
            load_errors += 1

    # Clean up temp dir
    if not project.keep_tempfiles:
        utils.delete_path(project.temp_path)

    # Project end logging
    job_end_time = time()
    project_ended_at = datetime.fromtimestamp(job_end_time)
    execution_time = job_end_time - job_start_time
    status_time = " in {execution_time:0.2f}s".format(execution_time=execution_time)
    end_msg = (
        "Finished loading "
        + str(project.num_tables_to_load)
        + " tables in "
        + status_time
        + ": "
        + str(load_successes)
        + " successful, "
        + str(load_warnings)
        + " with warnings and "
        + str(load_errors)
        + " with errors"
    )
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
        logdb.log(
            project.logdb["schema"],
            project.logdb["table"],
            project_started_at=project_started_at,
            project=project_name,
            ended_at=project_ended_at,
            status="END",
        )

        logdb.close()


def run_load(project_load):
    # Common attributes
    load_order = project_load.get("load_order")
    num_tables_to_load = project_load.get("num_tables_to_load")
    project_name = project_load.get("project_name")
    project_started_at = project_load.get("project_started_at")
    project = project_load.get("project")
    temp_path = project_load.get("temp_path")
    index = load_order
    total = num_tables_to_load

    # Connections info
    source_conninfo = project_load.get("source_conninfo")
    target_conninfo = project_load.get("target_conninfo")
    if project_load.get("logdb"):
        logdb_conninfo = project_load.get("logdb")["conninfo"]
        logdb_schema = project_load.get("logdb")["schema"]
        logdb_table = project_load.get("logdb")["table"]
    else:
        logdb_conninfo = None
        logdb_schema = None
        logdb_table = None

    # Set initial returns
    return_code = "ERROR"
    export_row_count = 0
    import_row_count = 0

    # Set load starttime
    load_start_time = time()

    # Start logger. Seems to persist over load jobs when process are reused
    import eneel.logger as logger
    logger = logger.get_logger(project_name)

    # Remove duplicated handler if any
    for handler in logger.handlers[2:]:
        logger.removeHandler(handler)

    # Connect to databases
    source = config.connection_from_config(source_conninfo)
    target = config.connection_from_config(target_conninfo)

    csv_delimiter = project.get("csv_delimiter", "|")

    if project_load.get("schema"):
        # Project and load info

        schema = project_load.get("schema")
        table = project_load.get("table")

        # Load details
        source_schema = schema.get("source_schema")
        target_schema = schema.get("target_schema")
        source_table = table.get("table_name")
        full_source_table = source_schema + "." + source_table
        target_table = (
            schema.get("table_prefix", "")
            + table.get("table_name")
            + schema.get("table_suffix", "")
        )
        full_target_table = target_schema + "." + target_table

        # If source doesn't exist
        if not source.check_table_exist(full_source_table):
            printer.print_load_line(
                index, total, "ERROR", full_source_table, msg="does not exist in source"
            )
            return return_code

        # Temp path for specific load
        temp_path_schema = os.path.join(temp_path, source_schema)
        temp_path_load = os.path.join(temp_path_schema, source_table)
        utils.delete_path(temp_path_load)
        utils.create_path(temp_path_load)

        # Columns to load
        columns = source.table_columns(source_schema, source_table)
        columns = source.remove_unsupported_columns(columns)

        # Load type and settings
        replication_method = table.get("replication_method", "FULL_TABLE")
        parallelization_key = table.get("parallelization_key")
        replication_key = table.get("replication_key")
        primary_key = table.get("primary_key")

        return_code = "START"
        table_msg = full_source_table + " (" + replication_method + ")"
        printer.print_load_line(index, total, return_code, table_msg)

        # Full table load
        if not replication_method or replication_method == "FULL_TABLE":
            return_code, export_row_count, import_row_count = load_strategies.strategy_full_table_load(
                return_code,
                index,
                total,
                source,
                source_schema,
                source_table,
                columns,
                temp_path_load,
                csv_delimiter,
                target,
                target_schema,
                target_table,
                parallelization_key=parallelization_key,
            )

        # Incremental replication
        elif replication_method == "INCREMENTAL":
            return_code, export_row_count, import_row_count = load_strategies.strategy_incremental(
                return_code,
                index,
                total,
                source,
                source_schema,
                source_table,
                columns,
                temp_path_load,
                csv_delimiter,
                target,
                target_schema,
                target_table,
                replication_key=replication_key,
                parallelization_key=parallelization_key,
            )

        # Upsert replication
        elif replication_method == "UPSERT":
            return_code, export_row_count, import_row_count = load_strategies.strategy_upsert(
                return_code,
                index,
                total,
                source,
                source_schema,
                source_table,
                columns,
                temp_path_load,
                csv_delimiter,
                target,
                target_schema,
                target_table,
                replication_key=replication_key,
                parallelization_key=parallelization_key,
                primary_key=[primary_key],
            )

        else:
            printer.print_load_line(
                index,
                total,
                "ERROR",
                full_source_table,
                msg="replication_method not valid",
            )
            return return_code

        # delete temp folder
        if not project.get("keep_tempfiles", False):
            utils.delete_path(temp_path_load)

        # Close connections
        source.close()
        target.close()

        # Load end, and execution time
        end_time = time()
        execution_time = end_time - load_start_time

        printer.print_load_line(
            index,
            total,
            return_code,
            full_source_table,
            str(import_row_count),
            execution_time,
        )

        if logdb_conninfo:
            logdb = config.connection_from_config(logdb_conninfo)

            load_started_at = datetime.fromtimestamp(load_start_time)
            load_ended_at = datetime.fromtimestamp(end_time)
            logdb.log(
                logdb_schema,
                logdb_table,
                project=project_name,
                project_started_at=project_started_at,
                source_table=full_source_table,
                target_table=full_target_table,
                started_at=load_started_at,
                ended_at=load_ended_at,
                status=return_code,
                exported_rows=export_row_count,
                imported_rows=import_row_count,
            )
            logdb.close()

        return return_code

    # QUERIES
    if project_load.get("query"):

        # Load details
        query_item = project_load.get("query")
        query_name = query_item.get("query_name")
        query = query_item.get("query")
        target_schema = project_load.get("target_schema")
        target_table = query_item.get("table_name")
        full_target_table = target_schema + "." + target_table

        # Temp path for specific load
        temp_path_schema = os.path.join(temp_path, "queries")
        temp_path_load = os.path.join(temp_path_schema, query_name)
        utils.delete_path(temp_path_load)
        utils.create_path(temp_path_load)

        # Columns to load
        columns = source.query_columns(query)

        # Load type and settings
        replication_method = query_item.get("replication_method", "FULL_TABLE")
        parallelization_key = query_item.get("parallelization_key")
        replication_key = query_item.get("replication_key")

        return_code = "START"
        table_msg = query_name + " (" + replication_method + ")"
        printer.print_load_line(index, total, return_code, table_msg)

        # Full table load
        if not replication_method or replication_method == "FULL_TABLE":
            return_code, export_row_count, import_row_count = load_strategies.strategy_full_query_load(
                return_code,
                index,
                total,
                source,
                query_name,
                query,
                columns,
                temp_path_load,
                csv_delimiter,
                target,
                target_schema,
                target_table,
                parallelization_key=parallelization_key,
            )

        # Incremental replication
        elif replication_method == "INCREMENTAL":
            printer.print_load_line(
                index,
                total,
                "ERROR",
                query_name,
                msg="INCREMENTAL not implemented for queries",
            )

        else:
            printer.print_load_line(
                index, total, "ERROR", query_name, msg="replication_method not valid"
            )
            return return_code

        # delete temp folder
        if not project.get("keep_tempfiles", False):
            utils.delete_path(temp_path_load)

        # Close connections
        source.close()
        target.close()

        # Load end, and execution time
        end_time = time()
        execution_time = end_time - load_start_time

        printer.print_load_line(
            index, total, return_code, query_name, str(import_row_count), execution_time
        )

        if logdb_conninfo:
            logdb = config.connection_from_config(logdb_conninfo)

            load_started_at = datetime.fromtimestamp(load_start_time)
            load_ended_at = datetime.fromtimestamp(end_time)
            logdb.log(
                logdb_schema,
                logdb_table,
                project=project_name,
                project_started_at=project_started_at,
                source_table="query",
                target_table=full_target_table,
                started_at=load_started_at,
                ended_at=load_ended_at,
                status=return_code,
                exported_rows=export_row_count,
                imported_rows=import_row_count,
            )
            logdb.close()

        return return_code
