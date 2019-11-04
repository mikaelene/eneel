import eneel.utils as utils
from concurrent.futures import ThreadPoolExecutor as Executor
import os
import eneel.printer as printer
from time import time
from datetime import datetime
import eneel.config as config
from glob import glob

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
    with Executor(max_workers=workers) as executor:
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


def export_table(
    return_code,
    index,
    total,
    source,
    source_schema,
    source_table,
    columns,
    temp_path_load,
    csv_delimiter,
    replication_key=None,
    max_replication_key=None,
    parallelization_key=None,
):

    total_row_count = 0

    # Export table
    try:
        if parallelization_key:
            (
                min_parallelization_key,
                max_parallelization_key,
                batch_size_key,
            ) = source.get_min_max_batch(
                source_schema + "." + source_table, parallelization_key
            )
            batch_id = 1
            batch_start = min_parallelization_key

            file_paths = []
            querys = []
            csv_delimiters = []

            while batch_start <= max_parallelization_key:
                file_name = (
                    source._database
                    + "_"
                    + source_schema
                    + "_"
                    + source_table
                    + "_"
                    + str(batch_id)
                    + ".csv"
                )
                file_path = os.path.join(temp_path_load, file_name)

                # parallelization_where = source.get_parallelization_where(batch_start, batch_size_key)
                parallelization_where = (
                    parallelization_key
                    + " between "
                    + str(batch_start)
                    + " and "
                    + str(batch_start + batch_size_key - 1)
                )
                query = source.generate_export_query(
                    columns,
                    source_schema,
                    source_table,
                    replication_key,
                    max_replication_key,
                    parallelization_where,
                )

                file_paths.append(file_path)
                querys.append(query)
                csv_delimiters.append(csv_delimiter)

                batch_start += batch_size_key
                batch_id += 1

            table_workers = source._table_parallel_loads
            if len(querys) < table_workers:
                table_workers = len(querys)

            try:
                with Executor(max_workers=table_workers) as executor:
                    for row_count in executor.map(
                        source.export_query, querys, file_paths, csv_delimiters
                    ):
                        total_row_count += row_count
            except Exception as e:
                logger.error(e)

        else:

            file_name = (
                source._database + "_" + source_schema + "_" + source_table + ".csv"
            )
            file_path = os.path.join(temp_path_load, file_name)

            query = source.generate_export_query(
                columns,
                source_schema,
                source_table,
                replication_key,
                max_replication_key,
            )

            total_row_count = source.export_query(query, file_path, csv_delimiter)

        return_code = "RUN"
    except:
        return_code = "ERROR"
        full_source_table = source_schema + "." + source_table
        printer.print_load_line(
            index, total, return_code, full_source_table, msg="failed to export"
        )
    finally:
        return return_code, temp_path_load, csv_delimiter, total_row_count


def export_query(
    return_code,
    index,
    total,
    source,
    load_name,
    query,
    temp_path_load,
    csv_delimiter,
    parallelization_key=None,
):
    total_row_count = 0
    # Export table
    try:
        if parallelization_key:
            printer.print_load_line(
                index,
                total,
                return_code,
                load_name,
                msg="parallelization not implemented",
            )

        file_name = load_name + ".csv"
        file_path = os.path.join(temp_path_load, file_name)

        # query = source.generate_export_query(columns, source_schema, source_table,
        #                                        replication_key, max_replication_key)
        total_row_count = source.export_query(query, file_path, csv_delimiter)

        return_code = "RUN"
    except:
        return_code = "ERROR"
        printer.print_load_line(
            index, total, return_code, load_name, msg="failed to export"
        )
    finally:
        return return_code, temp_path_load, csv_delimiter, total_row_count


def create_temp_table(
    return_code,
    index,
    total,
    target,
    target_schema,
    target_table_tmp,
    columns,
    load_name,
):
    try:
        target.create_table_from_columns(target_schema, target_table_tmp, columns)
        return_code = "RUN"
    except:
        return_code = "ERROR"
        printer.print_load_line(
            index, total, return_code, load_name, msg="failed create temptable"
        )
    finally:
        return return_code


def import_into_temp_table(
    return_code,
    index,
    total,
    target,
    target_schema,
    target_table_tmp,
    temp_path_load,
    delimiter,
    load_name=None,
):
    try:
        csv_files = glob(os.path.join(temp_path_load, "*.csv"))
        target_schemas = []
        target_table_tmps = []
        temp_path_loads = []
        delimiters = []
        for file_path in csv_files:
            target_schemas.append(target_schema)
            target_table_tmps.append(target_table_tmp)
            temp_path_loads.append(file_path)
            delimiters.append(delimiter)

        table_workers = target._table_parallel_loads
        if len(temp_path_loads) < table_workers:
            table_workers = len(temp_path_loads)

        total_row_count = 0

        try:
            with Executor(max_workers=table_workers) as executor:
                for row_count in executor.map(
                    target.import_file,
                    target_schemas,
                    target_table_tmps,
                    temp_path_loads,
                    delimiters,
                ):
                    total_row_count += row_count
                return_code = "RUN"
        except Exception as e:
            logger.error(e)
            return_code = "ERROR"

        # return_code, import_row_count = target.import_table(target_schema, target_table_tmp, temp_path_load, delimiter)
    except:
        return_code = "ERROR"
        printer.print_load_line(
            index, total, return_code, load_name, msg="failed import into temptable"
        )
    finally:
        return return_code, total_row_count


def switch_table(
    return_code,
    index,
    total,
    target,
    target_schema,
    target_table,
    target_table_tmp,
    load_name=None,
):
    try:
        target.switch_tables(target_schema, target_table, target_table_tmp)
        return_code = "RUN"
    except:
        return_code = "ERROR"
        printer.print_load_line(
            index, total, return_code, load_name, msg="failed switching temptable"
        )
    finally:
        return return_code


def insert_from_table_and_drop_tmp(
    return_code,
    index,
    total,
    target,
    target_schema,
    target_table,
    target_table_tmp,
    load_name=None,
):
    try:
        return_code = target.insert_from_table_and_drop(
            target_schema, target_table, target_table_tmp
        )
    except:
        return_code = "ERROR"
        printer.print_load_line(
            index, total, "ERROR", load_name, msg="failed import from temptable"
        )
    finally:
        return return_code


def strategy_full_table_load(
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
    parallelization_key,
):
    export_row_count = None
    import_row_count = None
    # Full source table
    full_source_table = source_schema + "." + source_table

    try:
        # Temp table
        target_table_tmp = target_table + "_tmp"

        # Export table
        try:
            return_code, temp_path_load, delimiter, export_row_count = export_table(
                return_code,
                index,
                total,
                source,
                source_schema,
                source_table,
                columns,
                temp_path_load,
                csv_delimiter,
                replication_key=None,
                max_replication_key=None,
                parallelization_key=parallelization_key,
            )
        except Exception as e:
            logger.error(e)

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Create temp table
        try:
            return_code = create_temp_table(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table_tmp,
                columns,
                full_source_table,
            )
        except Exception as e:
            logger.error(e)

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Import into temp table
        try:
            return_code, import_row_count = import_into_temp_table(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table_tmp,
                temp_path_load,
                delimiter,
                full_source_table,
            )
        except Exception as e:
            logger.error(e)

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Switch tables
        try:
            return_code = switch_table(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table,
                target_table_tmp,
                full_source_table,
            )
        except Exception as e:
            logger.error(e)

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Return success
        if return_code == "RUN":
            return_code = "DONE"

    except:
        printer.print_load_line(
            index, total, return_code, full_source_table, msg="load failed"
        )
    finally:
        return return_code, export_row_count, import_row_count


def strategy_full_query_load(
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
    parallelization_key=None,
):
    export_row_count = None
    import_row_count = None

    try:
        # Temp table
        target_table_tmp = target_table + "_tmp"

        # Export table
        try:
            return_code, temp_path_load, delimiter, export_row_count = export_query(
                return_code,
                index,
                total,
                source,
                query_name,
                query,
                temp_path_load,
                csv_delimiter,
                parallelization_key,
            )
        except Exception as e:
            logger.error(e)

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Create temp table
        try:
            return_code = create_temp_table(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table_tmp,
                columns,
                query_name,
            )
        except Exception as e:
            logger.error(e)

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Import into temp table
        try:
            return_code, import_row_count = import_into_temp_table(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table_tmp,
                temp_path_load,
                delimiter,
            )
        except Exception as e:
            logger.error(e)

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Switch tables
        try:
            return_code = switch_table(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table,
                target_table_tmp,
            )
        except Exception as e:
            logger.error(e)

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Return success
        if return_code == "RUN":
            return_code = "DONE"

    except:
        printer.print_load_line(index, total, return_code, "query", msg="load failed")
    finally:
        return return_code, export_row_count, import_row_count


def strategy_incremental(
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
    replication_key=None,
    parallelization_key=None,
):
    export_row_count = None
    import_row_count = None
    full_source_table = source_schema + "." + source_table

    try:
        # Temp table
        target_table_tmp = target_table + "_tmp"

        full_target_table = target_schema + "." + target_table

        if not replication_key:
            printer.print_load_line(
                index,
                total,
                "ERROR",
                full_source_table,
                msg="replication key not defined",
            )
            return return_code

        if replication_key not in [column[1] for column in columns]:
            printer.print_load_line(
                index,
                total,
                "ERROR",
                full_source_table,
                msg="replication key not found in table",
            )
            return return_code

        # Get max replication key in target
        if target.check_table_exist(full_target_table):
            max_replication_key = target.get_max_column_value(
                full_target_table, replication_key
            )
        else:
            max_replication_key = None
            printer.print_load_line(
                index,
                total,
                return_code,
                full_target_table,
                msg="does not exist in target. Starts FULL_TABLE load",
            )

        # If no max replication key, do full load
        if not max_replication_key:
            return_code, export_row_count, import_row_count = strategy_full_table_load(
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

        else:
            # Export new rows
            return_code, temp_path_load, delimiter, export_row_count = export_table(
                return_code,
                index,
                total,
                source,
                source_schema,
                source_table,
                columns,
                temp_path_load,
                csv_delimiter,
                replication_key=replication_key,
                max_replication_key=max_replication_key,
                parallelization_key=parallelization_key,
            )
            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Create temp table
            return_code = create_temp_table(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table_tmp,
                columns,
                full_source_table,
            )
            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Import into temp table
            return_code, import_row_count = import_into_temp_table(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table_tmp,
                temp_path_load,
                delimiter,
                full_source_table,
            )
            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Insert into and drop
            return_code = insert_from_table_and_drop_tmp(
                return_code,
                index,
                total,
                target,
                target_schema,
                target_table,
                target_table_tmp,
                full_source_table,
            )
            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Return success
            if return_code == "RUN":
                return_code = "DONE"
    except:
        printer.print_load_line(
            index, total, return_code, full_source_table, msg="load failed"
        )
    finally:
        return return_code, export_row_count, import_row_count


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

    # Set initial return code
    return_code = "ERROR"

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

        # Source column types to exclude
        source_columntypes_to_exclude = project.get("source_columntypes_to_exclude")
        if source_columntypes_to_exclude:
            source_columntypes_to_exclude = (
                source_columntypes_to_exclude.lower().replace(" ", "").split(",")
            )

        # Columns to load
        try:
            columns = source.table_columns(source_schema, source_table)
            if source_columntypes_to_exclude:
                columns_to_load = columns.copy()
                for col in columns:
                    # data_type = col[2].lower()
                    data_type = col[2]
                    if data_type in source_columntypes_to_exclude:
                        columns_to_load.remove(col)
                columns = columns_to_load
        except:
            logger.error("Could not determine columns to load")
            return return_code
        print(columns)

        # Load type and settings
        replication_method = table.get("replication_method", "FULL_TABLE")
        parallelization_key = table.get("parallelization_key")
        replication_key = table.get("replication_key")

        return_code = "START"
        table_msg = full_source_table + " (" + replication_method + ")"
        printer.print_load_line(index, total, return_code, table_msg)

        # Full table load
        if not replication_method or replication_method == "FULL_TABLE":
            return_code, export_row_count, import_row_count = strategy_full_table_load(
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
            return_code, export_row_count, import_row_count = strategy_incremental(
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
        print(columns)

        # Load type and settings
        replication_method = query_item.get("replication_method", "FULL_TABLE")
        parallelization_key = query_item.get("parallelization_key")
        replication_key = query_item.get("replication_key")

        return_code = "START"
        table_msg = query_name + " (" + replication_method + ")"
        printer.print_load_line(index, total, return_code, table_msg)

        # Full table load
        if not replication_method or replication_method == "FULL_TABLE":
            return_code, export_row_count, import_row_count = strategy_full_query_load(
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
