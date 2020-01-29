from concurrent.futures import ThreadPoolExecutor as ThreadExecutor
import os
import eneel.printer as printer
from glob import glob

import logging

logger = logging.getLogger("main_logger")


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
                    + "_"
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
                with ThreadExecutor(max_workers=table_workers) as executor:
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
            logger.debug('Export query: ' + query)

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

        if total_row_count is not None:
            return_code = "RUN"
        else:
            return_code = "ERROR"

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
            with ThreadExecutor(max_workers=table_workers) as executor:
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


def merge_from_table_and_drop_tmp(
    return_code,
    index,
    total,
    target,
    target_schema,
    target_table,
    target_table_tmp,
    load_name,
    primary_key,
):
    try:
        return_code = target.merge_from_table_and_drop(
            target_schema, target_table, target_table_tmp, primary_key
        )
    except:
        return_code = "ERROR"
        printer.print_load_line(
            index, total, "ERROR", load_name, msg="failed merge from temptable"
        )
    finally:
        return return_code

