from concurrent.futures import ThreadPoolExecutor as ThreadExecutor, as_completed, wait, FIRST_COMPLETED
#import concurrent.futures
import os
import eneel.printer as printer
from glob import glob

import logging

logger = logging.getLogger("main_logger")



load_name = None,

def export_import_table(
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
    target_table_tmp,
    replication_key=None,
    max_replication_key=None,
    parallelization_key=None,
):

    total_export_row_count = 0
    total_import_row_count = 0

    # Export table
    try:
        if parallelization_key:
            (
                min_parallelization_key,
                max_parallelization_key,
                batch_size_key,
            ) = source.get_min_max_batch(
                f"{source_schema}.{source_table}", parallelization_key
            )
            logger.debug(f"{source_schema}.{source_table} parallelization_key=  {parallelization_key}, min: {min_parallelization_key}, max: {max_parallelization_key}, batch_size: {batch_size_key}")


            batch_start = min_parallelization_key

            batch_id = 1
            querys = []
            file_paths = []
            csv_delimiters = []
            target_schemas = []
            target_table_tmps = []

            while batch_start <= max_parallelization_key:
                file_name = (
                    f"{source._database}_{source_schema}_{source_table}_"
                    f"{str(batch_id)}_.csv"
                )
                file_path = os.path.join(temp_path_load, file_name)

                # parallelization_where = source.get_parallelization_where(batch_start, batch_size_key)
                parallelization_where = (
                    f"{parallelization_key} between {str(batch_start)} and {str(batch_start + batch_size_key - 1)}"
                )
                query = source.generate_export_query(
                    columns,
                    source_schema,
                    source_table,
                    replication_key,
                    max_replication_key,
                    parallelization_where,
                )

                batch_start += batch_size_key
                batch_id += 1
                querys.append(query)
                file_paths.append(file_path)
                csv_delimiters.append(csv_delimiter)
                target_schemas.append(target_schema)
                target_table_tmps.append(target_table_tmp)

            table_workers = source._table_parallel_loads
            if len(querys) < table_workers:
                table_workers = len(querys)

            import queue
            q = queue.Queue()

            total_exported = 0
            total_loaded = 0
            try:
                with ThreadExecutor(max_workers=table_workers) as export_executor:
                    future_extract = [export_executor.submit(source.export_query, query, file_path, csv_delimiter)
                                           for query, file_path, csv_delimiter in zip(querys, file_paths, csv_delimiters)]



                    #while future_extract:
                    #    done, not_done = wait(future_extract, timeout=None, return_when=FIRST_COMPLETED)

                    while future_extract:
                        extract_done, extract_not_done = wait(future_extract, timeout=None, return_when=FIRST_COMPLETED)

                        # while not q.empty():
                        #     logger.debug(q.qsize())
                        #     file_path_to_load = q.get()
                        #     logger.debug(file_path_to_load)
                        #     future_load = [export_executor.submit(target.import_file, target_schema, target_table_tmp,
                        #                                        file_path_to_load, csv_delimiter)]
                        #
                        #     while future_load:
                        #         load_done, load_not_done = wait(future_load, timeout=None,
                        #                                         return_when=FIRST_COMPLETED)
                        #
                        #     for future in load_done:
                        #         total_import_row_count += future.result()
                        #         total_loaded += 1
                        #         logger.debug(f"{target_schema}.{target_table_tmp}: "
                        #                      f"{str(total_loaded)} of {str(len(querys))} imports completed. "
                        #                      f"{str(total_import_row_count)} rows imported")
                        #         future_load.remove(future)

                        #for future in as_completed(future_extract):
                        for future in extract_done:
                            file_path_to_load = future.result()[1]
                            q.put(future.result()[1])
                            total_export_row_count += future.result()[0]
                            total_exported += 1
                            logger.debug(f"{source_schema}.{source_table}: "
                                  f"{str(total_exported)} of {str(len(querys))} exports completed. "
                                  f"{str(total_export_row_count)} rows exported")
                            future_extract.remove(future)

                            future_load = [export_executor.submit(target.import_file, target_schema, target_table_tmp,
                                                                  file_path_to_load, csv_delimiter)]

                            #while future_load:
                            #    load_done, load_not_done = wait(future_load, timeout=None,
                            #                                          return_when=FIRST_COMPLETED)

                            # If I remove the printing it loads as expected
                            for future in as_completed(future_load):
                            #for future in load_done:
                                print(future.result())
                                total_import_row_count += future.result()
                                #total_import_row_count += import_row_count
                                logger.debug(f"{target_schema}.{target_table_tmp}: "
                                                              f"{str(total_exported)} of {str(len(querys))} imports completed. "
                                                              f"{str(total_import_row_count)} rows imported")







                            # import_row_count = target.import_file(target_schema, target_table_tmp,
                            #                                             future.result()[1], csv_delimiter)
                            # total_import_row_count += import_row_count
                            # logger.debug(f"{target_schema}.{target_table_tmp}: "
                            #                               f"{str(total_exported)} of {str(len(querys))} imports completed. "
                            #                               f"{str(total_import_row_count)} rows imported")

                    # # if there is incoming work, start a new future
                    # while not q.empty():
                    #     print(q.qsize())
                    #     # fetch a url from the queue
                    #     file_path_to_load = q.get()
                    #     print(file_path_to_load)
                    #
                    #     # Start the load operation and mark the future with its URL
                    #     future_load = load_executor.submit(target.import_file, target_schema, target_table_tmp, file_path_to_load, csv_delimiter)
                    #
                    #     for future in as_completed(future_load):
                    #         print(future.result())
                    #         total_import_row_count += future.result()
                    #         #total_import_row_count += import_row_count
                    #         logger.debug(f"{target_schema}.{target_table_tmp}: "
                    #                                       f"{str(total_exported)} of {str(len(querys))} imports completed. "
                    #                                       f"{str(total_import_row_count)} rows imported")

            except Exception as e:
                logger.error(e)

        else:

            file_name = (
                f"{source._database}_{source_schema}_{source_table}.csv"
            )
            file_path = os.path.join(temp_path_load, file_name)

            query = source.generate_export_query(
                columns,
                source_schema,
                source_table,
                replication_key,
                max_replication_key,
            )
            #logger.debug(f"Export query: {query}")

            total_export_row_count = source.export_query(query, file_path, csv_delimiter)

        return_code = "RUN"
    except:
        return_code = "ERROR"
        full_source_table = f"{source_schema}.{source_table}"
        printer.print_load_line(
            index, total, return_code, full_source_table, msg="failed to export"
        )
    #
    # try:
    #     csv_files = glob(os.path.join(temp_path_load, "*.csv"))
    #     target_schemas = []
    #     target_table_tmps = []
    #     temp_path_loads = []
    #     csv_delimiters = []
    #     for file_path in csv_files:
    #         target_schemas.append(target_schema)
    #         target_table_tmps.append(target_table_tmp)
    #         temp_path_loads.append(file_path)
    #         csv_delimiters.append(csv_delimiter)
    #
    #     table_workers = target._table_parallel_loads
    #     if len(temp_path_loads) < table_workers:
    #         table_workers = len(temp_path_loads)
    #
    #     total_import_row_count = 0
    #
    #     total_loaded = 0
    #     try:
    #         with ThreadExecutor(max_workers=table_workers) as executor:
    #             future_to_row_count = [executor.submit(target.import_file, target_schema, target_table_tmp, temp_path_load, csv_delimiter)
    #                                    for target_schema, target_table_tmp, temp_path_load, csv_delimiter in zip(target_schemas, target_table_tmps, temp_path_loads, csv_delimiters)]
    #
    #             for future in as_completed(future_to_row_count):
    #                 total_import_row_count += future.result()
    #                 total_loaded += 1
    #                 logger.debug(f"{target_schema}.{target_table_tmp}: "
    #                              f"{str(total_loaded)} of {str(len(temp_path_loads))} imports completed. "
    #                              f"{str(total_import_row_count)} rows imported")
    #             return_code = "RUN"
    #     except Exception as e:
    #         logger.error(e)
    #         return_code = "ERROR"


    finally:
        return return_code, total_export_row_count, total_import_row_count


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
                f"{source_schema}.{source_table}", parallelization_key
            )
            logger.debug(f"{source_schema}.{source_table} parallelization_key=  {parallelization_key}, min: {min_parallelization_key}, max: {max_parallelization_key}, batch_size: {batch_size_key}")
            batch_id = 1
            batch_start = min_parallelization_key

            file_paths = []
            querys = []
            csv_delimiters = []

            while batch_start <= max_parallelization_key:
                file_name = (
                    f"{source._database}_{source_schema}_{source_table}_"
                    f"{str(batch_id)}_.csv"
                )
                file_path = os.path.join(temp_path_load, file_name)

                # parallelization_where = source.get_parallelization_where(batch_start, batch_size_key)
                parallelization_where = (
                    f"{parallelization_key} between {str(batch_start)} and {str(batch_start + batch_size_key - 1)}"
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

            total_loaded = 0
            try:
                with ThreadExecutor(max_workers=table_workers) as executor:
                    future_to_row_count = [executor.submit(source.export_query, query, file_path, csv_delimiter)
                                           for query, file_path, csv_delimiter in zip(querys, file_paths, csv_delimiters)]

                    for future in as_completed(future_to_row_count):
                        total_row_count += future.result()
                        total_loaded += 1
                        logger.debug(f"{source_schema}.{source_table}: "
                              f"{str(total_loaded)} of {str(len(querys))} exports completed. "
                              f"{str(total_row_count)} rows exported")
            except Exception as e:
                logger.error(e)

        else:

            file_name = (
                f"{source._database}_{source_schema}_{source_table}.csv"
            )
            file_path = os.path.join(temp_path_load, file_name)

            query = source.generate_export_query(
                columns,
                source_schema,
                source_table,
                replication_key,
                max_replication_key,
            )
            #logger.debug(f"Export query: {query}")

            total_row_count = source.export_query(query, file_path, csv_delimiter)

        return_code = "RUN"
    except:
        return_code = "ERROR"
        full_source_table = f"{source_schema}.{source_table}"
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

        file_name = f"{load_name}.csv"
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

        total_loaded = 0
        try:
            with ThreadExecutor(max_workers=table_workers) as executor:
                future_to_row_count = [executor.submit(target.import_file, target_schema, target_table_tmp, temp_path_load, delimiter)
                                       for target_schema, target_table_tmp, temp_path_load, delimiter in zip(target_schemas, target_table_tmps, temp_path_loads, delimiters)]

                for future in as_completed(future_to_row_count):
                    total_row_count += future.result()
                    total_loaded += 1
                    logger.debug(f"{target_schema}.{target_table_tmp}: "
                                 f"{str(total_loaded)} of {str(len(temp_path_loads))} imports completed. "
                                 f"{str(total_row_count)} rows imported")
                return_code = "RUN"
        except Exception as e:
            logger.error(e)
            return_code = "ERROR"

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

