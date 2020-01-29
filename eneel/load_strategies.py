import eneel.load_functions as load_functions
import eneel.printer as printer

import logging

logger = logging.getLogger("main_logger")


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
    # Set initial returns
    return_code = "ERROR"
    export_row_count = 0
    import_row_count = 0

    # Full source table
    full_source_table = source_schema + "." + source_table

    try:
        # Temp table
        target_table_tmp = target_table + "_tmp"

        # Export table
        try:
            return_code, temp_path_load, delimiter, export_row_count = load_functions.export_table(
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
            return_code = "ERROR"

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Create temp table
        try:
            return_code = load_functions.create_temp_table(
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
            return_code = "ERROR"

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Import into temp table
        try:
            return_code, import_row_count = load_functions.import_into_temp_table(
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
            return_code = "ERROR"

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Switch tables
        try:
            return_code = load_functions.switch_table(
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
            return_code = "ERROR"

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
    # Set initial returns
    return_code = "ERROR"
    export_row_count = 0
    import_row_count = 0

    try:
        # Temp table
        target_table_tmp = target_table + "_tmp"

        # Export table
        try:
            return_code, temp_path_load, delimiter, export_row_count = load_functions.export_query(
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
            return_code = "ERROR"

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Create temp table
        try:
            return_code = load_functions.create_temp_table(
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
            return_code = "ERROR"

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Import into temp table
        try:
            return_code, import_row_count = load_functions.import_into_temp_table(
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
            return_code = "ERROR"

        if return_code == "ERROR":
            return return_code, export_row_count, import_row_count

        # Switch tables
        try:
            return_code = load_functions.switch_table(
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
            return_code = "ERROR"

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
    # Set initial returns
    return_code = "ERROR"
    export_row_count = 0
    import_row_count = 0

    full_source_table = source_schema + "." + source_table

    try:
        # Temp table
        target_table_tmp = target_table + "_tmp"

        full_target_table = target_schema + "." + target_table

        if not replication_key:
            printer.print_load_line(
                index,
                total,
                return_code,
                full_source_table,
                msg="replication key not defined",
            )
            return return_code, export_row_count, import_row_count

        if replication_key not in [column[1] for column in columns]:
            printer.print_load_line(
                index,
                total,
                return_code,
                full_source_table,
                msg="replication key not found in table",
            )
            return return_code, export_row_count, import_row_count

        # Get max replication key in target
        if target.check_table_exist(full_target_table):
            max_replication_key = target.get_max_column_value(
                full_target_table, replication_key
            )
            logger.debug(full_target_table + ' Max ' + replication_key + ' = ' + max_replication_key)

        else:
            max_replication_key = None
            printer.print_load_line(
                index,
                total,
                "RUN",
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
            try:
                return_code, temp_path_load, delimiter, export_row_count = load_functions.export_table(
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
            except Exception as e:
                logger.error(e)
                return_code = "ERROR"

            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Create temp table
            try:
                return_code = load_functions.create_temp_table(
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
                return_code = "ERROR"

            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Import into temp table
            try:
                return_code, import_row_count = load_functions.import_into_temp_table(
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
                return_code = "ERROR"

            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Insert into and drop
            try:
                return_code = load_functions.insert_from_table_and_drop_tmp(
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
                return_code = "ERROR"

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


def strategy_upsert(
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
    primary_key=None,
):
    # Set initial returns
    return_code = "ERROR"
    export_row_count = 0
    import_row_count = 0

    full_source_table = source_schema + "." + source_table

    try:
        # Temp table
        target_table_tmp = target_table + "_tmp"

        full_target_table = target_schema + "." + target_table

        if not replication_key:
            printer.print_load_line(
                index,
                total,
                return_code,
                full_source_table,
                msg="replication key not defined",
            )
            return return_code, export_row_count, import_row_count

        if replication_key not in [column[1] for column in columns]:
            printer.print_load_line(
                index,
                total,
                return_code,
                full_source_table,
                msg="replication key not found in table",
            )
            return return_code, export_row_count, import_row_count

        # Get max replication key in target
        if target.check_table_exist(full_target_table):
            max_replication_key = target.get_max_column_value(
                full_target_table, replication_key
            )
            logger.debug(full_target_table + ' Max ' + replication_key + ' = ' + max_replication_key)

        else:
            max_replication_key = None
            printer.print_load_line(
                index,
                total,
                "RUN",
                full_target_table,
                msg="does not exist in target. Starts FULL_TABLE load",
            )

        if not primary_key:
            printer.print_load_line(
                index,
                total,
                return_code,
                full_source_table,
                msg="primary key not defined",
            )
            return return_code, export_row_count, import_row_count

        if primary_key not in [column[1] for column in columns]:
            printer.print_load_line(
                index,
                total,
                return_code,
                full_source_table,
                msg="primary key not found in table",
            )
            return return_code, export_row_count, import_row_count

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
            try:
                return_code, temp_path_load, delimiter, export_row_count = load_functions.export_table(
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
            except Exception as e:
                logger.error(e)
                return_code = "ERROR"

            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Create temp table
            try:
                return_code = load_functions.create_temp_table(
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
                return_code = "ERROR"

            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Import into temp table
            try:
                return_code, import_row_count = load_functions.import_into_temp_table(
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
                return_code = "ERROR"

            if return_code == "ERROR":
                return return_code, export_row_count, import_row_count

            # Merge into and drop
            try:
                return_code = load_functions.merge_from_table_and_drop_tmp(
                    return_code,
                    index,
                    total,
                    target,
                    target_schema,
                    target_table,
                    target_table_tmp,
                    full_source_table,
                    primary_key,
                )
            except Exception as e:
                logger.error(e)
                return_code = "ERROR"

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
