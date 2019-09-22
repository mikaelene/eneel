import colorama
import time

import logging
logger = logging.getLogger('main_logger')


COLOR_FG_RED = colorama.Fore.RED
COLOR_FG_GREEN = colorama.Fore.GREEN
COLOR_FG_YELLOW = colorama.Fore.YELLOW
COLOR_RESET_ALL = colorama.Style.RESET_ALL

PRINTER_WIDTH = 80


def get_timestamp():
    return time.strftime("%H:%M:%S")


def print_fancy_output_line(msg, status, index, total, execution_time=None,
                            truncate=False):
    if index is None or total is None:
        progress = ''
    else:
        progress = '{} of {} '.format(index, total)
    prefix = "{timestamp} | {progress}{message}".format(
        timestamp=get_timestamp(),
        progress=progress,
        message=msg)

    truncate_width = PRINTER_WIDTH - 3
    justified = prefix.ljust(PRINTER_WIDTH, ".")
    if truncate and len(justified) > truncate_width:
        justified = justified[:truncate_width] + '...'

    if execution_time is None:
        status_time = ""
    else:
        status_time = " in {execution_time:0.2f}s".format(
            execution_time=execution_time)

    if status == "OK":
        status = COLOR_FG_GREEN + status + COLOR_RESET_ALL

    status_txt = status

    output = "{justified} [{status}{status_time}]".format(
        justified=justified, status=status_txt, status_time=status_time)

    logger.info(output)


def print_output_line(msg):
    output = "{timestamp} | {message}".format(
        timestamp=get_timestamp(),
        message=msg)
    logger.info(output)


def print_load_line(index, total, status, table, rows=None, execution_time=None,
                    truncate=False):
    if index is None or total is None:
        progress = ''
    else:
        progress = '{} of {} '.format(index, total)
    prefix = "{timestamp} | {progress}{status} {table}".format(
        timestamp=get_timestamp(),
        progress=progress,
        status=status,
        table=table)

    truncate_width = PRINTER_WIDTH - 3
    justified = prefix.ljust(PRINTER_WIDTH, ".")
    if truncate and len(justified) > truncate_width:
        justified = justified[:truncate_width] + '...'

    if execution_time is None:
        status_time = ""
    else:
        status_time = " in {execution_time:0.2f}s".format(
            execution_time=execution_time)

    if status == "DONE":
        rows = COLOR_FG_GREEN + rows + COLOR_RESET_ALL
        output_txt = rows
    else:
        output_txt = "RUN"

    output = "{justified} [{output}{status_time}]".format(
        justified=justified, output=output_txt, status_time=status_time)

    logger.info(output)

