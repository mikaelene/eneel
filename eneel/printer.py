import colorama
import time

import logging

logger = logging.getLogger("main_logger")


COLOR_FG_RED = colorama.Fore.RED
COLOR_FG_GREEN = colorama.Fore.GREEN
COLOR_FG_YELLOW = colorama.Fore.YELLOW
COLOR_RESET_ALL = colorama.Style.RESET_ALL


def get_color(color):
    if color == "red":
        return COLOR_FG_RED
    if color == "green":
        return COLOR_FG_GREEN
    if color == "yellow":
        return COLOR_FG_YELLOW
    else:
        return ""


PRINTER_WIDTH = 80


def get_timestamp():
    return time.strftime("%H:%M:%S")


def print_output_line(msg):
    output = "{timestamp} | {message}".format(timestamp=get_timestamp(), message=msg)
    logger.info(output)


def print_load_line(
    index, total, status, table, rows=None, execution_time=None, truncate=False, msg=""
):
    if execution_time is not None and rows is not None:
        rows_per_sec = f"at {str(int(int(rows) / execution_time))} rows/sec"
    else:
        rows_per_sec = ""
    if index is None or total is None:
        progress = ""
    else:
        progress = "{} of {} ".format(index, total)
    prefix = "{timestamp} | {progress}{status} {table} {rows_per_sec}{msg}".format(
        timestamp=get_timestamp(),
        progress=progress,
        status=status,
        table=table,
        rows_per_sec=rows_per_sec,
        msg=msg,
    )

    truncate_width = PRINTER_WIDTH - 3
    justified = prefix.ljust(PRINTER_WIDTH, ".")
    if truncate and len(justified) > truncate_width:
        justified = f"{justified[:truncate_width]}..."

    if execution_time is None:
        status_time = ""
    # elif rows is None:
    else:
        status_time = " in {execution_time:0.2f}s".format(execution_time=execution_time)
    #    else:
    #        rows_per_sec = str(int(int(rows) / execution_time))
    #        status_time = " rows in {execution_time:0.2f}s ({rows_per_sec} /sec)".format(
    #            execution_time=execution_time,
    #            rows_per_sec=rows_per_sec)

    if status == "DONE":
        rows = f"{get_color('green')}{rows}{COLOR_RESET_ALL}"
        output_txt = rows
    elif status == "WARN":
        rows = f"{get_color('yellow')}{rows}{COLOR_RESET_ALL}"
        output_txt = rows
    elif status == "ERROR":
        output_txt = f"{get_color('red')}{status}{COLOR_RESET_ALL}"
    else:
        output_txt = "RUN"

    output = "{justified} [{output}{status_time}]".format(
        justified=justified, output=output_txt, status_time=status_time
    )

    logger.info(output)


def print_msg(msg, color=None):
    if color:
        color = get_color(color)
        msg = f"{color}{msg}{COLOR_RESET_ALL}"
    logger.info(msg)
