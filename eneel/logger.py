import logging
import os
import time
import colorama

colorama.init()


def get_logger(project="eneel"):

    logger = logging.getLogger("main_logger")
    logger.setLevel("DEBUG")

    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(message)s"))
    stream_handler.setLevel(logging.INFO)

    logger.addHandler(stream_handler)

    # File handler
    file_log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    log_filename = "logs/" + project + ".log"
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    if os.path.isfile(log_filename):
        if time.time() - os.path.getmtime(log_filename) > 600:
            try:
                os.remove(log_filename)
            except PermissionError:
                pass

    file_handler = logging.FileHandler(log_filename, mode='a')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(file_log_format)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)

    return logger
