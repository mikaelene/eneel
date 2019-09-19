import logging
import os


def create_relative_path(path_name):
    if not os.path.exists(path_name):
        os.makedirs(path_name)


def get_logger(name=__name__):

    logger = logging.getLogger(name)
    logger.setLevel('DEBUG')

    # Stream handler
    stream_log_format = '%(asctime)s - %(levelname)s - %(message)s'

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_formatter = logging.Formatter(stream_log_format)
    stream_handler.setFormatter(stream_formatter)

    logger.addHandler(stream_handler)

    # File handler
    file_log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    #full_log_path = os.path.join(log_path, log_file_name)
    log_filename = "logs/eneel.log"
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(file_log_format)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)

    return logger

