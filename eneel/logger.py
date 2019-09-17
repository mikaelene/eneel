import logging


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

    file_handler = logging.FileHandler("eneel.log")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(file_log_format)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)

    return logger

