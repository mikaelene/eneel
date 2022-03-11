import yaml

import logging

logging.basicConfig(format="%(levelname)s - %(asctime)s - %(processName)s - %(message)s",)
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


def read_yml(path: str) -> dict:
    with open(path, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(exc)


