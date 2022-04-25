import yaml

from pyarrow.filesystem import FileSystem
from pyarrow import fs
from adlfs import AzureBlobFileSystem

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


def file_system_from_uri(uri: str) -> FileSystem:
    if 'windows.net' in uri:
        return AzureBlobFileSystem(connection_string=uri)

    else:
        return fs.FileSystem.from_uri(uri)
