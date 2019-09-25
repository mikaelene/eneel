import os
import sys
import subprocess
import shutil
import yaml
import eneel.adapters.postgres as postgres
import eneel.adapters.oracle as oracle
import eneel.adapters.sqlserver as sqlserver
import logging
logger = logging.getLogger('main_logger')


def create_relative_path(path_name):
    if not os.path.exists(path_name):
        os.makedirs(path_name)


def create_path(path_name):
    # Create path
    if not os.path.exists(path_name):
        os.makedirs(path_name)

    # Absolute path
    abs_temp_file_dir = os.path.abspath(path_name)
    return abs_temp_file_dir


def delete_path(path_name):
    if os.path.exists(path_name):
        try:
            shutil.rmtree(path_name)
        except:
            logger.debug("Could not delete directory")
            pass


def delete_file(file):
    if os.path.exists(file):
        os.remove(file)


def load_yaml_from_path(path):
    with open(path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(exc)


def load_yaml(stream):
    try:
        return yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        logger.error(exc)


def load_file_contents(path, strip=True):
    if not os.path.exists(path):
        logger.error(path, ' not found')

    with open(path, 'rb') as handle:
        to_return = handle.read().decode('utf-8')

    if strip:
        to_return = to_return.strip()

    return to_return


def run_cmd(cmd):
    res = subprocess.run(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, universal_newlines=True)
    if res.returncode == 0:
        return res.returncode, res.stdout
    else:
        return res.returncode, res.stderr



