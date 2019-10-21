import os
import sys
import subprocess
import shutil
import yaml
import logging
logger = logging.getLogger('main_logger')


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


def delete_file(file):
    if os.path.exists(file):
        try:
            os.remove(file)
        except:
            logger.debug("Could not delete file")


def load_yaml(stream):
    try:
        return yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        logger.error(exc)


def load_file_contents(path, strip=True):
    if not os.path.exists(path):
        logger.error(path + ' not found')

    with open(path, 'rb') as handle:
        to_return = handle.read().decode('utf-8')

    if strip:
        to_return = to_return.strip()

    return to_return


def run_cmd(cmd, envs=None):
    try:
        my_env = os.environ
        if envs:
            for env in envs:
                my_env[env[0]] = env[1]
        res = subprocess.run(cmd,
                             text=True,
                             capture_output=True,
                             check=True,
                             env=my_env,
                             encoding='ISO-8859-2')
        return res.returncode, res.stdout
    except subprocess.CalledProcessError as error:
        return error.returncode, error.stdout
    except FileNotFoundError as error:
        return 2, error
    except OSError as error:
        return 8, error
    except:
        return -1, sys.exc_info()[0]



