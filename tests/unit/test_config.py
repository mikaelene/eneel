import pytest
from eneel.config import *
from shutil import copyfile
import os

test_path = 'testpath'
test_data = './tests/data/'
test_config_yml = 'test_connections.yml'
test_project_yml = 'test_project.yml'


def test_get_connections():

    connections = get_connections(test_data + test_config_yml, 'prod')
    assert connections['postgres1'].get('target') == 'prod'


def test_get_project():
    project_yml = os.path.join(test_data, test_project_yml)
    project_config = get_project(project_yml)
    assert type(project_config) == dict


def test_connection_from_config():
    credentials = {'host': 'localhost', 'port': 5432, 'user': 'mikaelene', 'password': 'password-1234',
                   'database': 'dvd2'}
    connection_info = {'name': 'postgres1', 'type': 'postgres', 'read_only': False, 'target': 'prod',
                          'credentials': credentials}

    connection = connection_from_config(connection_info)

    assert connection._dialect == 'postgres'


def test_Connections():
    connections_path = os.path.join(test_data, test_config_yml)
    connections_path = os.path.abspath(connections_path)
    connections = Connections(connections_path=connections_path, target='dev')

    assert connections.connections['postgres1']['type'] == 'postgres'


def test_Project():
    project_yml = os.path.join(test_data, test_project_yml)
    connections_path = os.path.join(test_data, test_config_yml)
    connections_path = os.path.abspath(connections_path)
    connections = Connections(connections_path=connections_path, target='dev')
    project = Project(project_yml, connections.connections)

    assert project.source_name == 'postgres1'