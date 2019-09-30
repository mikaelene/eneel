import pytest
from eneel.config import *
from shutil import copyfile
import os

test_path = 'testpath'
test_data = './tests/data/'
test_config_yml = 'test_connections.yml'
test_project_yml = 'test_project.yml'


class TestGetProject:

    def test_get_project_from_path(self):
        project_yml = os.path.join(test_data, test_project_yml)
        project_config = get_project(project_yml)
        assert type(project_config) == dict

    def test_get_project(self):
        project_config = get_project(test_project_yml)
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
    connections = Connections(connections_path=connections_path)

    assert connections.connections['postgres1']['type'] == 'postgres'


def test_Connections_target():
    connections_path = os.path.join(test_data, test_config_yml)
    connections_path = os.path.abspath(connections_path)
    connections = Connections(connections_path=connections_path, target='prod')

    assert connections.connections['postgres1']['type'] == 'postgres'


def test_Project():
    project_yml = os.path.join(test_data, test_project_yml)
    connections_path = os.path.join(test_data, test_config_yml)
    connections_path = os.path.abspath(connections_path)
    connections = Connections(connections_path=connections_path)
    project = Project(project_yml, connections.connections)

    assert project.source_name == 'postgres1'

