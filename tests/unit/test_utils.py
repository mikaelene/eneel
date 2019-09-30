import pytest
from eneel.utils import *
import os

test_path = 'testpath'
test_yml_path = './tests/data/test.yml'
test_yml = """schemas:
      - source_schema: "public"               # You can replicate from multiple schemas"""


def test_create_path(tmp_path):
    path = tmp_path / test_path
    created_rel_path = create_path(path)
    assert str(path) == created_rel_path


def test_delete_path(tmp_path):
    path = tmp_path / test_path
    delete_path(path)
    assert not os.path.isdir(path)


def test_delete_file(tmp_path):
    d = tmp_path
    p = d / 'hello.txt'
    p.write_text('test')
    delete_file(p)
    assert not os.path.isfile(p)


def test_load_yaml():
    stream = test_yml
    yaml = load_yaml(stream)
    assert type(yaml) == dict


def test_load_file_contents():
    content = load_file_contents(test_yml_path)
    assert content == test_yml


def test_run_cmd():
    cmd = 'ls'
    cmd_code, cmd_message = run_cmd(cmd)
    assert cmd_code == 0 and cmd_message
