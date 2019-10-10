from eneel.utils import *
import pytest
import os


@pytest.fixture
def test_yml():
    test_yml = """schemas:
          - source_schema: "public"               # You can replicate from multiple schemas"""

    yield test_yml


@pytest.fixture
def fixture_file_contents(tmp_path):
    test_yml_path = tmp_path / 'test.yml'
    test_yml_path.write_text("""schemas:
      - source_schema: "public"               # You can replicate from multiple schemas""")
    yield test_yml_path


def test_create_path(tmp_path):
    path = tmp_path / 'testpath'
    created_rel_path = create_path(path)
    assert str(path) == created_rel_path


def test_delete_path(tmp_path):
    path = tmp_path / 'testpath'
    delete_path(path)
    assert not os.path.isdir(path)


def test_delete_file(fixture_file_contents):
    delete_file(fixture_file_contents)
    assert not os.path.isfile(fixture_file_contents)


def test_load_yaml(test_yml):
    yaml = load_yaml(test_yml)
    assert type(yaml) == dict


def test_load_file_contents(fixture_file_contents):
    content = load_file_contents(fixture_file_contents)
    assert content[:4] == 'sche'


def test_run_cmd():
    cmd = 'bcp'
    cmd_code, cmd_message = run_cmd(cmd)
    assert cmd_code == 1 and cmd_message
