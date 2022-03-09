#!/usr/scripts/env python
from setuptools import find_packages
from distutils.core import setup
from eneel import __version__

import os
eneel_path = os.path.join(os.path.expanduser('~'), '.eneel')

package_name = "eneel"
package_version = __version__
description = """A package for fast loading av relational data"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    long_description_content_type='text/markdown',
    author="Mikael Ene",
    author_email="mikael.ene@gmail.com",
    url="https://github.com/mikaelene/eneel",
    packages=find_packages(),
    install_requires=[
        #'pyodbc>=4.0, <4.1',
        'psycopg2>=2.7, <3',
        'cx-Oracle>=7.0.0, <8',
        'PyYAML>=3, <7',
        'colorama>=0.3.9, <5',
        #'snowflake-connector-python>=1.8.4, <2.8',
        'filesplit==2.0.0',
    ],
    extras_require = {
        'sql_server':  [
            'pyodbc>=4.0, <4.1'
        ],
        'snowflake': ['snowflake-connector-python>=1.8.4, <2.8']
    },
    entry_points={
            'console_scripts': ['eneel=eneel.main:main'],
        },
    data_files = [
        (eneel_path, ['example_connections.yml']),
    ],
)
