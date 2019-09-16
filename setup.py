#!/usr/scripts/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "eneel"
package_version = "0.1.0"
description = """A package for fast loading av relation data"""

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
        'pyodbc>=4.0.27',
        'psycopg2>=2.7.7',
        'cx-Oracle>=7.0.0',
        'PyYAML>=3.11',
    ],
    entry_points={
            'console_scripts': ['eneel=eneel.main:main'],
        },
)
