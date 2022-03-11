from setuptools import find_packages
from distutils.core import setup
from src import __version__

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
    author_email="mikael.ene@eneanalytics.com",
    url="https://github.com/mikaelene/eneel",
    packages=find_packages(),
    install_requires=[
        'pyarrow~=6.0.0',
        'sqlalchemy~=1.4.31',
        'pydantic~=1.9.0',
        'PyYAML~=6.0'
    ],
    entry_points={
        'console_scripts': ['eneel=src.main:main'],
    },
)
