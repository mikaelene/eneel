# eneel
eneel is a cli utility for fast extracting and loading data to and from relational databases. The E and L in ELT!

The goal is to be the fastest tool in the market. Both in terms of development speed and data movements speed.


Features:
- Fastest way to extract and load large amounts of data between different databases
- Automatic table creation and conversion of datatypes
- Load a query from one database to another
- Loads are executed in parallel 
- Incremental loads
- Pretty runtime logging of load progress
- Database logging
- Limit rows load in development
- Configuration with yaml files. Just list the tables you want to replicate
- Configuration files approach makes versioning with git etc easier

## Installation
Make sure you have the cli-tools for your required databases then:
- With pip:


    pip install eneel


- Or clone the repository, go to the directory and:


    python setup.py install

## Configuration
After installation an example [connections.yml](example_connections.yml) file will be in your home directory (~/.eneel). That's where you configure your connection info to your sources and targets.

Next, create a [project configuration file](example_project.yml) in a directory for you EL projects.

## Running eneel
Go to the directory with the project configuration file and run eneel with the project file name excluding yml. I.e for a project configuration file as load_from_postgres_prod_to_dw.yml run:

    eneel load_from_postgres_prod_to_dw

The output will then be something like below for at successfull run:
![alt text](etc/output.png)

Optional parameters:
- `--connections`: add a path to the connections.yml you would like to use
- `--target`: specify which target in connections.yml you would like to use. This will be applied on both sources and targets


## Feature matrix
Database | Source | Target
--- | :---: | :---: |
Postgres | YES | YES
Sql Server | YES | YES
Oracle | YES | NO

## Roadmap
- Support for [Snowflake](https://www.snowflake.com)
- Support for [BigQuery](https://cloud.google.com/bigquery/)
- Incremental loads with updates
- Incremental loads with deletes

## Reporting bugs and contributing code
- Go ahead and report an [issue](https://github.com/mikaelene/eneel/issues)
- Want to help? Pull requests are most welcome!