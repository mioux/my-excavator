# my-excavator

This programs takes a list of .sql files in a folder, and converts its last output as csv.

All files are named like .sql file but with .csv extension.

## Requirements

This has been tested under Fedora 34 / CentOS 8 / Debian 10 with python 3.

All the requirements can be installed via pip.

- configparser 7.x (**Not ConfigParser** which is for Python 2.x)

You need to install packages accordingly to the engine you will use. SQLite don't need any additional packahe as it is provided with default package installation :

- PyMySQL 1.X (default in debian10 packages)
- Postgres 3.X
- mssql_python 1.X

    pip3 install --user 'PyMySQL<2' 'configparser<6' 'pymssql<3' 'postgres<4'

## Configuration

There is a template configuration file located in `config/config.ini-template`. Copy it to `config/config.ini` or create a new one.

It's a standard ini file. Here is an advanced example :

    [general]
    data-in=/path/to/sql/folder/
    data-out=/path/to/csv/folder/
    separator=;
    string-escape="
    out-format=utf-8

    [database]
    socket=/var/run/mysqld/mysqld.sock

    [param-int]
    group=17
    style=4

    [param-double]
    price=10.0

    [param-string]
    status=Ordered

    ;date format : YYYYMMDD hh:mm:ss
    ;            : YYYYMMDD
    ;because it is universaly converted as date by SQL Servers (works with SQL Server / MySQL / Postgres)
    [param-date]
    start_time=20210101

## Options

All the options in the ini file can be overloaded with `--<section>-<param>=<value>`
or with an environment variable named `<section>-<param>=<value>`.

To overload a parameter use `--param-<type>-<param-name>=<value>`.
