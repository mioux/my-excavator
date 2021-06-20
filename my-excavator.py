#!/bin/env python3
import configparser
import pymysql
import datetime
import csv
import sys
import distutils.util
import os
import subprocess
import re

# Read all params in an array, and sets values
def ReadParams(section: str, values: dict, int_values: list = None, bool_values: list = None):
    config = configparser.ConfigParser()
    config.read('config/config.ini')

    # First in INI file
    if section in config.sections():
        for key in config[section]:
            if int_values is not None and key in int_values:
                values[key] = config.getint(section, key)
            elif bool_values is not None and key in bool_values:
                values[key] = config.getboolean(section, key)
            elif key in values:
                values[key] = config.get(section, key)

    # Then in environment
    for cur_arg in os.environ:
        if cur_arg.startswith(section.upper() + '_'):
            arg_name = cur_arg[len(section) + 1:].lower()
            if int_values is not None and arg_name in int_values:
                values[arg_name] = int(os.environ[cur_arg])
            elif bool_values is not None and arg_name in bool_values:
                values[arg_name] = bool(distutils.util.strtobool(os.environ[cur_arg]))
            elif arg_name in values:
                values[arg_name] = os.environ[cur_arg]

    # Then on command line
    for cur_arg in sys.argv:
        arg = cur_arg.split('=', 1)
        if arg[0].startswith('--' + section.lower() + '-'):
            arg_name = arg[0][len(section) + 3:].replace('-', '_')
            if int_values is not None and arg_name in int_values:
                values[arg_name] = int(arg[1])
            elif bool_values is not None and arg_name in bool_values:
                values[arg_name] = bool(distutils.util.strtobool(arg[1]))
            elif arg_name in values:
                values[arg_name] = arg[1]

# Takes all params from ini, environment and cli, and sets it in environment for subprocesses
def set_env(data: dict, prefix: str):
    for key in data:
        if data[key] is not None:
            os.environ[prefix + '_' + key.upper()] = str(data[key])

# Execute all executables in pre-hook or post-hook folder
def execute_hooks(folder: str):
    hooks = os.listdir(folder)

    for file in hooks:
        real_file = os.path.join(folder, file)
        if os.path.isfile(real_file) == True and os.access(real_file, os.X_OK):
            subprocess.run(real_file)

# Convert string to datetime
def str_to_datetime(str_date: str):
    if (len(str_date) != 8) and (len(str_date) != 17):
        raise ValueError('Wrong datetime format.Should be YYYYMMDD HH:MM:SS with 24 hour.')
    elif len(str_date) == 8:
        data = datetime.datetime.strptime(str_date, '%Y%m%d')
    else:
        data = datetime.datetime.strptime(str_date, '%Y%m%d %H:%M:%S')

    return data

# Prepare params
def prepare_params(data_type: str, data: dict):
    param_type_name = 'param-' + data_type
    if param_type_name in config.sections():
        for key in config[param_type_name]:
            param_value = config[param_type_name][key]
            if data_type == 'int':
                data[key] = int(param_value)
            elif data_type == 'double':
                data[key] = float(param_value)
            elif data_type == 'datetime':
                data[key] = str_to_datetime(param_value)
            else:
                data[key] = str(param_value)

    for key in os.environ:
        if key.startswith(param_type_name.upper().replace('-', '_') + '_'):
            param_name = key.split('_', 2)[2]
            param_value = os.environ[key]
            if data_type == 'int':
                data[param_name] = int(param_value)
            elif data_type == 'double':
                data[param_name] = float(param_value)
            elif data_type == 'datetime':
                data[param_name] = str_to_datetime(param_value)
            else:
                data[param_name] = str(param_value)

    for key in sys.argv:
        argv_option = '--' + param_type_name + '-'
        if key.startswith(argv_option):
            arg = key.split('=', 1)
            param_value = arg[1]
            param_name = arg[0][len(argv_option):]
            if data_type == 'int':
                data[param_name] = int(param_value)
            elif data_type == 'double':
                data[param_name] = float(param_value)
            elif data_type == 'datetime':
                data[param_name] = str_to_datetime(param_value)
            else:
                data[param_name] = str(param_value)

def get_param_list(sql: str):
    param_names = '%\\(([^\\)]+)\\)s'
    param_list = re.findall(param_names, sql)
    return param_list

def run_extraction(input_file: str, output_file: str):
    print('Extract ' + input_file + ' to ' + output_file)
    output_file_ptr = open(output_file, 'w', encoding=general['output_format'])
    with open(input_file, 'r') as input_file_ptr:
        sql = input_file_ptr.read()

    if general['quote'].upper() == 'NONE':
        quote_type = csv.QUOTE_NONE
    elif general['quote'].upper() == 'ALL':
        quote_type = csv.QUOTE_ALL
    elif general['quote'].upper() == 'NONNUMERIC':
        quote_type = csv.QUOTE_NONNUMERIC
    else:
        quote_type = csv.QUOTE_MINIMAL

    output_csv = csv.writer(output_file_ptr, delimiter=general['separator'], quotechar=general['quotechar'], quoting=quote_type)

    cursor = connection.cursor()
    params_real = {}
    param_list = get_param_list(sql)
    for param_name in param_list:
        params_real[param_name] = params[param_name]

    cursor.execute(sql, params_real)
    data = cursor.fetchall()
    
    if general['headers'] == True:
        header_row = []
        if cursor.description is not None:
            for key in cursor.description:
                header_row.append(key[0])
            output_csv.writerow(header_row)

        if data is not None:
            for data_row in data:
                output_csv.writerow(data_row)

config = configparser.ConfigParser()
config.read('config/config.ini')

# Database param type
db_int_val  = [ 'client_flag', 'read_timeout', 'write_timeout', 'connect_timeout', 'max_allowed_packet', 'port' ]
db_bool_val = [ 'use_unicode', 'autocommit', 'defer_connect', 'binary_prefix' ]

# Default connection value
db = {
    'host':                None,
    'port':                0,
    'user':                None,
    'password':            None,
    'database':            None,
    'bind_address':        None,
    'unix_socket':         None,
    'read_timeout':        None,
    'write_timeout':       None,
    'charset':             '',
    'sql_mode':            None,
    'read_default_file':   False,
    'conv':                None,
    'use_unicode':         True,
    'client_flag':         0,
    'init_command':        None,
    'connect_timeout':     10,
    'ssl':                 None,
    'ssl_ca':              None,
    'ssl_cert':            None,
    'ssl_disabled':        None,
    'ssl_key':             None,
    'ssl_verify_cert':     None,
    'ssl_verify_identity': None,
    'read_default_group':  False,
    'autocommit':          False,
    'local_infile':        False,
    'max_allowed_packet':  16777216,
    'defer_connect':       False,
    'server_public_key':   None,
    'binary_prefix':       False
}

# Read databases connection parameters
ReadParams('database', db, db_int_val, db_bool_val)

connection = pymysql.Connection(user=db['user'],\
                                password=db['password'],\
                                host=db['host'],\
                                database=db['database'],\
                                unix_socket=db['unix_socket'],\
                                port=db['port'],\
                                charset=db['charset'],\
                                sql_mode=db['sql_mode'],\
                                read_default_file=db['read_default_file'],\
                                conv=db['conv'],\
                                use_unicode=db['use_unicode'],\
                                client_flag=db['client_flag'],\
                                init_command=db['init_command'],\
                                connect_timeout=db['connect_timeout'],\
                                read_default_group=db['read_default_group'],\
                                autocommit=db['autocommit'],\
                                local_infile=db['local_infile'],\
                                max_allowed_packet=db['max_allowed_packet'],\
                                defer_connect=db['defer_connect'],\
                                read_timeout=db['read_timeout'],\
                                write_timeout=db['write_timeout'],\
                                bind_address=db['bind_address'],\
                                binary_prefix=db['binary_prefix'],\
                                server_public_key=db['server_public_key'],\
                                ssl=db['ssl'])

general = {
    'data_in':       os.path.join('data', 'in'),
    'data_out':      os.path.join('data', 'out'),
    'prehooks':      os.path.join('hooks', 'prehook'),
    'posthooks':     os.path.join('hooks', 'posthook'),
    'headers':       True,
    'output_format': 'utf8',
    'separator':     ';',
    'quotechar':     '"',
    'quote':         'minimal',
    'end_line':      os.linesep
}

general_bool_val = [ 'headers' ]
general_int_val = None

ReadParams('general', general, db_int_val, general_int_val)

set_env(general, 'GENERAL')
set_env(db, 'DATABASE')

# Prepare params array
params = {}

# Define available types
param_types = [ 'int', 'double', 'string', 'datetime' ]

for cur_type in param_types:
    prepare_params(cur_type, params)

# Run prehooks
execute_hooks(general['prehooks'])

# Run extractions
for file in os.listdir(general['data_in']):
    if file.endswith(".sql"):
        input_file = os.path.join(general['data_in'], file)
        output_file = os.path.join(general['data_out'], file[:-4] + '.csv')
        run_extraction(input_file, output_file)
        

# Run posthooks
execute_hooks(general['posthooks'])
