#!/bin/env python3

import configparser
import os
import sys
import datetime

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
                values[arg_name] = os.environ[cur_arg].lower() in ['1', 'true', 'yes', 'y', 't']
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
                values[arg_name] = arg[1].lower() in ['1', 'true', 'yes', 'y', 't']
            elif arg_name in values:
                values[arg_name] = arg[1]

# Takes all params from ini, environment and cli, and sets it in environment for subprocesses
def SetEnv(data: dict, prefix: str):
    for key in data:
        if data[key] is not None:
            os.environ[prefix + '_' + key.upper()] = str(data[key])

# Convert string to datetime
def StrToDatetime(str_date: str):
    if (len(str_date) != 8) and (len(str_date) != 17):
        raise ValueError('Wrong datetime format.Should be YYYYMMDD HH:MM:SS with 24 hour.')
    elif len(str_date) == 8:
        data = datetime.datetime.strptime(str_date, '%Y%m%d')
    else:
        data = datetime.datetime.strptime(str_date, '%Y%m%d %H:%M:%S')

    return data
