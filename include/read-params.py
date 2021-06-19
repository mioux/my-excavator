#!/bin/env python3
import sys
import os
import configparser
import distutils.util


def ReadParams(section, values, int_values = None, bool_values = None):
    config = configparser.ConfigParser()
    config.read('config/config.ini')

    # First in INI file
    if section in config.sections():
        for key in config[section]:
            if key in int_values:
                values[key] = config.getint(section, key)
            elif key in bool_values:
                values[key] = config.getboolean(section, key)
            elif key in values:
                values[key] = config.get(section, key)

    # Then in environment
    for cur_arg in os.environ:
        if cur_arg.startswith(section.upper() + '_'):
            arg_name = cur_arg[len(section) + 1:].lower()
            if arg_name in int_values:
                values[arg_name] = int(os.environ[cur_arg])
            elif arg_name in bool_values:
                values[arg_name] = bool(distutils.util.strtobool(os.environ[cur_arg]))
            elif arg_name in values:
                values[arg_name] = os.environ[cur_arg]

    # Then on command line
    for cur_arg in sys.argv:
        arg = cur_arg.split('=', 1)
        if arg[0].startswith('--' + section.lower() + '-'):
            arg_name = arg[0][len(section) + 3:]
            if arg_name in int_values:
                values[arg_name] = int(arg[1])
            elif arg_name in bool_values:
                values[arg_name] = bool(distutils.util.strtobool(arg[1]))
            elif arg_name in values:
                values[arg_name] = arg[1]