#!/bin/env python3

import configparser
import os
import sys
import re
import include
import include.database_manager

class database_manager(object):
    """Common database methods"""

    # Set defaults parameters
    def __init__(self):
        self.params = { }

        self.config_int_val  = [ ]
        self.config_bool_val = [ ]

    # List all params in SQL file
    def _GetParamList(self, sql: str):
        param_names = '%\\(([^\\)]+)\\)s'
        param_list = re.findall(param_names, sql)
        return param_list

    # Open connection
    def SetConnection(self):
        raise NotImplementedError("SetConnection not implemented for this engine.")

    # Prepare params
    def PrepareParams(self, data_type: str, config: configparser.ConfigParser):
        param_type_name = 'param-' + data_type
        if param_type_name in config.sections():
            for key in config[param_type_name]:
                param_value = config[param_type_name][key]
                if data_type == 'int':
                    self.query_params[key] = int(param_value)
                elif data_type == 'double':
                    self.query_params[key] = float(param_value)
                elif data_type == 'datetime':
                    self.query_params[key] = include.config_manager.StrToDatetime(param_value)
                else:
                    self.query_params[key] = str(param_value)

        for key in os.environ:
            if key.startswith(param_type_name.upper().replace('-', '_') + '_'):
                param_name = key.split('_', 2)[2]
                param_value = os.environ[key]
                if data_type == 'int':
                    self.query_params[param_name] = int(param_value)
                elif data_type == 'double':
                    self.query_params[param_name] = float(param_value)
                elif data_type == 'datetime':
                    self.query_params[param_name] = include.config_manager.StrToDatetime(param_value)
                else:
                    self.query_params[param_name] = str(param_value)

        for key in sys.argv:
            argv_option = '--' + param_type_name + '-'
            if key.startswith(argv_option):
                arg = key.split('=', 1)
                param_value = arg[1]
                param_name = arg[0][len(argv_option):]
                if data_type == 'int':
                    self.query_params[param_name] = int(param_value)
                elif data_type == 'double':
                    self.query_params[param_name] = float(param_value)
                elif data_type == 'datetime':
                    self.query_params[param_name] = include.config_manager.StrToDatetime(param_value)
                else:
                    self.query_params[param_name] = str(param_value)

    # Set data in database_manager object
    def GetData(self):
        raise NotImplementedError("SetConnection not implemented for this engine.")
