#!/bin/env python3

import configparser
import os
import sys
import pymysql
import re
import include

class mysql_manager(object):
    """DB connection and data collector"""

    def _GetParamList(self, sql: str):
        param_names = '%\\(([^\\)]+)\\)s'
        param_list = re.findall(param_names, sql)
        return param_list

    def __init__(self):
        self.params = {
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
            'read_default_group':  False,
            'autocommit':          False,
            'local_infile':        False,
            'max_allowed_packet':  16777216,
            'defer_connect':       False,
            'server_public_key':   None,
            'binary_prefix':       False
        }

        self.config_int_val  = [ 'client_flag', 'read_timeout', 'write_timeout', 'connect_timeout', 'max_allowed_packet', 'port' ]
        self.config_bool_val = [ 'use_unicode', 'autocommit', 'defer_connect', 'binary_prefix' ]

        self.query_params = { }

        self.data = [ ]
        self.headers = [ ]

        self._connection = None

    def SetConnection(self):
        self._connection = pymysql.Connection(user = self.params['user'],\
                                              password = self.params['password'],\
                                              host = self.params['host'],\
                                              database = self.params['database'],\
                                              unix_socket = self.params['unix_socket'],\
                                              port = self.params['port'],\
                                              charset = self.params['charset'],\
                                              sql_mode = self.params['sql_mode'],\
                                              read_default_file = self.params['read_default_file'],\
                                              conv = self.params['conv'],\
                                              use_unicode = self.params['use_unicode'],\
                                              client_flag = self.params['client_flag'],\
                                              init_command = self.params['init_command'],\
                                              connect_timeout = self.params['connect_timeout'],\
                                              read_default_group = self.params['read_default_group'],\
                                              autocommit = self.params['autocommit'],\
                                              local_infile = self.params['local_infile'],\
                                              max_allowed_packet = self.params['max_allowed_packet'],\
                                              defer_connect = self.params['defer_connect'],\
                                              read_timeout = self.params['read_timeout'],\
                                              write_timeout = self.params['write_timeout'],\
                                              bind_address = self.params['bind_address'],\
                                              binary_prefix = self.params['binary_prefix'],\
                                              server_public_key = self.params['server_public_key'],\
                                              ssl = self.params['ssl'])

        
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

    # Set data in arrays
    def GetData(self, input_file: str):
        sql = ""
        with open(input_file, 'r') as input_file_ptr:
            sql = input_file_ptr.read()

        cursor = self._connection.cursor()
        params_real = {}
        param_list = self._GetParamList(sql)
        for param_name in param_list:
            params_real[param_name] = params[param_name]

        cursor.execute(sql, params_real)
        self.data = cursor.fetchall()
    
        if cursor.description is not None:
            for key in cursor.description:
                self.headers.append(key[0])
