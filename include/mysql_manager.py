#!/bin/env python3

import pymysql
import include.database_manager

class mysql_manager(include.database_manager.database_manager):
    """MySQL DB connection and data collector"""

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
        self._connection = pymysql.Connection(user = self.params['user'],
                                              password = self.params['password'],
                                              host = self.params['host'],
                                              database = self.params['database'],
                                              unix_socket = self.params['unix_socket'],
                                              port = self.params['port'],
                                              charset = self.params['charset'],
                                              sql_mode = self.params['sql_mode'],
                                              read_default_file = self.params['read_default_file'],
                                              conv = self.params['conv'],
                                              use_unicode = self.params['use_unicode'],
                                              client_flag = self.params['client_flag'],
                                              init_command = self.params['init_command'],
                                              connect_timeout = self.params['connect_timeout'],
                                              read_default_group = self.params['read_default_group'],
                                              autocommit = self.params['autocommit'],
                                              local_infile = self.params['local_infile'],
                                              max_allowed_packet = self.params['max_allowed_packet'],
                                              defer_connect = self.params['defer_connect'],
                                              read_timeout = self.params['read_timeout'],
                                              write_timeout = self.params['write_timeout'],
                                              bind_address = self.params['bind_address'],
                                              binary_prefix = self.params['binary_prefix'],
                                              server_public_key = self.params['server_public_key'],
                                              ssl = self.params['ssl'])

    # Set data in arrays
    def GetData(self, input_file: str):
        sql = ""
        with open(input_file, 'r') as input_file_ptr:
            sql = input_file_ptr.read()

        cursor = self._connection.cursor()
        params_real = {}
        param_list = self._GetParamList(sql)
        for param_name in param_list:
            params_real[param_name] = self.query_params[param_name]

        cursor.execute(sql, params_real)
        self.data = cursor.fetchall()
    
        if cursor.description is not None:
            for key in cursor.description:
                self.headers.append(key[0])
