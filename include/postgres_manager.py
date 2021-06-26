#!/bin/env python3

import configparser
import os
import sys
import postgres
import re
import include
import include.database_manager

class postgres_manager(include.database_manager.database_manager):
    """DB connection and data collector"""

    def __init__(self):
        self.params = {
            'url':               '',
            'minconn':           1,
            'maxconn':           10,
            'idle_timeout':      600,
            #'cursor_factory':   None,
            #'back_as_registry': None,
            #'pool_class':       None,
            #'cache':            None,
            'readonly':          True
        }

        self.config_int_val  = [ 'minconn', 'maxconn', 'idle_timeout' ]
        self.config_bool_val = [ 'readonly' ]

        self.query_params = { }

        self.data = [ ]
        self.headers = [ ]

        self._connection = None

    def SetConnection(self):
        self._connection = postgres.Postgres(url = self.params['url'],
                                            minconn = self.params['minconn'],
                                            maxconn = self.params['maxconn'],
                                            idle_timeout = self.params['idle_timeout'],
                                            #cursor_factory = self.params['unix_socket'],
                                            #back_as_registry = self.params['port'],
                                            #pool_class = self.params['charset'],
                                            #cache = self.params['sql_mode'],
                                            readonly = self.params['readonly'])

    # Set data in arrays
    def GetData(self, input_file: str):
        sql = ""
        with open(input_file, 'r') as input_file_ptr:
            sql = input_file_ptr.read()

        params_real = {}
        param_list = self._GetParamList(sql)
        for param_name in param_list:
            params_real[param_name] = self.query_params[param_name]

        self.data = self._connection.all(sql, params_real)
    
        if self.data.description is not None:
            for key in self.data.description:
                self.headers.append(key[0])
