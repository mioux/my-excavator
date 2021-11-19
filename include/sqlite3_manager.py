#!/bin/env python3

import sqlite3
import include.database_manager
import re

class sqlite3_manager(include.database_manager.database_manager):
    """Postgres DB connection and data collector"""

    # List all params in SQL file
    def _GetParamList(self, sql: str):
        param_names = ':([a-zA-Z0-9]+\\b)'
        param_list = re.findall(param_names, sql)
        return param_list

    # Defines defaults parameters
    def __init__(self):
        self.params = {
            'database': ':memory:',
            'timeout': 5,
            'detect_types': 0,
            'isolation_level': None,
            'check_same_thread': True,
            'cached_statements': 100,
            'uri': False
        }

        self.config_int_val  = [ 'detect_types', 'timeout' ]
        self.config_bool_val = [ 'uri', 'check_same_thread' ]

        self.query_params = { }

        self.data = [ ]
        self.headers = [ ]

        self._connection = None

    # Open connection
    def SetConnection(self):
        self._connection = sqlite3.connect(database = self.params['database'],
                                          timeout = self.params['timeout'],
                                          detect_types = self.params['detect_types'],
                                          isolation_level = self.params['isolation_level'],
                                          check_same_thread = self.params['check_same_thread'],
                                          cached_statements = self.params['cached_statements'],
                                          uri = self.params['uri'])

    # Set data in arrays
    def GetData(self, input_file: str):
        sql = ""
        with open(input_file, 'r', encoding='utf_8') as input_file_ptr:
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
