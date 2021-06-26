#!/bin/env python3

import postgres
import include.database_manager

class postgres_manager(include.database_manager.database_manager):
    """Postgres DB connection and data collector"""

    # Defines defaults parameters
    def __init__(self):
        self.params = {
            'url':               '',
            'minconn':           1,
            'maxconn':           10,
            'idle_timeout':      600,
            'readonly':          True
        }

        self.config_int_val  = [ 'minconn', 'maxconn', 'idle_timeout' ]
        self.config_bool_val = [ 'readonly' ]

        self.query_params = { }

        self.data = [ ]
        self.headers = [ ]

        self._connection = None

    # Open connection
    def SetConnection(self):
        self._connection = postgres.Postgres(url = self.params['url'],
                                            minconn = self.params['minconn'],
                                            maxconn = self.params['maxconn'],
                                            idle_timeout = self.params['idle_timeout'],
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

        buffer = self._connection.all(sql, params_real, back_as=dict)
    
        if buffer is not None:
            for key in buffer[0]:
                self.headers.append(key)

            for line in buffer:
                line_data = [ ]
                for key in self.headers:
                    line_data.append(line[key])
                self.data.append(line_data)

