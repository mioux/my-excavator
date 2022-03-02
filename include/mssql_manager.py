#!/bin/env python3

import pymssql
import include.database_manager

class mssql_manager(include.database_manager.database_manager):
    """Postgres DB connection and data collector"""

    # Defines defaults parameters
    def __init__(self):
        self.params = {
            'server':          '.',
            'user':            None,
            'password':        None,
            'database':        '',
            'timeout':         0,
            'login_timeout':   60,
            'charset':         'UTF-8',
            'host':            '',
            'appname':         'my-excavator',
            'port':            1433,
            'conn_properties': None,
            'autocommit':      False,
            'tds_version':     None
        }

        self.config_int_val  = [ 'timeout', 'login_timeout', 'port' ]
        self.config_bool_val = [ 'autocommit' ]

        self.query_params = { }

        self.data = [ ]
        self.headers = [ ]

        self._connection = None

    # Open connection
    def SetConnection(self):
        self._connection = pymssql.connect(server = self.params['server'],
                                          user = self.params['user'],
                                          password = self.params['password'],
                                          database = self.params['database'],
                                          timeout = self.params['timeout'],
                                          login_timeout = self.params['login_timeout'],
                                          charset = self.params['charset'],
                                          as_dict = True,
                                          host = self.params['host'],
                                          appname = self.params['appname'],
                                          port = self.params['port'],
                                          conn_properties = self.params['conn_properties'],
                                          autocommit = self.params['autocommit'],
                                          tds_version = self.params['tds_version'])

    # Set data in arrays
    def GetData(self, input_file: str):
        sql = ""
        self.headers = [ ]
        with open(input_file, 'r', encoding='utf_8') as input_file_ptr:
            sql = input_file_ptr.read()

        cursor = self._connection.cursor()
        params_real = {}
        param_list = self._GetParamList(sql)
        for param_name in param_list:
            params_real[param_name] = self.query_params[param_name]

        cursor.execute(sql, params_real)
        buffer = cursor.fetchall()

        if buffer is not None and len(buffer) > 0:
            for key in buffer[0]:
                self.headers.append(key)

            for line in buffer:
                line_data = [ ]
                for key in self.headers:
                    line_data.append(line[key])
                self.data.append(line_data)
