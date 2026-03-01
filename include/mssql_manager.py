#!/bin/env python3

import mssql_python
import include.database_manager
import re

class mssql_manager(include.database_manager.database_manager):
    """Postgres DB connection and data collector"""

    # Defines defaults parameters
    def __init__(self):
        self.params = {
            'server':                   '.',
            'uid':                      None,
            'pwd':                      None,
            'authentication':           None,
            'trusted_connection':       None,
            'database':                 None,
            'encrypt':                  None,
            'trustservercertificate':   None,
            'hostnameincertificate':    None,
            'servercertificate':        None,
            'serverspn':                None,
            'multisubnetfailover':      None,
            'applicationintent':        None,
            'connectretrycount':        None,
            'connectretryinterval':     None,
            'keepalive':                None,
            'keepaliveinterval':        None,
            'ipaddresspreference':      None,
            'packetsize':               None,
            ###################################
            'timeout':                  0,
            'autocommit':               True
        }

        self.config_int_val  = [ 'timeout', 'login_timeout', 'port' ]
        self.config_bool_val = [ 'autocommit' ]

        self.query_params = { }

        self.data = [ ]
        self.headers = [ ]

        self._connection = None

    def ParamsToCS(self):
        CS = ""

        ConnectionParameters = {
            'server',
            'uid',
            'pwd',
            'authentication',
            'trusted_connection',
            'database',
            'encrypt',
            'trustservercertificate',
            'hostnameincertificate',
            'servercertificate',
            'serverspn',
            'multisubnetfailover',
            'applicationintent',
            'connectretrycount',
            'connectretryinterval',
            'keepalive',
            'keepaliveinterval',
            'ipaddresspreference',
            'packetsize'
        }

        for item in ConnectionParameters:
            value = self.params[item]
            if value is None:
                continue

            if type(value) is bool and value == True:
                value = "yes"
            elif type(value) is bool:
                value = "no"

            if ";" in str(value) or str(value).startswith("{"):
                value = "{"+str(value).replace("}", "}}")+"}" # Dans le cas où on démarre par { ou s'il y a un ; => {valeur} avec les } doublés

            CS += f"{item}={value};"

        return CS

    # Open connection
    def SetConnection(self):
        self._connection = mssql_python.connect(connection_str = self.ParamsToCS(),
                                                autocommit = self.params['autocommit'] == True,
                                                timeout = self.params['timeout'])

    # Set data in arrays
    def GetData(self, input_file: str):
        whole_sql = ""
        sql = ""
        self.headers = [ ]
        self.data = [ ]

        with open(input_file, 'r', encoding='utf_8') as input_file_ptr:
            whole_sql = input_file_ptr.read()

        parts = re.split(r'(?m)^GO(?:\s+\d*|)$', whole_sql)

        cursor = self._connection.cursor()

        for sql in parts:

            params_real = {}
            param_list = self._GetParamList(sql)
            for param_name in param_list:
                params_real[param_name] = self.query_params[param_name]

            cursor.execute(sql, params_real)

            while True:

                if cursor.description is not None:
                    buffer = cursor.fetchall()
                    self.headers = [ col[0] for col in cursor.description ]
                    self.data = [ ]

                    for line in buffer:
                        line_data = [ ]
                        for key in range(len(cursor.description)):
                            line_data.append(line[key])
                        self.data.append(line_data)

                ns = cursor.nextset()
                if ns == False:
                    break
