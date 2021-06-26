#!/bin/env python3
import configparser
import datetime
import sys
import distutils.util
import os
import subprocess
import re

import include

# MySQL for now
import include.mysql_manager

config = configparser.ConfigParser()
config.read('config/config.ini')

general_params = include.general_manager.general_manager()
use_db = include.mysql_manager.mysql_manager()

# Read databases connection parameters
include.config_manager.ReadParams('database', use_db.params, use_db.config_int_val, use_db.config_bool_val)
include.config_manager.ReadParams('general', general_params.params, general_params.int_val, general_params.bool_val)

include.config_manager.SetEnv(general_params.params, 'GENERAL')
include.config_manager.SetEnv(use_db.params, 'DATABASE')

# Prepare params array
params = use_db.query_params

# Define available types
param_types = [ 'int', 'double', 'string', 'datetime' ]

for cur_type in param_types:
    use_db.PrepareParams(cur_type, config)

use_db.SetConnection()

# Run prehooks
include.hooks_manager.ExecuteHooks(general_params.params['prehooks'])

# Run extractions
for file in os.listdir(general_params.params['data_in']):
    if file.endswith(".sql"):
        input_file = os.path.join(general_params.params['data_in'], file)
        output_file = os.path.join(general_params.params['data_out'], file[:-4] + '.csv')
        use_db.GetData(input_file)
        include.csv_manager.RunExtraction(input_file, output_file, general_params.params, use_db.headers, use_db.data)
        

# Run posthooks
include.hooks_manager.ExecuteHooks(general_params.params['posthooks'])
