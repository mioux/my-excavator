#!/bin/env python3
import configparser
import os

import include

config = configparser.ConfigParser()
config.read('config/config.ini')

general_params = include.general_manager.general_manager()
# Read general params
include.config_manager.ReadParams('general', general_params.params, general_params.int_val, general_params.bool_val)

db: include.database_manager.database_manager = None

if general_params.params['db_type'] == 'mysql':
    db = include.mysql_manager.mysql_manager()
elif general_params.params['db_type'] == 'postgres':
    db = include.mysql_manager.postgres_manager()
else:
    raise NotImplementedError("{0} engine is not implemented.".format(general_params.params['db_type']))

db = include.mysql_manager.mysql_manager()
# Read databases connection parameters
include.config_manager.ReadParams('database', db.params, db.config_int_val, db.config_bool_val)

include.config_manager.SetEnv(general_params.params, 'GENERAL')
include.config_manager.SetEnv(db.params, 'DATABASE')

# Prepare params array
params = db.query_params

# Define available types
param_types = [ 'int', 'double', 'string', 'datetime' ]

for cur_type in param_types:
    db.PrepareParams(cur_type, config)

db.SetConnection()

# Run prehooks
include.hooks_manager.ExecuteHooks(general_params.params['prehooks'])

# Run extractions
for file in os.listdir(general_params.params['data_in']):
    if file.endswith(".sql"):
        input_file = os.path.join(general_params.params['data_in'], file)
        output_file = os.path.join(general_params.params['data_out'], file[:-4] + '.csv')
        db.GetData(input_file)
        include.csv_manager.RunExtraction(input_file, output_file, general_params.params, db.headers, db.data)
        

# Run posthooks
include.hooks_manager.ExecuteHooks(general_params.params['posthooks'])
