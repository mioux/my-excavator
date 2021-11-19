#!/bin/env python3
import configparser
import os
import sys

import include

help_options = [ '--help', '-h', '-?', '/?', '/h' ]
for option in sys.argv:
    if option.lower() in help_options:
        print('Full documentation is available online.')
        print('    https://github.com/mioux/my-excavator/wiki')
        print('You can clone documentation via github.')
        print('    git clone https://github.com/mioux/my-excavator.wiki.git')
        exit()

config = configparser.ConfigParser()
config.read('config/config.ini')

general_params = include.general_manager.general_manager()
# Read general params
include.config_manager.ReadParams('general', general_params.params, general_params.int_val, general_params.bool_val)

db: include.database_manager.database_manager = None

if general_params.params['db_type'] == 'mysql':
    import include.mysql_manager
    db = include.mysql_manager.mysql_manager()
elif general_params.params['db_type'] == 'postgres':
    import include.postgres_manager
    db = include.postgres_manager.postgres_manager()
elif general_params.params['db_type'] == 'mssql':
    import include.mssql_manager
    db = include.mssql_manager.mssql_manager()
elif general_params.params['db_type'] == 'sqlite3':
    import include.sqlite3_manager
    db = include.sqlite3_manager.sqlite3_manager()
else:
    raise NotImplementedError("{0} engine is not implemented.".format(general_params.params['db_type']))

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
for current_dir_path, current_subdirs, current_files in os.walk(general_params.params['data_in']):
    for file in current_files:
        if file.endswith(".sql"):
            input_file = str(os.path.join(current_dir_path, file))
            output_file = str(input_file).replace(general_params.params['data_in'], general_params.params['data_out'], 1)[:-4] + '.csv'
            db.GetData(input_file)
            include.csv_manager.RunExtraction(input_file, output_file, general_params.params, db.headers, db.data)


# Run posthooks
include.hooks_manager.ExecuteHooks(general_params.params['posthooks'])
