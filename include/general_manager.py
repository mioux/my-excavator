#!/bin/env python3

import os

class general_manager(object):
    """description of class"""

    def __init__(self):

        self.params = {
            'data_in':       os.path.join('data', 'in'),
            'data_out':      os.path.join('data', 'out'),
            'prehooks':      os.path.join('hooks', 'prehook'),
            'posthooks':     os.path.join('hooks', 'posthook'),
            'headers':       True,
            'output_format': 'utf8',
            'separator':     ';',
            'quotechar':     '"',
            'quote':         'minimal',
            'end_line':      os.linesep,
            'db_type':       'mysql'
        }

        self.bool_val = [ 'headers' ]
        self.int_val = None
