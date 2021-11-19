#!/bin/env python3
import csv
import os

# Run extraction
def RunExtraction(input_file: str, output_file: str, general: dict, headers_list: list = None, data: dict = None):
    print('Extract ' + input_file + ' to ' + output_file)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    output_file_ptr = open(output_file, 'w', encoding=general['output_format'])

    if general['quote'].upper() == 'NONE':
        quote_type = csv.QUOTE_NONE
    elif general['quote'].upper() == 'ALL':
        quote_type = csv.QUOTE_ALL
    elif general['quote'].upper() == 'NONNUMERIC':
        quote_type = csv.QUOTE_NONNUMERIC
    else:
        quote_type = csv.QUOTE_MINIMAL

    output_csv = csv.writer(output_file_ptr, delimiter=general['separator'], quotechar=general['quotechar'], quoting=quote_type)

    if general['headers'] == True and headers_list is not None:
        output_csv.writerow(headers_list)

    if data is not None:
        for data_row in data:
            output_csv.writerow(data_row)
