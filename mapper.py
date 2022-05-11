#! /usr/bin/env python
"""mapper.py"""

import sys

payment_type_dict = {
    '1': 'Credit card',
    '2': 'Cash',
    '3': 'No charge',
    '4': 'Dispute',
    '5': 'Unknown',
    '6': 'Voided trip'
}


def perform_map():
    for line in sys.stdin:
        line = line.strip()
        values = line.split(',')
        try:
            float(values[13])
        except ValueError:
            continue
        if values[1][0:4] == '2020':
            print(','.join([payment_type_dict.get(values[9], 'NULL'), values[1][0:7], values[13]]))


if __name__ == '__main__':
    perform_map()
