#! /usr/bin/env python
"""reducer.py"""

import sys


def perform_reduce():
    payment_type = None
    month = None
    tip_amount = None
    current_payment_type = None
    current_month = None
    current_sum = 0.0
    current_count = 0

    output = []

    for line in sys.stdin:
        line = line.strip()
        payment_type, month, tip_amount = line.split(',')

        try:
            tip_amount = float(tip_amount)
        except ValueError:
            continue

        if current_payment_type == payment_type and current_month == month:
            current_sum += tip_amount
            current_count += 1
        else:
            if current_payment_type and current_month:
                output.append((current_payment_type, current_month, str(round(current_sum / current_count, 2))))
            current_payment_type = payment_type
            current_month = month
            current_sum = tip_amount
            current_count = 1

    if current_payment_type and current_month:
        output.append((current_payment_type, current_month, str(round(current_sum / current_count, 2))))

    print('Payment type,Month,Tips average amount')
    for ln in sorted(output, key=lambda x: (x[1], x[0]), reverse=False):
        print(','.join(ln))


if __name__ == '__main__':
    perform_reduce()
