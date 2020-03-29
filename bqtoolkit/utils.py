from __future__ import division

import math


def format_bytes(n):
    b_units = ['bytes', 'KB', 'MB', 'GB', 'TB']
    if n > 1023:
        p = min([int(math.floor(math.log(n, 2) / 10.)), len(b_units) - 1])
    else:
        return '%.0f bytes' % n
    n = float(n) / (1024 ** p)
    return '%.2f %s' % (n, b_units[p])
