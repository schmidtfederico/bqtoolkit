from __future__ import division

import math
from datetime import datetime

from google.cloud.bigquery import QueryJobConfig

TABLE_PARTITIONS_QUERY = """
    #legacySQL
    SELECT partition_id,
           creation_timestamp,
           last_modified_timestamp,
           INTEGER(partition_id) AS partition_int,
           DATE(TIMESTAMP(partition_id)) AS partition_date
    FROM [{full_table_id}$__PARTITIONS_SUMMARY__]
    {where_clause}
    ORDER BY last_modified_timestamp DESC, partition_date DESC, partition_int
"""


def format_bytes(n):
    b_units = ['bytes', 'KB', 'MB', 'GB', 'TB']
    if n > 1023:
        p = min([int(math.floor(math.log(n, 2) / 10.)), len(b_units) - 1])
    else:
        return '%.0f bytes' % n
    n = float(n) / (1024 ** p)
    return '%.2f %s' % (n, b_units[p])


def execute_partitions_query(table, partition_id=None):
    filter_clause = ''
    if partition_id:
        filter_clause = "WHERE partition_id = '%s'" % partition_id

    query = TABLE_PARTITIONS_QUERY.format(full_table_id=table.full_table_id,
                                          where_clause=filter_clause)

    # The partitions query has no cost.
    query_job = table.client.query(query, job_config=QueryJobConfig(use_legacy_sql=True))

    for row in query_job.result():
        partition_properties = {
            'partition_id': row['partition_id'],
            'creation_timestamp': row['creation_timestamp'],
            'last_modified_timestamp': row['last_modified_timestamp']
        }

        if table.time_partitioning:
            partition_properties['partition_date'] = datetime.strptime(row['partition_date'], '%Y-%m-%d').date()

        yield partition_properties


# Deprecated from distutils
def strtobool(val):
    """Convert a string representation of truth to True or False.
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    else:
        raise ValueError(f"invalid truth value {val!r}")