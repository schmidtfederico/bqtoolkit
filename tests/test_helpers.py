import pytest
import os

from google.cloud.bigquery import TimePartitioning

from bqtoolkit.table import BQTable
from bqtoolkit._helpers import format_bytes, execute_partitions_query


def test_format_bytes():
    assert format_bytes(0) == '0 bytes'
    assert format_bytes(1023) == '1023 bytes'
    assert format_bytes(1024) == '1.00 KB'
    assert format_bytes(1024 * 1024 - 1).endswith('KB')
    assert format_bytes(1024 * 1024).endswith('MB')
    assert format_bytes(1024 ** 3).endswith('GB')
    assert format_bytes(1024 ** 4).endswith('TB')


@pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
def test_partitions_query():

    t = BQTable.from_string('bqtoolkit:test.date_partitioned')

    t.time_partitioning = TimePartitioning(field='date')

    total_partitions = 0

    for partition in execute_partitions_query(t, '20200101'):
        assert partition['partition_id'] is not None
        assert partition['creation_timestamp'] is not None

        total_partitions += 1

    assert total_partitions == 1
