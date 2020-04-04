import os
import unittest
import pickle
import pytest

from datetime import date

from google.cloud.bigquery import TimePartitioning
from google.cloud.exceptions import Conflict, NotFound

try:
    import mock
except ImportError:
    from unittest import mock

import bqtoolkit
from bqtoolkit.partition import BQPartition
from bqtoolkit.table import BQTable


class BQPartitionTest(unittest.TestCase):

    def setUp(self):
        # Reset library clients before each test.
        bqtoolkit.table._bq_clients = {}

    def test_is_serializable(self):
        p = BQPartition.from_string('bqtoolkit.test.table$partition')

        try:
            pickle.dumps(p)
        except pickle.PicklingError:
            self.fail('Got a PicklingError while trying to pickle an un-initialized BQPartition class')

        with mock.patch('google.cloud.bigquery.Client'):
            # Get the bq_client, which cannot be pickled.
            bq_client = p.table.client

            # Assert the bq_client cannot be pickled (MagicMocks are not pickleable, as bigquery.Client).
            self.assertRaises(pickle.PicklingError, pickle.dumps, bq_client)

            try:
                # Ensure that the BQTable can be pickled anyway.
                pickled_partition = pickle.dumps(p)
            except pickle.PicklingError:
                self.fail('Got a PicklingError when pickling a BQPartition that has already initialized a bq_client')

        # Restore table and check if it's equal to the original one.
        restored_p = pickle.loads(pickled_partition)

        self.assertEqual(p, restored_p)

    def test_partition_path_parsing(self):
        self.assertEqual(BQPartition.from_string('bqtoolkit.test.date_partitioned$20200101'),
                         BQPartition(BQTable.from_string('bqtoolkit.test.date_partitioned'), '20200101'))

        self.assertRaises(ValueError, BQPartition.from_string, 'bqtoolkit.test.date_partitioned')

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_partition_get(self):
        p = BQPartition.from_string('bqtoolkit.test.date_partitioned$20200101')

        self.assertEqual(p.full_partition_id, 'bqtoolkit:test.date_partitioned$20200101')

        p.get()

        self.assertIsNotNone(p.creation_timestamp)
        self.assertIsNotNone(p.last_modified_timestamp)
        self.assertEqual(p.partition_date, date(year=2020, month=1, day=1))

    @mock.patch('bqtoolkit.table.BQTable._init_properties', autospec=True)
    @mock.patch('bqtoolkit._helpers.execute_partitions_query')
    def test_get_failures(self, mock_execute_partitions_query, mock__init_properties):
        mock_execute_partitions_query.return_value = []

        p = BQPartition.from_string('bqtoolkit.test.date_partitioned$20200101')

        self.assertRaises(ValueError, p.get)

        def set_time_partitioning(table):
            table.time_partitioning = TimePartitioning()

        mock__init_properties.side_effect = set_time_partitioning
        self.assertRaises(NotFound, p.get)

        mock_execute_partitions_query.return_value = [{}, {}]
        self.assertRaises(Conflict, p.get)

    @mock.patch('bqtoolkit.table.BQTable.get', autospec=True)
    def test_initialized_tables_cause_inequalities(self, mock_bqtable_get):
        p1 = BQPartition.from_string('bqtoolkit.test.date_partitioned$20200101')

        def mock_get(table):
            table._properties['etag'] = 'an_etag'

        mock_bqtable_get.side_effect = mock_get

        p2 = BQPartition.from_string('bqtoolkit.test.date_partitioned$20200101')
        p2.table.get()

        self.assertNotEqual(p1, p2)
        self.assertEqual(str(p1), str(p2))
