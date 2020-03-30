import pickle
import unittest
import os

try:
    from unittest import mock
except ImportError:
    import mock

import pytest

import bqtoolkit
from bqtoolkit.table import BQTable


class BQLibTest(unittest.TestCase):

    def setUp(self):
        bqtoolkit.table._bq_clients = {}

    def test_init_from_path(self):
        # Semicolon can be used as project delimiter
        self.assertEqual(BQTable.from_string('project.dataset.table'),
                         BQTable.from_string('project:dataset.table'))

        self.assertEqual(BQTable.from_string('project.dataset.table_name'),
                         BQTable('project', 'dataset', 'table_name'))

        self.assertEqual(BQTable.from_string('p.d.table$partiton'),
                         BQTable('p', 'd', 'table'))

        # Hyphens should be allowed in project id only.
        self.assertEqual(BQTable.from_string('project-id.dataset_id.t'),
                         BQTable('project-id', 'dataset_id', 't'))

        with pytest.raises(ValueError):
            BQTable.from_string('project-id.dataset-id.t')

    def test_client_gets_created_if_missing(self):
        t = BQTable.from_string('bqtoolkit.tmp.table')

        with mock.patch('google.cloud.bigquery.Client'):
            self.assertIsNotNone(t.client, 'bq_client should be populated when not defined in constructor')
            self.assertIsInstance(t.client, mock.Mock)

    def test_set_bqtable_client(self):
        my_client = mock.MagicMock()

        self.assertEqual(BQTable('p', 'd', 't', client=my_client).client, my_client)

        # A specific client can be passed to from_table_path method too.
        self.assertEqual(BQTable.from_string('p.d.t', client=my_client).client, my_client)

    def test_client_is_not_part_of_object_state(self):
        t = BQTable.from_string('bqtoolkit.tmp.table')

        object_state = t.__getstate__()

        self.assertFalse('_bq_client' in object_state)

    def test_client_is_not_used_for_object_comparison(self):
        my_client = mock.MagicMock()

        table_path = 'bqtoolkit.tmp.table'

        with mock.patch('google.cloud.bigquery.Client'):
            if BQTable.from_string(table_path) != BQTable.from_string(table_path, client=my_client):
                self.fail('Changing the BQTable\'s client changed the output of equality comparison')

    def test_is_serializable(self):
        t = BQTable.from_string('bqtoolkit.tmp.table')

        try:
            pickle.dumps(t)
        except pickle.PicklingError:
            self.fail('Got a PicklingError while trying to pickle an un-initialized BQTable class')

        with mock.patch('google.cloud.bigquery.Client'):
            # Get the bq_client, which cannot be pickled.
            bq_client = t.client

            # Assert the bq_client cannot be pickled (MagicMocks are not pickleable, as bigquery.Client).
            self.assertRaises(pickle.PicklingError, pickle.dumps, bq_client)

            try:
                # Ensure that the BQTable can be pickled anyway.
                pickled_table = pickle.dumps(t)
            except pickle.PicklingError:
                self.fail('Got a PicklingError when pickling a BQTable that has already initialized a bq_client')

        # Restore table and check if it's equal to the original one.
        restored_t = pickle.loads(pickled_table)

        self.assertEqual(t, restored_t)

    def test_full_table_id(self):
        t = BQTable.from_string('bqtoolkit.tmp.table')

        self.assertEqual(t.full_table_id, t.get_full_table_id())
        self.assertEqual(t.get_full_table_id(standard=True), 'bqtoolkit.tmp.table')
        self.assertEqual(t.get_full_table_id(standard=True, quoted=True), '`bqtoolkit.tmp.table`')
        self.assertEqual(t.get_full_table_id(quoted=True), '[bqtoolkit:tmp.table]')

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_table_get(self):
        t = BQTable.from_string('bqtoolkit.tmp.t')

        self.assertEqual(t.schema, [])
        self.assertIsNone(t.num_rows)

        t.get()

        self.assertNotEqual(t.schema, [])
        self.assertIsInstance(t.num_rows, int)
