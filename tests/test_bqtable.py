import pickle
import unittest
import os

from datetime import datetime, date

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField, Table

try:
    import mock
except ImportError:
    from unittest import mock

import pytest

import bqtoolkit
from bqtoolkit.table import BQTable


class BQTableTest(unittest.TestCase):

    def setUp(self):
        # Reset library clients before each test.
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
        t = BQTable.from_string('bqtoolkit.test.table')

        with mock.patch('google.cloud.bigquery.Client'):
            self.assertIsNotNone(t.client, 'bq_client should be populated when not defined in constructor')
            self.assertIsInstance(t.client, mock.Mock)

    def test_set_bqtable_client(self):
        my_client = mock.MagicMock()

        self.assertEqual(BQTable('p', 'd', 't', client=my_client).client, my_client)

        # A specific client can be passed to from_table_path method too.
        self.assertEqual(BQTable.from_string('p.d.t', client=my_client).client, my_client)

    def test_client_is_not_part_of_object_state(self):
        t = BQTable.from_string('bqtoolkit.test.table')

        object_state = t.__getstate__()

        self.assertFalse('_bq_client' in object_state)

    def test_client_is_not_used_for_object_comparison(self):
        my_client = mock.MagicMock()

        table_path = 'bqtoolkit.test.table'

        with mock.patch('google.cloud.bigquery.Client'):
            if BQTable.from_string(table_path) != BQTable.from_string(table_path, client=my_client):
                self.fail('Changing the BQTable\'s client changed the output of equality comparison')

    def test_is_serializable(self):
        t = BQTable.from_string('bqtoolkit.test.table')

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
        t = BQTable.from_string('bqtoolkit.test.table')

        self.assertEqual(t.full_table_id, t.get_full_table_id())
        self.assertEqual(t.get_full_table_id(standard=True), 'bqtoolkit.test.table')
        self.assertEqual(t.get_full_table_id(standard=True, quoted=True), '`bqtoolkit.test.table`')
        self.assertEqual(t.get_full_table_id(quoted=True), '[bqtoolkit:test.table]')

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_table_get(self):
        t = BQTable.from_string('bqtoolkit.test.t')

        self.assertEqual(t.schema, [])
        self.assertIsNone(t.num_rows)

        t2 = t.get()

        self.assertEqual(t, t2, 'BQTable.get should return a reference to itself')

        self.assertNotEqual(t.schema, [])
        self.assertIsInstance(t.num_rows, int)

    @mock.patch('bqtoolkit.table.BQTable.get')
    def test_table_exists(self, mock_table_get):
        t = BQTable.from_string('bqtoolkit.test.t')

        self.assertTrue(t.exists())

        mock_table_get.side_effect = NotFound('')
        t = BQTable.from_string('bqtoolkit.test.nonexistingtable')

        self.assertFalse(t.exists())

    @mock.patch('sys.stdout.write')
    def test_table_deletion_prompt(self, stddout_write_mock):
        t = BQTable.from_string('bqtoolkit.test.nonexistingtable')

        with mock.patch('google.cloud.bigquery.Client.delete_table') as mock_delete_table:
            t.delete(prompt=False)
            stddout_write_mock.assert_not_called()

            mock_delete_table.assert_called()

            with mock.patch('six.moves.input') as mock_input:
                mock_input.return_value = 'yes'
                mock_delete_table.reset_mock()

                t.delete()

                mock_delete_table.assert_called()
                stddout_write_mock.assert_called()

                mock_input.return_value = ''
                mock_delete_table.reset_mock()

                t.delete()

                mock_delete_table.assert_not_called()

                mock_input.return_value = 'no'
                mock_delete_table.reset_mock()

                t.delete()

                mock_delete_table.assert_not_called()

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_table_creation(self):
        t = BQTable.from_string('bqtoolkit.test.table_name')

        t.delete(prompt=False, not_found_ok=True)

        t.schema = [SchemaField(name='c1', field_type='STRING'), SchemaField(name='c2', field_type='INTEGER')]

        t2 = t.create()

        self.assertEqual(t, t2, 'BQTable.create should return a reference to itself')

        self.assertEqual(len(t.schema), 2)
        self.assertEqual(t.num_rows, 0)

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_properties_diff(self):
        t = BQTable.from_string('bqtoolkit.test.t')

        t.get()

        t.description = datetime.now().isoformat()
        t.friendly_name = datetime.now().isoformat()

        diff_fields = t._properties_diff()

        self.assertEqual(len(diff_fields), 2)

        t.update()

        self.assertEqual(len(t._properties_diff()), 0)

        schema = t.schema

        schema[0] = SchemaField(name=schema[0].name,
                                field_type=schema[0].field_type,
                                description=datetime.now().isoformat())

        t.schema = schema

        self.assertEqual(t._properties_diff(), ['schema'])

    def test_properties_update_policy(self):
        t = BQTable.from_string('bqtoolkit.test.t')

        with mock.patch('google.cloud.bigquery.Client.get_table') as mock_get_table:
            mock_get_table.return_value = Table.from_api_repr({
                'tableReference': t.reference.to_api_repr(),
                'etag': 'CyDPC2Dt1HUktfmXVZtSpw=='
            })

            self.assertIsNone(t.etag)

            t.exists()

            self.assertIsNotNone(t.etag, '_properties must be set by BQTable.exists if it etags do not match')

            self.assertIsNone(t.friendly_name)

            t.friendly_name = 'new_friendly_name'

            self.assertEqual(t.friendly_name, 'new_friendly_name')

            t.exists()

            self.assertEqual(t.friendly_name, 'new_friendly_name', 'BQTable.exists should not override changes')

            t.get()

            self.assertIsNone(t.friendly_name, 'BQTable.get should override changes to properties')

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_partitions_get(self):
        t = BQTable.from_string('bqtoolkit.test.date_partitioned')

        with mock.patch('google.cloud.bigquery.Client.get_table') as mock_get_table:
            mock_get_table.return_value = Table.from_api_repr({
                'tableReference': t.reference.to_api_repr(),
                'etag': 'CyDPC2Dt1HUktfmXVZtSpw==',
                'timePartitioning': {'type': 'DAY', 'field': 'date'}
            })

            partitions = t.get_partitions()

            self.assertEqual(len(partitions), 4000)
            self.assertIsInstance(partitions[0].partition_date, date)

    def test_no_partitions_in_unpartitioned_table(self):
        t = BQTable.from_string('bqtoolkit.test.date_partitioned')

        with mock.patch('google.cloud.bigquery.Client.get_table') as mock_get_table:
            mock_get_table.return_value = Table.from_api_repr({
                'tableReference': t.reference.to_api_repr(),
                'etag': 'CyDPC2Dt1HUktfmXVZtSpw==',
            })

            self.assertIsNone(t.get_partitions())
