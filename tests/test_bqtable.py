import sys
import pickle
import unittest
import os

try:
    from unittest import mock
except ImportError:
    pass

import pytest

from bqtoolkit.table import BQTable


class BQLibTest(unittest.TestCase):

    def test_init_from_path(self):
        # Semicolon can be used as project delimiter
        self.assertEqual(BQTable.from_table_path('project.dataset.table'),
                         BQTable.from_table_path('project:dataset.table'))

        self.assertEqual(BQTable.from_table_path('project.dataset.table_name'),
                         BQTable('project', 'dataset', 'table_name'))

        self.assertEqual(BQTable.from_table_path('p.d.table$partiton'),
                         BQTable('p', 'd', 'table'))

        # Hyphens should be allowed in project id only.
        self.assertEqual(BQTable.from_table_path('project-id.dataset_id.t'),
                         BQTable('project-id', 'dataset_id', 't'))

        with pytest.raises(ValueError):
            BQTable.from_table_path('project-id.dataset-id.t')

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_client_gets_created_if_missing(self):
        t = BQTable.from_table_path('bqtoolkit.tmp.table')

        self.assertIsNotNone(t.bq_client, 'bq_client should be populated when not defined in constructor')

    @pytest.mark.skipif(sys.version_info < (3, 5), reason='Required unittest.mock')
    def test_set_bqtable_client(self):
        my_client = mock.MagicMock()

        self.assertEqual(BQTable('p', 'd', 't', client=my_client).bq_client, my_client)

        # A specific client can be passed to from_table_path method too.
        self.assertEqual(BQTable.from_table_path('p.d.t', client=my_client).bq_client, my_client)

    def test_client_is_not_part_of_object_state(self):
        t = BQTable.from_table_path('bqtoolkit.tmp.table')

        object_state = t.__getstate__()

        self.assertFalse('_bq_client' in object_state)

    @pytest.mark.skipif(sys.version_info < (3, 5), reason='Required unittest.mock')
    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_client_is_not_used_for_object_comparison(self):
        my_client = mock.MagicMock()

        table_path = 'bqtoolkit.tmp.table'

        if BQTable.from_table_path(table_path) != BQTable.from_table_path(table_path, client=my_client):
            self.fail('Changing the BQTable\'s client changed the output of equality comparison')

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_is_serializable(self):
        t = BQTable.from_table_path('bqtoolkit.tmp.table')

        try:
            pickle.dumps(t)
        except pickle.PicklingError:
            self.fail('Got a PicklingError while trying to pickle an un-initialized BQTable class')

        # Get the bq_client, which cannot be pickled.
        bq_client = t.bq_client

        # Assert the bq_client cannot be pickled.
        self.assertRaises(pickle.PicklingError, pickle.dumps, bq_client)

        try:
            # Ensure that the BQTable can be pickled anyway.
            pickle.dumps(t)
        except pickle.PicklingError:
            self.fail('Got a PicklingError when pickling a BQTable that has already initialized a bq_client')
