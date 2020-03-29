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
        assert BQTable.from_table_path('project.dataset.table') == BQTable.from_table_path('project:dataset.table')

        assert BQTable.from_table_path('project.dataset.table_name') == BQTable('project', 'dataset', 'table_name')

        assert BQTable.from_table_path('p.d.table$partiton') == BQTable('p', 'd', 'table')

        # Hyphens should be allowed in project id only.
        assert BQTable.from_table_path('project-id.dataset_id.t') == BQTable('project-id', 'dataset_id', 't')

        with pytest.raises(ValueError):
            BQTable.from_table_path('project-id.dataset-id.t')

    @pytest.mark.skipif(sys.version_info < (3, 5), reason='Required unittest.mock')
    def test_set_bqtable_client(self):
        my_client = mock.MagicMock()

        assert BQTable('p', 'd', 't', client=my_client).bq_client == my_client

        # A specific client can be passed to from_table_path method too.
        assert BQTable.from_table_path('p.d.t', client=my_client).bq_client == my_client

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
