import pickle
import unittest
import os
import sys
import gzip

from collections import OrderedDict
from datetime import datetime, date

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField, Table, Compression
from google.cloud.exceptions import Forbidden

try:
    import mock
except ImportError:
    from unittest import mock

try:
    FileNotFoundError
except NameError:
    # Python2.7
    FileNotFoundError = OSError

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

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('sys.stdout.write')
    def test_table_deletion_prompt(self, stddout_write_mock, mock_client):
        t = BQTable.from_string('bqtoolkit.test.nonexistingtable')

        mock_delete_table = mock_client.return_value.delete_table

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
    def test_table_creation_remote(self):
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

    @mock.patch('google.cloud.bigquery.Client')
    def test_properties_update_policy(self, mock_client):
        t = BQTable.from_string('bqtoolkit.test.t')

        mock_get_table = mock_client.return_value.get_table

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

    @mock.patch('google.cloud.bigquery.Client')
    def test_no_partitions_in_unpartitioned_table(self, mock_client):
        t = BQTable.from_string('bqtoolkit.test.date_partitioned')

        mock_get_table = mock_client.return_value.get_table

        mock_get_table.return_value = Table.from_api_repr({
            'tableReference': t.reference.to_api_repr(),
            'etag': 'CyDPC2Dt1HUktfmXVZtSpw==',
        })

        self.assertIsNone(t.get_partitions())

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.cloud.bigquery.Client')
    def test_load_errors(self, mock_bigquery_client, mock_storage_client):
        t = BQTable.from_string('bqtoolkit.test.load_table')

        self.assertRaises(FileNotFoundError, t.load, 'non_existing_file.csv')

        builtins = 'builtins' if sys.version_info.major == 3 else '__builtin__'

        with mock.patch('os.stat') as mock_os_stat, mock.patch('%s.open' % builtins):
            # Less than 10 MB
            mock_os_stat.return_value = mock.Mock(st_size=1024 * 1024 * 10 - 1)

            mock_load_table_from_file = mock_bigquery_client.return_value.load_table_from_file
            mock_load_job = mock_load_table_from_file.return_value

            type(mock_load_job).errors = mock.PropertyMock(return_value=[{'error': 'An error'}])
            mock_load_job.result.side_effect = RuntimeError

            self.assertRaises(RuntimeError, t.load, 'a_file.csv')

            mock_load_table_from_file.assert_called_once()

            # Exactly 10 MB.
            mock_os_stat.return_value = mock.Mock(st_size=1024 * 1024 * 10)

            # No bucket name provided, file larget than 10 MB.
            self.assertRaises(ValueError, t.load, 'a_file.csv')

            mock_storage_client.reset_mock()

            mock_load_table_from_uri = mock_bigquery_client.return_value.load_table_from_uri
            mock_load_job = mock_load_table_from_uri.return_value.result

            mock_load_job.return_value.errors = [{'error': 'an error'}]

            self.assertRaises(RuntimeError, t.load, 'a_file.csv', storage_bucket='my_bucket')

            with mock.patch('warnings.warn') as mock_warn, \
                    mock.patch('bqtoolkit.table.BQTable._get_storage_bucket') as mock__get_storage_bucket:
                mock_load_job.return_value.errors = None

                mock__get_storage_bucket.return_value.blob.return_value.delete.side_effect = Forbidden('')

                t.load('a_file.csv', storage_bucket='my_bucket')
                mock_warn.assert_called()

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_load(self):
        t = BQTable.from_string('bqtoolkit.tmp.%s' % datetime.now().strftime('%M%S%f'))

        t.load('tests/data/upload.csv')
        t.get()

        self.assertEqual(t.num_rows, 1)
        self.assertEqual(len(t.schema), 2)

        base_os_stat = os.stat

        with mock.patch('os.stat') as mock_os_stat:
            # Force upload via GCS.
            def mock_os_stat_f(path):
                if path == 'tests/data/upload.csv':
                    return mock.Mock(st_size=1024 * 1024 * 10, st_mode=33188)
                else:
                    return base_os_stat(path)

            mock_os_stat.side_effect = mock_os_stat_f

            t.load('tests/data/upload.csv', storage_bucket='bqtoolkit-test')

            t.get()

            self.assertEqual(t.num_rows, 2)

        t.delete(prompt=False)

    @mock.patch('warnings.warn')
    @mock.patch('bqtoolkit.table.BQTable._get_storage_bucket')
    @mock.patch('bqtoolkit.table.BQTable.get')
    @mock.patch('google.cloud.bigquery.Client')
    def test_download_strategies(self, mock_bigquery_client, mock_get, mock__get_storage_bucket, mock_warn):
        mock_extract_table = mock_bigquery_client.return_value.extract_table
        mock_list_rows = mock_bigquery_client.return_value.list_rows

        mock_list_rows.return_value = [{'field_1': 'a'}, {'field_1': 'b'}]

        mock_download_to_file = mock.Mock()
        mock_download_to_file.side_effect = lambda x: x.write(b'file_content')

        mock_blob = mock.Mock(download_to_file=mock_download_to_file,
                              delete=mock.Mock(side_effect=Forbidden('')))

        type(mock_blob).name = mock.PropertyMock(return_value='blob_1')

        mock__get_storage_bucket.return_value.list_blobs.return_value = [
            mock_blob, mock_blob
        ]

        with mock.patch('bqtoolkit.table.BQTable.num_bytes', new_callable=mock.PropertyMock) as mock_num_bytes:
            # File smaller than 1 GB.
            mock_num_bytes.return_value = 2 ** 30 - 1
            # No errors returned.
            type(mock_extract_table.return_value).errors = mock.PropertyMock(return_value=None)

            t = BQTable('p', 'd', 'table')

            for file in t.download(storage_bucket='bucket_name'):
                with open(file) as f:
                    self.assertEqual(f.read(), 'file_content')

            mock_extract_table.assert_called_once()
            self.assertEqual(len(mock_download_to_file.call_args), 2)

            extraction_path = mock_extract_table.call_args[0][1]
            self.assertFalse('*' in extraction_path, 'Table size was smaller than 1 GB, no wildcard should be used')

            mock_warn.assert_called()

            # Just shy of 10 MB.
            mock_num_bytes.return_value = 10 * (2 ** 20) - 1

            t.schema = [SchemaField(name='field_1', field_type='STRING')]

            for file in t.download():
                with open(file) as f:
                    self.assertEqual("\n".join(f.read().splitlines()), 'field_1\na\nb')

            mock_list_rows.assert_called()

            t.schema = [SchemaField(name='field_1', field_type='RECORD')]

            mock_list_rows.reset_mock()

            for file in t.download():
                self.assertTrue(os.path.exists(file))

            # RECORD type should not be supported by CSV extraction with list_rows
            mock_list_rows.assert_not_called()

            mock_num_bytes.return_value = 2 ** 30  # 1 GB.
            mock_extract_table.reset_mock()

            for file in t.download():
                self.assertTrue(os.path.exists(file))

            extraction_path = mock_extract_table.call_args[0][1]
            self.assertTrue('*' in extraction_path, 'Table size was bigger than 1 GB, wildcard should be used')

            type(mock_extract_table.return_value).errors = mock.PropertyMock(return_value=[{'error': 'an error'}])

            try:
                for file in t.download(storage_bucket='bucket_name'):
                    self.fail('Expected RuntimeError to be raised')
            except RuntimeError:
                pass

    @mock.patch('warnings.warn')
    @mock.patch('bqtoolkit.table.BQTable._get_storage_bucket')
    @mock.patch('bqtoolkit.table.BQTable.get')
    @mock.patch('google.cloud.bigquery.Client')
    def test_fast_tracking_download_does_not_ignore_additional_params(self, mock_bigquery_client, mock_get,
                                                                      mock__get_storage_bucket, mock_warn):
        # Force _get_storage_bucket to raise an error to faster test non-fast tracking approaches.
        mock__get_storage_bucket.side_effect = RuntimeError

        # Mock the rows returned by list_rows method.
        mock_list_rows = mock_bigquery_client.return_value.list_rows

        mock_list_rows.return_value = [
            OrderedDict([('field_1', 'a'), ('field_2', 5)]),
            OrderedDict([('field_1', 'b'), ('field_2', 10)])
        ]

        with mock.patch('bqtoolkit.table.BQTable.num_bytes', new_callable=mock.PropertyMock) as mock_num_bytes:
            # File smaller than 10 MB.
            mock_num_bytes.return_value = 10 * (2 ** 20) - 1

            t = BQTable('p', 'd', 'table')
            t.schema = [
                SchemaField(name='field_1', field_type='STRING'),
                SchemaField(name='field_2', field_type='INTEGER')
            ]

            # Changing delimiter and header should be supported by fast-track.
            for file in t.download(field_delimiter='\t', print_header=False):
                with open(file) as f:
                    self.assertEqual(f.read().replace('\r', ''), 'a\t5\nb\t10\n')

            # Exporting as GZIP should be supported when fast-tracking too.
            for file in t.download(compression=Compression.GZIP):
                with gzip.open(file, 'rt') as f:
                    self.assertEqual(f.read().replace('\r', ''), 'field_1,field_2\na,5\nb,10\n')

            try:
                for file in t.download(use_avro_logical_types=True):
                    self.fail('Additional parameters of ExportJobConfig should stop us form fast-tracking CSV export')
            except RuntimeError:
                pass

    @pytest.mark.skipif('GOOGLE_APPLICATION_CREDENTIALS' not in os.environ, reason='Undefined Google credentials')
    def test_download(self):
        with mock.patch('bqtoolkit.table.BQTable.num_bytes', new_callable=mock.PropertyMock) as mock_num_bytes:
            mock_num_bytes.return_value = 100

            t = BQTable.from_string('bqtoolkit:test.download_types')
            t.get()

            file_1 = ''

            for file in t.download():
                with open(file) as f:
                    file_1 = f.read()

            mock_num_bytes.return_value = 2 ** 20 * 10 + 1

            file_2 = ''

            for file in t.download(storage_bucket='bqtoolkit-test'):
                with open(file) as f:
                    file_2 = f.read()

            self.assertEqual(file_1, file_2)
