import json
import os
import re
import sys
import uuid
import warnings
from distutils.util import strtobool
import six.moves
import copy

try:
    from tempfile import TemporaryDirectory
except ImportError:  # pragma: no cover
    from backports.tempfile import TemporaryDirectory

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery import Table, LoadJobConfig, ExtractJobConfig, Compression
from google.cloud.exceptions import Forbidden

from bqtoolkit._helpers import execute_partitions_query

_bq_clients = {}


def _identity(x):
    return x


_csv_export_formatters = {
    'STRING': _identity,
    'INTEGER': _identity,
    'FLOAT': _identity,
    'NUMERIC': _identity,
    'BOOLEAN': lambda x: str(x).lower() if x is not None else x,
    'TIMESTAMP': lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f UTC') if hasattr(x, 'strftime') else x,
    'DATE': _identity,
    # 'TIME': lambda x: x.isoformat(),
    'TIME': _identity,
    'DATETIME': _identity
}


class BQTable(Table):

    TABLE_PATH_REGEX = re.compile(r'(?P<project>[\w-]+)[:.](?P<dataset>\w+)\.(?P<name>\w+)')

    def __init__(self, project, dataset, name, schema=None, client=None):
        self.dataset = dataset
        self.name = name

        self._bq_client = client
        self._get_properties = {}

        super(BQTable, self).__init__('%s.%s.%s' % (project, dataset, name), schema)

    @property
    def client(self):
        """
        :rtype :class:`~google.cloud.bigquery.Client`
        :return A BigQuery client for this table's project.
        """
        if self._bq_client is None:
            if self.project not in _bq_clients:
                _bq_clients[self.project] = bigquery.Client(project=self.project)
            self._bq_client = _bq_clients[self.project]
        return self._bq_client

    @classmethod
    def from_string(cls, table_path, **kwargs):
        """
        Creates an instance of this class from a full table identifier.

        :param str table_path: A string with the full path to this table (e.g. project-id:dataset_id.table_name).
        :param kwargs: Other keyword arguments passed to :class:`~bqtoolkit.table.BQTable` init method.
        :rtype :class:`~bqtoolkit.table.BQTable`
        :return: A BQTable, initialized from the full table path.
        """
        m = BQTable.TABLE_PATH_REGEX.match(table_path)

        if m is None:
            raise ValueError('Failed to parse "%s" as table path.' % table_path)

        kwargs.update(m.groupdict())

        return cls(**kwargs)

    def get_full_table_id(self, standard=False, quoted=False):
        """
        :param bool standard: Use Standard SQL delimited for project name (.). If false, a colon is used.
        :param bool quoted: Quote table path? If true, name is wrapped with apostrophes (standard) or brackets (legacy).
        :return: This table's full
        :rtype str
        """
        name_str = '{project}.{dataset}.{table}' if standard else '{project}:{dataset}.{table}'

        if quoted:
            quotes = '`%s`' if standard else '[%s]'
            name_str = quotes % name_str

        return name_str.format(project=self.project, dataset=self.dataset, table=self.name)

    @property
    def full_table_id(self):
        return self.get_full_table_id()

    def get(self, overwrite_changes=True, **kwargs):
        self._update_properties(self.client.get_table(self, **kwargs), force_update=overwrite_changes)
        return self

    def exists(self):
        try:
            self.get(overwrite_changes=False)
            return True
        except NotFound:
            return False

    def delete(self, prompt=True, **kwargs):
        if prompt:
            sys.stdout.write('Delete table %s? [y/N]' % self.full_table_id)
            sys.stdout.flush()
            proceed = six.moves.input().lower().strip()
            if len(proceed) == 0 or not strtobool(proceed):
                return False
        self.client.delete_table(self, **kwargs)
        # Reset properties.
        self._reset_properties()
        return True

    def create(self, **kwargs):
        self._update_properties(self.client.create_table(self, **kwargs))
        return self

    def update(self, fields=None, **kwargs):
        if fields is None:
            fields = self._properties_diff()
        self._update_properties(self.client.update_table(self, fields=fields, **kwargs))
        return self

    def get_partitions(self):
        self._init_properties()

        from bqtoolkit.partition import BQPartition
        if self.time_partitioning or self.range_partitioning:
            partitions = []

            for partition_info in execute_partitions_query(self):
                partitions.append(BQPartition._from_partition_query(self, partition_info))
            return partitions

        return None

    def load(self, file_path, storage_project=None, storage_bucket=None, write_disposition='WRITE_APPEND',
             autodetect=True, **kwargs):
        """
        Loads into this table the content of the given file path. Automatically handles large files uploads (> 10 MB)
        using Google Cloud Storage, if a storage_bucket is provided.

        :param str file_path: Path to the file.
        :param str storage_project: The GCS project to use to upload the file if it exceeds 10 MB in size.
                                    Defaults to the table's project.
        :param str storage_bucket: The GCS bucket to use to upload the file if it exceeds 10 MB in size.
                                   If not provided and file is larger than 10 MB, a ValueError will be raised.
        :param bool autodetect: Whether to let BigQuery automatically detect the schema of the file, defaults to True.
                                 See :class:`~google.cloud.bigquery.LoadJobConfig` for more details.
        :param bool write_disposition: What to do if table already exists. Defaults to append to it.
                                  See :class:`~google.cloud.bigquery.LoadJobConfig` for more details.
        :param kwargs: Other parameters passed to :class:`~google.cloud.bigquery.LoadJobConfig` constructor.
        :return:
        """
        # Create a load job configuration.
        job_config = LoadJobConfig(**kwargs)
        job_config.write_disposition = write_disposition
        job_config.autodetect = autodetect

        file_size_mb = os.stat(file_path).st_size / (1024. * 1024)

        if file_size_mb < 10:
            # Small files can be loaded directly from the Python API.
            with open(file_path, 'rb') as load_file:
                load_job = self.client.load_table_from_file(load_file, self, job_config=job_config)

                try:
                    load_job.result()
                except Exception:
                    raise RuntimeError(
                        'Failed to load data into table. Errors:\n%s' % json.dumps(load_job.errors, indent=4)
                    )
        else:
            # We need to use Google Cloud Storage and append to the table from a GCS URI.
            storage_bucket = self._get_storage_bucket(storage_project, storage_bucket)

            job_id = uuid.uuid4().hex

            blob = storage_bucket.blob('bqtoolkit_load_%s_%s' % (self.name, job_id))

            blob.upload_from_filename(file_path)

            load_job = self.client.load_table_from_uri('gs://%s/%s' % (storage_bucket.name, blob.name),
                                                       self.reference,
                                                       job_id=job_id,
                                                       job_config=job_config)

            result = load_job.result()

            if result.errors and len(result.errors) > 0:
                raise RuntimeError('Failed to load data into table. Errors:\n%s' % json.dumps(result.errors, indent=4))

            # Perform cleanup.
            try:
                blob.delete()
            except Forbidden:
                warnings.warn('Failed to delete uploaded blob %s' % blob.path)

    def download(self, storage_project=None, storage_bucket=None, destination_format='csv', **kwargs):
        # Make sure properties were populated.
        self._init_properties()

        table_size_mb = self.num_bytes / (1024. * 1024)
        job_id = uuid.uuid4().hex

        field_types = [field.field_type for field in self.schema]

        extract_job_config = ExtractJobConfig(destination_format=destination_format, **kwargs)

        # Decide if we can fast-track the CSV export for small (< 10 MB) tables.
        csv_fields_supported = all([t in _csv_export_formatters for t in field_types])
        export_params_supported = all(k in ['field_delimiter', 'compression', 'print_header'] for k in kwargs.keys())
        compression_supported = kwargs.get('compression', Compression.NONE) in [Compression.NONE, Compression.GZIP]

        can_fast_track_csv_export = destination_format == 'csv' and table_size_mb <= 10 and csv_fields_supported and \
            export_params_supported and compression_supported

        if can_fast_track_csv_export:
            import csv
            # Export by listing rows instead of requiring Google Cloud Storage.
            with TemporaryDirectory() as tmp_directory:
                gzip_compression = 'compression' in kwargs and kwargs['compression'] == Compression.GZIP

                tmp_file = os.path.join(tmp_directory, job_id + '.csv' + ('.gz' if gzip_compression else ''))

                if gzip_compression:
                    import gzip
                    out_f = gzip.open(tmp_file, 'wt')
                else:
                    out_f = open(tmp_file, 'w')

                try:
                    writer = csv.writer(out_f, delimiter=kwargs.get('field_delimiter', ','))
                    if kwargs.get('print_header', True) in [True, 'true']:
                        writer.writerow([field.name for field in self.schema])

                    for row in self.client.list_rows(self, selected_fields=self.schema):
                        writer.writerow([
                            _csv_export_formatters[field_types[i]](value)
                            for i, value in enumerate(row.values())
                        ])
                finally:
                    out_f.close()

                yield tmp_file
        else:
            export_bucket = self._get_storage_bucket(storage_project, storage_bucket)

            destination_uri = 'gs://{bucket_name}/bqtoolkit_extract_{table_name}_{job_id}{wildcard}.{format}'.format(
                bucket_name=storage_bucket,
                table_name=self.name,
                job_id=job_id,
                # Only tables with less than 1 GB can be exported as a single file.
                wildcard='_*' if table_size_mb >= 1024 else '',
                format=destination_format
            )

            extract_job = self.client.extract_table(self, destination_uri, job_id=job_id, job_config=extract_job_config)

            with TemporaryDirectory() as tmp_directory:
                extract_job.result()  # Wait for export job to be completed.

                if extract_job.errors is not None:
                    raise RuntimeError('Failed to export BQ table %s to GCS. Details:\n    %s' %
                                       (self.full_table_id, json.dumps(extract_job.errors, indent=4)))

                blobs_prefix = 'bqtoolkit_extract_{table_name}_{job_id}'.format(table_name=self.name, job_id=job_id)

                for idx, blob in enumerate(export_bucket.list_blobs(prefix=blobs_prefix)):
                    file_path = os.path.join(tmp_directory, blob.name)

                    with open(file_path, 'wb') as out_f:
                        blob.download_to_file(out_f)

                    # Yield path of downloaded blob for processing.
                    yield file_path

                    if os.path.exists(file_path):
                        os.remove(file_path)  # Remove file once processed.

                    try:
                        blob.delete()
                    except Forbidden:
                        warnings.warn('Failed to delete extracted blob %s' % blob.path)

    def _init_properties(self):
        # Populate table if was initialized with get() yet.
        if self.etag is None:
            self.get(overwrite_changes=False)

    def _reset_properties(self):
        self._properties = Table(self.reference, schema=self.schema)._properties
        self._get_properties = {}

    def _update_properties(self, api_table, force_update=True):
        if force_update or self.etag is None:
            self._properties = copy.deepcopy(api_table._properties)
            self._get_properties = copy.copy(self._properties)

    def _properties_diff(self):
        changed_properties = []
        for key, value in self._properties.items():
            old_value = self._get_properties.get(key, None)
            if old_value != value:
                changed_properties.append(key)
        return changed_properties

    def _get_storage_bucket(self, storage_project, storage_bucket):
        storage_project = storage_project if storage_project is not None else self.project

        storage_client = storage.Client(project=storage_project)

        if storage_bucket is None:
            raise ValueError(
                "A Google Cloud Storage bucket name should be provided in order to load results to "
                "table %s" % self.full_table_id
            )

        return storage_client.bucket(storage_bucket)

    def __eq__(self, other):
        return all([self.__getattribute__(a) == other.__getattribute__(a)
                    for a in ['project', 'name', 'dataset', 'etag']])

    def __ne__(self, other):
        return not self == other

    def __getstate__(self):
        # Remove the inner _bq_client when serializing an instance.
        state = self.__dict__.copy()
        del state["_bq_client"]
        del state["_get_properties"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._bq_client = None
        self._get_properties = {}
