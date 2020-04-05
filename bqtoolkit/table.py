import json
import os
import re
import sys
import warnings
from distutils.util import strtobool
import six.moves
import copy

from datetime import datetime
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery import Table, LoadJobConfig
from google.cloud.exceptions import Forbidden

from bqtoolkit._helpers import execute_partitions_query

_bq_clients = {}


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
                load_job = self.client.load_table_from_file(load_file, self.reference, job_config=job_config)

                try:
                    load_job.result()
                except Exception:
                    raise RuntimeError(
                        'Failed to load data into table. Errors:\n%s' % json.dumps(load_job.errors, indent=4)
                    )
        else:
            # We need to use Google Cloud Storage and append to the table from a GCS URI.

            # Default to the table project if the storage project is not defined.
            storage_project = storage_project if storage_project is not None else self.project

            storage_client = storage.Client(project=storage_project)

            if storage_bucket is None:
                raise ValueError(
                    "A Google Cloud Storage bucket name should be provided in order to load results to "
                    "table %s" % self.full_table_id
                )

            storage_bucket = storage_client.bucket(storage_bucket)

            blob_name = '_tmp_load_%s_%s' % (
                datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f'),
                file_path.replace('/', '_').replace('\\', '_')
            )

            blob = storage_bucket.blob(blob_name)

            blob.upload_from_filename(file_path)

            load_job = self.client.load_table_from_uri('gs://%s/%s' % (storage_bucket.name, blob.name),
                                                       self.reference,
                                                       job_config=job_config)

            result = load_job.result()

            if result.errors and len(result.errors) > 0:
                raise RuntimeError('Failed to load data into table. Errors:\n%s' % json.dumps(result.errors, indent=4))

            # Perform cleanup.
            try:
                blob.delete()
            except Forbidden:  # pragma: no cover
                warnings.warn('Failed to delete uploaded blob %s' % blob.path)

    def _init_properties(self):
        # Populate table if was initialized with get() yet.
        if self.etag is None:
            self.get(overwrite_changes=False)

    def _reset_properties(self):
        self._properties = Table(self.reference, schema=self.schema)._properties
        self._get_properties = {}

    def _update_properties(self, api_table, force_update=True):
        if force_update or self.etag != api_table.etag:
            self._properties = copy.deepcopy(api_table._properties)
            self._get_properties = copy.copy(self._properties)

    def _properties_diff(self):
        changed_properties = []
        for key, value in self._properties.items():
            old_value = self._get_properties.get(key, None)
            if old_value != value:
                changed_properties.append(key)
        return changed_properties

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
