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

_TABLE_PATH_REGEX = re.compile(r'(?P<project>[\w-]+)[:.](?P<dataset>\w+)\.(?P<name>\w+)')


class BQTable(Table):
    """A BigQuery table.

    Combines Google's BigQuery package Client, TableReference and Table classes in one easier to use class.

    Args:
        project (str):
            The Project ID of the table.
        dataset (str):
            The Dataset ID of the table.
        name (str):
            Table ID.
        schema:
            (Optional) The table Schema, if the intention is to create this table in the ~server.
            Otherwise schema will be populated when :meth:`~bqtoolkit.table.BQTable.get` is called.
        client:
            (Optional) The BigQuery :class:`google.cloud.bigquery.client.Client` to use when performing actions
            with this table. If not defined, a client using this table's project will be used.

    Basic Usage:
      >>> from bqtoolkit.table import BQTable
      >>> t = BQTable('project', 'dataset', 'table_name')
      >>> t.get()  # Populate this table's properties.
      >>> len(t.schema) # All properties are now populated.
    """

    def __init__(self, project, dataset, name, schema=None, client=None):
        self.dataset = dataset
        self.name = name

        self._bq_client = client
        self._get_properties = {}

        super(BQTable, self).__init__('%s.%s.%s' % (project, dataset, name), schema)

    @property
    def client(self):
        """google.cloud.bigquery.client.Client:The BigQuery client used when performing requests from this table.

        If not configured by the user on init, a client with this table's `project` is used.
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

        Args:
            table_path (str):
                A string with the full path to this table (e.g. project-id:dataset_id.table_name).

            kwargs (dict):
                (Optional) Other keyword arguments passed to :class:`~bqtoolkit.table.BQTable` init method.

        Returns:
            :class:`~bqtoolkit.table.BQTable`: A BQTable, initialized from the full table path.

        """
        m = _TABLE_PATH_REGEX.match(table_path)

        if m is None:
            raise ValueError('Failed to parse "%s" as table path.' % table_path)

        kwargs.update(m.groupdict())

        return cls(**kwargs)

    def get_full_table_id(self, standard=False, quoted=False):
        """
        Returns a full identifier for this table.

        Args:
            standard (bool):
                (Optional)
                If :data:`False`, use a colon to separate project from dataset 8default).
                If :data:`True`, a dot is used (like in Standard SQL).

            quoted (bool):
                (Optional)
                Quote table path? If :data:`True`, name is wrapped with apostrophes (standard) or brackets (legacy).

        Returns:
            str: This table's full id.
        """
        name_str = '{project}.{dataset}.{table}' if standard else '{project}:{dataset}.{table}'

        if quoted:
            quotes = '`%s`' if standard else '[%s]'
            name_str = quotes % name_str

        return name_str.format(project=self.project, dataset=self.dataset, table=self.name)

    @property
    def full_table_id(self):
        """str:ID for the table.

        In the format ``project_id:dataset_id.table_id``.

        Equal to the call `table.get_full_table_id(standard=False, quoted=False)`.

        .. note::
            Contrary to Google's Table, this property is available even when the server hasn't yet been called.
        """
        return self.get_full_table_id()

    def get(self, overwrite_changes=True, **kwargs):
        """Populates this table's properties from the server.

        If properties were already populated, ``overwrite_changes`` controls if any potential changes that
        user made to this table's properties get overwritten (only relevant when performing an update operation
        afterwards).

        Args:
            overwrite_changes (bool):
                (Optional) Overwrite user-made changes in this table's properties. Defaults to :data:`True`.

        Returns:
            :class:`~bqtoolkit.table.BQTable`:
                A reference to this same object, so that it's safe to do: `t = t.get()`

        Raises:
            google.api_core.exceptions.NotFound: if this table doesn't exist.
        """
        self._update_properties(self.client.get_table(self, **kwargs), force_update=overwrite_changes)
        return self

    def exists(self):
        """Checks if table exists in the server.

        .. note::
            This method also populates this table's properties (like :meth:`~bqtoolkit.table.BQTable.get` does),
            but only if the table exists in the server and :meth:`~bqtoolkit.table.BQTable.get` wasn't called before.

        Returns:
            bool:
                :data:`True` if table exists in BigQuery server, :data:`False` otherwise.

        """
        try:
            self.get(overwrite_changes=False)
            return True
        except NotFound:
            return False

    def delete(self, prompt=True, **kwargs):
        """Deletes table from the server.

        Args:
            prompt (bool):
                (Optional) Prompt for confirmation before deleting table. Defaults to :data:`True`.

            kwargs (dict):
                (Optional) Other parameters passed to method :meth:`~google.cloud.bigquery.client.Client.delete_table`
                from BigQuery's :class:`~google.cloud.bigquery.client.Client`.

        Returns:
            bool: :data:`True` if table was deleted, :data:`False` otherwise.
        """
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
        """Creates table in BigQuery.

        Args:
            kwargs (dict):
                (Optional) Other parameters passed to method :meth:`~google.cloud.bigquery.client.Client.create_table`
                from BigQuery's :class:`~google.cloud.bigquery.client.Client`.

        Returns:
            :class:`~bqtoolkit.table.BQTable`:
                A reference to this same object, so that it's safe to do: `t = t.create()`

        Example:

          >>> from google.cloud.bigquery.schema import SchemaField
          >>> t = BQTable('project', 'dataset', 'table_name')
          >>> t.schema = [SchemaField('my_string', 'STRING')]
          >>> t.create()
        """
        self._update_properties(self.client.create_table(self, **kwargs))
        return self

    def update(self, fields=None, **kwargs):
        """
        Updates this table fields in BigQuery.

        If fields are not specified, a diff is calculated based on which properties were modified by the user after
        after doing the last :meth:`~bqtoolkit.table.BQTable.get()` call.

        Args:
            fields (list[str]):
                (Optional) The list of properties to update. If not provided, calculated based on what changed since
                table's properties were last updated (see :meth:`~bqtoolkit.table.BQTable.get()` and
                :meth:`~bqtoolkit.table.BQTable.exists()`).

            kwargs (dict):
                (Optional) Other parameters passed to method :meth:`~google.cloud.bigquery.client.Client.update_table`
                from BigQuery's :class:`~google.cloud.bigquery.client.Client`.

        Returns:
            :class:`~bqtoolkit.table.BQTable`:
                A reference to this same object, so that it's safe to do: t = t.update()

        Example:

          >>> from google.cloud.bigquery.schema import SchemaField
          >>> t = BQTable('project', 'dataset', 'table_name')
          >>> t.get()
          >>> t.description = 'A new description for this table'
          >>> t.schema += [SchemaField('a_new_field', 'FLOAT')]
          >>> t.update()
          >>> # Same as doing t.update(fields=['description', 'schema'])
        """
        if fields is None:
            fields = self._properties_diff()
        self._update_properties(self.client.update_table(self, fields=fields, **kwargs))
        return self

    def get_partitions(self):
        """
        Lists all partitions in this table.

        Each partition has its id, creation timestamp and last modified timestamp.
        See :class:`~bqtoolkit.partition.BQPartition`.

        Returns:
            list[:class:`~bqtoolkit.partition.BQPartition`]
        """
        self._init_properties()

        from bqtoolkit.partition import BQPartition
        if self.time_partitioning or self.range_partitioning:
            partitions = []

            for partition_info in execute_partitions_query(self):
                partitions.append(BQPartition._from_partition_query(self, partition_info))
            return partitions

        return None

    def load(self, file_path, storage_bucket=None, storage_project=None, write_disposition='WRITE_APPEND',
             autodetect=True, **kwargs):
        """
        Loads into this table the content of the given file path. Automatically handles large files uploads (> 10 MB)
        using Google Cloud Storage, if a storage_bucket is provided.

        Files uploaded to Google Cloud Storage will be deleted as soon as the load job finishes (even if it fails).
        If credentials lack permission to perform the delete a warning will be raised.

        Args:
            file_path (str):
                Path to the file.

            storage_bucket (str):
                (Optional) The GCS bucket to use to upload the file if it exceeds 10 MB in size.
                If not provided and file is larger than 10 MB, a ValueError will be raised.

            storage_project (str):
                (Optional) (Optional) The GCS project to use to upload the file if it exceeds 10 MB in size.
                Defaults to the table's project.

            autodetect (bool):
                (Optional) Whether to let BigQuery automatically detect the schema of the file, defaults to True.
                See :class:`~google.cloud.bigquery.LoadJobConfig` for more details.

            write_disposition (bool):
                (Optional) What to do if table already exists. Defaults to append to it.
                See :class:`~google.cloud.bigquery.LoadJobConfig` for more details.

            kwargs (dict):
                (Optional) Other parameters passed to :class:`~google.cloud.bigquery.LoadJobConfig` constructor.

        Raises:
            ValueError:
                If the file to load is bigger than 10 MB and no ``storage_bucket`` was passed.
            RuntimeError:
                If the load to BigQuery failed.
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

            # Perform cleanup.
            try:
                blob.delete()
            except Forbidden:
                warnings.warn('Failed to delete uploaded blob %s' % blob.path)

            if result.errors and len(result.errors) > 0:
                raise RuntimeError('Failed to load data into table. Errors:\n%s' % json.dumps(result.errors, indent=4))

    def download(self, storage_bucket=None, storage_project=None, destination_format='csv', **kwargs):
        """Downloads a table's content to one or more *temporary* files for processing.

        Automatically handles downloading large tables (> 10 MB) via Google Cloud Storage and deletes temporary GCS
        files once user is done processing. If credentials lack delete permissions in ``storage_bucket`` a warning
        will be raised.

        Yields the path of each file for processing and deletes each file as the user is done processing
        (if the used didn't delete or move the file).

        .. note::
            Use :func:`shutil.move` to move the yielded file if you intend to keep the exported file.

        Args:
            storage_bucket (str):
                (Optional) The GCS bucket to use to extract the table if it exceeds 10 MB in size or the destination
                format is not CSV. If not provided and the table is larger than 10 MB, a ValueError will be raised.

            storage_project (str):
                The GCS project to use to export the table if it exceeds 10 MB in size or if the destination format
                is not CSV. Defaults to the table's project.

            destination_format (str):
                (Optional) Format in which to export the table, defaults to ``csv``.
                Other valid values are ``json`` and ``avro``. See :class:`~google.cloud.bigquery.ExtractJobConfig`.

            kwargs (dict):
                (Optional) Other parameters passed to :class:`~google.cloud.bigquery.ExtractJobConfig` constructor.

        Yields:
            str:
                Path to a *temporary* file where rows from this table were exported.
                File is automatically deleted after processing.

        Raises:
            ValueError:
                if the table to download is bigger than 10 MB or format is not CSV and no ``storage_bucket`` was passed.
            RuntimeError:
                if the extraction job from BigQuery failed.

        Example:

          >>> from bqtoolkit import BQTable
          >>> t = BQTable('project', 'dataset', 'table_name')
          >>> for file_path in t.download():
          >>>     with open(file_path) as f:
          >>>         f.read()
          >>>     # File will be deleted after iterating.
        """
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

                    try:
                        # Yield path of downloaded blob for processing.
                        yield file_path
                    finally:
                        # Perform local and remote cleanup.

                        # Although files will be deleted by TemporaryDirectory when the with block exits,
                        # in case we're dealing with large files, we want to make space in the working directory
                        # for the next blob.
                        if os.path.exists(file_path):
                            os.remove(file_path)

                        try:
                            # Don't leave blobs in GCS as these incur in charges.
                            blob.delete()
                        except Forbidden:
                            warnings.warn('Failed to delete extracted blob %s' % blob.path)

    def _init_properties(self):
        """
        Call this internal method when you need your method to work with an initialized BQTable.
        """
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
                "A Google Cloud Storage bucket name should be provided in order to perform load or download operations"
                "with table %s" % self.full_table_id
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
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._bq_client = None
