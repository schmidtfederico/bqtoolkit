import re

from google.cloud import bigquery

_bq_clients = {}


class BQTable(object):

    TABLE_PATH_REGEX = re.compile(r'(?P<project>[\w-]+)[:.](?P<dataset>\w+)\.(?P<name>\w+)')

    PARTITION_PATH_REGEX = re.compile(
        TABLE_PATH_REGEX.pattern + r'(\$(?P<partition>[\w]+))?'
    )

    def __init__(self, project, dataset, name, client=None):
        self.project = project
        self.dataset = dataset
        self.name = name

        self._bq_client = client

    @property
    def bq_client(self):
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
    def from_table_path(cls, table_path, **kwargs):
        """
        Creates an instance of this class from a full table identifier.

        :param table_path: A string with the full path to this table (e.g. project-id:dataset_id.table_name).
        :param kwargs: Other keyword arguments passed to :class:`~bqtoolkit.table.BQTable` init method.
        :rtype :class:`~bqtoolkit.table.BQTable`
        :return: A BQTable, initialized from the full table path.
        """
        m = BQTable.TABLE_PATH_REGEX.match(table_path)

        if m is None:
            raise ValueError('Failed to parse "%s" as table path.' % table_path)

        kwargs.update(m.groupdict())

        return cls(**kwargs)

    def __eq__(self, other):
        return all([self.__getattribute__(a) == other.__getattribute__(a) for a in ['project', 'name', 'dataset']])

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
