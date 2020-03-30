import re

from google.cloud import bigquery
from google.cloud.bigquery import Table

_bq_clients = {}


class BQTable(Table):

    TABLE_PATH_REGEX = re.compile(r'(?P<project>[\w-]+)[:.](?P<dataset>\w+)\.(?P<name>\w+)')

    PARTITION_PATH_REGEX = re.compile(
        TABLE_PATH_REGEX.pattern + r'(\$(?P<partition>[\w]+))?'
    )

    def __init__(self, project, dataset, name, schema=None, client=None):
        self.dataset = dataset
        self.name = name

        self._bq_client = client

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

    def get(self):
        result = self.client.get_table(self)
        self._properties.update(result._properties)
        return self

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
