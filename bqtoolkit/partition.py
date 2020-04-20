import re

from google.cloud.exceptions import Conflict, NotFound

from bqtoolkit.table import BQTable, _TABLE_PATH_REGEX
import bqtoolkit._helpers

_PARTITION_PATH_REGEX = re.compile(
    _TABLE_PATH_REGEX.pattern + r'\$(?P<partition_id>[\w]+)'
)


class BQPartition(object):

    def __init__(self, table, partition_id):
        self._table = table
        self._partition_id = partition_id

        self._properties = {}

    @classmethod
    def from_string(cls, partition_path, **kwargs):
        """
        Creates an instance of this class from a full partition identifier.

        :param str partition_path: A string with the full path to this partition
                                  (e.g. project-id:dataset_id.table_name$20190101).
        :param kwargs: Other keyword arguments passed to :class:`~bqtoolkit.table.BQTable` init method.
        :return: A BQPartition, initialized from the full table path.
        :rtype: :class:`~bqtoolkit.table.BQPartition`
        """
        m = _PARTITION_PATH_REGEX.match(partition_path)

        if m is None:
            raise ValueError('Failed to parse "%s" as partition path.' % partition_path)

        table_args = m.groupdict()

        partition_id = table_args['partition_id']

        del table_args['partition_id']

        kwargs.update(table_args)

        return cls(BQTable(**kwargs), partition_id)

    @property
    def full_partition_id(self):
        return self.table.full_table_id + '$' + self.partition_id

    @classmethod
    def _from_partition_query(cls, table, properties):
        partition = cls(table=table, partition_id=properties['partition_id'])
        partition._properties = properties
        return partition

    @property
    def table(self):
        return self._table

    @property
    def partition_id(self):
        return self._partition_id

    @property
    def creation_timestamp(self):
        return self._properties.get('creation_timestamp')

    @property
    def last_modified_timestamp(self):
        return self._properties.get('last_modified_timestamp')

    @property
    def partition_date(self):
        return self._properties.get('partition_date')

    def get(self):
        partition_info = None
        n_partitions = 0

        # Make sure table was initialized.
        self.table._init_properties()

        if not (self.table.time_partitioning or self.table.range_partitioning):
            raise ValueError('Table %s is not partitioned' % self.table.full_table_id)

        for result in bqtoolkit._helpers.execute_partitions_query(self.table, partition_id=self.partition_id):
            partition_info = result
            n_partitions += 1

        if partition_info is None:
            raise NotFound('Partition %s not found' % self.full_partition_id)

        if n_partitions > 1:
            raise Conflict('%d partitions with id %s found, expected only one' % (n_partitions, self.full_partition_id))

        self._properties = partition_info

        return self

    def __eq__(self, other):
        return all([self.__getattribute__(a) == other.__getattribute__(a) for a in ['table', 'partition_id']])

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return '(%s, BQPartition(%s))' % (self.table, self.partition_id)
