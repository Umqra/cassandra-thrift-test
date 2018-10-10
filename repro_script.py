#!/usr/bin/python2.7
import datetime

import unittest2
import ccmlib.cluster
import pycassa


def log(info):
    print('[{}]: {}'.format(datetime.datetime.now(), info))


Keyspace = 'test_keyspace'
TableName = 'test_table'


class GracefulCluster:
    def __init__(self, cluster_name, cassandra_version, partitioner, options):
        self.cluster_name = cluster_name
        self.cassandra_version = cassandra_version

        self.partitioner = partitioner
        self.options = options
        self.cluster = None

    def __enter__(self):
        full_cluster_name = self._normalize_cluster_name('{}-{}'.format(self.cluster_name, self.cassandra_version))
        self.cluster = ccmlib.cluster.Cluster(
            '.', full_cluster_name,
            partitioner=self.partitioner,
            cassandra_version=self.cassandra_version)
        self.cluster.set_configuration_options(self.options)
        log("""Created and configured initial cluster values:
    Name: {}, Version: {}, Partitioner: {}""".format(full_cluster_name, self.cassandra_version, self.partitioner))
        return self.cluster

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            log('Exception of type {} during the test: {}. Traceback: {}'.format(exc_type, exc_value, traceback))
        self.cluster.remove()
        log('Cluster removed')
        return False

    @staticmethod
    def _normalize_cluster_name(cluster_name):
        return cluster_name.replace('.', '-')


class ThriftMultigetTestCase(unittest2.TestCase):
    Payload = [(str(i), str(i)) for i in range(100)]
    ClusterOptions = {
        'read_request_timeout_in_ms': '3600000',
        'request_timeout_in_ms': '3600000',
        'start_rpc': 'true',
        'hinted_handoff_enabled': 'false', }
    CassandraVersionList = ['2.2.8', '2.2.9', '3.0.0-rc2',
                            '3.0.0', '3.1', '3.2', '3.3', '3.4', '3.5', '3.6', '3.7', '3.8',
                            '3.9', '3.10',
                            '3.11.0', '3.11.1', '3.11.2', '3.11.3']

    def test_multiget_query(self):
        for cassandra_version in ThriftMultigetTestCase.CassandraVersionList:
            with self.subTest(cassandra_version=cassandra_version):
                self.assertTrue(self.check_cassandra_version(cassandra_version))

    @staticmethod
    def check_cassandra_version(cassandra_version):
        with GracefulCluster('thrift-test', cassandra_version, 'RandomPartitioner',
                             ThriftMultigetTestCase.ClusterOptions) as cluster:
            cluster.set_log_level("TRACE")
            cluster.populate(3, debug=True)
            cluster.start()
            log('Populate nodes')
            ThriftMultigetTestCase._prepare_for_test(cluster)

            cluster.nodetool('settraceprobability 1')
            log('Enable tracing on every node')
            return ThriftMultigetTestCase._check_cassandra()

    @staticmethod
    def _prepare_for_test(nodes_cluster):
        log('Start actualizing scheme')

        sys = pycassa.SystemManager('127.0.0.1')
        sys.create_keyspace(Keyspace, pycassa.SIMPLE_STRATEGY, {'replication_factor': '3'})
        sys.create_column_family(Keyspace, TableName)
        log('Created keyspace {} and column family {}'.format(Keyspace, TableName))

        nodes_cluster.nodelist()[2].stop()
        log('Stopped third node')

        pool = pycassa.ConnectionPool(Keyspace, server_list=['127.0.0.1', '127.0.0.2'], timeout=0.5)
        cf = pycassa.ColumnFamily(pool, TableName)
        for key, value in ThriftMultigetTestCase.Payload:
            cf.insert(key, {'value': value}, write_consistency_level=pycassa.ConsistencyLevel.QUORUM)
        log('Inserted {} keys in the table {}.{}'.format(len(ThriftMultigetTestCase.Payload), Keyspace, TableName))

        nodes_cluster.nodelist()[2].start()
        log('Started third node')
        nodes_cluster.nodelist()[2].wait_for_thrift_interface()
        log('Thrift interface is active for the third node')

    @staticmethod
    def _check_cassandra():
        pool = pycassa.ConnectionPool(Keyspace, server_list=['127.0.0.3'], timeout=60 * 60 * 1000)
        cf = pycassa.ColumnFamily(pool, TableName)
        result_set = list(filter(lambda x: x[1] is not None,
                                 cf.multiget([s[0].decode() for s in ThriftMultigetTestCase.Payload],
                                             read_consistency_level=pycassa.cassandra.ttypes.ConsistencyLevel.QUORUM).items()))
        log('Queried {} records, returned {} records'.format(len(ThriftMultigetTestCase.Payload), len(result_set)))
        return len(result_set) == len(ThriftMultigetTestCase.Payload)


def main():
    ThriftMultigetTestCase.check_cassandra_version('3.11.2')


if __name__ == '__main__':
    main()
