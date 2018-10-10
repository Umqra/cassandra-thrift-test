#!/usr/bin/python2.7

import ccmlib.cluster
from pycassa import SystemManager, ConnectionPool, ColumnFamily, SIMPLE_STRATEGY, cassandra

values = list(map(str, range(10)))
keyspace, table = 'test_keyspace', 'test_table'

cluster = ccmlib.cluster.Cluster('.', 'thrift-test', partitioner='RandomPartitioner', cassandra_version='3.11.2')
cluster.set_configuration_options({'start_rpc': 'true', 'hinted_handoff_enabled': 'false'})
cluster.populate(3, debug=True).start()
sys = SystemManager('127.0.0.1')
sys.create_keyspace(keyspace, SIMPLE_STRATEGY, {'replication_factor': '3'})
sys.create_column_family(keyspace, table)

# Imitate temporary node unavailability that cause inconsistency in data across nodes
failing_node = cluster.nodelist()[2]
failing_node.stop()
cf = ColumnFamily(ConnectionPool(keyspace, server_list=['127.0.0.1']), table)
for value in values:
    cf.insert(value, {'value': value}, write_consistency_level=cassandra.ttypes.ConsistencyLevel.QUORUM)
failing_node.start()
failing_node.wait_for_thrift_interface()

cf = ColumnFamily(ConnectionPool(keyspace, server_list=['127.0.0.3']), table)
# Returns many Nones records
print(cf.multiget(values, read_consistency_level=cassandra.ttypes.ConsistencyLevel.QUORUM).values())
cf = ColumnFamily(ConnectionPool(keyspace, server_list=['127.0.0.1']), table)
# All returned records are fine
print(cf.multiget(values, read_consistency_level=cassandra.ttypes.ConsistencyLevel.ONE).values())
cluster.stop()
