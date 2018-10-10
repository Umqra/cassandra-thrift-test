#!/usr/bin/python2.7

import ccmlib.cluster
import pycassa

values = list(map(str, range(100)))
keyspace, table = 'test_keyspace', 'test_table'

cluster = ccmlib.cluster.Cluster('.', 'thrift-test', partitioner='RandomPartitioner', cassandra_version='3.11.2')
cluster.set_configuration_options({'start_rpc': 'true', 'hinted_handoff_enabled': 'false'})
cluster.populate(3, debug=True).start()
sys = pycassa.SystemManager('127.0.0.1')
sys.create_keyspace(keyspace, pycassa.SIMPLE_STRATEGY, {'replication_factor': '3'})
sys.create_column_family(keyspace, table)
cluster.nodelist()[2].stop()
pool = pycassa.ConnectionPool(keyspace, server_list=['127.0.0.1', '127.0.0.2'], timeout=0.5)
cf = pycassa.ColumnFamily(pool, table)
for value in values:
    cf.insert(value, {'value': value})
cluster.nodelist()[2].start()
cluster.nodelist()[2].wait_for_thrift_interface()

pool = pycassa.ConnectionPool(keyspace, server_list=['127.0.0.3'], timeout=60 * 60 * 1000)
cf = pycassa.ColumnFamily(pool, table)
print(cf.multiget(values, read_consistency_level=pycassa.cassandra.ttypes.ConsistencyLevel.QUORUM))
pool = pycassa.ConnectionPool(keyspace, server_list=['127.0.0.1'], timeout=60 * 60 * 1000)
cf = pycassa.ColumnFamily(pool, table)
print(cf.multiget(values, read_consistency_level=pycassa.cassandra.ttypes.ConsistencyLevel.ONE))
cluster.stop()
