#!/usr/bin/python2.7

import ccmlib.cluster
import pycassa


cluster = ccmlib.cluster.Cluster('.', 'thrift-test', partitioner='RandomPartitioner', cassandra_version='3.11.2')
cluster.set_configuration_options({'start_rpc': 'true', 'hinted_handoff_enabled': 'false'})
cluster.populate(3, debug=True).start()
sys = pycassa.SystemManager('127.0.0.1')
sys.create_keyspace('test_keyspace', pycassa.SIMPLE_STRATEGY, {'replication_factor': '3'})
sys.create_column_family('test_keyspace', 'test_table')
cluster.nodelist()[2].stop()
pool = pycassa.ConnectionPool('test_keyspace', server_list=['127.0.0.1', '127.0.0.2'], timeout=0.5)
cf = pycassa.ColumnFamily(pool, 'test_table')
for value in ['cassandra', 'thrift']:
    cf.insert(value, {'value': value})
cluster.nodelist()[2].start()
cluster.nodelist()[2].wait_for_thrift_interface()

pool = pycassa.ConnectionPool('test_keyspace', server_list=['127.0.0.3'], timeout=60 * 60 * 1000)
cf = pycassa.ColumnFamily(pool, 'test_table')
print(cf.multiget([b'cassandra', b'thrift'], read_consistency_level=pycassa.cassandra.ttypes.ConsistencyLevel.QUORUM))
print(cf.multiget([b'cassandra', b'thrift'], read_consistency_level=pycassa.cassandra.ttypes.ConsistencyLevel.ONE))
cluster.stop()
