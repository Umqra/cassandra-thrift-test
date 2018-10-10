# Bug in Cassandra multiget Thrift queries

Since Cassandra 3.0.0 there is a subtle bug that relates to the `multiget` Thrift query. 
It appears in the case, when you try to read many partitions and this read cause `DigestMismatch` at some point. 
When this situation happened, Cassandra cut your response stream right at the point when the first `DigestMismatch` error occured.


## Bug internals
This bug reproduced in all versions of Cassandra since 3.0.0. The pre-release version 3.0.0-rc2 works fine. 
Looks like the big refactoring related to the task [CASSANDRA-9975](https://issues.apache.org/jira/browse/CASSANDRA-9975) ([link to commit](https://github.com/apache/cassandra/commit/609497471441273367013c09a1e0e1c990726ec7)) in iterator hierarchy causes wrong behaviour.

When concatenated iterator returned from the [StorageProxy.fetchRows(...)](https://github.com/apache/cassandra/blob/a05785d82c621c9cd04d8a064c38fd2012ef981c/src/java/org/apache/cassandra/service/StorageProxy.java#L1770),
Cassandra start to consume this combined iterator. 
Because of `DigestMismatch` some elements of this combined iterator contain additional `ThriftCounter`, that was added during [DataResolver.resolve(...)](https://github.com/apache/cassandra/blob/ee9e06b5a75c0be954694b191ea4170456015b98/src/java/org/apache/cassandra/service/reads/DataResolver.java#L120) execution.
While consuming iterator for many partitions, Cassandra calls [BaseIterator.tryGetMoreContents(...)](https://github.com/apache/cassandra/blob/a05785d82c621c9cd04d8a064c38fd2012ef981c/src/java/org/apache/cassandra/db/transform/BaseIterator.java#L115)
method that must switch from one partition iterator to another in case of devastation of former. 
In this case all Transformations for next iterator applied to the whole BaseIterator that enumerate many partitions sequence. 
This behaviour cause iterator to stop enumeration after it fully consume partition with `DigestMismatch` error, 
because this partition has addition `ThriftCounter` data limit that was applied to the whole composite iterator.


## Steps to reproduce:

We can reproduce the problem with 3-node cluster. First, we need to create simple table and insert some records in different partitions during short outage of one node.
Then `multiget` Thrift query that queries all partitions inserted earlier returns not all records. 
We can see many `DigestMismatch` errors in the Trace log for the queries that trigger the bug.

All these steps can be reproduced with a [simple Python2.7](small_repro_script.py) script that uses [ccmlib](https://github.com/riptano/ccm) to manage local Cassandra cluster and [pycassa](https://github.com/pycassa/pycassa) lib to send Thrift queries
(to avoid automatic repairs of the outage node we disable hintedhandoff  for the cluster):
```Python
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
cluster.stop()
```

These repository also contains script [repro_script.py](repro_script.py) that contains 
more logging information and can be used to test bug reproducibility for many different Cassandra versions.

**Script may poorly work with in the Windows environment**
