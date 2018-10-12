# `multiget` Thrift query processing bug in modern versions of Apache Cassandra

It seems that in Cassandra 3.0.0 a nasty bug was introduced in `multiget` Thrift query processing logic. 
When one tries to read data from several partitions with a single `multiget` query and `DigestMismatch` exception is raised during this query processing, request coordinator prematurely terminates response stream right at the point where the first `DigestMismatch` error is occurring. This leads to situation where clients "do not see" some data contained in the database.

We managed to reproduce this bug in all versions of Cassandra starting with v3.0.0. The pre-release version 3.0.0-rc2 works correctly. 
It looks like [refactoring of iterator transformation hierarchy](https://github.com/apache/cassandra/commit/609497471441273367013c09a1e0e1c990726ec7) related to [CASSANDRA-9975](https://issues.apache.org/jira/browse/CASSANDRA-9975) is causing incorrect behaviour.

When concatenated iterator is returned from the [StorageProxy.fetchRows(...)](https://github.com/apache/cassandra/blob/a05785d82c621c9cd04d8a064c38fd2012ef981c/src/java/org/apache/cassandra/service/StorageProxy.java#L1770),
Cassandra starts to consume this combined iterator. Because of `DigestMismatch` exception some elements of this combined iterator contain additional `ThriftCounter`, that was added during [DataResolver.resolve(...)](https://github.com/apache/cassandra/blob/ee9e06b5a75c0be954694b191ea4170456015b98/src/java/org/apache/cassandra/service/reads/DataResolver.java#L120) execution.
While consuming iterator for many partitions Cassandra calls [BaseIterator.tryGetMoreContents(...)](https://github.com/apache/cassandra/blob/a05785d82c621c9cd04d8a064c38fd2012ef981c/src/java/org/apache/cassandra/db/transform/BaseIterator.java#L115)
method that must switch from one partition iterator to another in case of exhaustion of the former. 
In this case all Transformations contained in the next iterator are applied to the combined BaseIterator that enumerates partitions sequence which is wrong.
This behaviour causes BaseIterator to stop enumeration after it fully consumes partition with `DigestMismatch` error, 
because this partition iterator has additional `ThriftCounter` data limit.


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
