Apache Cassandra MV Repair Spark Job
------------------------------------
In 2017, Apache Cassandra designated Materialized Views (MV) an experimental feature due to concerns about data inconsistencies between base tables and their corresponding views. This job neither alters that status nor promotes the use of MV.
However, this job offers a method to resolve inconsistencies within MV tables for users who already have MVs in production. One thing to note is that Apache Cassandra does not officially support this Spark job, so please take it with a grain of salt.



Build
-----

$ `mvn package`

This will generate a fat jar in the target directory, the jar: `"./target/cassandra-mv-repair-spark-job-1.0-SNAPSHOT.jar"`

Configuration
-------------
| Configuration | Type    | Default  | Description                                                                                                                                                                                                                                                                                                                                                  |
|------------|---------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| keyspace   | String  | null     | Cassandra Keyspace name.                                                                                                                                                                                                                                                                                                                                     |
| basetablename | String  | null     | Cassandra Base table name.                                                                                                                                                                                                                                                                                                                                   |
| mvname     | String  | null     | Cassandra Materialized View (MV) name.                                                                                                                                                                                                                                                                                                                       |
| cassandra.host | String  | localhost | Cassandra cluster host to connect to.                                                                                                                                                                                                                                                                                                                        |
| cassandra.port | String  | 9042     | Cassandra cluster port to connect to.                                                                                                                                                                                                                                                                                                                        |
| cassandra.username | String  | null     | Cassandra username.                                                                                                                                                                                                                                                                                                                                          |
| cassandra.password | String  | null     | Cassandra password.                                                                                                                                                                                                                                                                                                                                          |
| cassandra.datacenter | String  | datacenter1 | Cassandra datacenter.                                                                                                                                                                                                                                                                                                                                        |
| starttsinsec | Long    | -1       | Cassandra records’ write timestamp value greater than this config should be considered in the Spark job. This applies to both the Base table and MV table. If there are columns with different timestamps in a record, then the smallest timestamp needs to be greater than this.                                                                            |
| endtsinsec | Long    | -1       | Cassandra records’ write timestamp value less than this config should be considered in the Spark job. This applies to both the Base table and MV table. If there are columns with different timestamps in a record, then the largest timestamp needs to be less than this.                                                                                   |
| readconsistency | String  | LOCAL_QUORUM | Spark job will use this consistency level while scanning the records.                                                                                                                                                                                                                                                                                        |
| mvwriteconsistency | String  | LOCAL_QUORUM | Spark job will use this consistency level to fix inconsistent MV record(s).                                                                                                                                                                                                                                                                                  |
| fixmissingmv | Boolean | false    | Should the job automatically insert the missing record in MV? By default, it does not repair them; it just lists the inconsistencies.                                                                                                                                                                                                                        |
| fixorphanmv | Boolean | false    | Should the job automatically delete the unexpected record in MV? By default, it does not repair them; it just lists the inconsistencies.                                                                                                                                                                                                                     |
| fixinconsistentmv | Boolean | false    | Should the job automatically fix the inconsistent record in MV? By default, it does not repair them; it just lists the inconsistencies.                                                                                                                                                                                                                      |
| scan.ratelimiter | Integer | 10       | Throttler while scanning Cassandra's records. By default, one Spark worker would scan 10 records per second. If you have N Spark workers, then the total read throughput would be = N * 10, so you need to size it accordingly.                                                                                                                               |
| mutation.ratelimiter | Integer | 5        | Throttler while fixing the inconsistent MV records. By default, one Spark worker would mutate 5 records per second. If you have N Spark workers, then the total mutation throughput would be = N * 5, so you need to size it accordingly.                                                                                                                     |
| mutation.uselatestts | Boolean | false    | By default, Spark workers use the base table timestamp for the MV mutation. For some reason (mostly for testing purposes), if you want to use the latest timestamp for MV mutations, then set this to `true`.                                                                                                                                                |
| output.dir | String  | /tmp/cassandra-mv-repair-spark-job/ | The job outputs the MV inconsistencies to an output folder.                                                                                                                                                                                                                                                                                                  |


Examples
--------

### Example 1: Record is consistent

Status in Cassandra
```bash

cassandra@cqlsh> select * from test_keyspace.test_table; SELECT * FROM test_keyspace.test_mv ;

 id                                   | ascii_col     | bigint_col | blob_col     | boolean_col | date_col   | decimal_col | double_col | float_col | inet_col    | int_col | list_col             | map_col                | set_col              | smallint_col | text_col     | time_col           | timestamp_col                   | timeuuid_col                         | tinyint_col | uuid_col                             | varchar_col     | varint_col
--------------------------------------+---------------+------------+--------------+-------------+------------+-------------+------------+-----------+-------------+---------+----------------------+------------------------+----------------------+--------------+--------------+--------------------+---------------------------------+--------------------------------------+-------------+--------------------------------------+-----------------+------------
 4f06ef60-9333-4473-bfe0-be22ef67a6a3 | example_ascii | 1234567890 | 0x48656c6c6f |        True | 2022-12-01 |      123.45 |    123.456 |   123.456 | 192.168.0.1 |     123 | ['value1', 'value2'] | {'key1': 1, 'key2': 2} | {'value1', 'value2'} |        12345 | example_text | 12:34:56.000000000 | 2024-02-28 12:34:56.000000+0000 | e1f84bd0-b292-11ef-ac23-7b8dec181fef |           1 | 84732e32-243e-48fb-b56a-00ef2d860cec | example_varchar |      12345

(1 rows)

 int_col | id                                   | ascii_col     | bigint_col | blob_col     | boolean_col | date_col   | decimal_col | double_col | float_col | inet_col    | list_col             | map_col                | set_col              | smallint_col | text_col     | time_col           | timestamp_col                   | timeuuid_col                         | tinyint_col | uuid_col                             | varchar_col     | varint_col
---------+--------------------------------------+---------------+------------+--------------+-------------+------------+-------------+------------+-----------+-------------+----------------------+------------------------+----------------------+--------------+--------------+--------------------+---------------------------------+--------------------------------------+-------------+--------------------------------------+-----------------+------------
     123 | 4f06ef60-9333-4473-bfe0-be22ef67a6a3 | example_ascii | 1234567890 | 0x48656c6c6f |        True | 2022-12-01 |      123.45 |    123.456 |   123.456 | 192.168.0.1 | ['value1', 'value2'] | {'key1': 1, 'key2': 2} | {'value1', 'value2'} |        12345 | example_text | 12:34:56.000000000 | 2024-02-28 12:34:56.000000+0000 | e1f84bd0-b292-11ef-ac23-7b8dec181fef |           1 | 84732e32-243e-48fb-b56a-00ef2d860cec | example_varchar |      12345

(1 rows)
cassandra@cqlsh> 
```

Now Run the job
```bash
$> java -Dspark.cass.mv.keyspace=test_keyspace -Dspark.cass.mv.basetablename=test_table -Dspark.cass.mv.mvname=test_mv -Dspark.cass.mv.starttsinsec=0 -Dspark.cass.mv.endtsinsec=1733425770 -Dspark.app.name=MvSyncJob -Dspark.master=local[*] -Dspark.cass.mv.cassandra.username=cassandra -Dspark.cass.mv.cassandra.password=cassandra -Dspark.cass.mv.mutation.uselatestts=true -Dspark.cass.mv.output.dir=/tmp/cassandra-mv-repair-spark-job/ -jar ./target/cassandra-mv-repair-spark-job-1.0-SNAPSHOT.jar

$> cat /tmp/cassandra-mv-repair-spark-job/stats.txt
totRecords: 1, skippedRecords: 0, consistentRecords: 1, inConsistentRecords: 0, missingBaseTableRecords: 0, missingMvRecords: 0, repairRecords: 0, notRepairRecords: 0, delAttemptedRecords: 0, delErrRecords: 0, delSuccessRecords: 0, notDelRecords: 0, upsertAttemptedRecords: 0, upsertErrRecords: 0, upsertSuccessRecords: 0
```


### Example 2: Record is inconsistent

Status in Cassandra
```bash

cassandra@cqlsh> select double_col from test_keyspace.test_table; select double_col from test_keyspace.test_mv ;

 double_col
------------
    123.456

(1 rows)

 double_col
------------
        111

(1 rows)
cassandra@cqlsh> 
```

Now Run the job
```bash

$> java -Dspark.cass.mv.keyspace=test_keyspace -Dspark.cass.mv.basetablename=test_table -Dspark.cass.mv.mvname=test_mv -Dspark.cass.mv.starttsinsec=0 -Dspark.cass.mv.endtsinsec=1733339370 -Dspark.app.name=MvSyncJob -Dspark.master=local[*] -Dspark.cass.mv.cassandra.username=cassandra -Dspark.cass.mv.cassandra.password=cassandra -Dspark.cass.mv.mutation.uselatestts=true -Dspark.cass.mv.output.dir=/tmp/cassandra-mv-repair-spark-job/ -jar ./target/cassandra-mv-repair-spark-job-1.0-SNAPSHOT.jar

$> cat /tmp/cassandra-mv-repair-spark-job/stats.txt
totRecords: 1, skippedRecords: 0, consistentRecords: 0, inConsistentRecords: 1, missingBaseTableRecords: 0, missingMvRecords: 0, repairRecords: 0, notRepairRecords: 1, delAttemptedRecords: 0, delErrRecords: 0, delSuccessRecords: 0, notDelRecords: 0, upsertAttemptedRecords: 0, upsertErrRecords: 0, upsertSuccessRecords: 0

$> cat /tmp/cassandra-mv-repair-spark-job/INCONSISTENT/12.txt 
Problem: INCONSISTENT
RowKey: id:UUID:4f06ef60-9333-4473-bfe0-be22ef67a6a3,int_col:INT:123
MainTableEntry: CassandraRow{id: 4f06ef60-9333-4473-bfe0-be22ef67a6a3, ascii_col: example_ascii, writetime(ascii_col): 1733352939772229, ttl(ascii_col): null, bigint_col: 1234567890, writetime(bigint_col): 1733352939772229, ttl(bigint_col): null, blob_col: 0x48656c6c6f, writetime(blob_col): 1733352939772229, ttl(blob_col): null, boolean_col: true, writetime(boolean_col): 1733352939772229, ttl(boolean_col): null, date_col: 2022-12-01, writetime(date_col): 1733352939772229, ttl(date_col): null, decimal_col: 123.45, writetime(decimal_col): 1733352939772229, ttl(decimal_col): null, double_col: 123.456, writetime(double_col): 1733352939772229, ttl(double_col): null, float_col: 123.456, writetime(float_col): 1733352939772229, ttl(float_col): null, inet_col: 192.168.0.1, writetime(inet_col): 1733352939772229, ttl(inet_col): null, int_col: 123, writetime(int_col): 1733352939772229, ttl(int_col): null, list_col: [value1,value2], map_col: {key1: 1,key2: 2}, set_col: {value1,value2}, smallint_col: 12345, writetime(smallint_col): 1733352939772229, ttl(smallint_col): null, text_col: example_text, writetime(text_col): 1733352939772229, ttl(text_col): null, time_col: 12:34:56, writetime(time_col): 1733352939772229, ttl(time_col): null, timestamp_col: 2024-02-28T12:34:56Z, writetime(timestamp_col): 1733352939772229, ttl(timestamp_col): null, timeuuid_col: e1f84bd0-b292-11ef-ac23-7b8dec181fef, writetime(timeuuid_col): 1733352939772229, ttl(timeuuid_col): null, tinyint_col: 1, writetime(tinyint_col): 1733352939772229, ttl(tinyint_col): null, uuid_col: 84732e32-243e-48fb-b56a-00ef2d860cec, writetime(uuid_col): 1733352939772229, ttl(uuid_col): null, varchar_col: example_varchar, writetime(varchar_col): 1733352939772229, ttl(varchar_col): null, varint_col: 12345, writetime(varint_col): 1733352939772229, ttl(varint_col): null}
MVTableEntry: CassandraRow{id: 4f06ef60-9333-4473-bfe0-be22ef67a6a3, int_col: 123, ascii_col: example_ascii, writetime(ascii_col): 1733352939772229, ttl(ascii_col): null, bigint_col: 1234567890, writetime(bigint_col): 1733352939772229, ttl(bigint_col): null, blob_col: 0x48656c6c6f, writetime(blob_col): 1733352939772229, ttl(blob_col): null, boolean_col: true, writetime(boolean_col): 1733352939772229, ttl(boolean_col): null, date_col: 2022-12-01, writetime(date_col): 1733352939772229, ttl(date_col): null, decimal_col: 123.45, writetime(decimal_col): 1733352939772229, ttl(decimal_col): null, double_col: 111.0, writetime(double_col): 1733355182588375, ttl(double_col): null, float_col: 123.456, writetime(float_col): 1733352939772229, ttl(float_col): null, inet_col: 192.168.0.1, writetime(inet_col): 1733352939772229, ttl(inet_col): null, list_col: [value1,value2], map_col: {key1: 1,key2: 2}, set_col: {value1,value2}, smallint_col: 12345, writetime(smallint_col): 1733352939772229, ttl(smallint_col): null, text_col: example_text, writetime(text_col): 1733352939772229, ttl(text_col): null, time_col: 12:34:56, writetime(time_col): 1733352939772229, ttl(time_col): null, timestamp_col: 2024-02-28T12:34:56Z, writetime(timestamp_col): 1733352939772229, ttl(timestamp_col): null, timeuuid_col: e1f84bd0-b292-11ef-ac23-7b8dec181fef, writetime(timeuuid_col): 1733352939772229, ttl(timeuuid_col): null, tinyint_col: 1, writetime(tinyint_col): 1733352939772229, ttl(tinyint_col): null, uuid_col: 84732e32-243e-48fb-b56a-00ef2d860cec, writetime(uuid_col): 1733352939772229, ttl(uuid_col): null, varchar_col: example_varchar, writetime(varchar_col): 1733352939772229, ttl(varchar_col): null, varint_col: 12345, writetime(varint_col): 1733352939772229, ttl(varint_col): null}
BaseColumn: double_col:DOUBLE:123.456
MvColumn: double_col:DOUBLE:111.0
```

### Example 3: Record is missing in MV

Status in Cassandra
```bash

cassandra@cqlsh> select * from test_keyspace.test_table; SELECT * FROM test_keyspace.test_mv;

 id                                   | ascii_col     | bigint_col | blob_col     | boolean_col | date_col   | decimal_col | double_col | float_col | inet_col    | int_col | list_col             | map_col                | set_col              | smallint_col | text_col     | time_col           | timestamp_col                   | timeuuid_col                         | tinyint_col | uuid_col                             | varchar_col     | varint_col
--------------------------------------+---------------+------------+--------------+-------------+------------+-------------+------------+-----------+-------------+---------+----------------------+------------------------+----------------------+--------------+--------------+--------------------+---------------------------------+--------------------------------------+-------------+--------------------------------------+-----------------+------------
 4f06ef60-9333-4473-bfe0-be22ef67a6a3 | example_ascii | 1234567890 | 0x48656c6c6f |        True | 2022-12-01 |      123.45 |    123.456 |   123.456 | 192.168.0.1 |     123 | ['value1', 'value2'] | {'key1': 1, 'key2': 2} | {'value1', 'value2'} |        12345 | example_text | 12:34:56.000000000 | 2024-02-28 12:34:56.000000+0000 | e1f84bd0-b292-11ef-ac23-7b8dec181fef |           1 | 84732e32-243e-48fb-b56a-00ef2d860cec | example_varchar |      12345

(1 rows)

 int_col | id | ascii_col | bigint_col | blob_col | boolean_col | date_col | decimal_col | double_col | float_col | inet_col | list_col | map_col | set_col | smallint_col | text_col | time_col | timestamp_col | timeuuid_col | tinyint_col | uuid_col | varchar_col | varint_col
---------+----+-----------+------------+----------+-------------+----------+-------------+------------+-----------+----------+----------+---------+---------+--------------+----------+----------+---------------+--------------+-------------+----------+-------------+------------

(0 rows)
cassandra@cqlsh> 
```

Now Run the job
```bash

$> java -Dspark.cass.mv.keyspace=test_keyspace -Dspark.cass.mv.basetablename=test_table -Dspark.cass.mv.mvname=test_mv -Dspark.cass.mv.starttsinsec=0 -Dspark.cass.mv.endtsinsec=1733339370 -Dspark.app.name=MvSyncJob -Dspark.master=local[*] -Dspark.cass.mv.cassandra.username=cassandra -Dspark.cass.mv.cassandra.password=cassandra -Dspark.cass.mv.mutation.uselatestts=true -Dspark.cass.mv.output.dir=/tmp/cassandra-mv-repair-spark-job/ -jar ./target/cassandra-mv-repair-spark-job-1.0-SNAPSHOT.jar

$> cat /tmp/cassandra-mv-repair-spark-job/stats.txt
totRecords: 1, skippedRecords: 0, consistentRecords: 0, inConsistentRecords: 0, missingBaseTableRecords: 0, missingMvRecords: 1, repairRecords: 0, notRepairRecords: 1, delAttemptedRecords: 0, delErrRecords: 0, delSuccessRecords: 0, notDelRecords: 0, upsertAttemptedRecords: 0, upsertErrRecords: 0, upsertSuccessRecords: 0

$> cat /tmp/cassandra-mv-repair-spark-job/MISSING_IN_MV_TABLE/12.txt 
Problem: MISSING_IN_MV_TABLE
RowKey: id:UUID:4f06ef60-9333-4473-bfe0-be22ef67a6a3,int_col:INT:123
MainTableEntry: CassandraRow{id: 4f06ef60-9333-4473-bfe0-be22ef67a6a3, ascii_col: example_ascii, writetime(ascii_col): 1733352939772229, ttl(ascii_col): null, bigint_col: 1234567890, writetime(bigint_col): 1733352939772229, ttl(bigint_col): null, blob_col: 0x48656c6c6f, writetime(blob_col): 1733352939772229, ttl(blob_col): null, boolean_col: true, writetime(boolean_col): 1733352939772229, ttl(boolean_col): null, date_col: 2022-12-01, writetime(date_col): 1733352939772229, ttl(date_col): null, decimal_col: 123.45, writetime(decimal_col): 1733352939772229, ttl(decimal_col): null, double_col: 123.456, writetime(double_col): 1733352939772229, ttl(double_col): null, float_col: 123.456, writetime(float_col): 1733352939772229, ttl(float_col): null, inet_col: 192.168.0.1, writetime(inet_col): 1733352939772229, ttl(inet_col): null, int_col: 123, writetime(int_col): 1733352939772229, ttl(int_col): null, list_col: [value1,value2], map_col: {key1: 1,key2: 2}, set_col: {value1,value2}, smallint_col: 12345, writetime(smallint_col): 1733352939772229, ttl(smallint_col): null, text_col: example_text, writetime(text_col): 1733352939772229, ttl(text_col): null, time_col: 12:34:56, writetime(time_col): 1733352939772229, ttl(time_col): null, timestamp_col: 2024-02-28T12:34:56Z, writetime(timestamp_col): 1733352939772229, ttl(timestamp_col): null, timeuuid_col: e1f84bd0-b292-11ef-ac23-7b8dec181fef, writetime(timeuuid_col): 1733352939772229, ttl(timeuuid_col): null, tinyint_col: 1, writetime(tinyint_col): 1733352939772229, ttl(tinyint_col): null, uuid_col: 84732e32-243e-48fb-b56a-00ef2d860cec, writetime(uuid_col): 1733352939772229, ttl(uuid_col): null, varchar_col: example_varchar, writetime(varchar_col): 1733352939772229, ttl(varchar_col): null, varint_col: 12345, writetime(varint_col): 1733352939772229, ttl(varint_col): null}
MVTableEntry: null
```

### Example 4: Record is missing in the base table

Status in Cassandra
```bash

cassandra@cqlsh> select * from test_keyspace.test_table; SELECT * FROM test_keyspace.test_mv;

 id | ascii_col | bigint_col | blob_col | boolean_col | date_col | decimal_col | double_col | float_col | inet_col | int_col | list_col | map_col | set_col | smallint_col | text_col | time_col | timestamp_col | timeuuid_col | tinyint_col | uuid_col | varchar_col | varint_col
----+-----------+------------+----------+-------------+----------+-------------+------------+-----------+----------+---------+----------+---------+---------+--------------+----------+----------+---------------+--------------+-------------+----------+-------------+------------

(0 rows)

 int_col | id                                   | ascii_col     | bigint_col | blob_col     | boolean_col | date_col   | decimal_col | double_col | float_col | inet_col    | list_col             | map_col                | set_col              | smallint_col | text_col     | time_col           | timestamp_col                   | timeuuid_col                         | tinyint_col | uuid_col                             | varchar_col     | varint_col
---------+--------------------------------------+---------------+------------+--------------+-------------+------------+-------------+------------+-----------+-------------+----------------------+------------------------+----------------------+--------------+--------------+--------------------+---------------------------------+--------------------------------------+-------------+--------------------------------------+-----------------+------------
     123 | a69b6b66-7bbf-44dc-8eef-eb0884369740 | example_ascii | 1234567890 | 0x48656c6c6f |        True | 2022-12-01 |      123.45 |    123.456 |   123.456 | 192.168.0.1 | ['value1', 'value2'] | {'key1': 1, 'key2': 2} | {'value1', 'value2'} |        12345 | example_text | 12:34:56.000000000 | 2024-02-28 12:34:56.000000+0000 | 291afe30-b299-11ef-b544-6b207d5b3d1b |           1 | 42c6ded9-ed9a-479c-abca-cee1d3a646cc | example_varchar |      12345

(1 rows)
cassandra@cqlsh> 
```

Now Run the job
```bash

$> java -Dspark.cass.mv.keyspace=test_keyspace -Dspark.cass.mv.basetablename=test_table -Dspark.cass.mv.mvname=test_mv -Dspark.cass.mv.starttsinsec=0 -Dspark.cass.mv.endtsinsec=1733339370 -Dspark.app.name=MvSyncJob -Dspark.master=local[*] -Dspark.cass.mv.cassandra.username=cassandra -Dspark.cass.mv.cassandra.password=cassandra -Dspark.cass.mv.mutation.uselatestts=true -Dspark.cass.mv.output.dir=/tmp/cassandra-mv-repair-spark-job/ -jar ./target/cassandra-mv-repair-spark-job-1.0-SNAPSHOT.jar

$> cat /tmp/cassandra-mv-repair-spark-job/stats.txt
totRecords: 1, skippedRecords: 0, consistentRecords: 0, inConsistentRecords: 0, missingBaseTableRecords: 1, missingMvRecords: 0, repairRecords: 0, notRepairRecords: 1, delAttemptedRecords: 0, delErrRecords: 0, delSuccessRecords: 0, notDelRecords: 0, upsertAttemptedRecords: 0, upsertErrRecords: 0, upsertSuccessRecords: 0

$> cat /tmp/cassandra-mv-repair-spark-job/MISSING_IN_BASE_TABLE/12.txt 
Problem: MISSING_IN_BASE_TABLE
RowKey: id:UUID:a69b6b66-7bbf-44dc-8eef-eb0884369740,int_col:INT:123
MainTableEntry: null
MVTableEntry: CassandraRow{id: a69b6b66-7bbf-44dc-8eef-eb0884369740, int_col: 123, ascii_col: example_ascii, writetime(ascii_col): 1733355636102616, ttl(ascii_col): null, bigint_col: 1234567890, writetime(bigint_col): 1733355636102616, ttl(bigint_col): null, blob_col: 0x48656c6c6f, writetime(blob_col): 1733355636102616, ttl(blob_col): null, boolean_col: true, writetime(boolean_col): 1733355636102616, ttl(boolean_col): null, date_col: 2022-12-01, writetime(date_col): 1733355636102616, ttl(date_col): null, decimal_col: 123.45, writetime(decimal_col): 1733355636102616, ttl(decimal_col): null, double_col: 123.456, writetime(double_col): 1733355636102616, ttl(double_col): null, float_col: 123.456, writetime(float_col): 1733355636102616, ttl(float_col): null, inet_col: 192.168.0.1, writetime(inet_col): 1733355636102616, ttl(inet_col): null, list_col: [value1,value2], map_col: {key1: 1,key2: 2}, set_col: {value1,value2}, smallint_col: 12345, writetime(smallint_col): 1733355636102616, ttl(smallint_col): null, text_col: example_text, writetime(text_col): 1733355636102616, ttl(text_col): null, time_col: 12:34:56, writetime(time_col): 1733355636102616, ttl(time_col): null, timestamp_col: 2024-02-28T12:34:56Z, writetime(timestamp_col): 1733355636102616, ttl(timestamp_col): null, timeuuid_col: 291afe30-b299-11ef-b544-6b207d5b3d1b, writetime(timeuuid_col): 1733355636102616, ttl(timeuuid_col): null, tinyint_col: 1, writetime(tinyint_col): 1733355636102616, ttl(tinyint_col): null, uuid_col: 42c6ded9-ed9a-479c-abca-cee1d3a646cc, writetime(uuid_col): 1733355636102616, ttl(uuid_col): null, varchar_col: example_varchar, writetime(varchar_col): 1733355636102616, ttl(varchar_col): null, varint_col: 12345, writetime(varint_col): 1733355636102616, ttl(varint_col): null}
```
