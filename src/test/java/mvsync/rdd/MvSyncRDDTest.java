package mvsync.rdd;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.spark.connector.japi.CassandraRow;
import mvsync.MVSyncSettings;
import mvsync.MvSync;
import mvsync.RecordPrimaryKey;
import mvsync.TableAndMVColumns;
import mvsync.MvSyncTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import mvsync.output.IBlobStreamer;
import mvsync.output.MVJobOutputStreamer;
import org.mockito.ArgumentCaptor;
import scala.Tuple2;
import scala.collection.JavaConverters;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.*;
import java.util.*;

import static mvsync.MvSyncTest.sdf;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class MvSyncRDDTest extends MvSync implements Serializable {

  public enum Schema {
    PK_STRING_AND_INTEGER,
    PK_DOUBLE_AND_TIMESTAMP,
    PK_BOOLEAN_AND_FLOAT,
    PK_UUID_AND_DECIMAL
  }

  public enum TestRDDType {
    SKIP_RECORD_IN_MV,
    SKIP_RECORD_IN_BASE_TABLE,
    MV_MISMATCH,
    MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE
  }

  public TestRDDType type;
  public Schema schema;

  public MvSyncRDDTest(TestRDDType type, Schema schema) {
    super();
    this.type = type;
    this.schema = schema;
  }

  @Override
  public TableAndMVColumns getBaseAndMvTableColumns(MVSyncSettings settings) throws Exception {
    super.getBaseAndMvTableColumns(settings);

    if (schema == Schema.PK_STRING_AND_INTEGER) {
      return new TableAndMVColumns(
          MvSyncTest.primaryKeyBaseTableStringAndInteger,
          MvSyncTest.nonPrimaryKeysBaseTable,
          MvSyncTest.primaryKeyMvTableStringAndInteger,
          MvSyncTest.nonPrimaryKeysMvTable);
    }
    if (schema == Schema.PK_DOUBLE_AND_TIMESTAMP) {
      return new TableAndMVColumns(
          MvSyncTest.primaryKeyBaseTableDoubleAndTimestamp,
          MvSyncTest.nonPrimaryKeysBaseTable,
          MvSyncTest.primaryKeyMvTableDoubleAndTimestamp,
          MvSyncTest.nonPrimaryKeysMvTable);
    }
    if (schema == Schema.PK_BOOLEAN_AND_FLOAT) {
      return new TableAndMVColumns(
          MvSyncTest.primaryKeyBaseTableBooleanAndFloat,
          MvSyncTest.nonPrimaryKeysBaseTable,
          MvSyncTest.primaryKeyMvTableBooleanAndFloat,
          MvSyncTest.nonPrimaryKeysMvTable);
    }
    if (schema == Schema.PK_UUID_AND_DECIMAL) {
      return new TableAndMVColumns(
          MvSyncTest.primaryKeyBaseTableUUIDAndDecimal,
          MvSyncTest.nonPrimaryKeysBaseTable,
          MvSyncTest.primaryKeyMvTableUUIDAndDecimal,
          MvSyncTest.nonPrimaryKeysMvTable);
    }
    throw new Exception("Unexpected schema type for getBaseAndMvTableColumns");
  }

  @Override
  public IBlobStreamer getStatsStreamer(String path, MVSyncSettings settings) throws Exception {
    return MvSyncTest.terraBlobStreamerStats;
  }

  @Override
  public MVJobOutputStreamer getOutputStreamers(MVSyncSettings settings) throws Exception {
    return MvSyncTest.MVJobOutputStreamer;
  }

  @Override
  public SparkConf getCassandraSparkConf(MVSyncSettings mvSyncSettings) {
    return getConf();
  }

  @Override
  public SparkConf getConf() {
    SparkConf conf = new SparkConf();
    conf.setAppName("MvSyncTest");
    conf.set("spark.master", "local");
    conf.set("spark.driver.allowMultipleContexts", "true");
    conf.set("spark.executor.cores", "1");
    conf.set("spark.cores.max", "1");
    conf.set("spark.cassandra.auth.password", "test");
    conf.set(MVSyncSettings.PREFIX + ".keyspace", MvSyncTest.KEYSPACE);
    conf.set(MVSyncSettings.PREFIX + ".basetablename", MvSyncTest.BASE_TABLE);
    conf.set(MVSyncSettings.PREFIX + ".mvname", MvSyncTest.MV);
    conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "0");
    conf.set(MVSyncSettings.PREFIX + ".endtsinsec", "1704067201"); // 2024-Jan-1 00:00:01;
    return conf;
  }

  @Override
  public JavaPairRDD<RecordPrimaryKey, CassandraRow> getRDD(
      String keyspace,
      String table,
      Map<String, String> mvTablePrimaryKeyColumns,
      Map<String, String> primaryKeyColumns,
      Map<String, String> nonPrimaryKeyColumns,
      JavaSparkContext jsc,
      MVSyncSettings mvSyncSettings) {
    // Build JavaPairRDD

    if (table.equals(MvSyncTest.BASE_TABLE)) {
      assertEquals(MvSyncTest.nonPrimaryKeysBaseTable, nonPrimaryKeyColumns);
      if (this.schema == Schema.PK_STRING_AND_INTEGER) {
        assertEquals(MvSyncTest.primaryKeyMvTableStringAndInteger, mvTablePrimaryKeyColumns);
        assertEquals(MvSyncTest.primaryKeyBaseTableStringAndInteger, primaryKeyColumns);
        return jsc.parallelizePairs(
            getInMemoryBaseTableCassandraForPKasStringAndInteger(mvTablePrimaryKeyColumns));
      }
      if (this.schema == Schema.PK_DOUBLE_AND_TIMESTAMP) {
        assertEquals(MvSyncTest.primaryKeyMvTableDoubleAndTimestamp, mvTablePrimaryKeyColumns);
        assertEquals(MvSyncTest.primaryKeyBaseTableDoubleAndTimestamp, primaryKeyColumns);
        return jsc.parallelizePairs(
            getInMemoryBaseTableCassandraForPKasDoubleAndTimestamp(mvTablePrimaryKeyColumns));
      }
      if (this.schema == Schema.PK_BOOLEAN_AND_FLOAT) {
        assertEquals(MvSyncTest.primaryKeyMvTableBooleanAndFloat, mvTablePrimaryKeyColumns);
        assertEquals(MvSyncTest.primaryKeyBaseTableBooleanAndFloat, primaryKeyColumns);

        return jsc.parallelizePairs(
            getInMemoryBaseTableCassandraForPKasBooleanAndFloat(mvTablePrimaryKeyColumns));
      }
      if (this.schema == Schema.PK_UUID_AND_DECIMAL) {
        assertEquals(MvSyncTest.primaryKeyMvTableUUIDAndDecimal, mvTablePrimaryKeyColumns);
        assertEquals(MvSyncTest.primaryKeyBaseTableUUIDAndDecimal, primaryKeyColumns);

        return jsc.parallelizePairs(
            getInMemoryBaseTableCassandraForPKasUUIDAndDecimal(mvTablePrimaryKeyColumns));
      }
    } else {
      if (type == TestRDDType.MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE) {
        assertEquals(MvSyncTest.nonPrimaryKeysMvTableWithFewerColumns, nonPrimaryKeyColumns);
      } else {
        assertEquals(MvSyncTest.nonPrimaryKeysMvTable, nonPrimaryKeyColumns);
      }
      if (this.schema == Schema.PK_STRING_AND_INTEGER) {
        assertEquals(MvSyncTest.primaryKeyMvTableStringAndInteger, primaryKeyColumns);
        return jsc.parallelizePairs(
            getInMemoryMVTableCassandraForPKasStringAndInteger(mvTablePrimaryKeyColumns));
      }
      if (this.schema == Schema.PK_DOUBLE_AND_TIMESTAMP) {
        assertEquals(MvSyncTest.primaryKeyMvTableDoubleAndTimestamp, primaryKeyColumns);
        return jsc.parallelizePairs(
            getInMemoryMVTableCassandraForDoubleAndTimestamp(mvTablePrimaryKeyColumns));
      }
      if (this.schema == Schema.PK_BOOLEAN_AND_FLOAT) {
        assertEquals(MvSyncTest.primaryKeyMvTableBooleanAndFloat, primaryKeyColumns);
        return jsc.parallelizePairs(
            getInMemoryMVTableCassandraForBooleanAndFloat(mvTablePrimaryKeyColumns));
      }
      if (this.schema == Schema.PK_UUID_AND_DECIMAL) {
        assertEquals(MvSyncTest.primaryKeyMvTableUUIDAndDecimal, primaryKeyColumns);
        return jsc.parallelizePairs(
            getInMemoryMVTableCassandraForUUIDAndDecimal(mvTablePrimaryKeyColumns));
      }
    }
    return null; // unexpected code path
  }

  private scala.collection.immutable.Map<String, Object> fillInBaseTableNonPrimaryKeyColumns1(
      scala.collection.immutable.Map<String, Object> row, long t) {
    row = row.$plus(new Tuple2<>("c1", 10));
    row = row.$plus(new Tuple2<>("writetime(c1)", t));
    row = row.$plus(new Tuple2<>("ttl(c1)", null));
    row = row.$plus(new Tuple2<>("c2", 20));
    row = row.$plus(new Tuple2<>("writetime(c2)", t));
    row = row.$plus(new Tuple2<>("ttl(c2)", null));
    row = row.$plus(new Tuple2<>("c3", 30));
    row = row.$plus(new Tuple2<>("writetime(c3)", t));
    row = row.$plus(new Tuple2<>("ttl(c3)", null));
    row = row.$plus(new Tuple2<>("c4", 40));
    row = row.$plus(new Tuple2<>("writetime(c4)", t));
    row = row.$plus(new Tuple2<>("ttl(c4)", null));
    return row;
  }

  private scala.collection.immutable.Map<String, Object> fillInBaseTableNonPrimaryKeyColumns2(
      scala.collection.immutable.Map<String, Object> row, long t) {
    row = row.$plus(new Tuple2<>("c1", 11));
    row = row.$plus(new Tuple2<>("writetime(c1)", t));
    row = row.$plus(new Tuple2<>("ttl(c1)", null));
    row = row.$plus(new Tuple2<>("c2", 22));
    row = row.$plus(new Tuple2<>("writetime(c2)", t));
    row = row.$plus(new Tuple2<>("ttl(c2)", null));
    row = row.$plus(new Tuple2<>("c3", 33));
    row = row.$plus(new Tuple2<>("writetime(c3)", t));
    row = row.$plus(new Tuple2<>("ttl(c3)", null));
    row = row.$plus(new Tuple2<>("c4", 44));
    row = row.$plus(new Tuple2<>("writetime(c4)", t));
    row = row.$plus(new Tuple2<>("ttl(c4)", null));
    return row;
  }

  private scala.collection.immutable.Map<String, Object> fillInMVTableNonPrimaryKeyColumns1(
      scala.collection.immutable.Map<String, Object> row, long t) {
    row = row.$plus(new Tuple2<>("c2", 20));
    row = row.$plus(new Tuple2<>("writetime(c2)", t));
    row = row.$plus(new Tuple2<>("ttl(c2)", null));
    row = row.$plus(new Tuple2<>("c3", 30));
    row = row.$plus(new Tuple2<>("writetime(c3)", t));
    row = row.$plus(new Tuple2<>("ttl(c3)", null));
    if (type != TestRDDType.MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE) {
      row = row.$plus(new Tuple2<>("c4", 40));
      row = row.$plus(new Tuple2<>("writetime(c4)", t));
      row = row.$plus(new Tuple2<>("ttl(c4)", null));
    }
    return row;
  }

  private scala.collection.immutable.Map<String, Object> fillInMVTableNonPrimaryKeyColumns2(
      scala.collection.immutable.Map<String, Object> row, long t) {
    row = row.$plus(new Tuple2<>("c2", 22));
    row = row.$plus(new Tuple2<>("writetime(c2)", t));
    row = row.$plus(new Tuple2<>("ttl(c2)", null));
    row = row.$plus(new Tuple2<>("c3", 33));
    row = row.$plus(new Tuple2<>("writetime(c3)", t));
    row = row.$plus(new Tuple2<>("ttl(c3)", null));
    if (type != TestRDDType.MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE) {
      if (type == TestRDDType.MV_MISMATCH) {
        row = row.$plus(new Tuple2<>("c4", 441));
      } else {
        row = row.$plus(new Tuple2<>("c4", 44));
      }
      row = row.$plus(new Tuple2<>("writetime(c4)", t));
      row = row.$plus(new Tuple2<>("ttl(c4)", null));
    }
    return row;
  }

  public List<Tuple2<RecordPrimaryKey, CassandraRow>>
      getInMemoryBaseTableCassandraForPKasStringAndInteger(Map<String, String> primaryKeyColumns) {
    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    long t2 = 1704153600000000L; // 2024-Jan-2 00:00:00

    scala.collection.immutable.Map<String, Object> row1 =
        new scala.collection.immutable.HashMap<>();
    row1 = row1.$plus(new Tuple2<>("pk", "Driver1"));
    row1 = row1.$plus(new Tuple2<>("ck1", "SF"));
    row1 = row1.$plus(new Tuple2<>("ck2", 2020));
    row1 = fillInBaseTableNonPrimaryKeyColumns1(row1, t1);

    scala.collection.immutable.Map<String, Object> row2 =
        new scala.collection.immutable.HashMap<>();
    row2 = row2.$plus(new Tuple2<>("pk", "Driver2"));
    row2 = row2.$plus(new Tuple2<>("ck1", "NY"));
    row2 = row2.$plus(new Tuple2<>("ck2", 2021));
    row2 = fillInBaseTableNonPrimaryKeyColumns2(row2, t2);

    CassandraRow cassandraRow1 = CassandraRow.fromMap(row1);
    CassandraRow cassandraRow2 = CassandraRow.fromMap(row2);

    if (type == TestRDDType.SKIP_RECORD_IN_BASE_TABLE
        || type == TestRDDType.MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE) {
      return Arrays.asList(
          new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1));
    }

    return Arrays.asList(
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1),
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow2, primaryKeyColumns), cassandraRow2));
  }

  public List<Tuple2<RecordPrimaryKey, CassandraRow>>
      getInMemoryMVTableCassandraForPKasStringAndInteger(Map<String, String> primaryKeyColumns) {
    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    long t2 = 1704153600000000L; // 2024-Jan-2 00:00:00
    scala.collection.immutable.Map<String, Object> row1 =
        new scala.collection.immutable.HashMap<>();
    row1 = row1.$plus(new Tuple2<>("c1", 10));
    row1 = row1.$plus(new Tuple2<>("pk", "Driver1"));
    row1 = row1.$plus(new Tuple2<>("ck1", "SF"));
    row1 = row1.$plus(new Tuple2<>("ck2", 2020));
    row1 = fillInMVTableNonPrimaryKeyColumns1(row1, t1);

    scala.collection.immutable.Map<String, Object> row2 =
        new scala.collection.immutable.HashMap<>();
    row2 = row2.$plus(new Tuple2<>("c1", 11));
    row2 = row2.$plus(new Tuple2<>("pk", "Driver2"));
    row2 = row2.$plus(new Tuple2<>("ck1", "NY"));
    row2 = row2.$plus(new Tuple2<>("ck2", 2021));
    row2 = fillInMVTableNonPrimaryKeyColumns2(row2, t2);

    CassandraRow cassandraRow1 = CassandraRow.fromMap(row1);
    CassandraRow cassandraRow2 = CassandraRow.fromMap(row2);

    if (type == TestRDDType.SKIP_RECORD_IN_MV) {
      return Arrays.asList(
          new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1));
    }

    return Arrays.asList(
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1),
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow2, primaryKeyColumns), cassandraRow2));
  }

  public List<Tuple2<RecordPrimaryKey, CassandraRow>>
      getInMemoryBaseTableCassandraForPKasDoubleAndTimestamp(
          Map<String, String> primaryKeyColumns) {
    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    long t2 = 1704153600000000L; // 2024-Jan-2 00:00:00
    scala.collection.immutable.Map<String, Object> row1 =
        new scala.collection.immutable.HashMap<>();
    row1 = row1.$plus(new Tuple2<>("pk", "Driver1"));
    try {
      row1 = row1.$plus(new Tuple2<>("ck1", sdf.parse("2024-02-18 04:34:56.000001+0000")));
    } catch (ParseException e) {
      e.printStackTrace();
    }
    row1 = row1.$plus(new Tuple2<>("ck2", 12.36d));
    row1 = row1.$plus(new Tuple2<>("c1", 10));
    row1 = row1.$plus(new Tuple2<>("writetime(c1)", t1));
    row1 = row1.$plus(new Tuple2<>("ttl(c1)", null));
    row1 = fillInBaseTableNonPrimaryKeyColumns1(row1, t1);

    scala.collection.immutable.Map<String, Object> row2 =
        new scala.collection.immutable.HashMap<>();
    row2 = row2.$plus(new Tuple2<>("pk", "Driver2"));
    try {
      row2 = row2.$plus(new Tuple2<>("ck1", sdf.parse("2024-02-18 04:34:56.000002+0000")));
    } catch (ParseException e) {
      e.printStackTrace();
    }

    row2 = row2.$plus(new Tuple2<>("ck2", 12.56d));
    row2 = row2.$plus(new Tuple2<>("c1", 11));
    row2 = row2.$plus(new Tuple2<>("writetime(c1)", t2));
    row2 = row2.$plus(new Tuple2<>("ttl(c1)", null));
    row2 = fillInBaseTableNonPrimaryKeyColumns2(row2, t2);

    CassandraRow cassandraRow1 = CassandraRow.fromMap(row1);
    String str = cassandraRow1.toString();
    CassandraRow cassandraRow2 = CassandraRow.fromMap(row2);

    if (type == TestRDDType.SKIP_RECORD_IN_BASE_TABLE
        || type == TestRDDType.MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE) {
      return Arrays.asList(
          new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1));
    }

    return Arrays.asList(
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1),
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow2, primaryKeyColumns), cassandraRow2));
  }

  public List<Tuple2<RecordPrimaryKey, CassandraRow>>
      getInMemoryMVTableCassandraForDoubleAndTimestamp(Map<String, String> primaryKeyColumns) {
    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    long t2 = 1704153600000000L; // 2024-Jan-2 00:00:00
    scala.collection.immutable.Map<String, Object> row1 =
        new scala.collection.immutable.HashMap<>();
    row1 = row1.$plus(new Tuple2<>("c1", 10));
    row1 = row1.$plus(new Tuple2<>("pk", "Driver1"));
    try {
      row1 = row1.$plus(new Tuple2<>("ck1", sdf.parse("2024-02-18 04:34:56.000001+0000")));
    } catch (ParseException e) {
      e.printStackTrace();
    }

    row1 = row1.$plus(new Tuple2<>("ck2", 12.36d));
    row1 = fillInMVTableNonPrimaryKeyColumns1(row1, t1);

    scala.collection.immutable.Map<String, Object> row2 =
        new scala.collection.immutable.HashMap<>();
    row2 = row2.$plus(new Tuple2<>("c1", 11));
    row2 = row2.$plus(new Tuple2<>("pk", "Driver2"));
    try {
      row2 = row2.$plus(new Tuple2<>("ck1", sdf.parse("2024-02-18 04:34:56.000002+0000")));
    } catch (ParseException e) {
      e.printStackTrace();
    }

    row2 = row2.$plus(new Tuple2<>("ck2", 12.56d));
    row2 = fillInMVTableNonPrimaryKeyColumns2(row2, t2);

    CassandraRow cassandraRow1 = CassandraRow.fromMap(row1);
    CassandraRow cassandraRow2 = CassandraRow.fromMap(row2);

    if (type == TestRDDType.SKIP_RECORD_IN_MV) {
      return Arrays.asList(
          new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1));
    }

    return Arrays.asList(
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1),
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow2, primaryKeyColumns), cassandraRow2));
  }

  public List<Tuple2<RecordPrimaryKey, CassandraRow>>
      getInMemoryBaseTableCassandraForPKasBooleanAndFloat(Map<String, String> primaryKeyColumns) {
    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    long t2 = 1704153600000000L; // 2024-Jan-2 00:00:00
    scala.collection.immutable.Map<String, Object> row1 =
        new scala.collection.immutable.HashMap<>();
    row1 = row1.$plus(new Tuple2<>("pk", "Driver1"));
    row1 = row1.$plus(new Tuple2<>("ck1", true));
    row1 = row1.$plus(new Tuple2<>("ck2", 1.08f));
    row1 = fillInBaseTableNonPrimaryKeyColumns1(row1, t1);

    scala.collection.immutable.Map<String, Object> row2 =
        new scala.collection.immutable.HashMap<>();
    row2 = row2.$plus(new Tuple2<>("pk", "Driver2"));
    row2 = row2.$plus(new Tuple2<>("ck1", false));
    row2 = row2.$plus(new Tuple2<>("ck2", 1.09f));
    row2 = fillInBaseTableNonPrimaryKeyColumns2(row2, t2);

    CassandraRow cassandraRow1 = CassandraRow.fromMap(row1);
    CassandraRow cassandraRow2 = CassandraRow.fromMap(row2);

    if (type == TestRDDType.SKIP_RECORD_IN_BASE_TABLE
        || type == TestRDDType.MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE) {
      return Arrays.asList(
          new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1));
    }

    return Arrays.asList(
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1),
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow2, primaryKeyColumns), cassandraRow2));
  }

  public List<Tuple2<RecordPrimaryKey, CassandraRow>> getInMemoryMVTableCassandraForBooleanAndFloat(
      Map<String, String> primaryKeyColumns) {
    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    long t2 = 1704153600000000L; // 2024-Jan-2 00:00:00
    scala.collection.immutable.Map<String, Object> row1 =
        new scala.collection.immutable.HashMap<>();
    row1 = row1.$plus(new Tuple2<>("c1", 10));
    row1 = row1.$plus(new Tuple2<>("pk", "Driver1"));
    row1 = row1.$plus(new Tuple2<>("ck1", true));
    row1 = row1.$plus(new Tuple2<>("ck2", 1.08f));
    row1 = fillInMVTableNonPrimaryKeyColumns1(row1, t1);

    scala.collection.immutable.Map<String, Object> row2 =
        new scala.collection.immutable.HashMap<>();
    row2 = row2.$plus(new Tuple2<>("c1", 11));
    row2 = row2.$plus(new Tuple2<>("pk", "Driver2"));
    row2 = row2.$plus(new Tuple2<>("ck1", false));

    row2 = row2.$plus(new Tuple2<>("ck2", 1.09f));
    row2 = fillInMVTableNonPrimaryKeyColumns2(row2, t2);

    CassandraRow cassandraRow1 = CassandraRow.fromMap(row1);
    CassandraRow cassandraRow2 = CassandraRow.fromMap(row2);

    if (type == TestRDDType.SKIP_RECORD_IN_MV) {
      return Arrays.asList(
          new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1));
    }

    return Arrays.asList(
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1),
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow2, primaryKeyColumns), cassandraRow2));
  }

  public List<Tuple2<RecordPrimaryKey, CassandraRow>>
      getInMemoryBaseTableCassandraForPKasUUIDAndDecimal(Map<String, String> primaryKeyColumns) {
    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    long t2 = 1704153600000000L; // 2024-Jan-2 00:00:00
    scala.collection.immutable.Map<String, Object> row1 =
        new scala.collection.immutable.HashMap<>();
    row1 = row1.$plus(new Tuple2<>("pk", "Driver1"));
    row1 = row1.$plus(new Tuple2<>("ck1", UUID.fromString("7309362c-4237-4e5d-b403-505820caba41")));
    row1 = row1.$plus(new Tuple2<>("ck2", new BigDecimal("100.05")));
    row1 = fillInBaseTableNonPrimaryKeyColumns1(row1, t1);

    scala.collection.immutable.Map<String, Object> row2 =
        new scala.collection.immutable.HashMap<>();
    row2 = row2.$plus(new Tuple2<>("pk", "Driver2"));
    row2 = row2.$plus(new Tuple2<>("ck1", UUID.fromString("7309362c-4237-4e5d-b403-505820caba42")));
    row2 = row2.$plus(new Tuple2<>("ck2", new BigDecimal("100.06")));
    row2 = fillInBaseTableNonPrimaryKeyColumns2(row2, t2);

    CassandraRow cassandraRow1 = CassandraRow.fromMap(row1);
    CassandraRow cassandraRow2 = CassandraRow.fromMap(row2);

    if (type == TestRDDType.SKIP_RECORD_IN_BASE_TABLE
        || type == TestRDDType.MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE) {
      return Arrays.asList(
          new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1));
    }

    return Arrays.asList(
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1),
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow2, primaryKeyColumns), cassandraRow2));
  }

  public List<Tuple2<RecordPrimaryKey, CassandraRow>> getInMemoryMVTableCassandraForUUIDAndDecimal(
      Map<String, String> primaryKeyColumns) {
    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    long t2 = 1704153600000000L; // 2024-Jan-2 00:00:00
    scala.collection.immutable.Map<String, Object> row1 =
        new scala.collection.immutable.HashMap<>();
    row1 = row1.$plus(new Tuple2<>("c1", 10));
    row1 = row1.$plus(new Tuple2<>("pk", "Driver1"));
    row1 = row1.$plus(new Tuple2<>("ck1", UUID.fromString("7309362c-4237-4e5d-b403-505820caba41")));
    row1 = row1.$plus(new Tuple2<>("ck2", new BigDecimal("100.05")));
    row1 = fillInMVTableNonPrimaryKeyColumns1(row1, t1);

    scala.collection.immutable.Map<String, Object> row2 =
        new scala.collection.immutable.HashMap<>();
    row2 = row2.$plus(new Tuple2<>("c1", 11));
    row2 = row2.$plus(new Tuple2<>("pk", "Driver2"));
    row2 = row2.$plus(new Tuple2<>("ck1", UUID.fromString("7309362c-4237-4e5d-b403-505820caba42")));

    row2 = row2.$plus(new Tuple2<>("ck2", new BigDecimal("100.06")));
    row2 = fillInMVTableNonPrimaryKeyColumns2(row2, t2);

    CassandraRow cassandraRow1 = CassandraRow.fromMap(row1);
    CassandraRow cassandraRow2 = CassandraRow.fromMap(row2);

    if (type == TestRDDType.SKIP_RECORD_IN_MV) {
      return Arrays.asList(
          new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1));
    }

    return Arrays.asList(
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow1, primaryKeyColumns), cassandraRow1),
        new Tuple2<>(MvSync.buildPrimaryKey(cassandraRow2, primaryKeyColumns), cassandraRow2));
  }

  public static Tuple2<TestSchema, CassandraRow> getNativeDataTypes(int c1Value) {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("c1", "INT");
    nonPrimaryKeys.put("c2", "INT");
    nonPrimaryKeys.put("c3", "INT");

    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));
    row = row.$plus(new Tuple2<>("c1", c1Value));
    row = row.$plus(new Tuple2<>("writetime(c1)", t1));
    row = row.$plus(new Tuple2<>("ttl(c1)", null));
    row = row.$plus(new Tuple2<>("c2", 30));
    row = row.$plus(new Tuple2<>("writetime(c2)", t1));
    row = row.$plus(new Tuple2<>("ttl(c2)", null));
    row = row.$plus(new Tuple2<>("c3", 40));
    row = row.$plus(new Tuple2<>("writetime(c3)", t1));
    row = row.$plus(new Tuple2<>("ttl(c3)", 7200));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getListDataType() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("c1", "INT");
    nonPrimaryKeys.put("c2", "INT");
    nonPrimaryKeys.put("c3", "LIST");

    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));
    row = row.$plus(new Tuple2<>("c1", 20));
    row = row.$plus(new Tuple2<>("writetime(c1)", t1));
    row = row.$plus(new Tuple2<>("ttl(c1)", null));
    row = row.$plus(new Tuple2<>("c2", 30));
    row = row.$plus(new Tuple2<>("writetime(c2)", t1));
    row = row.$plus(new Tuple2<>("ttl(c2)", null));
    row =
        row.$plus(
            new Tuple2<>(
                "c3",
                JavaConverters.asScalaBuffer(Arrays.asList(10, 20, 30)).toList())); // This is List
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getSetDataType() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("c1", "INT");
    nonPrimaryKeys.put("c2", "INT");
    nonPrimaryKeys.put("c3", "SET");

    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));
    row = row.$plus(new Tuple2<>("c1", 20));
    row = row.$plus(new Tuple2<>("writetime(c1)", t1));
    row = row.$plus(new Tuple2<>("ttl(c1)", null));
    row = row.$plus(new Tuple2<>("c2", 30));
    row = row.$plus(new Tuple2<>("writetime(c2)", t1));
    row = row.$plus(new Tuple2<>("ttl(c2)", null));
    row =
        row.$plus(
            new Tuple2<>(
                "c3",
                JavaConverters.asScalaBuffer(Arrays.asList(11, 22, 33)).toSet())); // This is Set
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getMapDataType() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("c1", "INT");
    nonPrimaryKeys.put("c2", "INT");
    nonPrimaryKeys.put("c3", "MAP");

    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));
    row = row.$plus(new Tuple2<>("c1", 20));
    row = row.$plus(new Tuple2<>("writetime(c1)", t1));
    row = row.$plus(new Tuple2<>("ttl(c1)", null));
    row = row.$plus(new Tuple2<>("c2", 30));
    row = row.$plus(new Tuple2<>("writetime(c2)", t1));
    row = row.$plus(new Tuple2<>("ttl(c2)", null));
    Map<Integer, Integer> map =
        new TreeMap<>() {
          {
            put(1, 10);
            put(2, 20);
            put(3, 30);
          }
        };
    row = row.$plus(new Tuple2<>("c3", JavaConverters.mapAsScalaMap(map))); // This is Map
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getAllDataTypes()
      throws UnknownHostException, ParseException {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("uuid_col", "UUID");
    nonPrimaryKeys.put("int_col", "INT");
    nonPrimaryKeys.put("inet_col", "INET");
    nonPrimaryKeys.put("list_col", "LIST");
    nonPrimaryKeys.put("timeuuid_col", "TIMEUUID");
    nonPrimaryKeys.put("varint_col", "VARINT");
    nonPrimaryKeys.put("bigint_col", "BIGINT");
    nonPrimaryKeys.put("boolean_col", "BOOLEAN");
    nonPrimaryKeys.put("ascii_col", "ASCII");
    nonPrimaryKeys.put("date_col", "DATE");
    nonPrimaryKeys.put("blob_col", "BLOB");
    nonPrimaryKeys.put("float_col", "FLOAT");
    nonPrimaryKeys.put("varchar_col", "VARCHAR");
    nonPrimaryKeys.put("set_col", "SET");
    nonPrimaryKeys.put("smallint_col", "SMALLINT");
    nonPrimaryKeys.put("timestamp_col", "TIMESTAMP");
    nonPrimaryKeys.put("map_col", "MAP");
    nonPrimaryKeys.put("tinyint_col", "TINYINT");
    nonPrimaryKeys.put("text_col", "TEXT");
    nonPrimaryKeys.put("time_col", "TIME");
    nonPrimaryKeys.put("double_col", "DOUBLE");
    nonPrimaryKeys.put("decimal_col", "DECIMAL");

    long t1 = 1704067200000000L; // 2024-Jan-1 00:00:00
    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "uuid_col", UUID.fromString("550e8400-e29b-11d4-a716-446655440000")));
    row = row.$plus(new Tuple2<>("writetime(uuid_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(uuid_col)", null));

    row = row.$plus(new Tuple2<>("int_col", 10));
    row = row.$plus(new Tuple2<>("writetime(int_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(int_col)", null));

    row = row.$plus(new Tuple2<>("inet_col", InetAddress.getByName("192.168.0.1")));
    row = row.$plus(new Tuple2<>("writetime(inet_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(inet_col)", null));

    row =
        row.$plus(
            new Tuple2<>(
                "timeuuid_col", UUID.fromString("550e8400-e29b-11d4-a716-446655440000")));
    row = row.$plus(new Tuple2<>("writetime(timeuuid_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(timeuuid_col)", null));

    row = row.$plus(new Tuple2<>("varint_col", new BigInteger("20")));
    row = row.$plus(new Tuple2<>("writetime(varint_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(varint_col)", null));

    row = row.$plus(new Tuple2<>("bigint_col", new Long(5000)));
    row = row.$plus(new Tuple2<>("writetime(bigint_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(bigint_col)", null));

    row = row.$plus(new Tuple2<>("boolean_col", true));
    row = row.$plus(new Tuple2<>("writetime(boolean_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(boolean_col)", null));

    row = row.$plus(new Tuple2<>("ascii_col", "hello world!"));
    row = row.$plus(new Tuple2<>("writetime(ascii_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(ascii_col)", null));

    row = row.$plus(new Tuple2<>("date_col", "2022-12-01"));
    row = row.$plus(new Tuple2<>("writetime(date_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(date_col)", null));

    row =
        row.$plus(new Tuple2<>("blob_col", ByteBuffer.wrap("Hello, Cassandra!".getBytes())));
    row = row.$plus(new Tuple2<>("writetime(blob_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(blob_col)", null));

    row = row.$plus(new Tuple2<>("float_col", 10.50f));
    row = row.$plus(new Tuple2<>("writetime(float_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(float_col)", null));

    row = row.$plus(new Tuple2<>("varchar_col", "hello123"));
    row = row.$plus(new Tuple2<>("writetime(varchar_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(varchar_col)", 3600));

    row = row.$plus(new Tuple2<>("smallint_col", new Short("3")));
    row = row.$plus(new Tuple2<>("writetime(smallint_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(smallint_col)", null));

    row =
        row.$plus(
            new Tuple2<>("timestamp_col", sdf.parse("2024-02-18 04:34:56.000000+0000")));
    row = row.$plus(new Tuple2<>("writetime(timestamp_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(timestamp_col)", null));

    row = row.$plus(new Tuple2<>("tinyint_col", new Byte("1")));
    row = row.$plus(new Tuple2<>("writetime(tinyint_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(tinyint_col)", null));

    row = row.$plus(new Tuple2<>("text_col", "This is text!"));
    row = row.$plus(new Tuple2<>("writetime(text_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(text_col)", null));

    long nanosSinceMidnight = 45296000000000L; // value representing nanoseconds since midnight
    row = row.$plus(new Tuple2<>("time_col", nanosSinceMidnight));
    row = row.$plus(new Tuple2<>("writetime(time_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(time_col)", null));

    row = row.$plus(new Tuple2<>("double_col", 12.36d));
    row = row.$plus(new Tuple2<>("writetime(double_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(double_col)", null));

    row = row.$plus(new Tuple2<>("decimal_col", new BigDecimal("100.05")));
    row = row.$plus(new Tuple2<>("writetime(decimal_col)", t1++));
    row = row.$plus(new Tuple2<>("ttl(decimal_col)", 7200));

    row =
        row.$plus(
            new Tuple2<>(
                "list_col", JavaConverters.asScalaBuffer(Arrays.asList(100, 200, 300)).toList()));
    row =
        row.$plus(
            new Tuple2<>(
                "set_col", JavaConverters.asScalaBuffer(Arrays.asList(111, 222, 333)).toSet()));
    Map<Integer, Integer> map =
        new TreeMap<>() {
          {
            put(1, 10);
            put(2, 20);
            put(3, 30);
          }
        };
    row =
        row.$plus(new Tuple2<>("map_col", JavaConverters.mapAsScalaMap(map))); // This is Map

    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getRowWriteTimeMissing() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("int_col", "INT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("int_col", 10));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getUUIDDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("uuid_col", "UUID");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "uuid_col", UUID.fromString("550e8400-e29b-11d4-a716-446655440000")));
    row = row.$plus(new Tuple2<>("writetime(uuid_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(uuid_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getUUIDDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("uuid_col", "UUID");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "uuid_col", UUID.fromString("550e8400-e29b-11d4-a716-446655440001")));
    row = row.$plus(new Tuple2<>("writetime(uuid_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(uuid_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getIntDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("int_col", "INT");
    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("int_col", new Integer(10)));
    row = row.$plus(new Tuple2<>("writetime(int_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(int_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getIntDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("int_col", "INT");
    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("int_col", new Integer(11)));
    row = row.$plus(new Tuple2<>("writetime(int_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(int_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getInetDataTypes1() throws UnknownHostException {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("inet_col", "INET");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("inet_col", InetAddress.getByName("192.168.0.1")));
    row = row.$plus(new Tuple2<>("writetime(inet_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(inet_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getInetDataTypes2() throws UnknownHostException {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("inet_col", "INET");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("inet_col", InetAddress.getByName("192.168.0.2")));
    row = row.$plus(new Tuple2<>("writetime(inet_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(inet_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getListDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("list_col", "LIST");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "list_col", JavaConverters.asScalaBuffer(Arrays.asList(100, 200, 300)).toList()));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getListDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("list_col", "LIST");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "list_col", JavaConverters.asScalaBuffer(Arrays.asList(101, 201, 301)).toList()));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTimeUUIDDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("timeuuid_col", "TIMEUUID");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "timeuuid_col", UUID.fromString("550e8400-e29b-11d4-a716-446655440000")));
    row = row.$plus(new Tuple2<>("writetime(timeuuid_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(timeuuid_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTimeUUIDDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("timeuuid_col", "TIMEUUID");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "timeuuid_col", UUID.fromString("550e8400-e29b-11d4-a716-446655440001")));
    row = row.$plus(new Tuple2<>("writetime(timeuuid_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(timeuuid_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getVarIntDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("varint_col", "VARINT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("varint_col", new BigInteger("20")));
    row = row.$plus(new Tuple2<>("writetime(varint_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(varint_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getVarIntDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("varint_col", "VARINT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("varint_col", new BigInteger("21")));
    row = row.$plus(new Tuple2<>("writetime(varint_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(varint_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getBigIntDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("bigint_col", "BIGINT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("bigint_col", new Long(5000)));
    row = row.$plus(new Tuple2<>("writetime(bigint_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(bigint_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getBigIntDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("bigint_col", "BIGINT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("bigint_col", new Long(5001)));
    row = row.$plus(new Tuple2<>("writetime(bigint_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(bigint_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getBooleanDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("boolean_col", "BOOLEAN");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("boolean_col", new Boolean(true)));
    row = row.$plus(new Tuple2<>("writetime(boolean_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(boolean_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getBooleanDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("boolean_col", "BOOLEAN");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("boolean_col", new Boolean(false)));
    row = row.$plus(new Tuple2<>("writetime(boolean_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(boolean_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getAsciiDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("ascii_col", "ASCII");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("ascii_col", new String("hello world!")));
    row = row.$plus(new Tuple2<>("writetime(ascii_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(ascii_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getAsciiDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("ascii_col", "ASCII");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("ascii_col", new String("hi world!")));
    row = row.$plus(new Tuple2<>("writetime(ascii_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(ascii_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getDateDataTypes1() throws ParseException {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("date_col", "DATE");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("date_col", MvSyncTest.sdfYYYYMMDD.parse("2022-12-01")));
    row = row.$plus(new Tuple2<>("writetime(date_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(date_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getDateDataTypes2() throws ParseException {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("date_col", "DATE");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("date_col", MvSyncTest.sdfYYYYMMDD.parse("2022-12-02")));
    row = row.$plus(new Tuple2<>("writetime(date_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(date_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getBlobDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("blob_col", "BLOB");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(new Tuple2<>("blob_col", ByteBuffer.wrap("Hello, Cassandra!".getBytes())));
    row = row.$plus(new Tuple2<>("writetime(blob_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(blob_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getBlobDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("blob_col", "BLOB");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("blob_col", ByteBuffer.wrap("Hi, Cassandra!".getBytes())));
    row = row.$plus(new Tuple2<>("writetime(blob_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(blob_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getFloatDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("float_col", "FLOAT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("float_col", new Float(10.50)));
    row = row.$plus(new Tuple2<>("writetime(float_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(float_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getFloatDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("float_col", "FLOAT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("float_col", new Float(10.51f)));
    row = row.$plus(new Tuple2<>("writetime(float_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(float_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getVarcharDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("varchar_col", "TEXT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("varchar_col", new String("hello123")));
    row = row.$plus(new Tuple2<>("writetime(varchar_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(varchar_col)", 3600));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getVarcharDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("varchar_col", "TEXT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("varchar_col", new String("hello456")));
    row = row.$plus(new Tuple2<>("writetime(varchar_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(varchar_col)", 3600));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getSetDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("set_col", "SET");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "set_col", JavaConverters.asScalaBuffer(Arrays.asList(111, 222, 333)).toSet()));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getSetDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("set_col", "SET");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "set_col", JavaConverters.asScalaBuffer(Arrays.asList(1110, 2220, 3330)).toSet()));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getSmallIntDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("smallint_col", "SMALLINT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("smallint_col", new Short("3")));
    row = row.$plus(new Tuple2<>("writetime(smallint_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(smallint_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getSmallIntDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("smallint_col", "SMALLINT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("smallint_col", new Short("4")));
    row = row.$plus(new Tuple2<>("writetime(smallint_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(smallint_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTimestampDataTypes1() throws ParseException {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("timestamp_col", "TIMESTAMP");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>("timestamp_col", sdf.parse("2024-02-18 04:34:56.101005+0000")));
    row = row.$plus(new Tuple2<>("writetime(timestamp_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(timestamp_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTimestampDataTypes2() throws ParseException {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("timestamp_col", "TIMESTAMP");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>("timestamp_col", sdf.parse("2024-02-18 04:34:56.101006+0000")));
    row = row.$plus(new Tuple2<>("writetime(timestamp_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(timestamp_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getMapDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("map_col", "MAP");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    Map<Integer, Integer> map =
        new TreeMap<>() {
          {
            put(1, 10);
            put(2, 20);
            put(3, 30);
          }
        };
    row =
        row.$plus(new Tuple2<>("map_col", JavaConverters.mapAsScalaMap(map))); // This is Map

    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getMapDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("map_col", "MAP");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    Map<Integer, Integer> map =
        new TreeMap<>() {
          {
            put(1, 11);
            put(2, 22);
            put(3, 33);
          }
        };
    row =
        row.$plus(new Tuple2<>("map_col", JavaConverters.mapAsScalaMap(map))); // This is Map

    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTinyIntDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("tinyint_col", "TINYINT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("tinyint_col", new Byte("1")));
    row = row.$plus(new Tuple2<>("writetime(tinyint_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(tinyint_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTinyIntDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("tinyint_col", "TINYINT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("tinyint_col", new Byte("2")));
    row = row.$plus(new Tuple2<>("writetime(tinyint_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(tinyint_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTextDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("text_col", "TEXT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("text_col", new String("This is text!")));
    row = row.$plus(new Tuple2<>("writetime(text_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(text_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTextDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("text_col", "TEXT");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("text_col", new String("This is a test case!")));
    row = row.$plus(new Tuple2<>("writetime(text_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(text_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTimeDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("time_col", "TIME");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "time_col", MvSyncTest.formatter.parseDateTime("12:34:56.000000000")));
    row = row.$plus(new Tuple2<>("writetime(time_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(time_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getTimeDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("time_col", "TIME");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row =
        row.$plus(
            new Tuple2<>(
                "time_col", MvSyncTest.formatter.parseDateTime("12:34:56.100000001")));
    row = row.$plus(new Tuple2<>("writetime(time_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(time_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getDoubleDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("double_col", "DOUBLE");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("double_col", new Double(12.36)));
    row = row.$plus(new Tuple2<>("writetime(double_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(double_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getDoubleDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("double_col", "DOUBLE");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("double_col", new Double(12.37)));
    row = row.$plus(new Tuple2<>("writetime(double_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(double_col)", null));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getDecimalDataTypes1() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("decimal_col", "DECIMAL");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("decimal_col", new BigDecimal("100.05")));
    row = row.$plus(new Tuple2<>("writetime(decimal_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(decimal_col)", 7200));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static Tuple2<TestSchema, CassandraRow> getDecimalDataTypes2() {
    Map<String, String> primaryKeys = new TreeMap<>();
    primaryKeys.put("pk", "TEXT");
    primaryKeys.put("ck1", "TEXT");
    primaryKeys.put("ck2", "INT");

    Map<String, String> nonPrimaryKeys = new TreeMap<>();
    nonPrimaryKeys.put("decimal_col", "DECIMAL");

    scala.collection.immutable.Map<String, Object> row = new scala.collection.immutable.HashMap<>();
    row = row.$plus(new Tuple2<>("pk", "Driver1"));
    row = row.$plus(new Tuple2<>("ck1", "SF"));
    row = row.$plus(new Tuple2<>("ck2", 2020));

    row = row.$plus(new Tuple2<>("decimal_col", new BigDecimal("100.06")));
    row = row.$plus(new Tuple2<>("writetime(decimal_col)", 1704067200000000L));
    row = row.$plus(new Tuple2<>("ttl(decimal_col)", 7200));
    return Tuple2.apply(new TestSchema(primaryKeys, nonPrimaryKeys), CassandraRow.fromMap(row));
  }

  public static BoundStatementBuilder helperBoundStatementAsciiCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,ascii_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyAsciiCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(3))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("ascii_col", columnNameAC1.getAllValues().get(2));
    assertEquals("hello world!", columnValAC1.getAllValues().get(2));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(2));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000007L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementBigintCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,bigint_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyBigintCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1LongType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> columnValAC1LongType = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1LongType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);
    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1LongType.capture(), columnValAC1LongType.capture(), columnDataTypeAC1LongType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());
    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("bigint_col", columnNameAC1LongType.getAllValues().get(0));
    assertEquals(new Long(5000), columnValAC1LongType.getAllValues().get(0));
    assertEquals(TypeCodecs.BIGINT, columnDataTypeAC1LongType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000005L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementBlobCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,blob_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyBlobCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1BlobType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<ByteBuffer> columnValAC1BlobType = ArgumentCaptor.forClass(ByteBuffer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1BlobType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1BlobType.capture(), columnValAC1BlobType.capture(), columnDataTypeAC1BlobType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("blob_col", columnNameAC1BlobType.getAllValues().get(0));
    assertEquals(
        ByteBuffer.wrap("Hello, Cassandra!".getBytes()), columnValAC1BlobType.getAllValues().get(0));
    assertEquals(TypeCodecs.BLOB, columnDataTypeAC1BlobType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000009L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementBooleanCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,boolean_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyBooleanCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1BooleanType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Boolean> columnValAC1BooleanType = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1BooleanType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1BooleanType.capture(), columnValAC1BooleanType.capture(), columnDataTypeAC1BooleanType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());

    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());
    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("boolean_col", columnNameAC1BooleanType.getAllValues().get(0));
    assertEquals(true, columnValAC1BooleanType.getAllValues().get(0));
    assertEquals(TypeCodecs.BOOLEAN, columnDataTypeAC1BooleanType.getAllValues().get(0));
    assertEquals(1704067200000006L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementDateCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,date_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyDateCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1DateType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<LocalDate> columnValAC1DateType = ArgumentCaptor.forClass(LocalDate.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1DateType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1DateType.capture(), columnValAC1DateType.capture(), columnDataTypeAC1DateType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("date_col", columnNameAC1DateType.getAllValues().get(0));
    assertEquals(LocalDate.of(2022, 12, 1), columnValAC1DateType.getAllValues().get(0));
    assertEquals(TypeCodecs.DATE, columnDataTypeAC1DateType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000008L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementDecimalCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,decimal_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyDecimalCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1DecimalType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<BigDecimal> columnValAC1DecimalType = ArgumentCaptor.forClass(BigDecimal.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1DecimalType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> columnIdxAC3 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> columnValLongAC3 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC4 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1DecimalType.capture(), columnValAC1DecimalType.capture(), columnDataTypeAC1DecimalType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setInt(columnIdxAC3.capture(), columnValLongAC3.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC4.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("decimal_col", columnNameAC1DecimalType.getAllValues().get(0));
    assertEquals(new BigDecimal("100.05"), columnValAC1DecimalType.getAllValues().get(0));
    assertEquals(TypeCodecs.DECIMAL, columnDataTypeAC1DecimalType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000018L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(5, columnIdxAC3.getAllValues().get(0).intValue());
    assertEquals(7200, columnValLongAC3.getAllValues().get(0).intValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC4.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementDoubleCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,double_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyDoubleCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1DoubleType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Double> columnValAC1DoubleType = ArgumentCaptor.forClass(Double.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1DoubleType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1DoubleType.capture(), columnValAC1DoubleType.capture(), columnDataTypeAC1DoubleType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("double_col", columnNameAC1DoubleType.getAllValues().get(0));
    assertEquals(Double.valueOf(12.36d), columnValAC1DoubleType.getAllValues().get(0));
    assertEquals(TypeCodecs.DOUBLE, columnDataTypeAC1DoubleType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000017L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementFloatCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,float_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyFloatCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1FloatType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Float> columnValAC1FloatType = ArgumentCaptor.forClass(Float.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1FloatType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1FloatType.capture(), columnValAC1FloatType.capture(), columnDataTypeAC1FloatType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("float_col", columnNameAC1FloatType.getAllValues().get(0));
    assertEquals(Float.valueOf(10.5f), columnValAC1FloatType.getAllValues().get(0));
    assertEquals(TypeCodecs.FLOAT, columnDataTypeAC1FloatType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000010L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementInetCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,inet_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyInetCol(BoundStatementBuilder boundStatement)
      throws UnknownHostException {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1InetType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<InetAddress> columnValAC1InetType = ArgumentCaptor.forClass(InetAddress.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1InetType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1InetType.capture(), columnValAC1InetType.capture(), columnDataTypeAC1InetType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("inet_col", columnNameAC1InetType.getAllValues().get(0));
    assertEquals(InetAddress.getByName("192.168.0.1"), columnValAC1InetType.getAllValues().get(0));
    assertEquals(TypeCodecs.INET, columnDataTypeAC1InetType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000002L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementIntCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,int_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyIntCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(2))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("int_col", columnNameAC1IntType.getAllValues().get(1));
    assertEquals(Integer.valueOf(10), columnValAC1IntType.getAllValues().get(1));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(1));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000001L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementListCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,list_col) VALUES (?,?,?,?)"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyListCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);
    ArgumentCaptor<String> columnNameAC4 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<List> columnValByteBufferAC4 = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<Class<Integer>> columnListElementTypeAC4 = ArgumentCaptor.forClass((Class<Class<Integer>>) (Class<?>) Class.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(0))
        .setLong(columnIdxAC2.capture(), columnValLongAC2.capture()); // no timestamp supplied
    verify(boundStatement, times(1))
        .setList(columnNameAC4.capture(), columnValByteBufferAC4.capture(), columnListElementTypeAC4.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("list_col", columnNameAC4.getAllValues().get(0));
    assertEquals(Arrays.asList(100, 200, 300), columnValByteBufferAC4.getAllValues().get(0));
    assertEquals(Integer.class, columnListElementTypeAC4.getAllValues().get(0));
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementMapCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,map_col) VALUES (?,?,?,?)"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyMapCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);
    ArgumentCaptor<String> columnNameAC4 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map> columnValByteBufferAC4 = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Class<Integer>> columnMapElementKeyTypeAC4 = ArgumentCaptor.forClass((Class<Class<Integer>>) (Class<?>) Class.class);
    ArgumentCaptor<Class<Integer>> columnMapElementValTypeAC4 = ArgumentCaptor.forClass((Class<Class<Integer>>) (Class<?>) Class.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(0))
        .setLong(columnIdxAC2.capture(), columnValLongAC2.capture()); // no timestamp supplied
    verify(boundStatement, times(1))
        .setMap(columnNameAC4.capture(), columnValByteBufferAC4.capture(), columnMapElementKeyTypeAC4.capture(), columnMapElementValTypeAC4.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("map_col", columnNameAC4.getAllValues().get(0));
    assertEquals(
        new TreeMap<>() {
          {
            put(1, 10);
            put(2, 20);
            put(3, 30);
          }
        },
        columnValByteBufferAC4.getAllValues().get(0));
    assertEquals(Integer.class, columnMapElementKeyTypeAC4.getAllValues().get(0));
    assertEquals(Integer.class, columnMapElementValTypeAC4.getAllValues().get(0));
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementSetCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,set_col) VALUES (?,?,?,?)"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifySetCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);
    ArgumentCaptor<String> columnNameAC4 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Set> columnValByteBufferAC4 = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<Class<Integer>> columnSetElementTypeAC4 = ArgumentCaptor.forClass((Class<Class<Integer>>) (Class<?>) Class.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(0))
        .setLong(columnIdxAC2.capture(), columnValLongAC2.capture()); // no timestamp supplied
    verify(boundStatement, times(1))
        .setSet(columnNameAC4.capture(), columnValByteBufferAC4.capture(), columnSetElementTypeAC4.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("set_col", columnNameAC4.getAllValues().get(0));
    assertEquals(
        new TreeSet(Arrays.asList(333, 222, 111)), columnValByteBufferAC4.getAllValues().get(0));
    assertEquals(Integer.class, columnSetElementTypeAC4.getAllValues().get(0));
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementSmallintCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,smallint_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifySmallintCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1ShortType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Short> columnValAC1ShortType = ArgumentCaptor.forClass(Short.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1ShortType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1ShortType.capture(), columnValAC1ShortType.capture(), columnDataTypeAC1ShortType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("smallint_col", columnNameAC1ShortType.getAllValues().get(0));
    assertEquals(new Short("3"), columnValAC1ShortType.getAllValues().get(0));
    assertEquals(TypeCodecs.SMALLINT, columnDataTypeAC1ShortType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000012L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementTextCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,text_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyTextCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(3))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("text_col", columnNameAC1.getAllValues().get(2));
    assertEquals("This is text!", columnValAC1.getAllValues().get(2));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(2));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000015L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementTimeCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,time_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyTimeCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1LongType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> columnValAC1LongType = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1LongType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1LongType.capture(), columnValAC1LongType.capture(), columnDataTypeAC1LongType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("time_col", columnNameAC1LongType.getAllValues().get(0));
    assertEquals(Long.valueOf(45296000000000L), columnValAC1LongType.getAllValues().get(0));
    assertEquals(TypeCodecs.BIGINT, columnDataTypeAC1LongType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000016L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementTimestampCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timestamp_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyTimestampCol(BoundStatementBuilder boundStatement) throws ParseException {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1DateType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Instant> columnValAC1DateType = ArgumentCaptor.forClass(Instant.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1DateType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1DateType.capture(), columnValAC1DateType.capture(), columnDataTypeAC1DateType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("timestamp_col", columnNameAC1DateType.getAllValues().get(0));
    assertEquals(sdf.parse("2024-02-18 04:34:56.000000+0000").toInstant(), columnValAC1DateType.getAllValues().get(0));
    assertEquals(TypeCodecs.TIMESTAMP, columnDataTypeAC1DateType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000013L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementTimeUUIDCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timeuuid_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyTimeUUIDCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1UUIDType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<UUID> columnValAC1UUIDType = ArgumentCaptor.forClass(UUID.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1UUIDType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1UUIDType.capture(), columnValAC1UUIDType.capture(), columnDataTypeAC1UUIDType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("timeuuid_col", columnNameAC1UUIDType.getAllValues().get(0));
    assertEquals(
        UUID.fromString("550e8400-e29b-11d4-a716-446655440000"),
        columnValAC1UUIDType.getAllValues().get(0));
    assertEquals(TypeCodecs.TIMEUUID, columnDataTypeAC1UUIDType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000003L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementTinyintCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,tinyint_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyTinyintCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1ByteType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Byte> columnValAC1ByteType = ArgumentCaptor.forClass(Byte.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1ByteType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1ByteType.capture(), columnValAC1ByteType.capture(), columnDataTypeAC1ByteType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("tinyint_col", columnNameAC1ByteType.getAllValues().get(0));
    assertEquals(new Byte("1"), columnValAC1ByteType.getAllValues().get(0));
    assertEquals(TypeCodecs.TINYINT, columnDataTypeAC1ByteType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000014L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementUUIDCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,uuid_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyUUIDCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1UUIDType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<UUID> columnValAC1UUIDType = ArgumentCaptor.forClass(UUID.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1UUIDType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1UUIDType.capture(), columnValAC1UUIDType.capture(), columnDataTypeAC1UUIDType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("uuid_col", columnNameAC1UUIDType.getAllValues().get(0));
    assertEquals(
        UUID.fromString("550e8400-e29b-11d4-a716-446655440000"),
        columnValAC1UUIDType.getAllValues().get(0));
    assertEquals(TypeCodecs.UUID, columnDataTypeAC1UUIDType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000000L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementVarcharCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,varchar_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyVarcharCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);
    ArgumentCaptor<Integer> columnNameAC4 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> columnValAC4 = ArgumentCaptor.forClass(Integer.class);

    verify(boundStatement, times(3))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setInt(columnNameAC4.capture(), columnValAC4.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("varchar_col", columnNameAC1.getAllValues().get(2));
    assertEquals("hello123", columnValAC1.getAllValues().get(2));
    assertEquals(TypeCodecs.TEXT, columnDataTypeAC1.getAllValues().get(2));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000011L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(5, columnNameAC4.getAllValues().get(0).intValue());
    assertEquals(3600, columnValAC4.getAllValues().get(0).intValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static BoundStatementBuilder helperBoundStatementVarintCol(CqlSession session) {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(session.prepare(
            "INSERT INTO test_ks.test_mv (ck1,ck2,pk,varint_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
        .thenReturn(ps);
    BoundStatementBuilder boundStatement = mock(BoundStatementBuilder.class);
    when(ps.boundStatementBuilder()).thenReturn(boundStatement);
    return boundStatement;
  }

  public static void helperVerifyVarintCol(BoundStatementBuilder boundStatement) {
    ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<String> columnNameAC1BigIntType = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<BigInteger> columnValAC1BigIntType = ArgumentCaptor.forClass(BigInteger.class);
    ArgumentCaptor<TypeCodec> columnDataTypeAC1BigIntType = ArgumentCaptor.forClass(TypeCodec.class);
    ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
        ArgumentCaptor.forClass(ConsistencyLevel.class);

    verify(boundStatement, times(2))
        .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
    verify(boundStatement, times(1))
            .set(columnNameAC1BigIntType.capture(), columnValAC1BigIntType.capture(), columnDataTypeAC1BigIntType.capture());
    verify(boundStatement, times(1)).setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
    verify(boundStatement, times(1)).setConsistencyLevel(columnValConsistencyLevelAC3.capture());

    assertEquals("ck1", columnNameAC1.getAllValues().get(0));
    assertEquals("SF", columnValAC1.getAllValues().get(0));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(0));
    assertEquals("ck2", columnNameAC1IntType.getAllValues().get(0));
    assertEquals(Integer.valueOf(2020), columnValAC1IntType.getAllValues().get(0));
    assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(0));
    assertEquals("pk", columnNameAC1.getAllValues().get(1));
    assertEquals("Driver1", columnValAC1.getAllValues().get(1));
    assertEquals(TypeCodecs.ASCII, columnDataTypeAC1.getAllValues().get(1));
    assertEquals("varint_col", columnNameAC1BigIntType.getAllValues().get(0));
    assertEquals(new BigInteger("20"), columnValAC1BigIntType.getAllValues().get(0));
    assertEquals(TypeCodecs.VARINT, columnDataTypeAC1BigIntType.getAllValues().get(0));
    assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
    assertEquals(1704067200000004L, columnValLongAC2.getAllValues().get(0).longValue());
    assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
  }

  public static class TestSchema {

    public Map<String, String> primaryKeys;
    public Map<String, String> nonPrimaryKeys;

    TestSchema(Map<String, String> primaryKeys, Map<String, String> nonPrimaryKeys) {
      this.primaryKeys = primaryKeys;
      this.nonPrimaryKeys = nonPrimaryKeys;
    }
  }
}
