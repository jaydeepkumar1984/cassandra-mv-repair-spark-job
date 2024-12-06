package mvsync;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import mvsync.db.CassandraClient;
import mvsync.db.PreparedStatementHelper;
import mvsync.db.UpsertFlavors;
import mvsync.rdd.MvSyncRDDTest;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import mvsync.db.DBOperations;
import mvsync.output.MVJobOutputStreamer;
import mvsync.output.IBlobStreamer;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static mvsync.MVSyncSettings.PREFIX;
import static org.mockito.Mockito.times;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MvSyncTest {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSZ");
    public static DateTimeFormatter formatter = DateTimeFormat.forPattern("HH:mm:ss.SSSSSSSSS");
    public static SimpleDateFormat sdfYYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");

    public static final String KEYSPACE = "test_ks";
    public static final String BASE_TABLE = "test_table";
    public static final String MV = "test_mv";
    public static Map<String, String> primaryKeyBaseTableStringAndInteger =
            new TreeMap<>() {
                {
                    put("pk", "ASCII");
                    put("ck1", "ASCII");
                    put("ck2", "INT");
                }
            };
    public static Map<String, String> primaryKeyBaseTableDoubleAndTimestamp =
            new TreeMap<>() {
                {
                    put("pk", "ASCII");
                    put("ck1", "TIMESTAMP");
                    put("ck2", "DOUBLE");
                }
            };
    public static Map<String, String> primaryKeyBaseTableBooleanAndFloat =
            new TreeMap<>() {
                {
                    put("pk", "ASCII");
                    put("ck1", "BOOLEAN");
                    put("ck2", "FLOAT");
                }
            };
    public static Map<String, String> primaryKeyBaseTableUUIDAndDecimal =
            new TreeMap<>() {
                {
                    put("pk", "ASCII");
                    put("ck1", "UUID");
                    put("ck2", "DECIMAL");
                }
            };

    public static Map<String, String> nonPrimaryKeysBaseTable =
            new TreeMap<>() {
                {
                    put("c1", "INT");
                    put("c2", "INT");
                    put("c3", "INT");
                    put("c4", "INT");
                }
            };

    public static Map<String, String> primaryKeyMvTableStringAndInteger =
            new TreeMap<>() {
                {
                    put("c1", "INT");
                    put("pk", "ASCII");
                    put("ck1", "ASCII");
                    put("ck2", "INT");
                }
            };
    public static Map<String, String> primaryKeyMvTableDoubleAndTimestamp =
            new TreeMap<>() {
                {
                    put("c1", "INT");
                    put("pk", "ASCII");
                    put("ck1", "TIMESTAMP");
                    put("ck2", "DOUBLE");
                }
            };
    public static Map<String, String> primaryKeyMvTableBooleanAndFloat =
            new TreeMap<>() {
                {
                    put("c1", "INT");
                    put("pk", "ASCII");
                    put("ck1", "BOOLEAN");
                    put("ck2", "FLOAT");
                }
            };
    public static Map<String, String> primaryKeyMvTableUUIDAndDecimal =
            new TreeMap<>() {
                {
                    put("c1", "INT");
                    put("pk", "ASCII");
                    put("ck1", "UUID");
                    put("ck2", "DECIMAL");
                }
            };

    public static Map<String, String> nonPrimaryKeysMvTable =
            new TreeMap<>() {
                {
                    put("c2", "INT");
                    put("c3", "INT");
                    put("c4", "INT");
                }
            };
    public static Map<String, String> nonPrimaryKeysMvTableWithFewerColumns =
            new TreeMap<>() {
                {
                    put("c2", "INT");
                    put("c3", "INT");
                }
            };
    static CqlSession session = mock(CqlSession.class);

    public static IBlobStreamer terraBlobStreamerStats;
    public static MVJobOutputStreamer MVJobOutputStreamer;
    List<MvSyncRDDTest.Schema> schemas =
            Arrays.asList(
                    MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER,
                    MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP,
                    MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT,
                    MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL);

    @Before
    public void beforeClass() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        sdfYYYYMMDD.setTimeZone(TimeZone.getTimeZone("UTC"));
        MvSync.mvJobOutputStreamFactory = new MVJobOutputStreamFactoryTest();
        MVJobOutputStreamer = new MVJobOutputStreamer(new MVSyncSettings(getConf()));
        Metadata metadata = mock(Metadata.class);
        when(metadata.getClusterName()).thenReturn(Optional.of("Test Cluster"));
        when(session.getMetadata()).thenReturn(metadata);
    }

    private SparkConf getConf() {
        SparkConf conf = new SparkConf();
        conf.setAppName("MvSyncTest");
        conf.set("spark.master", "local");
        conf.set("spark.driver.allowMultipleContexts", "true");
        conf.set("spark.executor.cores", "1");
        conf.set("spark.cores.max", "1");
        conf.set("spark.cassandra.auth.password", "test");
        conf.set(MVSyncSettings.PREFIX + ".keyspace", KEYSPACE);
        conf.set(MVSyncSettings.PREFIX + ".basetablename", BASE_TABLE);
        conf.set(MVSyncSettings.PREFIX + ".mvname", MV);
        conf.set(PREFIX + ".fixmissingmv", "true");
        conf.set(PREFIX + ".fixorphanmv", "true");
        conf.set(PREFIX + ".fixinconsistentmv", "true");
        return conf;
    }

    @Test(expected = Exception.class)
    public void failedToInitializeCassandraSession() throws Exception {
        CassandraClient.setSession(null);
        new CassandraClient().getSession();
    }

    @Test
    public void successToInitializeCassandraSession() throws Exception {
        CassandraClient.setSession(session);
        new CassandraClient().getSession();
    }

    @Test
    public void initCassandra() throws Exception {
        CassandraClient.setSession(null);
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.set(MVSyncSettings.PREFIX + ".cassandra.username", "testuser");
        sparkConf.set(MVSyncSettings.PREFIX + ".cassandra.password", "testpassword");
        MVSyncSettings mvSyncSettings = new MVSyncSettings(sparkConf);
        new CassandraClient().initCassandra(mvSyncSettings);
    }

    @Test(expected = Exception.class)
    public void failInvalidKeyspaceFillBaseTableColumns() throws Exception {
        mockSchema();
        CassandraClient.setSession(session);
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.set("spark.cass.mv.keyspace", "ks_does_not_exist");
        sparkConf.set("spark.cass.mv.basetablename", BASE_TABLE);
        sparkConf.set("spark.cass.mv.mvname", MV);
        new MvSync().getBaseAndMvTableColumns(new MVSyncSettings(sparkConf));
    }

    @Test(expected = Exception.class)
    public void failInvalidTableFillBaseTableColumns() throws Exception {
        mockSchema();
        CassandraClient.setSession(session);
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.set("spark.cass.mv.keyspace", KEYSPACE);
        sparkConf.set("spark.cass.mv.basetablename", "table_does_not_exist");
        sparkConf.set("spark.cass.mv.mvname", MV);
        new MvSync().getBaseAndMvTableColumns(new MVSyncSettings(sparkConf));
    }

    @Test(expected = Exception.class)
    public void failInvalidMVFillBaseTableColumns() throws Exception {
        mockSchema();
        CassandraClient.setSession(session);
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.set("spark.cass.mv.keyspace", KEYSPACE);
        sparkConf.set("spark.cass.mv.basetablename", BASE_TABLE);
        sparkConf.set("spark.cass.mv.mvname", "mv_does_not_exist");
        new MvSync().getBaseAndMvTableColumns(new MVSyncSettings(sparkConf));
    }

    @Test
    public void successFillBaseTableColumns() throws Exception {
        mockSchema();
        CassandraClient.setSession(session);
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.set("spark.cass.mv.keyspace", KEYSPACE);
        sparkConf.set("spark.cass.mv.basetablename", BASE_TABLE);
        sparkConf.set("spark.cass.mv.mvname", MV);

        TableAndMVColumns tableAndMVColumns =
                new MvSync().getBaseAndMvTableColumns(new MVSyncSettings(sparkConf));

        assertEquals(primaryKeyBaseTableStringAndInteger, tableAndMVColumns.baseTablePrimaryKeyColumns);
        assertEquals(nonPrimaryKeysBaseTable, tableAndMVColumns.baseTableNonPrimaryKeyColumns);
        assertEquals(primaryKeyMvTableStringAndInteger, tableAndMVColumns.mvPrimaryKeyColumns);
        assertEquals(nonPrimaryKeysMvTable, tableAndMVColumns.mvNonPrimaryKeyColumns);
    }

    @Test
    public void successGetRDD() {
        mockSchema();
        SparkConf conf = new SparkConf();
        conf.setAppName("CompareBaseTableAndMV");
        conf.set("spark.master", "local");
        conf.set("spark.driver.allowMultipleContexts", "true");

        for (MvSyncRDDTest.Schema schema : schemas) {
            JavaSparkContext jsc = new JavaSparkContext(SparkContext.getOrCreate(getConf()));
            MvSyncRDDTest mvSync = new MvSyncRDDTest(null, schema);
            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                JavaPairRDD<RecordPrimaryKey, CassandraRow> pairRDD =
                        mvSync.getRDD(
                                KEYSPACE,
                                BASE_TABLE,
                                primaryKeyMvTableStringAndInteger,
                                primaryKeyBaseTableStringAndInteger,
                                nonPrimaryKeysBaseTable,
                                jsc,
                                new MVSyncSettings(conf));
                assertEquals(
                        mvSync.getInMemoryBaseTableCassandraForPKasStringAndInteger(
                                primaryKeyMvTableStringAndInteger),
                        pairRDD.collect());
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                JavaPairRDD<RecordPrimaryKey, CassandraRow> pairRDD =
                        mvSync.getRDD(
                                KEYSPACE,
                                BASE_TABLE,
                                primaryKeyMvTableDoubleAndTimestamp,
                                primaryKeyBaseTableDoubleAndTimestamp,
                                nonPrimaryKeysBaseTable,
                                jsc,
                                new MVSyncSettings(conf));
                assertEquals(
                        mvSync.getInMemoryBaseTableCassandraForPKasDoubleAndTimestamp(
                                primaryKeyMvTableDoubleAndTimestamp),
                        pairRDD.collect());
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                JavaPairRDD<RecordPrimaryKey, CassandraRow> pairRDD =
                        mvSync.getRDD(
                                KEYSPACE,
                                BASE_TABLE,
                                primaryKeyMvTableBooleanAndFloat,
                                primaryKeyBaseTableBooleanAndFloat,
                                nonPrimaryKeysBaseTable,
                                jsc,
                                new MVSyncSettings(conf));
                assertEquals(
                        mvSync.getInMemoryBaseTableCassandraForPKasBooleanAndFloat(
                                primaryKeyMvTableBooleanAndFloat),
                        pairRDD.collect());
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                JavaPairRDD<RecordPrimaryKey, CassandraRow> pairRDD =
                        mvSync.getRDD(
                                KEYSPACE,
                                BASE_TABLE,
                                primaryKeyMvTableUUIDAndDecimal,
                                primaryKeyBaseTableUUIDAndDecimal,
                                nonPrimaryKeysBaseTable,
                                jsc,
                                new MVSyncSettings(conf));
                assertEquals(
                        mvSync.getInMemoryBaseTableCassandraForPKasUUIDAndDecimal(
                                primaryKeyMvTableUUIDAndDecimal),
                        pairRDD.collect());
            }
            jsc.stop();
        }
    }

    private void prepareAndCompareRDDs(
            JavaSparkContext jsc,
            MvSyncRDDTest mvSync,
            TableAndMVColumns tableAndMVColumns,
            MVSyncSettings settings,
            JobStats jobStats) {
        JavaPairRDD<RecordPrimaryKey, CassandraRow> baseTableRDD =
                mvSync.getRDD(
                        KEYSPACE,
                        BASE_TABLE,
                        tableAndMVColumns.mvPrimaryKeyColumns,
                        tableAndMVColumns.baseTablePrimaryKeyColumns,
                        tableAndMVColumns.baseTableNonPrimaryKeyColumns,
                        jsc,
                        settings);

        JavaPairRDD<RecordPrimaryKey, CassandraRow> mvRDD =
                mvSync.getRDD(
                        KEYSPACE,
                        MV,
                        tableAndMVColumns.mvPrimaryKeyColumns,
                        tableAndMVColumns.mvPrimaryKeyColumns,
                        tableAndMVColumns.mvNonPrimaryKeyColumns,
                        jsc,
                        settings);

        if (mvSync.schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
            assertEquals(
                    mvSync.getInMemoryBaseTableCassandraForPKasStringAndInteger(
                            primaryKeyMvTableStringAndInteger),
                    baseTableRDD.collect());
            assertEquals(
                    mvSync.getInMemoryMVTableCassandraForPKasStringAndInteger(
                            primaryKeyMvTableStringAndInteger),
                    mvRDD.collect());
        }
        if (mvSync.schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
            assertEquals(
                    mvSync.getInMemoryBaseTableCassandraForPKasDoubleAndTimestamp(
                            primaryKeyMvTableDoubleAndTimestamp),
                    baseTableRDD.collect());
            assertEquals(
                    mvSync.getInMemoryMVTableCassandraForDoubleAndTimestamp(
                            primaryKeyMvTableDoubleAndTimestamp),
                    mvRDD.collect());
        }
        if (mvSync.schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
            assertEquals(
                    mvSync.getInMemoryBaseTableCassandraForPKasBooleanAndFloat(
                            primaryKeyMvTableBooleanAndFloat),
                    baseTableRDD.collect());
            assertEquals(
                    mvSync.getInMemoryMVTableCassandraForBooleanAndFloat(primaryKeyMvTableBooleanAndFloat),
                    mvRDD.collect());
        }
        if (mvSync.schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
            assertEquals(
                    mvSync.getInMemoryBaseTableCassandraForPKasUUIDAndDecimal(
                            primaryKeyMvTableUUIDAndDecimal),
                    baseTableRDD.collect());
            assertEquals(
                    mvSync.getInMemoryMVTableCassandraForUUIDAndDecimal(primaryKeyMvTableUUIDAndDecimal),
                    mvRDD.collect());
        }
        mvSync.compareAndRepairMV(baseTableRDD, mvRDD, settings, tableAndMVColumns, jobStats).collect();
    }

    @Test
    public void recordsNotInScopeLowerBound() {
        for (MvSyncRDDTest.Schema schema : schemas) {
            JavaSparkContext jsc = new JavaSparkContext(SparkContext.getOrCreate(getConf()));
            MvSyncRDDTest mvSync = new MvSyncRDDTest(null, schema);
            JobStats jobStats = new JobStats(jsc);
            CassandraClient.setSession(session);
            SparkConf conf = getConf();
            conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "1546300800"); // 2019-01-01 00:00:00
            conf.set(MVSyncSettings.PREFIX + ".endtsinsec", "1577836800"); // 2020-01-01 00:00:00

            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableStringAndInteger,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableStringAndInteger,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableDoubleAndTimestamp,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableDoubleAndTimestamp,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableBooleanAndFloat,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableBooleanAndFloat,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableUUIDAndDecimal,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableUUIDAndDecimal,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }

            assertEquals(2, jobStats.totRecords.count());
            assertEquals(2, jobStats.recordNotInScope.count());
            assertEquals(0, jobStats.consistentRecords.count());
            assertEquals(0, jobStats.inConsistentRecords.count());
            assertEquals(0, jobStats.missingBaseTableRecords.count());
            assertEquals(0, jobStats.missingMvRecords.count());
            jsc.stop();
        }
    }

    @Test
    public void recordsNotInScopeUpperBound() {
        for (MvSyncRDDTest.Schema schema : schemas) {
            JavaSparkContext jsc = new JavaSparkContext(SparkContext.getOrCreate(getConf()));
            MvSyncRDDTest mvSync = new MvSyncRDDTest(null, schema);
            JobStats jobStats = new JobStats(jsc);
            CassandraClient.setSession(session);
            SparkConf conf = getConf();
            conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "" + System.currentTimeMillis() / 1000);
            conf.set(
                    MVSyncSettings.PREFIX + ".endtsinsec",
                    "" + (System.currentTimeMillis() + (24 * 3600 * 1000)) / 1000);

            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableStringAndInteger,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableStringAndInteger,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableDoubleAndTimestamp,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableDoubleAndTimestamp,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableBooleanAndFloat,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableBooleanAndFloat,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableUUIDAndDecimal,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableUUIDAndDecimal,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            assertEquals(2, jobStats.totRecords.count());
            assertEquals(2, jobStats.recordNotInScope.count());
            assertEquals(0, jobStats.consistentRecords.count());
            assertEquals(0, jobStats.inConsistentRecords.count());
            assertEquals(0, jobStats.missingBaseTableRecords.count());
            assertEquals(0, jobStats.missingMvRecords.count());
            jsc.stop();
        }
    }

    @Test
    public void recordsMatchInBaseTableAndInMV() {
        for (MvSyncRDDTest.Schema schema : schemas) {
            JavaSparkContext jsc = new JavaSparkContext(SparkContext.getOrCreate(getConf()));
            MvSyncRDDTest mvSync = new MvSyncRDDTest(null, schema);
            JobStats jobStats = new JobStats(jsc);
            CassandraClient.setSession(session);
            SparkConf conf = getConf();
            conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "0");
            conf.set(MVSyncSettings.PREFIX + ".endtsinsec", "" + System.currentTimeMillis() / 1000);

            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableStringAndInteger,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableStringAndInteger,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableDoubleAndTimestamp,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableDoubleAndTimestamp,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableBooleanAndFloat,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableBooleanAndFloat,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableUUIDAndDecimal,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableUUIDAndDecimal,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }

            assertEquals(2, jobStats.totRecords.count());
            assertEquals(0, jobStats.recordNotInScope.count());
            assertEquals(2, jobStats.consistentRecords.count());
            assertEquals(0, jobStats.inConsistentRecords.count());
            assertEquals(0, jobStats.missingBaseTableRecords.count());
            assertEquals(0, jobStats.missingMvRecords.count());
            jsc.stop();
        }
    }

    public void helperMissingRecordInMV(boolean upsertMV) throws IOException {
        for (MvSyncRDDTest.Schema schema : schemas) {
            JavaSparkContext jsc = new JavaSparkContext(SparkContext.getOrCreate(getConf()));
            CassandraClient.setSession(session);
            PreparedStatement preparedStatement = mock(PreparedStatement.class);
            when(session.prepare(anyString())).thenReturn(preparedStatement);
            when(preparedStatement.boundStatementBuilder()).thenReturn(mock(BoundStatementBuilder.class));

            MvSyncRDDTest mvSync = new MvSyncRDDTest(MvSyncRDDTest.TestRDDType.SKIP_RECORD_IN_MV, schema);
            MvSyncTest.MVJobOutputStreamer.streamerMissingInMv = mock(IBlobStreamer.class);
            JobStats jobStats = new JobStats(jsc);
            SparkConf conf = getConf();
            conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "0");
            conf.set(MVSyncSettings.PREFIX + ".endtsinsec", "" + System.currentTimeMillis() / 1000);
            conf.set(MVSyncSettings.PREFIX + ".basetablename", "test_basetable");
            conf.set(MVSyncSettings.PREFIX + ".mvname", "test_mv");
            if (upsertMV) {
                conf.set(MVSyncSettings.PREFIX + ".fixmissingmv", "true");
            } else {
                conf.remove(MVSyncSettings.PREFIX + ".fixmissingmv");
            }
            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableStringAndInteger,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableStringAndInteger,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableDoubleAndTimestamp,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableDoubleAndTimestamp,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableBooleanAndFloat,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableBooleanAndFloat,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableUUIDAndDecimal,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableUUIDAndDecimal,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            int expectedRepairedRecord = 0;
            int expectedNotRepairedRecord = 0;
            int expectedUpsertedRecord = 0;
            if (upsertMV) {
                expectedRepairedRecord = 1;
                expectedUpsertedRecord = 1;
            } else {
                expectedNotRepairedRecord = 1;
            }

            assertEquals(2, jobStats.totRecords.count());
            assertEquals(0, jobStats.recordNotInScope.count());
            assertEquals(1, jobStats.consistentRecords.count());
            assertEquals(0, jobStats.inConsistentRecords.count());
            assertEquals(0, jobStats.missingBaseTableRecords.count());
            assertEquals(1, jobStats.missingMvRecords.count());
            assertEquals(0, jobStats.delSuccessRecords.count());
            assertEquals(expectedRepairedRecord, jobStats.repairRecords.count());
            assertEquals(expectedNotRepairedRecord, jobStats.notRepairRecords.count());
            assertEquals(expectedUpsertedRecord, jobStats.upsertAttemptedRecords.count());
            assertEquals(expectedUpsertedRecord, jobStats.upsertSuccessRecords.count());
            assertEquals(0, jobStats.upsertErrRecords.count());
            ArgumentCaptor<String> arg1Captor = ArgumentCaptor.forClass(String.class);

            verify(MvSyncTest.MVJobOutputStreamer.streamerMissingInMv, times(2))
                    .append(arg1Captor.capture());
            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                assertEquals(
                        "Problem: MISSING_IN_MV_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:ASCII:NY,ck2:INT:2021,pk:ASCII:Driver2\n"
                                + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}\n"
                                + "MVTableEntry: null",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                assertEquals(
                        "Problem: MISSING_IN_MV_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:TIMESTAMP:1708230896002,ck2:DOUBLE:12.56,pk:ASCII:Driver2\n"
                                + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}\n"
                                + "MVTableEntry: null",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                assertEquals(
                        "Problem: MISSING_IN_MV_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:BOOLEAN:false,ck2:FLOAT:1.09,pk:ASCII:Driver2\n"
                                + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}\n"
                                + "MVTableEntry: null",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                assertEquals(
                        "Problem: MISSING_IN_MV_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:UUID:7309362c-4237-4e5d-b403-505820caba42,ck2:DECIMAL:100.06,pk:ASCII:Driver2\n"
                                + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}\n"
                                + "MVTableEntry: null",
                        arg1Captor.getAllValues().get(0));
            }
            assertEquals("==============================", arg1Captor.getAllValues().get(1));
            jsc.stop();
        }
    }

    @Test
    public void missingRecordInMVNoUpsertMV() throws IOException {
        helperMissingRecordInMV(false);
    }

    @Test
    public void missingRecordInMVOutputUpsertMV() throws IOException {
        helperMissingRecordInMV(true);
    }

    public void helperMissingRecordInBaseTable(boolean deleteFromMV) throws IOException {
        for (MvSyncRDDTest.Schema schema : schemas) {
            JavaSparkContext jsc = new JavaSparkContext(SparkContext.getOrCreate(getConf()));
            CassandraClient.setSession(session);
            PreparedStatement preparedStatement = mock(PreparedStatement.class);
            when(session.prepare(anyString())).thenReturn(preparedStatement);
            when(preparedStatement.boundStatementBuilder()).thenReturn(mock(BoundStatementBuilder.class));
            MvSyncRDDTest mvSync =
                    new MvSyncRDDTest(MvSyncRDDTest.TestRDDType.SKIP_RECORD_IN_BASE_TABLE, schema);

            MvSyncTest.MVJobOutputStreamer.streamerMissingInBaseTable = mock(IBlobStreamer.class);
            JobStats jobStats = new JobStats(jsc);
            SparkConf conf = getConf();
            conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "0");
            conf.set(MVSyncSettings.PREFIX + ".endtsinsec", "" + System.currentTimeMillis() / 1000);
            conf.set(MVSyncSettings.PREFIX + ".keyspace", "test_keyspace");
            conf.set(MVSyncSettings.PREFIX + ".basetablename", "test_basetable");
            conf.set(MVSyncSettings.PREFIX + ".mvname", "test_mv");

            if (deleteFromMV) {
                conf.set(MVSyncSettings.PREFIX + ".fixorphanmv", "true");
            } else {
                conf.remove(MVSyncSettings.PREFIX + ".fixorphanmv");
            }

            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableStringAndInteger,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableStringAndInteger,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableDoubleAndTimestamp,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableDoubleAndTimestamp,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableBooleanAndFloat,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableBooleanAndFloat,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableUUIDAndDecimal,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableUUIDAndDecimal,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }

            int expectedRepairedRecord = 0;
            int expectedNotRepairedRecord = 0;
            int expectedDeletedRecord = 0;
            if (deleteFromMV) {
                expectedRepairedRecord = 1;
                expectedDeletedRecord = 1;
            } else {
                expectedNotRepairedRecord = 1;
            }

            assertEquals(2, jobStats.totRecords.count());
            assertEquals(0, jobStats.recordNotInScope.count());
            assertEquals(1, jobStats.consistentRecords.count());
            assertEquals(0, jobStats.inConsistentRecords.count());
            assertEquals(1, jobStats.missingBaseTableRecords.count());
            assertEquals(0, jobStats.missingMvRecords.count());
            assertEquals(0, jobStats.upsertSuccessRecords.count());
            assertEquals(expectedRepairedRecord, jobStats.repairRecords.count());
            assertEquals(expectedNotRepairedRecord, jobStats.notRepairRecords.count());
            assertEquals(expectedDeletedRecord, jobStats.delSuccessRecords.count());

            ArgumentCaptor<String> arg1Captor = ArgumentCaptor.forClass(String.class);
            verify(MvSyncTest.MVJobOutputStreamer.streamerMissingInBaseTable, times(2))
                    .append(arg1Captor.capture());
            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                assertEquals(
                        "Problem: MISSING_IN_BASE_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:ASCII:NY,ck2:INT:2021,pk:ASCII:Driver2\n"
                                + "MainTableEntry: null\n"
                                + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                assertEquals(
                        "Problem: MISSING_IN_BASE_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:TIMESTAMP:1708230896002,ck2:DOUBLE:12.56,pk:ASCII:Driver2\n"
                                + "MainTableEntry: null\n"
                                + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                assertEquals(
                        "Problem: MISSING_IN_BASE_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:BOOLEAN:false,ck2:FLOAT:1.09,pk:ASCII:Driver2\n"
                                + "MainTableEntry: null\n"
                                + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                assertEquals(
                        "Problem: MISSING_IN_BASE_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:UUID:7309362c-4237-4e5d-b403-505820caba42,ck2:DECIMAL:100.06,pk:ASCII:Driver2\n"
                                + "MainTableEntry: null\n"
                                + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}",
                        arg1Captor.getAllValues().get(0));
            }
            assertEquals("==============================", arg1Captor.getAllValues().get(1));
            jsc.stop();
        }
    }

    @Test
    public void missingRecordInBaseTableDoNotDeleteFromMV() throws IOException {
        helperMissingRecordInBaseTable(false);
    }

    @Test
    public void missingRecordInBaseTableDeleteFromMV() throws IOException {
        helperMissingRecordInBaseTable(true);
    }

    public void helperRecordMismatch(boolean upsertMV) throws IOException {
        CassandraClient.setSession(session);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.boundStatementBuilder()).thenReturn(mock(BoundStatementBuilder.class));
        for (MvSyncRDDTest.Schema schema : schemas) {
            JavaSparkContext jsc = new JavaSparkContext(SparkContext.getOrCreate(getConf()));
            MvSyncRDDTest mvSync = new MvSyncRDDTest(MvSyncRDDTest.TestRDDType.MV_MISMATCH, schema);
            MvSyncTest.MVJobOutputStreamer.streamerMismatch = mock(IBlobStreamer.class);
            JobStats jobStats = new JobStats(jsc);
            SparkConf conf = getConf();
            conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "0");
            conf.set(MVSyncSettings.PREFIX + ".endtsinsec", "" + System.currentTimeMillis() / 1000);
            conf.set(MVSyncSettings.PREFIX + ".keyspace", "test_keyspace");
            conf.set(MVSyncSettings.PREFIX + ".basetablename", "test_basetable");
            conf.set(MVSyncSettings.PREFIX + ".mvname", "test_mv");

            if (upsertMV) {
                conf.set(MVSyncSettings.PREFIX + ".fixinconsistentmv", "true");
            } else {
                conf.remove(MVSyncSettings.PREFIX + ".fixinconsistentmv");
            }

            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableStringAndInteger,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableStringAndInteger,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableDoubleAndTimestamp,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableDoubleAndTimestamp,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableBooleanAndFloat,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableBooleanAndFloat,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableUUIDAndDecimal,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableUUIDAndDecimal,
                                nonPrimaryKeysMvTable),
                        new MVSyncSettings(conf),
                        jobStats);
            }

            int expectedRepairedRecord = 0;
            int expectedNotRepairedRecord = 0;
            int expectedUpsertedRecord = 0;
            if (upsertMV) {
                expectedRepairedRecord = 1;
                expectedUpsertedRecord = 1;
            } else {
                expectedNotRepairedRecord = 1;
            }

            assertEquals(2, jobStats.totRecords.count());
            assertEquals(0, jobStats.recordNotInScope.count());
            assertEquals(1, jobStats.consistentRecords.count());
            assertEquals(1, jobStats.inConsistentRecords.count());
            assertEquals(0, jobStats.missingBaseTableRecords.count());
            assertEquals(0, jobStats.missingMvRecords.count());
            assertEquals(0, jobStats.delSuccessRecords.count());
            assertEquals(expectedRepairedRecord, jobStats.repairRecords.count());
            assertEquals(expectedNotRepairedRecord, jobStats.notRepairRecords.count());
            assertEquals(expectedUpsertedRecord, jobStats.upsertSuccessRecords.count());

            ArgumentCaptor<String> arg1Captor = ArgumentCaptor.forClass(String.class);
            verify(MvSyncTest.MVJobOutputStreamer.streamerMismatch, times(2))
                    .append(arg1Captor.capture());
            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                assertEquals(
                        "Problem: INCONSISTENT\n"
                                + "RowKey: c1:INT:11,ck1:ASCII:NY,ck2:INT:2021,pk:ASCII:Driver2\n"
                                + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}\n"
                                + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 441, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}\n"
                                + "BaseColumn: c4:INT:44\n"
                                + "MvColumn: c4:INT:441",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                assertEquals(
                        "Problem: INCONSISTENT\n"
                                + "RowKey: c1:INT:11,ck1:TIMESTAMP:1708230896002,ck2:DOUBLE:12.56,pk:ASCII:Driver2\n"
                                + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}\n"
                                + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 441, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}\n"
                                + "BaseColumn: c4:INT:44\n"
                                + "MvColumn: c4:INT:441",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                assertEquals(
                        "Problem: INCONSISTENT\n"
                                + "RowKey: c1:INT:11,ck1:BOOLEAN:false,ck2:FLOAT:1.09,pk:ASCII:Driver2\n"
                                + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}\n"
                                + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 441, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}\n"
                                + "BaseColumn: c4:INT:44\n"
                                + "MvColumn: c4:INT:441",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                assertEquals(
                        "Problem: INCONSISTENT\n"
                                + "RowKey: c1:INT:11,ck1:UUID:7309362c-4237-4e5d-b403-505820caba42,ck2:DECIMAL:100.06,pk:ASCII:Driver2\n"
                                + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}\n"
                                + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 441, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}\n"
                                + "BaseColumn: c4:INT:44\n"
                                + "MvColumn: c4:INT:441",
                        arg1Captor.getAllValues().get(0));
            }
            assertEquals("==============================", arg1Captor.getAllValues().get(1));
            jsc.stop();
        }
    }

    @Test
    public void recordMismatchNoUpsertMV() throws IOException {
        helperRecordMismatch(false);
    }

    @Test
    public void recordMismatchUpsertMV() throws IOException {
        helperRecordMismatch(true);
    }

    @Test
    public void missingRecordInBaseTableFewerColumnsInMv() throws IOException {
        for (MvSyncRDDTest.Schema schema : schemas) {
            JavaSparkContext jsc = new JavaSparkContext(SparkContext.getOrCreate(getConf()));
            MvSyncRDDTest mvSync =
                    new MvSyncRDDTest(
                            MvSyncRDDTest.TestRDDType.MV_HAS_FEWER_COLUMNS_AND_SKIP_IN_BASE_TABLE, schema);
            CassandraClient.setSession(session);
            JobStats jobStats = new JobStats(jsc);
            MvSyncTest.MVJobOutputStreamer.streamerMissingInBaseTable = mock(IBlobStreamer.class);
            SparkConf conf = getConf();
            conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "0");
            conf.set(MVSyncSettings.PREFIX + ".endtsinsec", "" + System.currentTimeMillis() / 1000);

            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableStringAndInteger,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableStringAndInteger,
                                nonPrimaryKeysMvTableWithFewerColumns),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableDoubleAndTimestamp,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableDoubleAndTimestamp,
                                nonPrimaryKeysMvTableWithFewerColumns),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableBooleanAndFloat,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableBooleanAndFloat,
                                nonPrimaryKeysMvTableWithFewerColumns),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                prepareAndCompareRDDs(
                        jsc,
                        mvSync,
                        new TableAndMVColumns(
                                primaryKeyBaseTableUUIDAndDecimal,
                                nonPrimaryKeysBaseTable,
                                primaryKeyMvTableUUIDAndDecimal,
                                nonPrimaryKeysMvTableWithFewerColumns),
                        new MVSyncSettings(conf),
                        jobStats);
            }
            assertEquals(2, jobStats.totRecords.count());
            assertEquals(0, jobStats.recordNotInScope.count());
            assertEquals(1, jobStats.consistentRecords.count());
            assertEquals(0, jobStats.inConsistentRecords.count());
            assertEquals(1, jobStats.missingBaseTableRecords.count());
            assertEquals(0, jobStats.missingMvRecords.count());

            ArgumentCaptor<String> arg1Captor = ArgumentCaptor.forClass(String.class);
            verify(MvSyncTest.MVJobOutputStreamer.streamerMissingInBaseTable, times(2))
                    .append(arg1Captor.capture());
            if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                assertEquals(
                        "Problem: MISSING_IN_BASE_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:ASCII:NY,ck2:INT:2021,pk:ASCII:Driver2\n"
                                + "MainTableEntry: null\n"
                                + "MVTableEntry: CassandraRow{c3: 33, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                assertEquals(
                        "Problem: MISSING_IN_BASE_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:TIMESTAMP:1708230896002,ck2:DOUBLE:12.56,pk:ASCII:Driver2\n"
                                + "MainTableEntry: null\n"
                                + "MVTableEntry: CassandraRow{c3: 33, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                assertEquals(
                        "Problem: MISSING_IN_BASE_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:BOOLEAN:false,ck2:FLOAT:1.09,pk:ASCII:Driver2\n"
                                + "MainTableEntry: null\n"
                                + "MVTableEntry: CassandraRow{c3: 33, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}",
                        arg1Captor.getAllValues().get(0));
            }
            if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                assertEquals(
                        "Problem: MISSING_IN_BASE_TABLE\n"
                                + "RowKey: c1:INT:11,ck1:UUID:7309362c-4237-4e5d-b403-505820caba42,ck2:DECIMAL:100.06,pk:ASCII:Driver2\n"
                                + "MainTableEntry: null\n"
                                + "MVTableEntry: CassandraRow{c3: 33, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}",
                        arg1Captor.getAllValues().get(0));
            }
            assertEquals("==============================", arg1Captor.getAllValues().get(1));
            jsc.stop();
        }
    }

    @Test
    public void e2eTestNoInconsistency() throws Exception {
        String endTS = "1704153601"; // 2024-Jan-2 00:00:01
        helperE2ETesting(KEYSPACE, BASE_TABLE, MV, endTS, null);
    }

    @Test
    public void e2eTestNoInconsistencySkipOneRecord() throws Exception {
        String endTS = "1704067201"; // 2024-Jan-1 00:00:01
        helperE2ETesting(KEYSPACE, BASE_TABLE, MV, endTS, null);
    }

    @Test
    public void e2eTestMissingInMVOneRecord() throws Exception {
        String endTS = "1704153601"; // 2024-Jan-2 00:00:01
        helperE2ETesting(KEYSPACE, BASE_TABLE, MV, endTS, MvSyncRDDTest.TestRDDType.SKIP_RECORD_IN_MV);
    }

    @Test
    public void e2eTestMismatchMvAndBaseTableOneRecord() throws Exception {
        String endTS = "1704153601"; // 2024-Jan-2 00:00:01
        helperE2ETesting(KEYSPACE, BASE_TABLE, MV, endTS, MvSyncRDDTest.TestRDDType.MV_MISMATCH);
    }

    @Test
    public void e2eTestMissingInBaseTableOneRecord() throws Exception {
        String endTS = "1704153601"; // 2024-Jan-2 00:00:01
        helperE2ETesting(
                KEYSPACE, BASE_TABLE, MV, endTS, MvSyncRDDTest.TestRDDType.SKIP_RECORD_IN_BASE_TABLE);
    }

    @Test(expected = Exception.class)
    public void e2eTestInvalidKeyspace() throws Exception {
        helperE2ETesting("Invalid_KS", BASE_TABLE, MV, "1704067201", null);
    }

    @Test(expected = Exception.class)
    public void e2eTestInvalidBaseTable() throws Exception {
        helperE2ETesting(KEYSPACE, "Invalid_BASE_TABLE", MV, "1704067201", null);
    }

    @Test(expected = Exception.class)
    public void e2eTestInvalidMV() throws Exception {
        helperE2ETesting(KEYSPACE, BASE_TABLE, "Invalid_MV", "1704067201", null);
    }

    @Test(expected = Exception.class)
    public void runTestError() throws Exception {
        for (MvSyncRDDTest.Schema schema : schemas) {
            mockSchema();
            CassandraClient.setSession(null);
            MvSync mvSync = new MvSyncRDDTest(null, schema);
            mvSync.run();
        }
    }

    @Test
    public void runTestSuccess() throws Exception {
        for (MvSyncRDDTest.Schema schema : schemas) {
            mockSchema();
            CassandraClient.setSession(session);
            MvSyncTest.terraBlobStreamerStats = mock(IBlobStreamer.class);
            MvSync mvSync = new MvSyncRDDTest(null, schema);
            mvSync.run();
        }
    }

    @Test
    public void primitiveAndNonPrimitiveDataTypes() {
        for (MvSyncRDDTest.Schema schema : schemas) {
            MvSyncRDDTest mvSync = new MvSyncRDDTest(null, schema);
            Map<String, String> primaryKey =
                    new TreeMap<String, String>() {
                        {
                            put("pk", "ASCII");
                            put("ck1", "ASCII");
                            put("ck2", "ASCII");
                        }
                    };
            Map<String, String> nonPrimaryKeyPrimitiveOnly =
                    new TreeMap<String, String>() {
                        {
                            put("c1", "INT");
                            put("c2", "INT");
                            put("c3", "LIST");
                            put("c4", "MAP");
                            put("c5", "SET");
                        }
                    };

            ArrayList<ColumnRef> selectStatements =
                    mvSync.buildSelectStatement(primaryKey, nonPrimaryKeyPrimitiveOnly);
            ArrayList<ColumnRef> expected = new ArrayList<ColumnRef>();

            for (String colName : primaryKey.keySet()) {
                expected.add(CassandraJavaUtil.column(colName));
            }
            for (Map.Entry<String, String> column : nonPrimaryKeyPrimitiveOnly.entrySet()) {
                expected.add(CassandraJavaUtil.column(column.getKey()));
                // write time is not included for c3, c4, and c5 because they are not primitive types
                if (column.getKey().equals("c1") || column.getKey().equals("c2")) {
                    expected.add(CassandraJavaUtil.writeTime(column.getKey()));
                    expected.add(CassandraJavaUtil.ttl(column.getKey()));
                }
            }
            assertEquals(new HashSet<>(expected), new HashSet<>(selectStatements));
        }
    }

    @Test
    public void collectionTypes() {
        assertTrue(DBOperations.isSet("SET"));
        assertTrue(DBOperations.isCollection("SET"));
        assertFalse(DBOperations.isSet("INT"));

        assertTrue(DBOperations.isMap("MAP"));
        assertTrue(DBOperations.isCollection("MAP"));
        assertFalse(DBOperations.isMap("INT"));

        assertTrue(DBOperations.isList("LIST"));
        assertTrue(DBOperations.isCollection("LIST"));
        assertFalse(DBOperations.isMap("INT"));
    }

    static class CassandraSession {

        CqlSession session;
        BoundStatementBuilder boundStatementSel;
        BoundStatementBuilder boundStatementDel;

        public CassandraSession(
                CqlSession session, BoundStatementBuilder boundStatementSel, BoundStatementBuilder boundStatementDel) {
            this.session = session;
            this.boundStatementSel = boundStatementSel;
            this.boundStatementDel = boundStatementDel;
        }
    }

    private CassandraSession helperSetup(boolean rowPresentInBaseTable) {
        CqlSession session = mock(CqlSession.class);
        PreparedStatement preparedStatementSel = mock(PreparedStatement.class);
        when(session.prepare(
                "SELECT * FROM test_ks.test_table WHERE ck1=? AND ck2=? AND pk=? ALLOW FILTERING"))
                .thenReturn(preparedStatementSel);

        BoundStatementBuilder boundStatementSel = mock(BoundStatementBuilder.class);
        when(preparedStatementSel.boundStatementBuilder()).thenReturn(boundStatementSel);
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(boundStatementSel.build()).thenReturn(boundStatement);
        ResultSet rsSel = mock(ResultSet.class);
        Iterator<Row> rowIterableSel = mock(Iterator.class);
        when(session.execute(boundStatement)).thenReturn(rsSel);
        when(rsSel.iterator()).thenReturn(rowIterableSel);
        when(rowIterableSel.hasNext()).thenReturn(rowPresentInBaseTable);

        BoundStatementBuilder boundStatementDel = null;
        if (!rowPresentInBaseTable) {
            PreparedStatement preparedStatementDel = mock(PreparedStatement.class);
            when(session.prepare("DELETE FROM test_ks.test_mv WHERE ck1=? AND ck2=? AND pk=?"))
                    .thenReturn(preparedStatementDel);
            boundStatementDel = mock(BoundStatementBuilder.class);
            when(preparedStatementDel.boundStatementBuilder()).thenReturn(boundStatementDel);
        }

        return new CassandraSession(session, boundStatementSel, boundStatementDel);
    }

    private void helperVerify(
            DBOperations.DBResult result,
            boolean rowPresentInBaseTable,
            CassandraSession cassandraSession) {
        assertTrue(result.success);
        assertEquals(rowPresentInBaseTable, result.entryPresent);
        assertEquals("", result.errMessage);

        ArgumentCaptor<String> columnNameSelAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> columnValSelAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeCodec<String>> columnDataTypeSelAC1 = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<String> columnNameSelAC1IntType = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> columnValSelAC1IntType = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<TypeCodec<Integer>> columnDataTypeSelAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);


        verify(cassandraSession.boundStatementSel, times(2))
                .set(columnNameSelAC1.capture(), columnValSelAC1.capture(), columnDataTypeSelAC1.capture());
        verify(cassandraSession.boundStatementSel, times(1))
                .set(columnNameSelAC1IntType.capture(), columnValSelAC1IntType.capture(), columnDataTypeSelAC1IntType.capture());

        assertEquals("ck1", columnNameSelAC1.getAllValues().get(0));
        assertEquals("SF", columnValSelAC1.getAllValues().get(0));
        assertEquals(TypeCodecs.ASCII, columnDataTypeSelAC1.getAllValues().get(0));
        assertEquals("ck2", columnNameSelAC1IntType.getAllValues().get(0));
        assertEquals(Integer.valueOf(2020), columnValSelAC1IntType.getAllValues().get(0));
        assertEquals(TypeCodecs.INT, columnDataTypeSelAC1IntType.getAllValues().get(0));
        assertEquals("pk", columnNameSelAC1.getAllValues().get(1));
        assertEquals("Driver1", columnValSelAC1.getAllValues().get(1));

        if (!rowPresentInBaseTable) {
            ArgumentCaptor<String> columnNameDelAC1 = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> columnValDelAC1 = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<TypeCodec<String>> columnDataTypeDelAC1 = ArgumentCaptor.forClass(TypeCodec.class);
            ArgumentCaptor<String> columnNameDelAC1IntType = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Integer> columnValDelAC1IntType = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<TypeCodec<Integer>> columnDataTypeDelAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);


            verify(cassandraSession.boundStatementDel, times(2))
                    .set(
                            columnNameDelAC1.capture(),
                            columnValDelAC1.capture(),
                            columnDataTypeDelAC1.capture());
            verify(cassandraSession.boundStatementDel, times(1))
                    .set(
                            columnNameDelAC1IntType.capture(),
                            columnValDelAC1IntType.capture(),
                            columnDataTypeDelAC1IntType.capture());

            assertEquals("ck1", columnNameDelAC1.getAllValues().get(0));
            assertEquals("SF", columnValDelAC1.getAllValues().get(0));
            assertEquals(TypeCodecs.ASCII, columnDataTypeDelAC1.getAllValues().get(0));
            assertEquals("ck2", columnNameDelAC1IntType.getAllValues().get(0));
            assertEquals(Integer.valueOf(2020), columnValDelAC1IntType.getAllValues().get(0));
            assertEquals(TypeCodecs.INT, columnDataTypeDelAC1IntType.getAllValues().get(0));
            assertEquals("pk", columnNameDelAC1.getAllValues().get(1));
            assertEquals("Driver1", columnValDelAC1.getAllValues().get(1));
        }
    }

    @Test
    public void deleteNativeType() {
        List<Boolean> rowPresentInBaseTableVals = Arrays.asList(false, true);
        for (Boolean rowPresentInBaseTable : rowPresentInBaseTableVals) {
            CassandraSession cassandraSession = helperSetup(rowPresentInBaseTable);

            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getNativeDataTypes(20)._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns =
                    MvSyncRDDTest.getNativeDataTypes(20)._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(cassandraSession.session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(
                            cassandraSession.session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result =
                    dbOperations.deleteFromMV(MvSyncRDDTest.getNativeDataTypes(20)._2);
            helperVerify(result, rowPresentInBaseTable, cassandraSession);
        }
    }

    @Test
    public void deleteMVListType() {
        List<Boolean> rowPresentInBaseTableVals = Arrays.asList(false, true);
        for (Boolean rowPresentInBaseTable : rowPresentInBaseTableVals) {
            CassandraSession cassandraSession = helperSetup(rowPresentInBaseTable);

            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getListDataType()._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getListDataType()._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(cassandraSession.session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(
                            cassandraSession.session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result = dbOperations.deleteFromMV(MvSyncRDDTest.getListDataType()._2);
            helperVerify(result, rowPresentInBaseTable, cassandraSession);
        }
    }

    @Test
    public void deleteMVSetType() {
        List<Boolean> rowPresentInBaseTableVals = Arrays.asList(false, true);
        for (Boolean rowPresentInBaseTable : rowPresentInBaseTableVals) {
            CassandraSession cassandraSession = helperSetup(rowPresentInBaseTable);

            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getSetDataType()._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getSetDataType()._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(cassandraSession.session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(
                            cassandraSession.session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result = dbOperations.deleteFromMV(MvSyncRDDTest.getSetDataType()._2);
            helperVerify(result, rowPresentInBaseTable, cassandraSession);
        }
    }

    @Test
    public void deleteMVMapType() {
        List<Boolean> rowPresentInBaseTableVals = Arrays.asList(false, true);
        for (Boolean rowPresentInBaseTable : rowPresentInBaseTableVals) {
            CassandraSession cassandraSession = helperSetup(rowPresentInBaseTable);
            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getMapDataType()._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getMapDataType()._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(cassandraSession.session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(
                            cassandraSession.session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result = dbOperations.deleteFromMV(MvSyncRDDTest.getMapDataType()._2);
            helperVerify(result, rowPresentInBaseTable, cassandraSession);
        }
    }

    @Test
    public void deleteAllTypes() throws UnknownHostException, ParseException {
        List<Boolean> rowPresentInBaseTableVals = Arrays.asList(false, true);
        for (Boolean rowPresentInBaseTable : rowPresentInBaseTableVals) {
            CassandraSession cassandraSession = helperSetup(rowPresentInBaseTable);
            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(cassandraSession.session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(
                            cassandraSession.session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result = dbOperations.deleteFromMV(MvSyncRDDTest.getAllDataTypes()._2);
            helperVerify(result, rowPresentInBaseTable, cassandraSession);
        }
    }

    @Test
    public void deleteFromMVCassandraErrorDuringSelect() {
        CqlSession session = mock(CqlSession.class);
        PreparedStatement preparedStatementSel = mock(PreparedStatement.class);
        when(session.prepare(
                "SELECT * FROM test_ks.test_table WHERE ck1=? AND ck2=? AND pk=? ALLOW FILTERING"))
                .thenReturn(preparedStatementSel);
        BoundStatementBuilder boundStatementSel = mock(BoundStatementBuilder.class);
        when(preparedStatementSel.boundStatementBuilder()).thenReturn(boundStatementSel);
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(boundStatementSel.build()).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenThrow(new InvalidQueryException(null, "Invalid Query"));
        when(preparedStatementSel.getQuery())
                .thenReturn(
                        "SELECT * FROM test_ks.test_table WHERE ck1=? AND ck2=? AND pk=? ALLOW FILTERING");

        MVSyncSettings settings = new MVSyncSettings(getConf());
        TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
        tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getNativeDataTypes(20)._1.primaryKeys;
        tableAndMVColumns.mvNonPrimaryKeyColumns =
                MvSyncRDDTest.getNativeDataTypes(20)._1.nonPrimaryKeys;
        PreparedStatementHelper preparedStatementHelper =
                new PreparedStatementHelper(session, settings, tableAndMVColumns);
        DBOperations dbOperations =
                new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

        DBOperations.DBResult result =
                dbOperations.deleteFromMV(MvSyncRDDTest.getNativeDataTypes(20)._2);
        assertFalse(result.success);
        assertEquals(
                "Error executing select query: SELECT error msg: Invalid Query\n"
                        + "select query: SELECT * FROM test_ks.test_table WHERE ck1=? AND ck2=? AND pk=? ALLOW FILTERING\n"
                        + "mvTableEntry: CassandraRow{c3: 40, writetime(c1): 1704067200000000, c2: 30, writetime(c2): 1704067200000000, ttl(c1): null, c1: 20, ttl(c2): null, pk: Driver1, ck2: 2020, ttl(c3): 7200, ck1: SF, writetime(c3): 1704067200000000}\n"
                        + "primaryKeyColumnsMV: {ck1=TEXT, ck2=INT, pk=TEXT}\n"
                        + "consistencyLevel: LOCAL_QUORUM",
                result.errMessage);
    }

    @Test
    public void deleteFromMVCassandraErrorDuringDelete() {
        CqlSession session = mock(CqlSession.class);
        PreparedStatement preparedStatementSel = mock(PreparedStatement.class);
        when(session.prepare(
                "SELECT * FROM test_ks.test_table WHERE ck1=? AND ck2=? AND pk=? ALLOW FILTERING"))
                .thenReturn(preparedStatementSel);

        BoundStatementBuilder boundStatementSel = mock(BoundStatementBuilder.class);
        when(preparedStatementSel.boundStatementBuilder()).thenReturn(boundStatementSel);
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(boundStatementSel.build()).thenReturn(boundStatement);
        ResultSet rsSel = mock(ResultSet.class);
        Iterator<Row> rowIterableSel = mock(Iterator.class);
        when(session.execute(boundStatement)).thenReturn(rsSel);
        when(rsSel.iterator()).thenReturn(rowIterableSel);
        when(rowIterableSel.hasNext()).thenReturn(false);

        PreparedStatement preparedStatementDel = mock(PreparedStatement.class);
        when(session.prepare("DELETE FROM test_ks.test_mv WHERE ck1=? AND ck2=? AND pk=?"))
                .thenReturn(preparedStatementDel);
        BoundStatementBuilder boundStatementDel = mock(BoundStatementBuilder.class);
        when(preparedStatementDel.boundStatementBuilder()).thenReturn(boundStatementDel);
        BoundStatement boundStatement1 = mock(BoundStatement.class);
        when(boundStatementDel.build()).thenReturn(boundStatement1);
        when(session.execute(boundStatement1)).thenThrow(new InvalidQueryException(null, "Invalid query"));
        when(preparedStatementDel.getQuery())
                .thenReturn("DELETE FROM test_ks.test_mv WHERE ck1=? AND ck2=? AND pk=?");

        MVSyncSettings settings = new MVSyncSettings(getConf());
        TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
        tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getNativeDataTypes(20)._1.primaryKeys;
        tableAndMVColumns.mvNonPrimaryKeyColumns =
                MvSyncRDDTest.getNativeDataTypes(20)._1.nonPrimaryKeys;
        PreparedStatementHelper preparedStatementHelper =
                new PreparedStatementHelper(session, settings, tableAndMVColumns);
        DBOperations dbOperations =
                new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

        DBOperations.DBResult result =
                dbOperations.deleteFromMV(MvSyncRDDTest.getNativeDataTypes(20)._2);
        assertFalse(result.success);
        assertEquals(
                "Error deleting data: DELETE error msg: Invalid query\n"
                        + "delete query: DELETE FROM test_ks.test_mv WHERE ck1=? AND ck2=? AND pk=?\n"
                        + "mvTableEntry: CassandraRow{c3: 40, writetime(c1): 1704067200000000, c2: 30, writetime(c2): 1704067200000000, ttl(c1): null, c1: 20, ttl(c2): null, pk: Driver1, ck2: 2020, ttl(c3): 7200, ck1: SF, writetime(c3): 1704067200000000}\n"
                        + "primaryKeyColumnsMV: {ck1=TEXT, ck2=INT, pk=TEXT}\n"
                        + "consistencyLevel: LOCAL_QUORUM",
                result.errMessage);
    }

    static class UpsertBoundStatements {

        BoundStatementBuilder boundStatementC1;
        BoundStatementBuilder boundStatementC2;
        BoundStatementBuilder boundStatementC3;

        UpsertBoundStatements(
                BoundStatementBuilder boundStatementC1,
                BoundStatementBuilder boundStatementC2,
                BoundStatementBuilder boundStatementC3) {
            this.boundStatementC1 = boundStatementC1;
            this.boundStatementC2 = boundStatementC2;
            this.boundStatementC3 = boundStatementC3;
        }
    }

    private UpsertBoundStatements helperBoundStatementUpsertNativeType(
            CqlSession session, boolean useLatestTS) {
        PreparedStatement preparedStatementC1 = mock(PreparedStatement.class);
        if (useLatestTS) {
            when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,c1) VALUES (?,?,?,?)"))
                    .thenReturn(preparedStatementC1);
        } else {
            when(session.prepare(
                    "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c1) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                    .thenReturn(preparedStatementC1);
        }
        BoundStatementBuilder boundStatementC1 = mock(BoundStatementBuilder.class);
        when(preparedStatementC1.boundStatementBuilder()).thenReturn(boundStatementC1);

        PreparedStatement preparedStatementC2 = mock(PreparedStatement.class);
        if (useLatestTS) {
            when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,c2) VALUES (?,?,?,?)"))
                    .thenReturn(preparedStatementC2);
        } else {
            when(session.prepare(
                    "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c2) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                    .thenReturn(preparedStatementC2);
        }
        BoundStatementBuilder boundStatementC2 = mock(BoundStatementBuilder.class);
        when(preparedStatementC2.boundStatementBuilder()).thenReturn(boundStatementC2);

        PreparedStatement preparedStatementC3 = mock(PreparedStatement.class);
        if (useLatestTS) {
            when(session.prepare(
                    "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c3) VALUES (?,?,?,?) USING TTL ?"))
                    .thenReturn(preparedStatementC3);
        } else {
            when(session.prepare(
                    "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c3) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                    .thenReturn(preparedStatementC3);
        }
        BoundStatementBuilder boundStatementC3 = mock(BoundStatementBuilder.class);
        when(preparedStatementC3.boundStatementBuilder()).thenReturn(boundStatementC3);
        return new UpsertBoundStatements(boundStatementC1, boundStatementC2, boundStatementC3);
    }

    private void helperVerifyUpsertNativeType(
            BoundStatementBuilder boundStatement,
            String expectedColName,
            int expectedColVal,
            boolean useLatestTS) {
        ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeCodec<String>> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<TypeCodec<Integer>> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<Integer> columnIdxAC2 = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Long> columnValLongAC2 = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC3 =
                ArgumentCaptor.forClass(ConsistencyLevel.class);
        ArgumentCaptor<Integer> columnIdxAC4 = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> columnValAC4 = ArgumentCaptor.forClass(Integer.class);

        verify(boundStatement, times(2))
                .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
        verify(boundStatement, times(2))
                .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
        verify(boundStatement, times(useLatestTS ? 0 : 1))
                .setLong(columnIdxAC2.capture(), columnValLongAC2.capture());
        verify(boundStatement, times(expectedColName.equals("c3") ? 1 : 0))
                .setInt(columnIdxAC4.capture(), columnValAC4.capture());
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
        assertEquals(expectedColName, columnNameAC1IntType.getAllValues().get(1));
        assertEquals(Integer.valueOf(expectedColVal), columnValAC1IntType.getAllValues().get(1));
        assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(1));
        if (!useLatestTS) {
            assertEquals(4, columnIdxAC2.getAllValues().get(0).intValue());
            assertEquals(1704067200000000L, columnValLongAC2.getAllValues().get(0).longValue());
        }
        if (expectedColName.equals("c3")) {
            int ttlIndex = 4;
            if (!useLatestTS) {
                ttlIndex = 5;
            }
            assertEquals(ttlIndex, columnIdxAC4.getAllValues().get(0).intValue());
            assertEquals(7200, columnValAC4.getAllValues().get(0).intValue());
        }
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC3.getAllValues().get(0));
    }

    @Test
    public void upsertAllColumnsMVNativeType() {
        List<Boolean> useLatestTimestamps = Arrays.asList(false, true);

        for (boolean useLatestTS : useLatestTimestamps) {
            CqlSession session = mock(CqlSession.class);
            UpsertBoundStatements boundStatements =
                    helperBoundStatementUpsertNativeType(session, useLatestTS);

            SparkConf conf = getConf();
            conf.set(MVSyncSettings.PREFIX + ".mutation.uselatestts", String.valueOf(useLatestTS));
            MVSyncSettings settings = new MVSyncSettings(conf);
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getNativeDataTypes(20)._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns =
                    MvSyncRDDTest.getNativeDataTypes(20)._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result =
                    dbOperations.upsert(MvSyncRDDTest.getNativeDataTypes(20)._2, null);

            assertTrue(result.success);
            assertEquals("", result.errMessage);
            helperVerifyUpsertNativeType(boundStatements.boundStatementC1, "c1", 20, useLatestTS);
            helperVerifyUpsertNativeType(boundStatements.boundStatementC2, "c2", 30, useLatestTS);
            helperVerifyUpsertNativeType(boundStatements.boundStatementC3, "c3", 40, useLatestTS);
        }
    }

    @Test
    public void upsertAFewColumnsMVNativeType() {
        CqlSession session = mock(CqlSession.class);
        UpsertBoundStatements boundStatements = helperBoundStatementUpsertNativeType(session, false);

        MVSyncSettings settings = new MVSyncSettings(getConf());
        TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
        tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getNativeDataTypes(20)._1.primaryKeys;
        tableAndMVColumns.mvNonPrimaryKeyColumns =
                MvSyncRDDTest.getNativeDataTypes(20)._1.nonPrimaryKeys;
        PreparedStatementHelper preparedStatementHelper =
                new PreparedStatementHelper(session, settings, tableAndMVColumns);
        DBOperations dbOperations =
                new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

        DBOperations.DBResult result =
                dbOperations.upsert(
                        MvSyncRDDTest.getNativeDataTypes(20)._2, MvSyncRDDTest.getNativeDataTypes(21)._2);

        assertTrue(result.success);
        assertEquals("", result.errMessage);

        helperVerifyUpsertNativeType(boundStatements.boundStatementC1, "c1", 20, false);

        // 0 interactions for "c2" column as it is not modified
        ArgumentCaptor<String> columnNameAC1C2 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> columnValStringAC1C2 = ArgumentCaptor.forClass(String.class);
        verify(boundStatements.boundStatementC2, times(0))
                .setString(columnNameAC1C2.capture(), columnValStringAC1C2.capture());

        // 0 interactions for "c3" column as it is not modified
        ArgumentCaptor<String> columnNameAC1C3 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> columnValStringAC1C3 = ArgumentCaptor.forClass(String.class);
        verify(boundStatements.boundStatementC3, times(0))
                .setString(columnNameAC1C3.capture(), columnValStringAC1C3.capture());
    }

    private UpsertBoundStatements helperBoundStatementUpsertListType(CqlSession session) {
        PreparedStatement preparedStatementC1 = mock(PreparedStatement.class);
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c1) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(preparedStatementC1);
        BoundStatementBuilder boundStatementC1 = mock(BoundStatementBuilder.class);
        when(preparedStatementC1.boundStatementBuilder()).thenReturn(boundStatementC1);

        PreparedStatement preparedStatementC2 = mock(PreparedStatement.class);
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c2) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(preparedStatementC2);
        BoundStatementBuilder boundStatementC2 = mock(BoundStatementBuilder.class);
        when(preparedStatementC2.boundStatementBuilder()).thenReturn(boundStatementC2);

        PreparedStatement preparedStatementC3 = mock(PreparedStatement.class);
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,c3) VALUES (?,?,?,?)"))
                .thenReturn(preparedStatementC3);
        BoundStatementBuilder boundStatementC3 = mock(BoundStatementBuilder.class);
        when(preparedStatementC3.boundStatementBuilder()).thenReturn(boundStatementC3);
        return new UpsertBoundStatements(boundStatementC1, boundStatementC2, boundStatementC3);
    }

    private void helperVerifyUpsertListType(
            BoundStatementBuilder boundStatement,
            String expectedColName,
            Integer expectedColValInt,
            List<Integer> expectedColValList) {
        ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeCodec<String>> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<TypeCodec<Integer>> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<String> columnNameAC2 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List> columnValAC2 = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Class<Integer>> columnListElementTypeAC2 = ArgumentCaptor.forClass((Class<Class<Integer>>) (Class<?>) Class.class);
        ArgumentCaptor<Integer> columnIdxAC3 = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Long> columnValLongAC3 = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC4 =
                ArgumentCaptor.forClass(ConsistencyLevel.class);

        verify(boundStatement, times(2))
                .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
        verify(boundStatement, times(expectedColValInt != null ? 2 : 1))
                .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
        verify(boundStatement, times(expectedColValList != null ? 1 : 0))
                .setList(columnNameAC2.capture(), columnValAC2.capture(), columnListElementTypeAC2.capture());
        verify(boundStatement, times(expectedColValList != null ? 0 : 1))
                .setLong(columnIdxAC3.capture(), columnValLongAC3.capture());
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
        if (expectedColValInt != null) {
            assertEquals(expectedColName, columnNameAC1IntType.getAllValues().get(1));
            assertEquals(expectedColValInt, columnValAC1IntType.getAllValues().get(1));
            assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(1));
            assertEquals(4, columnIdxAC3.getAllValues().get(0).intValue());
            assertEquals(1704067200000000L, columnValLongAC3.getAllValues().get(0).longValue());
        } else {
            assertEquals(expectedColName, columnNameAC2.getAllValues().get(0));
            assertEquals(expectedColValList, columnValAC2.getAllValues().get(0));
            assertEquals(Integer.class, columnListElementTypeAC2.getAllValues().get(0));
        }
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC4.getAllValues().get(0));
    }

    @Test
    public void upsertMVListType() {
        boolean[] recordsMatch = {true, false};
        for (boolean recordMatches : recordsMatch) {
            CqlSession session = mock(CqlSession.class);
            UpsertBoundStatements boundStatements = helperBoundStatementUpsertListType(session);

            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getListDataType()._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getListDataType()._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result =
                    dbOperations.upsert(
                            MvSyncRDDTest.getListDataType()._2,
                            recordMatches ? MvSyncRDDTest.getListDataType()._2 : null);

            assertTrue(result.success);
            assertEquals("", result.errMessage);

            if (!recordMatches) {
                helperVerifyUpsertListType(boundStatements.boundStatementC1, "c1", new Integer(20), null);
                helperVerifyUpsertListType(boundStatements.boundStatementC2, "c2", new Integer(30), null);
                helperVerifyUpsertListType(
                        boundStatements.boundStatementC3, "c3", null, Arrays.asList(10, 20, 30));
            }
        }
    }

    private UpsertBoundStatements helperBoundStatementUpsertSetType(CqlSession session) {
        PreparedStatement preparedStatementC1 = mock(PreparedStatement.class);
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c1) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(preparedStatementC1);
        BoundStatementBuilder boundStatementC1 = mock(BoundStatementBuilder.class);
        when(preparedStatementC1.boundStatementBuilder()).thenReturn(boundStatementC1);

        PreparedStatement preparedStatementC2 = mock(PreparedStatement.class);
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c2) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(preparedStatementC2);
        BoundStatementBuilder boundStatementC2 = mock(BoundStatementBuilder.class);
        when(preparedStatementC2.boundStatementBuilder()).thenReturn(boundStatementC2);

        PreparedStatement preparedStatementC3 = mock(PreparedStatement.class);
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,c3) VALUES (?,?,?,?)"))
                .thenReturn(preparedStatementC3);
        BoundStatementBuilder boundStatementC3 = mock(BoundStatementBuilder.class);
        when(preparedStatementC3.boundStatementBuilder()).thenReturn(boundStatementC3);
        return new UpsertBoundStatements(boundStatementC1, boundStatementC2, boundStatementC3);
    }

    private void helperVerifyUpsertSetType(
            BoundStatementBuilder boundStatement,
            String expectedColName,
            Integer expectedColValInt,
            Set<Integer> expectedColValSet) {
        ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeCodec<String>> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<TypeCodec<Integer>> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<String> columnNameAC2 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set> columnValAC2 = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Class<Integer>> columnSetElementTypeAC2 = ArgumentCaptor.forClass((Class<Class<Integer>>) (Class<?>) Class.class);
        ArgumentCaptor<Integer> columnIdxAC3 = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Long> columnValLongAC3 = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC4 =
                ArgumentCaptor.forClass(ConsistencyLevel.class);

        verify(boundStatement, times(2))
                .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
        verify(boundStatement, times(expectedColValInt != null ? 2 : 1))
                .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
        verify(boundStatement, times(expectedColValSet != null ? 1 : 0))
                .setSet(columnNameAC2.capture(), columnValAC2.capture(), columnSetElementTypeAC2.capture());
        verify(boundStatement, times(expectedColValSet != null ? 0 : 1))
                .setLong(columnIdxAC3.capture(), columnValLongAC3.capture());
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
        if (expectedColValInt != null) {
            assertEquals(expectedColName, columnNameAC1IntType.getAllValues().get(1));
            assertEquals(expectedColValInt, columnValAC1IntType.getAllValues().get(1));
            assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(1));
            assertEquals(4, columnIdxAC3.getAllValues().get(0).intValue());
            assertEquals(1704067200000000L, columnValLongAC3.getAllValues().get(0).longValue());
        } else {
            assertEquals(expectedColName, columnNameAC2.getAllValues().get(0));
            assertEquals(expectedColValSet, columnValAC2.getAllValues().get(0));
            assertEquals(Integer.class, columnSetElementTypeAC2.getAllValues().get(0));
        }
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC4.getAllValues().get(0));
    }

    @Test
    public void upsertMVSetType() {
        boolean[] recordsMatch = {true, false};
        for (boolean recordMatches : recordsMatch) {
            CqlSession session = mock(CqlSession.class);
            UpsertBoundStatements boundStatements = helperBoundStatementUpsertSetType(session);

            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getSetDataType()._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getSetDataType()._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result =
                    dbOperations.upsert(
                            MvSyncRDDTest.getSetDataType()._2,
                            recordMatches ? MvSyncRDDTest.getSetDataType()._2 : null);

            assertTrue(result.success);
            assertEquals("", result.errMessage);

            if (!recordMatches) {
                helperVerifyUpsertSetType(boundStatements.boundStatementC1, "c1", new Integer(20), null);
                helperVerifyUpsertSetType(boundStatements.boundStatementC2, "c2", new Integer(30), null);
                helperVerifyUpsertSetType(
                        boundStatements.boundStatementC3, "c3", null, new TreeSet<>(Arrays.asList(11, 22, 33)));
            }
        }
    }

    private UpsertBoundStatements helperBoundStatementUpsertMapType(CqlSession session) {
        PreparedStatement preparedStatementC1 = mock(PreparedStatement.class);
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c1) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(preparedStatementC1);
        BoundStatementBuilder boundStatementC1 = mock(BoundStatementBuilder.class);
        when(preparedStatementC1.boundStatementBuilder()).thenReturn(boundStatementC1);

        PreparedStatement preparedStatementC2 = mock(PreparedStatement.class);
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c2) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(preparedStatementC2);
        BoundStatementBuilder boundStatementC2 = mock(BoundStatementBuilder.class);
        when(preparedStatementC2.boundStatementBuilder()).thenReturn(boundStatementC2);

        PreparedStatement preparedStatementC3 = mock(PreparedStatement.class);
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,c3) VALUES (?,?,?,?)"))
                .thenReturn(preparedStatementC3);
        BoundStatementBuilder boundStatementC3 = mock(BoundStatementBuilder.class);
        when(preparedStatementC3.boundStatementBuilder()).thenReturn(boundStatementC3);
        return new UpsertBoundStatements(boundStatementC1, boundStatementC2, boundStatementC3);
    }

    private void helperVerifyUpsertMapType(
            BoundStatementBuilder boundStatement,
            String expectedColName,
            Integer expectedColValInt,
            Map<Integer, Integer> expectedColValMap) {
        ArgumentCaptor<String> columnNameAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> columnValAC1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeCodec<String>> columnDataTypeAC1 = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<String> columnNameAC1IntType = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> columnValAC1IntType = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<TypeCodec<Integer>> columnDataTypeAC1IntType = ArgumentCaptor.forClass(TypeCodec.class);
        ArgumentCaptor<String> columnNameAC2 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> columnValAC2 = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Class<Integer>> columnMapElementKeyTypeAC2 = ArgumentCaptor.forClass((Class<Class<Integer>>) (Class<?>) Class.class);
        ArgumentCaptor<Class<Integer>> columnMapElementValTypeAC2 = ArgumentCaptor.forClass((Class<Class<Integer>>) (Class<?>) Class.class);
        ArgumentCaptor<Integer> columnIdxAC3 = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Long> columnValLongAC3 = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ConsistencyLevel> columnValConsistencyLevelAC4 =
                ArgumentCaptor.forClass(ConsistencyLevel.class);

        verify(boundStatement, times(2))
                .set(columnNameAC1.capture(), columnValAC1.capture(), columnDataTypeAC1.capture());
        verify(boundStatement, times(expectedColValInt != null ? 2 : 1))
                .set(columnNameAC1IntType.capture(), columnValAC1IntType.capture(), columnDataTypeAC1IntType.capture());
        verify(boundStatement, times(expectedColValMap != null ? 1 : 0))
                .setMap(columnNameAC2.capture(), columnValAC2.capture(), columnMapElementKeyTypeAC2.capture(), columnMapElementValTypeAC2.capture());
        verify(boundStatement, times(expectedColValMap != null ? 0 : 1))
                .setLong(columnIdxAC3.capture(), columnValLongAC3.capture());
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
        if (expectedColValInt != null) {
            assertEquals(expectedColName, columnNameAC1IntType.getAllValues().get(1));
            assertEquals(expectedColValInt, columnValAC1IntType.getAllValues().get(1));
            assertEquals(TypeCodecs.INT, columnDataTypeAC1IntType.getAllValues().get(1));
            assertEquals(4, columnIdxAC3.getAllValues().get(0).intValue());
            assertEquals(1704067200000000L, columnValLongAC3.getAllValues().get(0).longValue());
        } else {
            assertEquals(expectedColName, columnNameAC2.getAllValues().get(0));
            assertEquals(expectedColValMap, columnValAC2.getAllValues().get(0));
            assertEquals(Integer.class, columnMapElementKeyTypeAC2.getAllValues().get(0));
            assertEquals(Integer.class, columnMapElementValTypeAC2.getAllValues().get(0));
        }
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, columnValConsistencyLevelAC4.getAllValues().get(0));
    }

    @Test
    public void upsertMVMapType() {
        boolean[] recordsMatch = {true, false};
        for (boolean recordMatches : recordsMatch) {
            CqlSession session = mock(CqlSession.class);
            UpsertBoundStatements boundStatements = helperBoundStatementUpsertMapType(session);

            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getMapDataType()._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getMapDataType()._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result =
                    dbOperations.upsert(
                            MvSyncRDDTest.getMapDataType()._2,
                            recordMatches ? MvSyncRDDTest.getMapDataType()._2 : null);

            assertTrue(result.success);
            assertEquals("", result.errMessage);
            if (!recordMatches) {
                helperVerifyUpsertMapType(boundStatements.boundStatementC1, "c1", new Integer(20), null);
                helperVerifyUpsertMapType(boundStatements.boundStatementC2, "c2", new Integer(30), null);
                helperVerifyUpsertMapType(
                        boundStatements.boundStatementC3,
                        "c3",
                        null,
                        new TreeMap<>() {
                            {
                                put(1, 10);
                                put(2, 20);
                                put(3, 30);
                            }
                        });
            }
        }
    }

    @Test
    public void upsertAllDataTypes() throws UnknownHostException, ParseException {
        boolean[] recordsMatch = {true, false};
        for (boolean recordMatches : recordsMatch) {
            CqlSession session = mock(CqlSession.class);
            BoundStatementBuilder boundStatementAsciiCol = MvSyncRDDTest.helperBoundStatementAsciiCol(session);
            BoundStatementBuilder boundStatementBigintCol = MvSyncRDDTest.helperBoundStatementBigintCol(session);
            BoundStatementBuilder boundStatementBlobCol = MvSyncRDDTest.helperBoundStatementBlobCol(session);
            BoundStatementBuilder boundStatementBooleanCol =
                    MvSyncRDDTest.helperBoundStatementBooleanCol(session);
            BoundStatementBuilder boundStatementDateCol = MvSyncRDDTest.helperBoundStatementDateCol(session);
            BoundStatementBuilder boundStatementDecimalCol =
                    MvSyncRDDTest.helperBoundStatementDecimalCol(session);
            BoundStatementBuilder boundStatementDoubleCol = MvSyncRDDTest.helperBoundStatementDoubleCol(session);
            BoundStatementBuilder boundStatementFloatCol = MvSyncRDDTest.helperBoundStatementFloatCol(session);
            BoundStatementBuilder boundStatementInetCol = MvSyncRDDTest.helperBoundStatementInetCol(session);
            BoundStatementBuilder boundStatementIntCol = MvSyncRDDTest.helperBoundStatementIntCol(session);
            BoundStatementBuilder boundStatementListCol = MvSyncRDDTest.helperBoundStatementListCol(session);
            BoundStatementBuilder boundStatementMapCol = MvSyncRDDTest.helperBoundStatementMapCol(session);
            BoundStatementBuilder boundStatementSetCol = MvSyncRDDTest.helperBoundStatementSetCol(session);
            BoundStatementBuilder boundStatementSmallintCol =
                    MvSyncRDDTest.helperBoundStatementSmallintCol(session);
            BoundStatementBuilder boundStatementTextCol = MvSyncRDDTest.helperBoundStatementTextCol(session);
            BoundStatementBuilder boundStatementTimeCol = MvSyncRDDTest.helperBoundStatementTimeCol(session);
            BoundStatementBuilder boundStatementTimestampCol =
                    MvSyncRDDTest.helperBoundStatementTimestampCol(session);
            BoundStatementBuilder boundStatementTimeUUIDCol =
                    MvSyncRDDTest.helperBoundStatementTimeUUIDCol(session);
            BoundStatementBuilder boundStatementTinyintCol =
                    MvSyncRDDTest.helperBoundStatementTinyintCol(session);
            BoundStatementBuilder boundStatementUUIDCol = MvSyncRDDTest.helperBoundStatementUUIDCol(session);
            BoundStatementBuilder boundStatementVarcharCol =
                    MvSyncRDDTest.helperBoundStatementVarcharCol(session);
            BoundStatementBuilder boundStatementVarintCol = MvSyncRDDTest.helperBoundStatementVarintCol(session);

            MVSyncSettings settings = new MVSyncSettings(getConf());
            TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
            tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.primaryKeys;
            tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.nonPrimaryKeys;
            PreparedStatementHelper preparedStatementHelper =
                    new PreparedStatementHelper(session, settings, tableAndMVColumns);
            DBOperations dbOperations =
                    new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

            DBOperations.DBResult result =
                    dbOperations.upsert(
                            MvSyncRDDTest.getAllDataTypes()._2,
                            recordMatches ? MvSyncRDDTest.getAllDataTypes()._2 : null);

            assertTrue(result.success);
            assertEquals("", result.errMessage);

            if (!recordMatches) {
                MvSyncRDDTest.helperVerifyAsciiCol(boundStatementAsciiCol);
                MvSyncRDDTest.helperVerifyBigintCol(boundStatementBigintCol);
                MvSyncRDDTest.helperVerifyBlobCol(boundStatementBlobCol);
                MvSyncRDDTest.helperVerifyBooleanCol(boundStatementBooleanCol);
                MvSyncRDDTest.helperVerifyDateCol(boundStatementDateCol);
                MvSyncRDDTest.helperVerifyDecimalCol(boundStatementDecimalCol);
                MvSyncRDDTest.helperVerifyDoubleCol(boundStatementDoubleCol);
                MvSyncRDDTest.helperVerifyFloatCol(boundStatementFloatCol);
                MvSyncRDDTest.helperVerifyInetCol(boundStatementInetCol);
                MvSyncRDDTest.helperVerifyIntCol(boundStatementIntCol);
                MvSyncRDDTest.helperVerifyListCol(boundStatementListCol);
                MvSyncRDDTest.helperVerifyMapCol(boundStatementMapCol);
                MvSyncRDDTest.helperVerifySetCol(boundStatementSetCol);
                MvSyncRDDTest.helperVerifySmallintCol(boundStatementSmallintCol);
                MvSyncRDDTest.helperVerifyTextCol(boundStatementTextCol);
                MvSyncRDDTest.helperVerifyTimeCol(boundStatementTimeCol);
                MvSyncRDDTest.helperVerifyTimestampCol(boundStatementTimestampCol);
                MvSyncRDDTest.helperVerifyTimeUUIDCol(boundStatementTimeUUIDCol);
                MvSyncRDDTest.helperVerifyTinyintCol(boundStatementTinyintCol);
                MvSyncRDDTest.helperVerifyUUIDCol(boundStatementUUIDCol);
                MvSyncRDDTest.helperVerifyVarcharCol(boundStatementVarcharCol);
                MvSyncRDDTest.helperVerifyVarintCol(boundStatementVarintCol);
            }
        }
    }

    @Test
    public void upsertCassandraError() {
        CqlSession session = mock(CqlSession.class);
        PreparedStatement preparedStatementC1 = mock(PreparedStatement.class);
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c1) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(preparedStatementC1);
        BoundStatementBuilder boundStatementC1 = mock(BoundStatementBuilder.class);
        when(preparedStatementC1.boundStatementBuilder()).thenReturn(boundStatementC1);
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(boundStatementC1.build()).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenThrow(new InvalidQueryException(null, "Invalid query"));
        when(preparedStatementC1.getQuery())
                .thenReturn(
                        "INSERT INTO test_ks.test_mv (ck1,ck2,pk,c1) VALUES (?,?,?,?) USING TIMESTAMP ?");

        MVSyncSettings settings = new MVSyncSettings(getConf());
        TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
        tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getMapDataType()._1.primaryKeys;
        tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getMapDataType()._1.nonPrimaryKeys;
        PreparedStatementHelper preparedStatementHelper =
                new PreparedStatementHelper(session, settings, tableAndMVColumns);
        DBOperations dbOperations =
                new DBOperations(session, preparedStatementHelper, settings, tableAndMVColumns);

        DBOperations.DBResult result = dbOperations.upsert(MvSyncRDDTest.getMapDataType()._2, null);

        assertFalse(result.success);
        assertEquals(
                "Error upserting data: INSERT error msg: Invalid query\n"
                        + "insert query: INSERT INTO test_ks.test_mv (ck1,ck2,pk,c1) VALUES (?,?,?,?) USING TIMESTAMP ?\n"
                        + "baseTableEntry: CassandraRow{c3: Map(1 -> 10, 2 -> 20, 3 -> 30), writetime(c1): 1704067200000000, c2: 30, writetime(c2): 1704067200000000, ttl(c1): null, c1: 20, ttl(c2): null, pk: Driver1, ck2: 2020, ck1: SF}\n"
                        + "primaryKeyColumnsMV: {ck1=TEXT, ck2=INT, pk=TEXT}\n"
                        + "nonPrimaryKeyColumnsMV: {c1=INT, c2=INT, c3=MAP}consistencyLevel: LOCAL_QUORUM",
                result.errMessage);
    }

    @Test
    public void leastAndMostTimestampOfACassandraRow() throws Exception {
        Tuple2<Long, Long> leastAndMostModificationTS =
                MvSync.getTheLeastAndMostModificationTimeInMicroSeconds(
                        MvSyncRDDTest.getAllDataTypes()._2, MvSyncRDDTest.getAllDataTypes()._1.nonPrimaryKeys);
        assertEquals(1704067200000000L, leastAndMostModificationTS._1.longValue());
        assertEquals(1704067200000018L, leastAndMostModificationTS._2.longValue());
    }

    @Test(expected = Exception.class)
    public void leastAndMostTimestampOfACassandraRowError() throws Exception {
        MvSync.getTheLeastAndMostModificationTimeInMicroSeconds(
                MvSyncRDDTest.getRowWriteTimeMissing()._2,
                MvSyncRDDTest.getRowWriteTimeMissing()._1.nonPrimaryKeys);
    }

    private void helperE2ETesting(
            String keyspaceName,
            String baseTableName,
            String mvName,
            String endTsInSec,
            MvSyncRDDTest.TestRDDType type)
            throws Exception {
        for (MvSyncRDDTest.Schema schema : schemas) {
            mockSchema();
            CassandraClient.setSession(session);
            SparkConf conf = new SparkConf();
            conf.setAppName("E2ETest");
            conf.set("spark.master", "local");
            conf.set("spark.driver.allowMultipleContexts", "true");
            conf.set(MVSyncSettings.PREFIX + ".keyspace", keyspaceName);
            conf.set(MVSyncSettings.PREFIX + ".basetablename", baseTableName);
            conf.set(MVSyncSettings.PREFIX + ".mvname", mvName);
            conf.set(MVSyncSettings.PREFIX + ".starttsinsec", "0");
            conf.set(MVSyncSettings.PREFIX + ".endtsinsec", endTsInSec);
            conf.set("spark.cassandra.auth.password", "test121");

            MvSync mvSync = new MvSyncRDDTest(type, schema);
            MvSyncTest.terraBlobStreamerStats = mock(IBlobStreamer.class);
            MvSyncTest.MVJobOutputStreamer.streamerMissingInBaseTable = mock(IBlobStreamer.class);
            MvSyncTest.MVJobOutputStreamer.streamerMissingInMv = mock(IBlobStreamer.class);
            MvSyncTest.MVJobOutputStreamer.streamerMismatch = mock(IBlobStreamer.class);
            mvSync.buildAndRunSparkJob(new MVSyncSettings(conf));

            ArgumentCaptor<String> argMissingInBaseTable = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> argMissingInMvTable = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> argMissingInInconsistent = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> argStats = ArgumentCaptor.forClass(String.class);

            if (type == MvSyncRDDTest.TestRDDType.SKIP_RECORD_IN_MV) {
                verify(MvSyncTest.MVJobOutputStreamer.streamerMissingInMv, times(2))
                        .append(argMissingInMvTable.capture());
            }
            if (type == MvSyncRDDTest.TestRDDType.SKIP_RECORD_IN_BASE_TABLE) {
                verify(MvSyncTest.MVJobOutputStreamer.streamerMissingInBaseTable, times(2))
                        .append(argMissingInBaseTable.capture());
            }
            if (type == MvSyncRDDTest.TestRDDType.MV_MISMATCH) {
                verify(MvSyncTest.MVJobOutputStreamer.streamerMismatch, times(2))
                        .append(argMissingInInconsistent.capture());
            }
            verify(MvSyncTest.terraBlobStreamerStats, times(1)).append(argStats.capture());
            if (type == MvSyncRDDTest.TestRDDType.SKIP_RECORD_IN_MV) {
                if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                    assertEquals(
                            "Problem: MISSING_IN_MV_TABLE\n"
                                    + "RowKey: c1:INT:11,ck1:ASCII:NY,ck2:INT:2021,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}\n"
                                    + "MVTableEntry: null",
                            argMissingInMvTable.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                    assertEquals(
                            "Problem: MISSING_IN_MV_TABLE\n"
                                    + "RowKey: c1:INT:11,ck1:TIMESTAMP:1708230896002,ck2:DOUBLE:12.56,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}\n"
                                    + "MVTableEntry: null",
                            argMissingInMvTable.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                    assertEquals(
                            "Problem: MISSING_IN_MV_TABLE\n"
                                    + "RowKey: c1:INT:11,ck1:BOOLEAN:false,ck2:FLOAT:1.09,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}\n"
                                    + "MVTableEntry: null",
                            argMissingInMvTable.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                    assertEquals(
                            "Problem: MISSING_IN_MV_TABLE\n"
                                    + "RowKey: c1:INT:11,ck1:UUID:7309362c-4237-4e5d-b403-505820caba42,ck2:DECIMAL:100.06,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}\n"
                                    + "MVTableEntry: null",
                            argMissingInMvTable.getAllValues().get(0));
                }
                assertEquals("==============================", argMissingInMvTable.getAllValues().get(1));
                assertEquals(
                        "totRecords: 2, skippedRecords: 0, consistentRecords: 1, inConsistentRecords: 0, missingBaseTableRecords: 0, missingMvRecords: 1, repairRecords: 0, notRepairRecords: 1, delAttemptedRecords: 0, delErrRecords: 0, delSuccessRecords: 0, notDelRecords: 0, upsertAttemptedRecords: 0, upsertErrRecords: 0, upsertSuccessRecords: 0",
                        argStats.getAllValues().get(0));
            }

            if (type == MvSyncRDDTest.TestRDDType.SKIP_RECORD_IN_BASE_TABLE) {
                if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                    assertEquals(
                            "Problem: MISSING_IN_BASE_TABLE\n"
                                    + "RowKey: c1:INT:11,ck1:ASCII:NY,ck2:INT:2021,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: null\n"
                                    + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}",
                            argMissingInBaseTable.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                    assertEquals(
                            "Problem: MISSING_IN_BASE_TABLE\n"
                                    + "RowKey: c1:INT:11,ck1:TIMESTAMP:1708230896002,ck2:DOUBLE:12.56,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: null\n"
                                    + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}",
                            argMissingInBaseTable.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                    assertEquals(
                            "Problem: MISSING_IN_BASE_TABLE\n"
                                    + "RowKey: c1:INT:11,ck1:BOOLEAN:false,ck2:FLOAT:1.09,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: null\n"
                                    + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}",
                            argMissingInBaseTable.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                    assertEquals(
                            "Problem: MISSING_IN_BASE_TABLE\n"
                                    + "RowKey: c1:INT:11,ck1:UUID:7309362c-4237-4e5d-b403-505820caba42,ck2:DECIMAL:100.06,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: null\n"
                                    + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}",
                            argMissingInBaseTable.getAllValues().get(0));
                }
                assertEquals("==============================", argMissingInBaseTable.getAllValues().get(1));
                assertEquals(
                        "totRecords: 2, skippedRecords: 0, consistentRecords: 1, inConsistentRecords: 0, missingBaseTableRecords: 1, missingMvRecords: 0, repairRecords: 0, notRepairRecords: 1, delAttemptedRecords: 0, delErrRecords: 0, delSuccessRecords: 0, notDelRecords: 0, upsertAttemptedRecords: 0, upsertErrRecords: 0, upsertSuccessRecords: 0",
                        argStats.getAllValues().get(0));
            }

            if (type == MvSyncRDDTest.TestRDDType.MV_MISMATCH) {
                if (schema == MvSyncRDDTest.Schema.PK_STRING_AND_INTEGER) {
                    assertEquals(
                            "Problem: INCONSISTENT\n"
                                    + "RowKey: c1:INT:11,ck1:ASCII:NY,ck2:INT:2021,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}\n"
                                    + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 441, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 2021, ttl(c3): null, ck1: NY, writetime(c3): 1704153600000000}\n"
                                    + "BaseColumn: c4:INT:44\n"
                                    + "MvColumn: c4:INT:441",
                            argMissingInInconsistent.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_DOUBLE_AND_TIMESTAMP) {
                    assertEquals(
                            "Problem: INCONSISTENT\n"
                                    + "RowKey: c1:INT:11,ck1:TIMESTAMP:1708230896002,ck2:DOUBLE:12.56,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}\n"
                                    + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 441, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 12.56, ttl(c3): null, ck1: 2024-02-18 04:34:56+0000, writetime(c3): 1704153600000000}\n"
                                    + "BaseColumn: c4:INT:44\n"
                                    + "MvColumn: c4:INT:441",
                            argMissingInInconsistent.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_BOOLEAN_AND_FLOAT) {
                    assertEquals(
                            "Problem: INCONSISTENT\n"
                                    + "RowKey: c1:INT:11,ck1:BOOLEAN:false,ck2:FLOAT:1.09,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}\n"
                                    + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 441, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 1.09, ttl(c3): null, ck1: false, writetime(c3): 1704153600000000}\n"
                                    + "BaseColumn: c4:INT:44\n"
                                    + "MvColumn: c4:INT:441",
                            argMissingInInconsistent.getAllValues().get(0));
                }
                if (schema == MvSyncRDDTest.Schema.PK_UUID_AND_DECIMAL) {
                    assertEquals(
                            "Problem: INCONSISTENT\n"
                                    + "RowKey: c1:INT:11,ck1:UUID:7309362c-4237-4e5d-b403-505820caba42,ck2:DECIMAL:100.06,pk:ASCII:Driver2\n"
                                    + "MainTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 44, writetime(c4): 1704153600000000, writetime(c1): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, ttl(c1): null, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}\n"
                                    + "MVTableEntry: CassandraRow{ttl(c4): null, c3: 33, c4: 441, writetime(c4): 1704153600000000, c2: 22, writetime(c2): 1704153600000000, c1: 11, ttl(c2): null, pk: Driver2, ck2: 100.06, ttl(c3): null, ck1: 7309362c-4237-4e5d-b403-505820caba42, writetime(c3): 1704153600000000}\n"
                                    + "BaseColumn: c4:INT:44\n"
                                    + "MvColumn: c4:INT:441",
                            argMissingInInconsistent.getAllValues().get(0));
                }
                assertEquals(
                        "==============================", argMissingInInconsistent.getAllValues().get(1));
                assertEquals(
                        "totRecords: 2, skippedRecords: 0, consistentRecords: 1, inConsistentRecords: 1, missingBaseTableRecords: 0, missingMvRecords: 0, repairRecords: 0, notRepairRecords: 1, delAttemptedRecords: 0, delErrRecords: 0, delSuccessRecords: 0, notDelRecords: 0, upsertAttemptedRecords: 0, upsertErrRecords: 0, upsertSuccessRecords: 0",
                        argStats.getAllValues().get(0));
            }
        }
    }

    @Test
    public void shouldSkipField() {
        CassandraRow baseTableRow = MvSyncRDDTest.getNativeDataTypes(20)._2;
        CassandraRow mvTableRow = MvSyncRDDTest.getNativeDataTypes(21)._2;

        assertFalse(DBOperations.shouldSkipField(baseTableRow, mvTableRow, "c1"));
        assertFalse(DBOperations.shouldSkipField(baseTableRow, null, "c1"));

        assertTrue(DBOperations.shouldSkipField(baseTableRow, mvTableRow, "c2"));
        assertFalse(DBOperations.shouldSkipField(baseTableRow, null, "c2"));

        assertTrue(DBOperations.shouldSkipField(baseTableRow, mvTableRow, "c3"));
        assertFalse(DBOperations.shouldSkipField(baseTableRow, null, "c3"));
    }

    @Test
    public void supportedTypesForAutomaticFixingInconsistencies() {
        assertTrue(DBOperations.isSupportedType("UUID"));
        assertTrue(DBOperations.isSupportedType("INT"));
        assertTrue(DBOperations.isSupportedType("INET"));
        assertTrue(DBOperations.isSupportedType("LIST"));
        assertTrue(DBOperations.isSupportedType("TIMEUUID"));
        assertTrue(DBOperations.isSupportedType("VARINT"));
        assertTrue(DBOperations.isSupportedType("BIGINT"));
        assertTrue(DBOperations.isSupportedType("BOOLEAN"));
        assertTrue(DBOperations.isSupportedType("ASCII"));
        assertTrue(DBOperations.isSupportedType("DATE"));
        assertTrue(DBOperations.isSupportedType("BLOB"));
        assertTrue(DBOperations.isSupportedType("FLOAT"));
        assertTrue(DBOperations.isSupportedType("TEXT"));
        assertTrue(DBOperations.isSupportedType("SET"));
        assertTrue(DBOperations.isSupportedType("SMALLINT"));
        assertTrue(DBOperations.isSupportedType("TIMESTAMP"));
        assertTrue(DBOperations.isSupportedType("MAP"));
        assertTrue(DBOperations.isSupportedType("TINYINT"));
        assertTrue(DBOperations.isSupportedType("TEXT"));
        assertTrue(DBOperations.isSupportedType("TIME"));
        assertTrue(DBOperations.isSupportedType("DOUBLE"));
        assertTrue(DBOperations.isSupportedType("DECIMAL"));

        assertFalse(DBOperations.isSupportedType("TUPLE"));
        assertFalse(DBOperations.isSupportedType("DURATION"));
    }

    @Test
    public void checkForUnsupportedTypes() throws Exception {
        Map<String, String> m = new TreeMap<>();
        m.put("c1", "FLOAT");
        m.put("c2", "INT");
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.set(MVSyncSettings.PREFIX + ".fixmissingmv", "true");

        // no error
        MvSync.checkForUnsupportedTypesForAutomaticFixingInconsistencies(
                m, new MVSyncSettings(sparkConf));

        // it should throw an error, but the automated fixing the inconsistency indicator is disabled
        sparkConf.set(MVSyncSettings.PREFIX + ".fixmissingmv", "false");
        sparkConf.set(MVSyncSettings.PREFIX + ".fixinconsistentmv", "false");
        m.put("c3", "TUPLE");
        MvSync.checkForUnsupportedTypesForAutomaticFixingInconsistencies(
                m, new MVSyncSettings(sparkConf));

        // throw an error
        sparkConf.set(MVSyncSettings.PREFIX + ".fixmissingmv", "true");
        sparkConf.set(MVSyncSettings.PREFIX + ".fixinconsistentmv", "false");
        m.put("c3", "TUPLE");
        try {
            MvSync.checkForUnsupportedTypesForAutomaticFixingInconsistencies(
                    m, new MVSyncSettings(sparkConf));
            fail("an exception should be thrown");
        } catch (Exception e) {
            assertEquals(
                    "Cannot do an automated fixing of inconsistencies for the unsupported type: TUPLE",
                    e.getMessage());
        }

        // throw an error
        sparkConf.set(MVSyncSettings.PREFIX + ".fixmissingmv", "false");
        sparkConf.set(MVSyncSettings.PREFIX + ".fixinconsistentmv", "true");
        m.remove("c3");
        m.put("c3", "DURATION");
        try {
            MvSync.checkForUnsupportedTypesForAutomaticFixingInconsistencies(
                    m, new MVSyncSettings(sparkConf));
            fail("an exception should be thrown");
        } catch (Exception e) {
            assertEquals(
                    "Cannot do an automated fixing of inconsistencies for the unsupported type: DURATION",
                    e.getMessage());
        }
    }

    @Test
    public void dataMatchForUUIDType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getUUIDDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getUUIDDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo(
                        "uuid_col:UUID", UUID.fromString("550e8400-e29b-11d4-a716-446655440000").toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo(
                        "uuid_col:UUID", UUID.fromString("550e8400-e29b-11d4-a716-446655440001").toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getUUIDDataTypes1()._2,
                        MvSyncRDDTest.getUUIDDataTypes1()._2,
                        MvSyncRDDTest.getUUIDDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForIntType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getIntDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getIntDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(new RecordColumnInfo("int_col:INT", new Integer(10).toString()), result._1);
        assertEquals(new RecordColumnInfo("int_col:INT", new Integer(11).toString()), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getIntDataTypes1()._2,
                        MvSyncRDDTest.getIntDataTypes1()._2,
                        MvSyncRDDTest.getIntDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForInetType() throws UnknownHostException {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getInetDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getInetDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo("inet_col:INET", InetAddress.getByName("192.168.0.1").toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo("inet_col:INET", InetAddress.getByName("192.168.0.2").toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getInetDataTypes1()._2,
                        MvSyncRDDTest.getInetDataTypes1()._2,
                        MvSyncRDDTest.getInetDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForListType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getListDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getListDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo(
                        "list_col:LIST",
                        JavaConverters.asScalaBuffer(Arrays.asList(100, 200, 300)).toList().toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo(
                        "list_col:LIST",
                        JavaConverters.asScalaBuffer(Arrays.asList(101, 201, 301)).toList().toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getListDataTypes1()._2,
                        MvSyncRDDTest.getListDataTypes1()._2,
                        MvSyncRDDTest.getListDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForTimeUUIDType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getTimeUUIDDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getTimeUUIDDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo(
                        "timeuuid_col:TIMEUUID",
                        UUID.fromString("550e8400-e29b-11d4-a716-446655440000").toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo(
                        "timeuuid_col:TIMEUUID",
                        UUID.fromString("550e8400-e29b-11d4-a716-446655440001").toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getTimeUUIDDataTypes1()._2,
                        MvSyncRDDTest.getTimeUUIDDataTypes1()._2,
                        MvSyncRDDTest.getTimeUUIDDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForVarIntType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getVarIntDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getVarIntDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo("varint_col:VARINT", new BigInteger("20").toString()), result._1);
        assertEquals(
                new RecordColumnInfo("varint_col:VARINT", new BigInteger("21").toString()), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getVarIntDataTypes1()._2,
                        MvSyncRDDTest.getVarIntDataTypes1()._2,
                        MvSyncRDDTest.getVarIntDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForBigIntType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getBigIntDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getBigIntDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo("bigint_col:BIGINT", new Integer(5000).toString()), result._1);
        assertEquals(
                new RecordColumnInfo("bigint_col:BIGINT", new Integer(5001).toString()), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getBigIntDataTypes1()._2,
                        MvSyncRDDTest.getBigIntDataTypes1()._2,
                        MvSyncRDDTest.getBigIntDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForBooleanType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getBooleanDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getBooleanDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo("boolean_col:BOOLEAN", new Boolean(true).toString()), result._1);
        assertEquals(
                new RecordColumnInfo("boolean_col:BOOLEAN", new Boolean(false).toString()), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getBooleanDataTypes1()._2,
                        MvSyncRDDTest.getBooleanDataTypes1()._2,
                        MvSyncRDDTest.getBooleanDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForASCIIType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getAsciiDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getAsciiDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(new RecordColumnInfo("ascii_col:ASCII", new String("hello world!")), result._1);
        assertEquals(new RecordColumnInfo("ascii_col:ASCII", new String("hi world!")), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getAsciiDataTypes1()._2,
                        MvSyncRDDTest.getAsciiDataTypes1()._2,
                        MvSyncRDDTest.getAsciiDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchFoDateType() throws ParseException {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getDateDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getDateDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals("date_col:DATE:1669852800000", result._1.toString());
        assertEquals("date_col:DATE:1669939200000", result._2.toString());

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getDateDataTypes1()._2,
                        MvSyncRDDTest.getDateDataTypes1()._2,
                        MvSyncRDDTest.getDateDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForBlobType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getBlobDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getBlobDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo(
                        "blob_col:BLOB",
                        Charset.forName("UTF-8")
                                .decode(ByteBuffer.wrap("Hello, Cassandra!".getBytes()))
                                .toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo(
                        "blob_col:BLOB",
                        Charset.forName("UTF-8")
                                .decode(ByteBuffer.wrap("Hi, Cassandra!".getBytes()))
                                .toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getBlobDataTypes1()._2,
                        MvSyncRDDTest.getBlobDataTypes1()._2,
                        MvSyncRDDTest.getBlobDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForFloatType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getFloatDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getFloatDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(new RecordColumnInfo("float_col:FLOAT", new Float(10.50f).toString()), result._1);
        assertEquals(new RecordColumnInfo("float_col:FLOAT", new Float(10.51f).toString()), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getFloatDataTypes1()._2,
                        MvSyncRDDTest.getFloatDataTypes1()._2,
                        MvSyncRDDTest.getFloatDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForVarCharType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getVarcharDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getVarcharDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(new RecordColumnInfo("varchar_col:TEXT", new String("hello123")), result._1);
        assertEquals(new RecordColumnInfo("varchar_col:TEXT", new String("hello456")), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getVarcharDataTypes1()._2,
                        MvSyncRDDTest.getVarcharDataTypes1()._2,
                        MvSyncRDDTest.getVarcharDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForSetType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getSetDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getSetDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo(
                        "set_col:SET",
                        JavaConverters.asScalaBuffer(Arrays.asList(111, 222, 333)).toSet().toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo(
                        "set_col:SET",
                        JavaConverters.asScalaBuffer(Arrays.asList(1110, 2220, 3330)).toSet().toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getSetDataTypes1()._2,
                        MvSyncRDDTest.getSetDataTypes1()._2,
                        MvSyncRDDTest.getSetDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForSmallIntType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getSmallIntDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getSmallIntDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo("smallint_col:SMALLINT", new Integer(3).toString()), result._1);
        assertEquals(
                new RecordColumnInfo("smallint_col:SMALLINT", new Integer(4).toString()), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getSmallIntDataTypes1()._2,
                        MvSyncRDDTest.getSmallIntDataTypes1()._2,
                        MvSyncRDDTest.getSmallIntDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForTimeStampType() throws ParseException {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 =
                MvSyncRDDTest.getTimestampDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 =
                MvSyncRDDTest.getTimestampDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals("timestamp_col:TIMESTAMP:1708230997005", result._1.toString());
        assertEquals("timestamp_col:TIMESTAMP:1708230997006", result._2.toString());

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getTimestampDataTypes1()._2,
                        MvSyncRDDTest.getTimestampDataTypes1()._2,
                        MvSyncRDDTest.getTimestampDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForMapType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getMapDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getMapDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);

        Map<Integer, Integer> map1 =
                new TreeMap<>() {
                    {
                        put(1, 10);
                        put(2, 20);
                        put(3, 30);
                    }
                };
        Map<Integer, Integer> map2 =
                new TreeMap<>() {
                    {
                        put(1, 11);
                        put(2, 22);
                        put(3, 33);
                    }
                };

        assertEquals(
                new RecordColumnInfo("map_col:MAP", JavaConverters.mapAsScalaMap(map1).toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo("map_col:MAP", JavaConverters.mapAsScalaMap(map2).toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getMapDataTypes1()._2,
                        MvSyncRDDTest.getMapDataTypes1()._2,
                        MvSyncRDDTest.getMapDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForTinyIntType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getTinyIntDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getTinyIntDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(new RecordColumnInfo("tinyint_col:TINYINT", new Integer(1).toString()), result._1);
        assertEquals(new RecordColumnInfo("tinyint_col:TINYINT", new Integer(2).toString()), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getUUIDDataTypes1()._2,
                        MvSyncRDDTest.getUUIDDataTypes1()._2,
                        MvSyncRDDTest.getUUIDDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForTextType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getTextDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getTextDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(new RecordColumnInfo("text_col:TEXT", new String("This is text!")), result._1);
        assertEquals(
                new RecordColumnInfo("text_col:TEXT", new String("This is a test case!")), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getTextDataTypes1()._2,
                        MvSyncRDDTest.getTextDataTypes1()._2,
                        MvSyncRDDTest.getTextDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForTimeType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getTimeDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getTimeDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo(
                        "time_col:TIME", formatter.parseDateTime("12:34:56.000000000").toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo(
                        "time_col:TIME", formatter.parseDateTime("12:34:56.100000001").toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getTimeDataTypes1()._2,
                        MvSyncRDDTest.getTimeDataTypes1()._2,
                        MvSyncRDDTest.getTimeDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForDoubleType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getDoubleDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getDoubleDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo("double_col:DOUBLE", new Double(12.36).toString()), result._1);
        assertEquals(
                new RecordColumnInfo("double_col:DOUBLE", new Double(12.37).toString()), result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getDoubleDataTypes1()._2,
                        MvSyncRDDTest.getDoubleDataTypes1()._2,
                        MvSyncRDDTest.getDoubleDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void dataMatchForDecimalType() {
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet1 = MvSyncRDDTest.getDecimalDataTypes1();
        Tuple2<MvSyncRDDTest.TestSchema, CassandraRow> dataSet2 = MvSyncRDDTest.getDecimalDataTypes2();
        Tuple2<RecordColumnInfo, RecordColumnInfo> result =
                MvSync.getInconsistentTuple(dataSet1._2, dataSet2._2, dataSet1._1.nonPrimaryKeys);
        assertEquals(
                new RecordColumnInfo("decimal_col:DECIMAL", new BigDecimal("100.05").toString()),
                result._1);
        assertEquals(
                new RecordColumnInfo("decimal_col:DECIMAL", new BigDecimal("100.06").toString()),
                result._2);

        assertNull(
                MvSync.getInconsistentTuple(
                        MvSyncRDDTest.getDecimalDataTypes1()._2,
                        MvSyncRDDTest.getDecimalDataTypes1()._2,
                        MvSyncRDDTest.getDecimalDataTypes1()._1.nonPrimaryKeys));
    }

    @Test
    public void convertToString() throws ParseException {
        assertEquals("Hello", DBOperations.convertToString("Hello"));
        assertEquals("10", DBOperations.convertToString(10));
        assertEquals("10.11", DBOperations.convertToString(10.11d));
        assertEquals("10.12", DBOperations.convertToString(10.12f));
        assertEquals("101112", DBOperations.convertToString(new BigInteger("101112")));
        assertEquals("10.11123", DBOperations.convertToString(new BigDecimal("10.11123")));
        assertEquals(
                "7309362c-4237-4e5d-b403-505820caba42",
                DBOperations.convertToString(UUID.fromString("7309362c-4237-4e5d-b403-505820caba42")));
        assertEquals("101", DBOperations.convertToString(new Long(101)));
        assertEquals(
                "1706445316005",
                DBOperations.convertToString(sdf.parse("2024-01-28 12:34:56.020005+0000")));
    }

    @Test
    public void possibleUpsertPreparedStatementFlavors() throws UnknownHostException, ParseException {
        CqlSession session = mock(CqlSession.class);

        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,ascii_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,bigint_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,blob_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,boolean_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,date_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,decimal_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,double_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,float_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,inet_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,int_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,list_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,map_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,set_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,smallint_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,text_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,time_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timestamp_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,timeuuid_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,tinyint_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,uuid_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,varchar_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare("INSERT INTO test_ks.test_mv (ck1,ck2,pk,varint_col) VALUES (?,?,?,?)"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,ascii_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,bigint_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,blob_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,boolean_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,date_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,decimal_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,double_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,float_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,inet_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,int_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,list_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,map_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,set_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,smallint_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,text_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,time_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timestamp_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timeuuid_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,tinyint_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,uuid_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,varchar_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,varint_col) VALUES (?,?,?,?) USING TIMESTAMP ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,ascii_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,bigint_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,blob_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,boolean_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,date_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,decimal_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,double_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,float_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,inet_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,int_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,list_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,map_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,set_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,smallint_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,text_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,time_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timestamp_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timeuuid_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,tinyint_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,uuid_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,varchar_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,varint_col) VALUES (?,?,?,?) USING TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,ascii_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,bigint_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,blob_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,boolean_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,date_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,decimal_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,double_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,float_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,inet_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,int_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,list_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,map_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,set_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,smallint_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,text_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,time_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timestamp_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,timeuuid_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,tinyint_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,uuid_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,varchar_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));
        when(session.prepare(
                "INSERT INTO test_ks.test_mv (ck1,ck2,pk,varint_col) VALUES (?,?,?,?) USING TIMESTAMP ? AND TTL ?"))
                .thenReturn(mock(PreparedStatement.class));

        MVSyncSettings settings = new MVSyncSettings(getConf());
        TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
        tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.primaryKeys;
        tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.nonPrimaryKeys;
        PreparedStatementHelper preparedStatementHelper =
                new PreparedStatementHelper(session, settings, tableAndMVColumns);

        Map<String, PreparedStatement> preparedStatementMap =
                preparedStatementHelper.buildUpsertPreparedStatement();

        // 22 columns and per column there are three flavors of prepared statements; hence 66
        assertEquals(88, preparedStatementMap.size());

        // check for the prepared statements
        Set<String> expectedPreparedStatementsKey = new HashSet<>();
        expectedPreparedStatementsKey.add("ascii_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("ascii_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("ascii_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("ascii_col_TTL");
        expectedPreparedStatementsKey.add("bigint_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("bigint_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("bigint_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("bigint_col_TTL");
        expectedPreparedStatementsKey.add("blob_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("blob_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("blob_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("blob_col_TTL");
        expectedPreparedStatementsKey.add("boolean_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("boolean_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("boolean_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("boolean_col_TTL");
        expectedPreparedStatementsKey.add("date_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("date_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("date_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("date_col_TTL");
        expectedPreparedStatementsKey.add("decimal_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("decimal_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("decimal_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("decimal_col_TTL");
        expectedPreparedStatementsKey.add("double_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("double_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("double_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("double_col_TTL");
        expectedPreparedStatementsKey.add("float_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("float_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("float_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("float_col_TTL");
        expectedPreparedStatementsKey.add("inet_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("inet_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("inet_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("inet_col_TTL");
        expectedPreparedStatementsKey.add("int_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("int_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("int_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("int_col_TTL");
        expectedPreparedStatementsKey.add("list_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("list_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("list_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("list_col_TTL");
        expectedPreparedStatementsKey.add("map_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("map_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("map_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("map_col_TTL");
        expectedPreparedStatementsKey.add("set_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("set_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("set_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("set_col_TTL");
        expectedPreparedStatementsKey.add("smallint_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("smallint_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("smallint_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("smallint_col_TTL");
        expectedPreparedStatementsKey.add("text_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("text_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("text_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("text_col_TTL");
        expectedPreparedStatementsKey.add("time_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("time_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("time_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("time_col_TTL");
        expectedPreparedStatementsKey.add("timestamp_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("timestamp_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("timestamp_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("timestamp_col_TTL");
        expectedPreparedStatementsKey.add("timeuuid_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("timeuuid_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("timeuuid_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("timeuuid_col_TTL");
        expectedPreparedStatementsKey.add("tinyint_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("tinyint_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("tinyint_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("tinyint_col_TTL");
        expectedPreparedStatementsKey.add("uuid_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("uuid_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("uuid_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("uuid_col_TTL");
        expectedPreparedStatementsKey.add("varchar_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("varchar_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("varchar_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("varchar_col_TTL");
        expectedPreparedStatementsKey.add("varint_col_NO_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("varint_col_TIMESTAMP");
        expectedPreparedStatementsKey.add("varint_col_TIMESTAMP_TTL");
        expectedPreparedStatementsKey.add("varint_col_TTL");

        assertEquals(expectedPreparedStatementsKey, preparedStatementMap.keySet());
        for (PreparedStatement ps : preparedStatementMap.values()) {
            assertNotNull(ps);
        }
    }

    @Test
    public void preparedStatementKey() {
        assertEquals("c1_NO_TIMESTAMP_TTL", DBOperations.getKey("c1", UpsertFlavors.NO_TIMESTAMP_TTL));
        assertEquals("c1_TIMESTAMP", DBOperations.getKey("c1", UpsertFlavors.TIMESTAMP));
        assertEquals("c1_TIMESTAMP_TTL", DBOperations.getKey("c1", UpsertFlavors.TIMESTAMP_TTL));
    }

    @Test
    public void deletePreparedStatement() throws UnknownHostException, ParseException {
        CqlSession session = mock(CqlSession.class);

        when(session.prepare("DELETE FROM test_ks.test_mv WHERE ck1=? AND ck2=? AND pk=?"))
                .thenReturn(mock(PreparedStatement.class));

        MVSyncSettings settings = new MVSyncSettings(getConf());
        TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
        tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.primaryKeys;
        tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.nonPrimaryKeys;
        PreparedStatementHelper preparedStatementHelper =
                new PreparedStatementHelper(session, settings, tableAndMVColumns);

        PreparedStatement preparedStatement = preparedStatementHelper.buildDeletePreparedStatement();

        assertNotNull(preparedStatement);
    }

    @Test
    public void selectPreparedStatement() throws UnknownHostException, ParseException {
        CqlSession session = mock(CqlSession.class);

        when(session.prepare(
                "SELECT * FROM test_ks.test_table WHERE ck1=? AND ck2=? AND pk=? ALLOW FILTERING"))
                .thenReturn(mock(PreparedStatement.class));

        MVSyncSettings settings = new MVSyncSettings(getConf());
        TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
        tableAndMVColumns.mvPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.primaryKeys;
        tableAndMVColumns.mvNonPrimaryKeyColumns = MvSyncRDDTest.getAllDataTypes()._1.nonPrimaryKeys;
        PreparedStatementHelper preparedStatementHelper =
                new PreparedStatementHelper(session, settings, tableAndMVColumns);
        PreparedStatement preparedStatement = preparedStatementHelper.buildSelectPreparedStatement();
        assertNotNull(preparedStatement);
    }

    @Test
    public void mockSchema() {
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        ColumnMetadata pk = mock(ColumnMetadata.class);
        ClusteringOrder pko = mock(ClusteringOrder.class);
        CqlIdentifier pkId = mock(CqlIdentifier.class);
        ColumnMetadata ck1 = mock(ColumnMetadata.class);
        ClusteringOrder cko1 = mock(ClusteringOrder.class);
        CqlIdentifier ck1Id = mock(CqlIdentifier.class);
        ColumnMetadata ck2 = mock(ColumnMetadata.class);
        ClusteringOrder cko2 = mock(ClusteringOrder.class);
        CqlIdentifier ck2Id = mock(CqlIdentifier.class);
        ColumnMetadata c1 = mock(ColumnMetadata.class);
        CqlIdentifier c1Id = mock(CqlIdentifier.class);
        ColumnMetadata c2 = mock(ColumnMetadata.class);
        CqlIdentifier c2Id = mock(CqlIdentifier.class);
        ColumnMetadata c3 = mock(ColumnMetadata.class);
        CqlIdentifier c3Id = mock(CqlIdentifier.class);
        ColumnMetadata c4 = mock(ColumnMetadata.class);
        CqlIdentifier c4Id = mock(CqlIdentifier.class);

        ViewMetadata mvMetadata = mock(ViewMetadata.class);
        ColumnMetadata mvpk = mock(ColumnMetadata.class);
        ColumnMetadata mvck1 = mock(ColumnMetadata.class);
        ColumnMetadata mvck2 = mock(ColumnMetadata.class);
        ColumnMetadata mvc1 = mock(ColumnMetadata.class);
        ColumnMetadata mvc2 = mock(ColumnMetadata.class);
        ColumnMetadata mvc3 = mock(ColumnMetadata.class);
        ColumnMetadata mvc4 = mock(ColumnMetadata.class);

        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace(KEYSPACE)).thenReturn(Optional.of(keyspaceMetadata));
        when(keyspaceMetadata.getTable(BASE_TABLE)).thenReturn(Optional.of(tableMetadata));
        when(keyspaceMetadata.getView(MV)).thenReturn(Optional.of(mvMetadata));

        // base table
        when(tableMetadata.getPartitionKey()).thenReturn(Arrays.asList(pk));
        when(tableMetadata.getClusteringColumns()).thenReturn(Map.of(ck1, cko1, ck2, cko2));
        when(tableMetadata.getColumns()).thenReturn(Map.of(pkId, pk, ck1Id, ck1, ck2Id, ck2, c1Id, c1, c2Id, c2, c3Id, c3, c4Id, c4));
        when(pkId.toString()).thenReturn("pk");
        when(pk.getName()).thenReturn(CqlIdentifier.fromCql("pk"));
        when(pk.getType()).thenReturn(DataTypes.ASCII);
        when(ck1Id.toString()).thenReturn("ck1");
        when(ck1.getName()).thenReturn(CqlIdentifier.fromCql("ck1"));
        when(ck1.getType()).thenReturn(DataTypes.ASCII);
        when(ck2Id.toString()).thenReturn("ck1");
        when(ck2.getName()).thenReturn(CqlIdentifier.fromCql("ck2"));
        when(ck2.getType()).thenReturn(DataTypes.INT);
        when(c1Id.toString()).thenReturn("c1");
        when(c1.getName()).thenReturn(CqlIdentifier.fromCql("c1"));
        when(c1.getType()).thenReturn(DataTypes.INT);
        when(c2Id.toString()).thenReturn("c2");
        when(c2.getName()).thenReturn(CqlIdentifier.fromCql("c2"));
        when(c2.getType()).thenReturn(DataTypes.INT);
        when(c3Id.toString()).thenReturn("c3");
        when(c3.getName()).thenReturn(CqlIdentifier.fromCql("c3"));
        when(c3.getType()).thenReturn(DataTypes.INT);
        when(c4Id.toString()).thenReturn("c4");
        when(c4.getName()).thenReturn(CqlIdentifier.fromCql("c4"));
        when(c4.getType()).thenReturn(DataTypes.INT);

        // mv table
        when(mvMetadata.getPartitionKey()).thenReturn(Arrays.asList(c1));
        when(mvMetadata.getClusteringColumns()).thenReturn(Map.of(pk, pko, ck1, cko1, ck2, cko2));
        when(mvMetadata.getColumns()).thenReturn(Map.of(c1Id, c1, pkId, pk, ck1Id, ck1, ck2Id, ck2, c2Id, c2, c3Id, c3, c4Id, c4));
    }

    @Test
    public void validateMVOutputStreamer() throws Exception {
        MVJobOutputStreamer outputStreamer = new MVJobOutputStreamer(new MVSyncSettings(getConf()));
        assertNotNull(outputStreamer.streamerMissingInBaseTable);
        assertNotNull(outputStreamer.streamerMissingInMv);
        assertNotNull(outputStreamer.streamerMismatch);
        assertNotNull(outputStreamer.streamerErrDeleting);
        assertNotNull(outputStreamer.streamerErrUpserting);
    }
}
