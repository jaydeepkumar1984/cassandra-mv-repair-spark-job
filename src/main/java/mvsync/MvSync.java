package mvsync;

import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.google.common.util.concurrent.RateLimiter;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import io.micrometer.core.instrument.util.TimeUtils;
import mvsync.db.CassandraClient;
import mvsync.db.DBOperations;
import mvsync.db.PreparedStatementHelper;
import mvsync.output.IBlobStreamer;
import mvsync.output.MVJobOutputStreamFactory;
import mvsync.output.MVJobOutputStreamer;
import mvsync.output.MvSyncJobMetrics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MvSync implements Serializable {
    public static MVJobOutputStreamFactory mvJobOutputStreamFactory = new MVJobOutputStreamFactory();
    private static Logger log = LoggerFactory.getLogger(MvSync.class);
    private static final Map<Integer, RateLimiter> rateLimiters = new ConcurrentHashMap<>();

    private static CassandraClient cassandraClient = new CassandraClient();


    public static void main(String[] args) throws Exception {
        new MvSync().run();
    }

    public void run() throws Exception {
        try {
            SparkConf sparkConf = getConf();
            MVSyncSettings mvSyncSettings = new MVSyncSettings(sparkConf);
            MvSyncJobMetrics.jobStarted.inc();
            buildAndRunSparkJob(mvSyncSettings);
            MvSyncJobMetrics.jobCompleted.inc();
        } catch (Exception e) {
            MvSyncJobMetrics.jobError.inc();
            log.error("Exception encountered", e);
            throw e;
        } finally {
            cassandraClient.close();
        }
    }

    public SparkConf getConf() {
        return new SparkConf(true);
    }

    public SparkConf getCassandraSparkConf(MVSyncSettings mvSyncSettings) {
        return getSparkConnConfig(mvSyncSettings);
    }

    public SparkConf getSparkConnConfig(MVSyncSettings mvSyncSettings) {
        SparkConf connConfig = new SparkConf(true);
        connConfig.remove("spark.cassandra.migration.upsert.rate.upperLimit");
        connConfig.remove("spark.cassandra.migration.cassandra.instance");
        connConfig.remove("spark.cassandra.migration.direction");
        connConfig.remove("spark.cassandra.migration.cassandra.table");
        connConfig.remove("spark.cassandra.migration.upsert.rate.lowerLimit");
        connConfig.remove("spark.cassandra.migration.cassandra.keyspace");
        connConfig.remove("spark.cassandra.udg.path");

        connConfig.set("spark.cassandra.connection.host", mvSyncSettings.getCassandraConnectionHost());
        connConfig.set(
                "spark.cassandra.connection.port", mvSyncSettings.getCassandraConnectionPort());

        if (mvSyncSettings.getCassandraUserName() != null && mvSyncSettings.getCassandraPassword() != null) {
            connConfig.set("spark.cassandra.auth.username", mvSyncSettings.getCassandraUserName());
            connConfig.set("spark.cassandra.auth.password", mvSyncSettings.getCassandraPassword());
        }
        return connConfig;
    }

    public static void checkForUnsupportedTypesForAutomaticFixingInconsistencies(
            Map<String, String> mvNonPrimaryKeyColumns, MVSyncSettings settings) throws Exception {
        // Currently, we do not support automated fixing of inconsistencies for Duration and Tuple data
        // types. Fail early.
        // One can, however, use the job to find out the culprit records for unsupported types; just the
        // automated repair is not supported
        if (settings.getFixMissingMvRecords() || settings.getFixInconsistentMvRecords()) {
            for (String type : mvNonPrimaryKeyColumns.values()) {
                if (!DBOperations.isSupportedType(type)) {
                    throw new Exception(
                            "Cannot do an automated fixing of inconsistencies for the unsupported type: " + type);
                }
            }
        }
    }

    public void buildAndRunSparkJob(MVSyncSettings mvSyncSettings) throws Exception {
        log.info("Spark settings : {}", mvSyncSettings);
        cassandraClient.initCassandra(mvSyncSettings);

        TableAndMVColumns tableAndMVColumns = getBaseAndMvTableColumns(mvSyncSettings);

        checkForUnsupportedTypesForAutomaticFixingInconsistencies(
                tableAndMVColumns.mvNonPrimaryKeyColumns, mvSyncSettings);

        SparkConf cassSparkConf = getCassandraSparkConf(mvSyncSettings);
        cassSparkConf.set(
                "spark.cassandra.input.consistency.level", mvSyncSettings.getSparkReadConsistencyLevel());
        for (Tuple2<String, String> property : cassSparkConf.getAll()) {
            log.info("Spark Cassandra Property key: {}, value: {}", property._1(), property._2());
        }

        JavaSparkContext jsc = new JavaSparkContext(cassSparkConf);
        JavaPairRDD<RecordPrimaryKey, CassandraRow> baseTableRDD =
                getRDD(
                        mvSyncSettings.getBaseTableKeyspaceName(),
                        mvSyncSettings.getBaseTableName(),
                        tableAndMVColumns.mvPrimaryKeyColumns,
                        tableAndMVColumns.baseTablePrimaryKeyColumns,
                        tableAndMVColumns.baseTableNonPrimaryKeyColumns,
                        jsc,
                        mvSyncSettings);
        JavaPairRDD<RecordPrimaryKey, CassandraRow> mvTableRDD =
                getRDD(
                        mvSyncSettings.getBaseTableKeyspaceName(),
                        mvSyncSettings.getMvName(),
                        tableAndMVColumns.mvPrimaryKeyColumns,
                        tableAndMVColumns.mvPrimaryKeyColumns,
                        tableAndMVColumns.mvNonPrimaryKeyColumns,
                        jsc,
                        mvSyncSettings);

        JobStats jobStats = new JobStats(jsc);
        compareAndRepairMV(baseTableRDD, mvTableRDD, mvSyncSettings, tableAndMVColumns, jobStats)
                .collect();

        String stats = String.format("%s/%s.txt", mvSyncSettings.getOutputDir(), "stats");
        log.info("Upload stats to : {}", stats);
        IBlobStreamer streamer = getStatsStreamer(stats, mvSyncSettings);
        streamer.append(jobStats.toString());
        streamer.commit();

        log.info("Job output {}", jobStats);
        jsc.close();
    }

    private String getType(String type) {
        if (type.toUpperCase().contains("LIST(")) {
            return "LIST";
        }
        if (type.toUpperCase().contains("SET(")) {
            return "SET";
        }
        if (type.toUpperCase().contains("MAP(")) {
            return "MAP";
        }
        return type;
    }

    public TableAndMVColumns getBaseAndMvTableColumns(MVSyncSettings settings) throws Exception {
        TableAndMVColumns tableAndMVColumns = new TableAndMVColumns();
        CqlSession session = cassandraClient.getSession();
        Metadata metadata = session.getMetadata();
        Optional<KeyspaceMetadata> keyspaceMetadata = metadata.getKeyspace(settings.getBaseTableKeyspaceName());
        if (keyspaceMetadata.isEmpty()) {
            MvSyncJobMetrics.keyspaceMetadataError.inc();
            throw new Exception("The keyspace does not exist");
        }
        Optional<TableMetadata> tableMetadata = keyspaceMetadata.get().getTable(settings.getBaseTableName());
        if (tableMetadata.isEmpty()) {
            MvSyncJobMetrics.keyspaceMetadataError.inc();
            throw new Exception("The table does not exist");
        }
        Optional<ViewMetadata> mvMetadata = keyspaceMetadata.get().getView(settings.getMvName());
        if (mvMetadata.isEmpty()) {
            MvSyncJobMetrics.mvDoesNotExist.inc();
            throw new Exception("The MV does not exist");
        }
        populateSchemaInformation(tableAndMVColumns.baseTablePrimaryKeyColumns, tableAndMVColumns.baseTableNonPrimaryKeyColumns,
                tableMetadata.get().getPartitionKey(), tableMetadata.get().getClusteringColumns(), tableMetadata.get().getColumns());
        populateSchemaInformation(tableAndMVColumns.mvPrimaryKeyColumns, tableAndMVColumns.mvNonPrimaryKeyColumns,
                mvMetadata.get().getPartitionKey(), mvMetadata.get().getClusteringColumns(), mvMetadata.get().getColumns());
        return tableAndMVColumns;
    }

    public void populateSchemaInformation(Map<String, String> primaryKeyColumns, Map<String, String> nonPrimaryKeyColumns,
                                          List<ColumnMetadata> partitionKeys, Map<ColumnMetadata, ClusteringOrder> clusteringColumns, Map<CqlIdentifier, ColumnMetadata> regularColumns) {
        for (ColumnMetadata partitionKey : partitionKeys) {
            // we cannot add com.datastax.driver.core.DataType in Map because it is not serializable
            primaryKeyColumns.put(
                    partitionKey.getName().toString(), partitionKey.getType().toString());
        }
        for (Map.Entry<ColumnMetadata, ClusteringOrder> clusteringKey : clusteringColumns.entrySet()) {
            primaryKeyColumns.put(
                    clusteringKey.getKey().getName().toString(), clusteringKey.getKey().getType().toString());
        }
        for (Map.Entry<CqlIdentifier, ColumnMetadata> regularColumn : regularColumns.entrySet()) {
            if (!primaryKeyColumns.containsKey(regularColumn.getKey().toString())) {
                nonPrimaryKeyColumns.put(
                        regularColumn.getKey().toString(), getType(regularColumn.getValue().getType().toString()));
            }
        }
    }

    public ArrayList<ColumnRef> buildSelectStatement(
            Map<String, String> primaryKeyColumns, Map<String, String> nonPrimaryKeyColumns) {
        ArrayList<ColumnRef> selectStmt = new ArrayList<>();
        for (String colName : primaryKeyColumns.keySet()) {
            selectStmt.add(CassandraJavaUtil.column(colName));
        }
        for (Map.Entry<String, String> column : nonPrimaryKeyColumns.entrySet()) {
            selectStmt.add(CassandraJavaUtil.column(column.getKey()));
            if (!DBOperations.isCollection(column.getValue())) {
                selectStmt.add(CassandraJavaUtil.writeTime(column.getKey()));
                selectStmt.add(CassandraJavaUtil.ttl(column.getKey()));
            }
        }
        return selectStmt;
    }

    public JavaPairRDD<RecordPrimaryKey, CassandraRow> getRDD(
            String keyspace,
            String table,
            Map<String, String> mvTablePrimaryKeyColumns,
            Map<String, String> primaryKeyColumns,
            Map<String, String> nonPrimaryKeyColumns,
            JavaSparkContext jsc,
            MVSyncSettings settings) {

        ArrayList<ColumnRef> selectStmt = buildSelectStatement(primaryKeyColumns, nonPrimaryKeyColumns);
        ColumnRef[] selectedCols = new ColumnRef[selectStmt.size()];
        selectedCols = selectStmt.toArray(selectedCols);

        return CassandraJavaUtil.javaFunctions(jsc)
                .cassandraTable(keyspace, table)
                .select(selectedCols)
                .mapToPair(
                        row -> {
                            MvSyncJobMetrics.readRows.inc();
                            RateLimiter rateLimiter =
                                    rateLimiters.computeIfAbsent(
                                            settings.getCassandraScanPerSecondRateLimiter(),
                                            k -> RateLimiter.create(settings.getCassandraScanPerSecondRateLimiter()));
                            rateLimiter.acquire();
                            return new Tuple2<>(buildPrimaryKey(row, mvTablePrimaryKeyColumns), row);
                        });
    }

    public IBlobStreamer getStatsStreamer(String path, MVSyncSettings settings) throws Exception {
        return mvJobOutputStreamFactory.getStream(path, settings);
    }

    public MVJobOutputStreamer getOutputStreamers(MVSyncSettings settings) throws Exception {
        return new MVJobOutputStreamer(settings);
    }

    public JavaRDD<Void> compareAndRepairMV(
            JavaPairRDD<RecordPrimaryKey, CassandraRow> mainTableRecords,
            JavaPairRDD<RecordPrimaryKey, CassandraRow> mvRecords,
            MVSyncSettings settings,
            TableAndMVColumns tableAndMVColumns,
            JobStats jobStats) {
        Map<String, String> commonNonPrimaryKeyColumns =
                tableAndMVColumns
                        .baseTableNonPrimaryKeyColumns
                        .entrySet()
                        .stream()
                        .filter(
                                entry ->
                                        tableAndMVColumns.mvNonPrimaryKeyColumns.containsKey(entry.getKey())
                                                && Objects.equals(
                                                entry.getValue(),
                                                tableAndMVColumns.mvNonPrimaryKeyColumns.get(entry.getKey())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return mainTableRecords
                .cogroup(mvRecords)
                .mapPartitions(
                        (FlatMapFunction<
                                Iterator<
                                        Tuple2<
                                                RecordPrimaryKey,
                                                Tuple2<Iterable<CassandraRow>, Iterable<CassandraRow>>>>,
                                Void>)
                                itr -> {
                                    cassandraClient.initCassandra(settings);
                                    CqlSession session = cassandraClient.getSession();
                                    if (session == null) {
                                        throw new Exception(
                                                String.format(
                                                        "Could not initialized Cassandra session for udg"));
                                    }
                                    MVJobOutputStreamer outputStreamer = getOutputStreamers(settings);
                                    try {
                                        RateLimiter mutationRateLimiter =
                                                RateLimiter.create(settings.getCassandraMutationPerSecondRateLimiter());
                                        DBOperations dbOperations =
                                                new DBOperations(
                                                        session,
                                                        new PreparedStatementHelper(session, settings, tableAndMVColumns),
                                                        settings,
                                                        tableAndMVColumns);

                                        while (itr.hasNext()) {
                                            jobStats.totRecords.add(1);
                                            final Tuple2<
                                                    RecordPrimaryKey,
                                                    Tuple2<Iterable<CassandraRow>, Iterable<CassandraRow>>>
                                                    t = itr.next();
                                            MvSyncJobMetrics.processRecord.inc();
                                            MVConsistencyState problem = MVConsistencyState.CONSISTENT;
                                            CassandraRow mainTableEntry = null;
                                            RecordColumnInfo baseColumn = null;
                                            RecordColumnInfo mvColumn = null;
                                            if (t._2._1.iterator().hasNext()) {
                                                mainTableEntry = t._2._1.iterator().next();
                                                if (mainTableEntry != null
                                                        && shouldSkip(
                                                        settings,
                                                        jobStats,
                                                        mainTableEntry,
                                                        commonNonPrimaryKeyColumns)) {
                                                    continue;
                                                }
                                            }
                                            CassandraRow mvTableEntry = null;
                                            if (t._2._2.iterator().hasNext()) {
                                                mvTableEntry = t._2._2.iterator().next();
                                                if (mvTableEntry != null
                                                        && shouldSkip(
                                                        settings,
                                                        jobStats,
                                                        mvTableEntry,
                                                        commonNonPrimaryKeyColumns)) {
                                                    continue;
                                                }
                                            }

                                            if (mainTableEntry == null) {
                                                jobStats.missingBaseTableRecords.add(1);
                                                MvSyncJobMetrics.missingBaseTable.inc();
                                                problem = MVConsistencyState.MISSING_IN_BASE_TABLE;
                                            } else if (mvTableEntry == null) {
                                                jobStats.missingMvRecords.add(1);
                                                MvSyncJobMetrics.missingMV.inc();
                                                problem = MVConsistencyState.MISSING_IN_MV_TABLE;
                                            }

                                            // if entry present in both main table and entry and hashcode of data
                                            // doesn't match.
                                            if (mainTableEntry != null && mvTableEntry != null) {
                                                Tuple2<RecordColumnInfo, RecordColumnInfo> mismatch =
                                                        getInconsistentTuple(
                                                                mainTableEntry, mvTableEntry, commonNonPrimaryKeyColumns);
                                                if (mismatch != null) {
                                                    baseColumn = mismatch._1;
                                                    mvColumn = mismatch._2;
                                                    jobStats.inConsistentRecords.add(1);
                                                    problem = MVConsistencyState.INCONSISTENT;
                                                    MvSyncJobMetrics.inconsistentRecord.inc();
                                                }
                                            }

                                            if (problem == MVConsistencyState.CONSISTENT) {
                                                jobStats.consistentRecords.add(1);
                                                MvSyncJobMetrics.consistentRecord.inc();
                                                continue;
                                            }

                                            DBOperations.DBResult delResults = null;
                                            DBOperations.DBResult upsertResults = null;
                                            if (problem == MVConsistencyState.MISSING_IN_BASE_TABLE && settings.getFixOrphanMvRecords()) {
                                                mutationRateLimiter.acquire();
                                                jobStats.repairRecords.add(1);
                                                MvSyncJobMetrics.recordRepairOn.inc();
                                                jobStats.delAttemptedRecords.add(1);
                                                MvSyncJobMetrics.deleteRecord.inc();
                                                delResults = dbOperations.deleteFromMV(mvTableEntry);
                                                if (!delResults.success) {
                                                    jobStats.delErrRecords.add(1);
                                                } else if (!delResults.entryPresent) {
                                                    jobStats.delSuccessRecords.add(1);
                                                } else {
                                                    jobStats.notDelRecords.add(1);
                                                }
                                            } else if ((problem == MVConsistencyState.INCONSISTENT
                                                    && settings.getFixInconsistentMvRecords())
                                                    || (problem == MVConsistencyState.MISSING_IN_MV_TABLE
                                                    && settings.getFixMissingMvRecords())) {
                                                mutationRateLimiter.acquire();
                                                jobStats.repairRecords.add(1);
                                                MvSyncJobMetrics.recordRepairOn.inc();
                                                jobStats.upsertAttemptedRecords.add(1);
                                                MvSyncJobMetrics.upsertRecord.inc();
                                                upsertResults = dbOperations.upsert(mainTableEntry, mvTableEntry);
                                                if (!upsertResults.success) {
                                                    jobStats.upsertErrRecords.add(1);
                                                } else {
                                                    jobStats.upsertSuccessRecords.add(1);
                                                }
                                            } else {
                                                jobStats.notRepairRecords.add(1);
                                                MvSyncJobMetrics.recordRepairOff.inc();
                                            }
                                            MVInconsistentRow inconsistentRow =
                                                    new MVInconsistentRow(
                                                            problem, t._1, mainTableEntry, mvTableEntry, baseColumn, mvColumn);
                                            outputStreamer.streamOutput(
                                                    outputStreamer, problem, inconsistentRow, delResults, upsertResults);
                                        }
                                    } finally {
                                        if (outputStreamer != null) {
                                            outputStreamer.close();
                                        }
                                    }
                                    return new ArrayList<Void>().iterator();
                                });
    }

    private boolean shouldSkip(
            MVSyncSettings settings,
            JobStats jobStats,
            CassandraRow cassandraRow,
            Map<String, String> commonNonPrimaryKeyColumns)
            throws Exception {
        Tuple2<Long, Long> leastAndMostModificationTime =
                getTheLeastAndMostModificationTimeInMicroSeconds(cassandraRow, commonNonPrimaryKeyColumns);
        long leastTS = (long)
                TimeUtils.convert(
                        leastAndMostModificationTime._1, TimeUnit.MICROSECONDS, TimeUnit.SECONDS);
        long mostTS = (long)
                TimeUtils.convert(
                        leastAndMostModificationTime._2, TimeUnit.MICROSECONDS, TimeUnit.SECONDS);
        if (settings.getStartTSEpochInSec() > leastTS || mostTS > settings.getEndTSEpochInSec()) {
            jobStats.recordNotInScope.add(1);
            MvSyncJobMetrics.recordNotInScope.inc();
            return true;
        }
        return false;
    }

    public static Tuple2<Long, Long> getTheLeastAndMostModificationTimeInMicroSeconds(
            CassandraRow row, Map<String, String> nonPrimaryKeyColumns) throws Exception {
        long leastTS = Long.MAX_VALUE;
        long mostTS = Long.MIN_VALUE;
        if (row != null) {
            for (Map.Entry<String, String> column : nonPrimaryKeyColumns.entrySet()) {
                if (!DBOperations.isCollection(column.getValue())) {
                    if (row.getObject(column.getKey()) != null) {
                        Long columnTS = row.getLong(String.format("writetime(%s)", column.getKey()));
                        if (columnTS != null) {
                            if (leastTS > columnTS) {
                                leastTS = columnTS;
                            }
                            if (mostTS < columnTS) {
                                mostTS = columnTS;
                            }
                        } else {
                            throw new Exception(
                                    String.format(
                                            "Modification time is not present for %s, details: %s",
                                            column.getKey(), row.toString()));
                        }
                    }
                }
            }
        }
        return Tuple2.apply(leastTS, mostTS);
    }

    @Nullable
    public static Tuple2<RecordColumnInfo, RecordColumnInfo> getInconsistentTuple(
            CassandraRow rowFromBaseTable,
            CassandraRow rowFromMV,
            Map<String, String> nonPrimaryKeyColumns) {
        for (Map.Entry<String, String> column : nonPrimaryKeyColumns.entrySet()) {
            Object baseTableColumn = rowFromBaseTable.getObject(column.getKey());
            Object mvColumn = rowFromMV.getObject(column.getKey());
            if (baseTableColumn != null
                    && mvColumn != null
                    && !Objects.deepEquals(baseTableColumn, mvColumn)) {
                return Tuple2.apply(
                        new RecordColumnInfo(
                                column.getKey() + ":" + column.getValue(),
                                DBOperations.convertToString(baseTableColumn)),
                        new RecordColumnInfo(
                                column.getKey() + ":" + column.getValue(), DBOperations.convertToString(mvColumn)));
            } else if ((baseTableColumn != null && mvColumn == null)
                    || ((baseTableColumn == null && mvColumn != null))) {
                return Tuple2.apply(
                        new RecordColumnInfo(
                                column.getKey() + ":" + column.getValue(),
                                baseTableColumn != null ? DBOperations.convertToString(baseTableColumn) : null),
                        new RecordColumnInfo(
                                column.getKey() + ":" + column.getValue(),
                                mvColumn != null ? DBOperations.convertToString(mvColumn) : null));
            }
        }
        return null;
    }

    public static RecordPrimaryKey buildPrimaryKey(
            CassandraRow row, Map<String, String> primaryKeyColumns) {
        RecordPrimaryKey pk = new RecordPrimaryKey();
        for (Map.Entry<String, String> entry : primaryKeyColumns.entrySet()) {
            pk.columnNames.add(entry.getKey());
            pk.columnTypes.add(entry.getValue());
            pk.keys.add(row.getObject(entry.getKey()));
        }
        return pk;
    }
}
