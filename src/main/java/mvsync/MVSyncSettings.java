package mvsync;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import scala.Option;

import java.io.Serializable;

import static java.lang.System.setProperty;

public class MVSyncSettings implements Serializable {

    public static final String PREFIX = "cass.mv";
    SparkConf cfg;

    public MVSyncSettings(SparkConf cfg) {
        this.cfg = cfg;
    }

    // Cassandra records’ writetimestamp value greater than this config should be considered in the Spark job.
    // This applies to both the Base table and MV table. If there are columns with different timestamps in a record,
    // then the smallest timestamp needs to be greater than this.
    public long getStartTSEpochInSec() {
        return Long.parseLong(getProperty(PREFIX + ".starttsinsec", "-1"));
    }

    // Cassandra records’ writetimestamp value is less than this config and should be considered in the Spark job.
    // This applies to both the Base table and MV table. If there are columns with different timestamps in a record,
    // then the biggest timestamp needs to be less than this.
    public long getEndTSEpochInSec() {
        return Long.parseLong(getProperty(PREFIX + ".endtsinsec", "-1"));
    }

    // Cassandra Keyspace name
    public String getBaseTableKeyspaceName() {
        return getProperty(PREFIX + ".keyspace");
    }

    // Cassandra Base table name
    public String getBaseTableName() {
        return getProperty(PREFIX + ".basetablename");
    }

    // Cassandra Materialized View (MV) name
    public String getMvName() {
        return getProperty(PREFIX + ".mvname");
    }

    // Spark job will use this consistency while scanning the records
    public String getSparkReadConsistencyLevel() {
        return getProperty(PREFIX + ".readconsistency", "LOCAL_QUORUM");
    }

    // Spark job will use this consistency to fix inconsistent MV record(s)
    public String getMvRecordsWriteConsistencyLevel() {
        return getProperty(PREFIX + ".mvwriteconsistency", "LOCAL_QUORUM");
    }

    // Should the job automatically insert the missing record in MV? By default, do not repair them; just list the inconsistencies.
    public Boolean getFixMissingMvRecords() {
        return Boolean.parseBoolean(getProperty(PREFIX + ".fixmissingmv", "false"));
    }

    // Should the job automatically delete the unexpected record in MV? By default, do not repair them; just list the inconsistencies.
    public Boolean getFixOrphanMvRecords() {
        return Boolean.parseBoolean(getProperty(PREFIX + ".fixorphanmv", "false"));
    }

    // Should the job automatically fix the inconsistent record in MV? By default, do not repair them; just list the inconsistencies.
    public Boolean getFixInconsistentMvRecords() {
        return Boolean.parseBoolean(getProperty(PREFIX + ".fixinconsistentmv", "false"));
    }

    // Cassandra cluster host to connect to
    public String getCassandraConnectionHost() {
        return getProperty(PREFIX + ".cassandra.host", "localhost");
    }

    // Cassandra cluster port to connect to
    public String getCassandraConnectionPort() {
        return getProperty(PREFIX + ".cassandra.port", "9042");
    }

    // Throttler while scanning Cassandra's records. By default, one Spark worker would scan 10 records per second.
    // If you have N Spark workers, then the total read throughput would be = N * 10, so you need to size it accordingly.
    public int getCassandraScanPerSecondRateLimiter() {
        return Integer.parseInt(getProperty(PREFIX + ".scan.ratelimiter", "10"));
    }

    // Throttler while fixing the inconsistent MV records. By default, one Spark worker would mutate 5 records per second.
    // If you have N Spark workers, then the total mutation throughput would be = N * 5, so you need to size it accordingly.
    public int getCassandraMutationPerSecondRateLimiter() {
        return Integer.parseInt(getProperty(PREFIX + ".mutation.ratelimiter", "5"));
    }

    // By default, Spark workers use the base table timestamp for the MV mutation.
    // For some reason (mostly for testing purposes), if you want to use the latest timestamp for MV mutations, then set this to “true”
    public boolean getCassandraMutationUseLatestTS() {
        return Boolean.parseBoolean(getProperty(PREFIX + ".mutation.uselatestts", "false"));
    }

    // Cassandra username
    public String getCassandraUserName() {
        return getProperty(PREFIX + ".cassandra.username");
    }

    // Cassandra password
    public String getCassandraPassword() {
        return getProperty(PREFIX + ".cassandra.password");
    }

    // Cassandra datacenter
    public String getCassandraDatacenter() {
        return getProperty(PREFIX + ".cassandra.datacenter", "datacenter1");
    }

    // The job would output the MV inconsistencies to an output folder.
    public String getOutputDir() {
        return getProperty(PREFIX + ".output.dir", "/tmp/cassandra-mv-repair-spark-job/");
    }

    private String getProperty(String name) {
        Option<String> op = cfg.getOption(name);
        if (!op.isDefined()) {
            op = cfg.getOption("spark." + name);
        }
        return (op.isDefined() ? op.get() : null);
    }

    protected String getProperty(String name, String defaultValue) {
        String value = getProperty(name);
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }
}
