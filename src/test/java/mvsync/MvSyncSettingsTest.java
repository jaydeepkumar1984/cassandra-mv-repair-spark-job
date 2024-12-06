package mvsync;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.junit.Test;

import static mvsync.MVSyncSettings.PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MvSyncSettingsTest {

  @Test
  public void defaultConfig() {
    MVSyncSettings settings = new MVSyncSettings(new SparkConf(true));
    assertEquals(-1L, settings.getStartTSEpochInSec());
    assertEquals(-1L, settings.getEndTSEpochInSec());
    assertNull(settings.getBaseTableKeyspaceName());
    assertNull(settings.getBaseTableName());
    assertNull(settings.getMvName());
    assertEquals("LOCAL_QUORUM", settings.getSparkReadConsistencyLevel());
    assertEquals("LOCAL_QUORUM", settings.getMvRecordsWriteConsistencyLevel());
    assertFalse(settings.getFixMissingMvRecords());
    assertFalse(settings.getFixOrphanMvRecords());
    assertFalse(settings.getFixInconsistentMvRecords());
    assertEquals(10, settings.getCassandraScanPerSecondRateLimiter());
    assertEquals(5, settings.getCassandraMutationPerSecondRateLimiter());
    assertFalse(settings.getCassandraMutationUseLatestTS());
    assertNull(settings.getCassandraUserName());
    assertNull(settings.getCassandraPassword());
    assertEquals("/tmp/cassandra-mv-repair-spark-job/", settings.getOutputDir());
    assertEquals("datacenter1", settings.getCassandraDatacenter());
    assertEquals("localhost", settings.getCassandraConnectionHost());
    assertEquals("9042", settings.getCassandraConnectionPort());
  }

  @Test
  public void validateConfigParametersAreParsedProperly() {
    SparkConf sparkConf = new SparkConf(true);
    sparkConf.set(PREFIX + ".starttsinsec", "10");
    sparkConf.set(PREFIX + ".endtsinsec", "20");
    sparkConf.set(PREFIX + ".keyspace", "test_keyspace");
    sparkConf.set(PREFIX + ".basetablename", "test_basetable");
    sparkConf.set(PREFIX + ".readconsistency", "ONE");
    sparkConf.set(PREFIX + ".mvwriteconsistency", "ALL");
    sparkConf.set(PREFIX + ".mvname", "test_mv");
    sparkConf.set(PREFIX + ".fixmissingmv", "true");
    sparkConf.set(PREFIX + ".fixorphanmv", "true");
    sparkConf.set(PREFIX + ".fixinconsistentmv", "true");
    sparkConf.set(PREFIX + ".scan.ratelimiter", "100");
    sparkConf.set(PREFIX + ".mutation.ratelimiter", "50");
    sparkConf.set(PREFIX + ".mutation.uselatestts", "true");
    sparkConf.set(PREFIX + ".password.usecret.name", "secret123");
    sparkConf.set(PREFIX + ".cassandra.username", "test_user");
    sparkConf.set(PREFIX + ".cassandra.password", "test_secret");
    sparkConf.set(PREFIX + ".output.dir", "/a/b/c/d/");
    sparkConf.set(PREFIX + ".cassandra.datacenter", "datacenter2");
    sparkConf.set(PREFIX + ".cassandra.host", "127.0.0.2");
    sparkConf.set(PREFIX + ".cassandra.port", "9043");

    MVSyncSettings settings = new MVSyncSettings(sparkConf);
    assertEquals(10, settings.getStartTSEpochInSec());
    assertEquals(20, settings.getEndTSEpochInSec());
    assertEquals("test_keyspace", settings.getBaseTableKeyspaceName());
    assertEquals("test_basetable", settings.getBaseTableName());
    assertEquals("test_mv", settings.getMvName());
    assertEquals("ONE", settings.getSparkReadConsistencyLevel());
    assertEquals("ALL", settings.getMvRecordsWriteConsistencyLevel());
    assertTrue(settings.getFixMissingMvRecords());
    assertTrue(settings.getFixOrphanMvRecords());
    assertTrue(settings.getFixInconsistentMvRecords());
    assertEquals(100, settings.getCassandraScanPerSecondRateLimiter());
    assertEquals(50, settings.getCassandraMutationPerSecondRateLimiter());
    assertTrue(settings.getCassandraMutationUseLatestTS());
    assertEquals("test_user",settings.getCassandraUserName());
    assertEquals("test_secret",settings.getCassandraPassword());
    assertEquals("/a/b/c/d/", settings.getOutputDir());
    assertEquals("datacenter2", settings.getCassandraDatacenter());
    assertEquals("127.0.0.2", settings.getCassandraConnectionHost());
    assertEquals("9043", settings.getCassandraConnectionPort());
  }
}
