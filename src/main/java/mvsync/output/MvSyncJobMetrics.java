package mvsync.output;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class MvSyncJobMetrics {
    public static final MetricRegistry Metrics = new MetricRegistry();
    public static Counter jobStarted = Metrics.counter("JobStarted");
    public static Counter jobCompleted = Metrics.counter("JobCompleted");
    public static Counter jobError = Metrics.counter("JobError");
    public static Counter keyspaceMetadataError = Metrics.counter("KeyspaceMetadataError");
    public static Counter mvDoesNotExist = Metrics.counter("MVDoesNotExist");
    public static Counter processRecord = Metrics.counter("ProcessRecord");
    public static Counter missingBaseTable = Metrics.counter("MissingBaseTable");
    public static Counter missingMV = Metrics.counter("MissingMV");
    public static Counter inconsistentRecord = Metrics.counter("InconsistentRecord");
    public static Counter consistentRecord = Metrics.counter("ConsistentRecord");
    public static Counter recordRepairOn = Metrics.counter("RecordRepairOn");
    public static Counter recordRepairOff = Metrics.counter("RecordRepairOff");
    public static Counter deleteRecord = Metrics.counter("DeleteRecord");
    public static Counter upsertRecord = Metrics.counter("UpsertRecord");
    public static Counter recordNotInScope = Metrics.counter("RecordNotInScope");
    public static Counter readRows = Metrics.counter("ReadRows");
}
