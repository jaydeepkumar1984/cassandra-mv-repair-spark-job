package mvsync.output;

import mvsync.MVConsistencyState;
import mvsync.MVInconsistentRow;
import mvsync.MVSyncSettings;
import mvsync.db.DBOperations;

import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

import static mvsync.MVConsistencyState.*;
import static mvsync.MvSync.mvJobOutputStreamFactory;

public class MVJobOutputStreamer {

  private static Logger log = LoggerFactory.getLogger(MVJobOutputStreamer.class);
  @Nullable public IBlobStreamer streamerMissingInBaseTable;
  @Nullable public IBlobStreamer streamerMissingInMv;
  @Nullable public IBlobStreamer streamerMismatch;
  @Nullable public IBlobStreamer streamerErrDeleting;
  @Nullable public IBlobStreamer streamerErrUpserting;

  public MVJobOutputStreamer(MVSyncSettings settings) throws Exception {
    String pathMissingInBaseTable =
        String.format(
            "%s/%s/%d.txt",
            settings.getOutputDir(), MISSING_IN_BASE_TABLE, TaskContext.getPartitionId());
    String pathMissingInMv =
        String.format(
            "%s/%s/%d.txt",
            settings.getOutputDir(), MISSING_IN_MV_TABLE, TaskContext.getPartitionId());
    String pathMismatch =
        String.format(
            "%s/%s/%d.txt",
            settings.getOutputDir(), INCONSISTENT, TaskContext.getPartitionId());
    String pathErrDeleting =
        String.format(
            "%s/%s/%d.txt",
            settings.getOutputDir(), "ERR_DELETING", TaskContext.getPartitionId());
    String pathErrUpserting =
        String.format(
            "%s/%s/%d.txt",
            settings.getOutputDir(), "ERR_UPSERTING", TaskContext.getPartitionId());
    log.info(
        "Upload data to paths: {}, {}, {}, {}, {}",
        pathMissingInBaseTable,
        pathMissingInMv,
        pathMismatch,
        pathErrDeleting,
        pathErrUpserting);

    this.streamerMissingInBaseTable =
        mvJobOutputStreamFactory.getStream(pathMissingInBaseTable, settings);
    this.streamerMissingInMv = mvJobOutputStreamFactory.getStream(pathMissingInMv, settings);
    this.streamerMismatch = mvJobOutputStreamFactory.getStream(pathMismatch, settings);
    this.streamerErrDeleting = mvJobOutputStreamFactory.getStream(pathErrDeleting, settings);
    this.streamerErrUpserting = mvJobOutputStreamFactory.getStream(pathErrUpserting, settings);
  }

  public void streamOutput(
      MVJobOutputStreamer MVJobOutputStreamer,
      MVConsistencyState problem,
      MVInconsistentRow inconsistentRow,
      @Nullable DBOperations.DBResult delResults,
      @Nullable DBOperations.DBResult upsertResults)
      throws IOException {
    if (problem == MISSING_IN_BASE_TABLE
        && MVJobOutputStreamer.streamerMissingInBaseTable != null) {
      MVJobOutputStreamer.streamerMissingInBaseTable.append(inconsistentRow.toString());
      MVJobOutputStreamer.streamerMissingInBaseTable.append("==============================");
    } else if (problem == MISSING_IN_MV_TABLE && MVJobOutputStreamer.streamerMissingInMv != null) {
      MVJobOutputStreamer.streamerMissingInMv.append(inconsistentRow.toString());
      MVJobOutputStreamer.streamerMissingInMv.append("==============================");
    } else if (problem == INCONSISTENT && MVJobOutputStreamer.streamerMismatch != null) {
      MVJobOutputStreamer.streamerMismatch.append(inconsistentRow.toString());
      MVJobOutputStreamer.streamerMismatch.append("==============================");
    }
    if (delResults != null
        && !delResults.success
        && MVJobOutputStreamer.streamerErrDeleting != null) {
      MVJobOutputStreamer.streamerErrDeleting.append(inconsistentRow.toString());
      MVJobOutputStreamer.streamerErrDeleting.append(delResults.errMessage);
      MVJobOutputStreamer.streamerErrDeleting.append("==============================");
    }
    if (upsertResults != null
        && !upsertResults.success
        && MVJobOutputStreamer.streamerErrUpserting != null) {
      MVJobOutputStreamer.streamerErrUpserting.append(inconsistentRow.toString());
      MVJobOutputStreamer.streamerErrUpserting.append(upsertResults.errMessage);
      MVJobOutputStreamer.streamerErrUpserting.append("==============================");
    }
  }

  public void close() {
    try {
      if (streamerMissingInBaseTable != null) {
        streamerMissingInBaseTable.commit();
      }
    } catch (Exception e) {
      log.error("Error committing streamerMissingInBaseTable", e);
    }
    try {
      if (streamerMissingInMv != null) {
        streamerMissingInMv.commit();
      }
    } catch (Exception e) {
      log.error("Error committing streamerMissingInMv", e);
    }
    try {
      if (streamerMismatch != null) {
        streamerMismatch.commit();
      }
    } catch (Exception e) {
      log.error("Error committing streamerMismatch", e);
    }
    try {
      if (streamerErrDeleting != null) {
        streamerErrDeleting.commit();
      }
    } catch (Exception e) {
      log.error("Error committing streamerErrDeleting", e);
    }
    try {
      if (streamerErrUpserting != null) {
        streamerErrUpserting.commit();
      }
    } catch (Exception e) {
      log.error("Error committing streamerErrUpserting", e);
    }
  }
}
