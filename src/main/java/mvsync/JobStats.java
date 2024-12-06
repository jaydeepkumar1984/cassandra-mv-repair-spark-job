package mvsync;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;

public class JobStats implements Serializable {
  LongAccumulator totRecords;
  LongAccumulator recordNotInScope;
  LongAccumulator consistentRecords;
  LongAccumulator inConsistentRecords;
  LongAccumulator missingBaseTableRecords;
  LongAccumulator missingMvRecords;
  LongAccumulator repairRecords;
  LongAccumulator notRepairRecords;
  LongAccumulator delAttemptedRecords;
  LongAccumulator delErrRecords;
  LongAccumulator delSuccessRecords;
  LongAccumulator notDelRecords;
  LongAccumulator upsertAttemptedRecords;
  LongAccumulator upsertErrRecords;
  LongAccumulator upsertSuccessRecords;

  JobStats(JavaSparkContext jsc) {
    totRecords = jsc.sc().longAccumulator();
    recordNotInScope = jsc.sc().longAccumulator();
    consistentRecords = jsc.sc().longAccumulator();
    inConsistentRecords = jsc.sc().longAccumulator();
    missingBaseTableRecords = jsc.sc().longAccumulator();
    missingMvRecords = jsc.sc().longAccumulator();
    repairRecords = jsc.sc().longAccumulator();
    notRepairRecords = jsc.sc().longAccumulator();
    delAttemptedRecords = jsc.sc().longAccumulator();
    delErrRecords = jsc.sc().longAccumulator();
    delSuccessRecords = jsc.sc().longAccumulator();
    notDelRecords = jsc.sc().longAccumulator();
    upsertAttemptedRecords = jsc.sc().longAccumulator();
    upsertErrRecords = jsc.sc().longAccumulator();
    upsertSuccessRecords = jsc.sc().longAccumulator();
  }

  @Override
  public String toString() {
    return "totRecords: "
        + totRecords.count()
        + ", skippedRecords: "
        + recordNotInScope.count()
        + ", consistentRecords: "
        + consistentRecords.count()
        + ", inConsistentRecords: "
        + inConsistentRecords.count()
        + ", missingBaseTableRecords: "
        + missingBaseTableRecords.count()
        + ", missingMvRecords: "
        + missingMvRecords.count()
        + ", repairRecords: "
        + repairRecords.count()
        + ", notRepairRecords: "
        + notRepairRecords.count()
        + ", delAttemptedRecords: "
        + delAttemptedRecords.count()
        + ", delErrRecords: "
        + delErrRecords.count()
        + ", delSuccessRecords: "
        + delSuccessRecords.count()
        + ", notDelRecords: "
        + notDelRecords.count()
        + ", upsertAttemptedRecords: "
        + upsertAttemptedRecords.count()
        + ", upsertErrRecords: "
        + upsertErrRecords.count()
        + ", upsertSuccessRecords: "
        + upsertSuccessRecords.count();
  }
}
