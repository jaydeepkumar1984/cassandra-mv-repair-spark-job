package mvsync;

import com.datastax.spark.connector.japi.CassandraRow;

import javax.annotation.Nullable;
import java.io.Serializable;

public class MVInconsistentRow implements Serializable {
  MVConsistencyState problem;
  RecordPrimaryKey rowKey;
  @Nullable CassandraRow baseTableEntry;
  @Nullable CassandraRow mvTableEntry;
  @Nullable RecordColumnInfo baseColumn;
  @Nullable RecordColumnInfo mvColumn;

  public MVInconsistentRow(
      MVConsistencyState problem,
      RecordPrimaryKey rowKey,
      @Nullable CassandraRow baseTableEntry,
      @Nullable CassandraRow mvTableEntry,
      @Nullable RecordColumnInfo baseColumn,
      @Nullable RecordColumnInfo mvColumn) {
    this.rowKey = rowKey;
    this.baseTableEntry = baseTableEntry;
    this.mvTableEntry = mvTableEntry;
    this.problem = problem;
    this.baseColumn = baseColumn;
    this.mvColumn = mvColumn;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Problem: ")
        .append(problem)
        .append("\n")
        .append("RowKey: ")
        .append(rowKey)
        .append("\n")
        .append("MainTableEntry: ")
        .append(baseTableEntry)
        .append("\n")
        .append("MVTableEntry: ")
        .append(mvTableEntry);
    if (baseColumn != null) {
      sb.append("\n").append("BaseColumn: ").append(baseColumn.toString());
    }
    if (mvColumn != null) {
      sb.append("\n").append("MvColumn: ").append(mvColumn.toString());
    }
    return sb.toString();
  }
}
