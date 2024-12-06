package mvsync;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class TableAndMVColumns implements Serializable {
  public Map<String, String> baseTablePrimaryKeyColumns;
  public Map<String, String> baseTableNonPrimaryKeyColumns;
  public Map<String, String> mvPrimaryKeyColumns;
  public Map<String, String> mvNonPrimaryKeyColumns;

  TableAndMVColumns() {
    this.baseTablePrimaryKeyColumns = new TreeMap<>();
    this.baseTableNonPrimaryKeyColumns = new TreeMap<>();
    this.mvPrimaryKeyColumns = new TreeMap<>();
    this.mvNonPrimaryKeyColumns = new TreeMap<>();
  }

  public TableAndMVColumns(Map<String, String> baseTablePrimaryKeyColumns,
      Map<String, String> baseTableNonPrimaryKeyColumns, Map<String, String> mvPrimaryKeyColumns,
      Map<String, String> mvNonPrimaryKeyColumns) {
    this.baseTablePrimaryKeyColumns = baseTablePrimaryKeyColumns;
    this.baseTableNonPrimaryKeyColumns = baseTableNonPrimaryKeyColumns;
    this.mvPrimaryKeyColumns = mvPrimaryKeyColumns;
    this.mvNonPrimaryKeyColumns = mvNonPrimaryKeyColumns;
  }
}
