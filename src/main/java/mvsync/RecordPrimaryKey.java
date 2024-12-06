package mvsync;

import mvsync.db.DBOperations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RecordPrimaryKey implements Serializable {

  public List<Object> keys;
  public List<Object> columnNames;
  public List<Object> columnTypes;

  RecordPrimaryKey() {
    this.keys = new ArrayList<>();
    this.columnNames = new ArrayList<>();
    this.columnTypes = new ArrayList<>();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    RecordPrimaryKey that = (RecordPrimaryKey) obj;
    if (keys.size() != that.keys.size()
        || columnNames.size() != that.columnNames.size()
        || columnTypes.size() != that.columnTypes.size()) {
      return false;
    }
    for (int i = 0; i < keys.size(); i++) {
      if (!Objects.deepEquals(keys.get(i), that.keys.get(i))
          || !Objects.deepEquals(columnNames.get(i), that.columnNames.get(i))
          || !Objects.deepEquals(columnTypes.get(i), that.columnTypes.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (int i = 0; i < keys.size() && keys.get(i) != null; i++) {
      if (keys.get(i) instanceof byte[]) {
        hash = 31 * hash + Arrays.hashCode((byte[]) keys.get(i));
      } else {
        hash = 31 * hash + keys.get(i).hashCode();
      }
      hash = 31 * hash + columnNames.get(i).hashCode();
      hash = 31 * hash + columnTypes.get(i).hashCode();
    }
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(DBOperations.convertToString(columnNames.get(i)));
      sb.append(":");
      sb.append(DBOperations.convertToString(columnTypes.get(i)));
      sb.append(":");
      sb.append(DBOperations.convertToString(keys.get(i)));
    }
    return sb.toString();
  }
}
