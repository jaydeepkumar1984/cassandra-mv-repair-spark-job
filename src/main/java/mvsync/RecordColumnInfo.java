package mvsync;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;

public class RecordColumnInfo implements Serializable {

  public String name;
  @Nullable public String value;

  RecordColumnInfo(String name, @Nullable String value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    RecordColumnInfo that = (RecordColumnInfo) obj;
    if (this.value != null) {
      return this.name.equals(that.name) && this.value.equals(that.value);
    }
    return this.name.equals(that.name) && that.value == null;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    if (value != null) {
      sb.append(":");
      sb.append(value);
    }
    return sb.toString();
  }
}
