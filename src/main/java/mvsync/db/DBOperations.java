package mvsync.db;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.spark.connector.japi.CassandraRow;
import mvsync.MVSyncSettings;
import mvsync.TableAndMVColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;

public class DBOperations {

  private static Logger log = LoggerFactory.getLogger(DBOperations.class);

  CqlSession session;

  PreparedStatement selPreparedStmt;
  PreparedStatement delPreparedStmt;
  Map<String, PreparedStatement> upsertPreparedStmts;
  MVSyncSettings settings;
  TableAndMVColumns tableAndMVColumns;
  ConsistencyLevel consistencyLevel;

  public DBOperations(
      CqlSession session,
      PreparedStatementHelper preparedStatementHelper,
      MVSyncSettings settings,
      TableAndMVColumns tableAndMVColumns) {
    this.session = session;
    this.selPreparedStmt = preparedStatementHelper.buildSelectPreparedStatement();
    if (settings.getFixOrphanMvRecords()) {
      this.delPreparedStmt = preparedStatementHelper.buildDeletePreparedStatement();
    }
    if (settings.getFixMissingMvRecords() || settings.getFixInconsistentMvRecords()) {
      this.upsertPreparedStmts = preparedStatementHelper.buildUpsertPreparedStatement();
    }
    this.settings = settings;
    this.tableAndMVColumns = tableAndMVColumns;
    this.consistencyLevel = DefaultConsistencyLevel.valueOf(settings.getMvRecordsWriteConsistencyLevel());
  }

  public DBResult deleteFromMV(@Nullable CassandraRow mvTableEntry) {
    if (mvTableEntry != null) {
      try {
        // Ensure that the entry indeed not present in the base table
        DBResult selResults =
            isPresentInBaseTable(mvTableEntry, tableAndMVColumns.mvPrimaryKeyColumns);
        if (!selResults.success) {
          return selResults;
        }
        if (selResults.entryPresent) {
          log.info("Entry present in base table, not deleting from MV");
          return new DBResult(true, true, "");
        }
        BoundStatementBuilder  boundStatement = delPreparedStmt.boundStatementBuilder();
        for (Map.Entry<String, String> column : tableAndMVColumns.mvPrimaryKeyColumns.entrySet()) {
          bindHelper(
              boundStatement,
              column.getKey(),
              column.getValue(),
              mvTableEntry.getObject(column.getKey()));
        }
        session.execute(boundStatement.build());
      } catch (Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE error msg: ").append(e.getMessage()).append("\n");
        sb.append("delete query: ").append(delPreparedStmt.getQuery()).append("\n");
        sb.append("mvTableEntry: ").append(mvTableEntry).append("\n");
        sb.append("primaryKeyColumnsMV: ")
            .append(tableAndMVColumns.mvPrimaryKeyColumns)
            .append("\n");
        sb.append("consistencyLevel: ").append(consistencyLevel);
        log.error("Error deleting data: ", e);
        return new DBResult(false, false, "Error deleting data: " + sb);
      }
    }
    return new DBResult(true, false, "");
  }

  public void bindHelper(
          BoundStatementBuilder boundStatement, String columnName, String columnType, Object o) {
    switch (columnType.toLowerCase()) {
      case "ascii":
        boundStatement.set(columnName, (String) o, TypeCodecs.ASCII);
        break;
      case "bigint":
        boundStatement.set(columnName, (Long) o, TypeCodecs.BIGINT);
        break;
      case "blob":
        if (o instanceof byte[]) {
          o = ByteBuffer.wrap((byte[]) o);
        }
        boundStatement.set(columnName, (ByteBuffer) o, TypeCodecs.BLOB);
        break;
      case "boolean":
        boundStatement.set(columnName, (Boolean) o, TypeCodecs.BOOLEAN);
        break;
      case "date":
        Date date = ((Date) o);
        LocalDate localDate = date.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        boundStatement.set(columnName, localDate, TypeCodecs.DATE);
        break;
      case "decimal":
        boundStatement.set(columnName, (BigDecimal) o, TypeCodecs.DECIMAL);
        break;
      case "double":
        boundStatement.set(columnName, (Double) o, TypeCodecs.DOUBLE);
        break;
      case "float":
        boundStatement.set(columnName, (Float) o, TypeCodecs.FLOAT);
        break;
      case "inet":
        boundStatement.set(columnName, (InetAddress) o, TypeCodecs.INET);
        break;
      case "int":
        boundStatement.set(columnName, (Integer) o, TypeCodecs.INT);
        break;
      case "list":
        List<Object> list = (List)o;
        if (list.size() > 0) {
          Class<?> elementType = list.get(0).getClass();
          boundStatement.setList(columnName, (List) o, elementType);
        }
        break;
      case "map":
        Map<Object, Object> map = (Map)o;
        if (map.size() > 0) {
          Class<?> keyType = map.keySet().iterator().next().getClass();
          Class<?> valueType = map.values().iterator().next().getClass();
          boundStatement.setMap(columnName, (Map) o, keyType, valueType);
        }
        break;
      case "set":
        Set<Object> set = (Set)o;
        if (set.size() > 0) {
          Class<?> elementType = set.iterator().next().getClass();
          boundStatement.setSet(columnName, (Set) o, elementType);
        }
        break;
      case "smallint":
        boundStatement.set(columnName, (Short) o, TypeCodecs.SMALLINT);
        break;
      case "text":
        boundStatement.set(columnName, (String) o, TypeCodecs.ASCII);
        break;
      case "time":
        if (o instanceof LocalTime) {
          o = ((LocalTime) o).toNanoOfDay();
        }
        boundStatement.set(columnName, (Long) o, TypeCodecs.BIGINT);
        break;
      case "timestamp":
        if (o instanceof java.time.Instant) {
          o = Date.from(((java.time.Instant) o));
        }
        boundStatement.set(columnName, ((Date) o).toInstant(), TypeCodecs.TIMESTAMP);
        break;
      case "timeuuid":
        boundStatement.set(columnName, (UUID) o, TypeCodecs.TIMEUUID);
        break;
      case "tinyint":
        boundStatement.set(columnName, (Byte) o, TypeCodecs.TINYINT);
        break;
      case "uuid":
        boundStatement.set(columnName, (UUID) o, TypeCodecs.UUID);
        break;
      case "varchar":
        boundStatement.set(columnName, (String) o, TypeCodecs.TEXT);
        break;
      case "varint":
        boundStatement.set(columnName, (BigInteger) o, TypeCodecs.VARINT);
        break;
      default:
        throw new IllegalArgumentException("Unsupported type: " + columnType);
    }
  }

  public DBResult isPresentInBaseTable(
      @Nullable CassandraRow mvTableEntry, Map<String, String> mvTablePrimaryKeyColumns) {
    if (mvTableEntry != null) {
      boolean entryPresentInBaseTable = false;
      try {
        BoundStatementBuilder boundStatement = selPreparedStmt.boundStatementBuilder();
        for (Map.Entry<String, String> column : mvTablePrimaryKeyColumns.entrySet()) {
          bindHelper(
              boundStatement,
              column.getKey(),
              column.getValue(),
              mvTableEntry.getObject(column.getKey()));
        }

        ResultSet rs = session.execute(boundStatement.build());
        boundStatement.setConsistencyLevel(consistencyLevel);
        if (rs != null && rs.iterator().hasNext()) {
          entryPresentInBaseTable = true;
        }
      } catch (Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT error msg: ").append(e.getMessage()).append("\n");
        sb.append("select query: ").append(selPreparedStmt.getQuery()).append("\n");
        sb.append("mvTableEntry: ").append(mvTableEntry).append("\n");
        sb.append("primaryKeyColumnsMV: ").append(mvTablePrimaryKeyColumns).append("\n");
        sb.append("consistencyLevel: ").append(consistencyLevel);
        log.error("Error executing select query: ", e);
        return new DBResult(false, false, "Error executing select query: " + sb);
      }
      return new DBResult(true, entryPresentInBaseTable, "");
    }
    return new DBResult(true, false, "");
  }

  public static boolean shouldSkipField(
      @Nullable CassandraRow baseTableEntry, @Nullable CassandraRow mvTableEntry, String column) {
    if (mvTableEntry == null || baseTableEntry == null) {
      return false;
    }
    Object o1 = baseTableEntry.getObject(column);
    Object o2 = mvTableEntry.getObject(column);
    return Objects.deepEquals(o1, o2);
  }

  public DBResult upsert(
      @Nullable CassandraRow baseTableEntry, @Nullable CassandraRow mvTableEntry) {
    if (baseTableEntry != null) {
      PreparedStatement preparedStatement = null;
      try {
        for (Map.Entry<String, String> nonPrimaryKeyMVColumn :
            tableAndMVColumns.mvNonPrimaryKeyColumns.entrySet()) {
          if (shouldSkipField(baseTableEntry, mvTableEntry, nonPrimaryKeyMVColumn.getKey())) {
            continue;
          }
          Long timestamp = null;
          Integer ttl = null;
          if (!settings.getCassandraMutationUseLatestTS()) {
            try {
              timestamp =
                  baseTableEntry.getLong(
                      String.format("writetime(%s)", nonPrimaryKeyMVColumn.getKey()));
            } catch (Exception e) {
              // the ts is not found
              log.error("ts not found for column: {}, error: ", nonPrimaryKeyMVColumn.getKey(), e);
            }
          }
          try {
            ttl = baseTableEntry.getInt(String.format("ttl(%s)", nonPrimaryKeyMVColumn.getKey()));
          } catch (Exception e) {
            // the ttl is not found
          }

          UpsertFlavors upsertFlavor = UpsertFlavors.NO_TIMESTAMP_TTL;
          if (timestamp != null && ttl != null) {
            upsertFlavor = UpsertFlavors.TIMESTAMP_TTL;
          } else if (timestamp != null) {
            upsertFlavor = UpsertFlavors.TIMESTAMP;
          } else if (ttl != null) {
            upsertFlavor = UpsertFlavors.TTL;
          }

          preparedStatement =
              upsertPreparedStmts.get(getKey(nonPrimaryKeyMVColumn.getKey(), upsertFlavor));
          if (preparedStatement == null) {
            log.error(
                "Prepared statement not found for key: {} column: {}, flavor: {}, all stmts: {}",
                getKey(nonPrimaryKeyMVColumn.getKey(), upsertFlavor),
                nonPrimaryKeyMVColumn.getKey(),
                upsertFlavor,
                upsertPreparedStmts);
            return new DBResult(
                false,
                false,
                String.format(
                    "Prepared statement not found for key: %s column: %s, flavor: %s",
                    getKey(nonPrimaryKeyMVColumn.getKey(), upsertFlavor),
                    nonPrimaryKeyMVColumn.getKey(),
                    upsertFlavor));
          }
          BoundStatementBuilder boundStatement = preparedStatement.boundStatementBuilder();
          for (Map.Entry<String, String> column :
              tableAndMVColumns.mvPrimaryKeyColumns.entrySet()) {
            Object o = baseTableEntry.getObject(column.getKey());
            if (o == null) {
              log.error("Primary key column {} is null, skipping upsert", column);
              return new DBResult(
                  false, false, String.format("Primary key column %s is null", column));
            }
            bindHelper(
                boundStatement,
                column.getKey(),
                column.getValue(),
                baseTableEntry.getObject(column.getKey()));
          }
          if (isList(nonPrimaryKeyMVColumn.getValue())) {
            bindHelper(
                boundStatement,
                nonPrimaryKeyMVColumn.getKey(),
                nonPrimaryKeyMVColumn.getValue(),
                baseTableEntry.getList(nonPrimaryKeyMVColumn.getKey()));
          } else if (isMap(nonPrimaryKeyMVColumn.getValue())) {
            bindHelper(
                boundStatement,
                nonPrimaryKeyMVColumn.getKey(),
                nonPrimaryKeyMVColumn.getValue(),
                baseTableEntry.getMap(nonPrimaryKeyMVColumn.getKey()));
          } else if (isSet(nonPrimaryKeyMVColumn.getValue())) {
            bindHelper(
                boundStatement,
                nonPrimaryKeyMVColumn.getKey(),
                nonPrimaryKeyMVColumn.getValue(),
                baseTableEntry.getSet(nonPrimaryKeyMVColumn.getKey()));
          } else if (isDate(nonPrimaryKeyMVColumn.getValue())) {
            Date d = baseTableEntry.getDate(nonPrimaryKeyMVColumn.getKey());
            bindHelper(
                boundStatement,
                nonPrimaryKeyMVColumn.getKey(),
                nonPrimaryKeyMVColumn.getValue(),
                d);
          } else if (isBlob(nonPrimaryKeyMVColumn.getValue())) {
            bindHelper(
                boundStatement,
                nonPrimaryKeyMVColumn.getKey(),
                nonPrimaryKeyMVColumn.getValue(),
                baseTableEntry.getBytes(nonPrimaryKeyMVColumn.getKey()));
          } else {
            bindHelper(
                boundStatement,
                nonPrimaryKeyMVColumn.getKey(),
                nonPrimaryKeyMVColumn.getValue(),
                baseTableEntry.getObject(nonPrimaryKeyMVColumn.getKey()));
          }
          int timeStampIdx = tableAndMVColumns.mvPrimaryKeyColumns.size() + 1;
          int ttlIdx = tableAndMVColumns.mvPrimaryKeyColumns.size() + 1;
          if (upsertFlavor == UpsertFlavors.TIMESTAMP
              || upsertFlavor == UpsertFlavors.TIMESTAMP_TTL) {
            boundStatement.setLong(
                timeStampIdx,
                baseTableEntry.getLong(
                    String.format("writetime(%s)", nonPrimaryKeyMVColumn.getKey())));
            ttlIdx = timeStampIdx + 1;
          }
          if (upsertFlavor == UpsertFlavors.TTL || upsertFlavor == UpsertFlavors.TIMESTAMP_TTL) {
            boundStatement.setInt(
                ttlIdx,
                baseTableEntry.getInt(String.format("ttl(%s)", nonPrimaryKeyMVColumn.getKey())));
          }
          System.out.println("Executing bound statement: " + preparedStatement.getQuery());
          boundStatement.setConsistencyLevel(consistencyLevel);
          session.execute(boundStatement.build());
        }
      } catch (Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT error msg: ").append(e.getMessage()).append("\n");
        sb.append("insert query: ")
            .append(preparedStatement != null ? preparedStatement.getQuery() : "null")
            .append("\n");
        sb.append("baseTableEntry: ").append(baseTableEntry).append("\n");
        if (mvTableEntry != null) {
          sb.append("mvTableEntry: ").append(mvTableEntry).append("\n");
        }
        sb.append("primaryKeyColumnsMV: ")
            .append(tableAndMVColumns.mvPrimaryKeyColumns)
            .append("\n");
        sb.append("nonPrimaryKeyColumnsMV: ").append(tableAndMVColumns.mvNonPrimaryKeyColumns);
        sb.append("consistencyLevel: ").append(consistencyLevel);
        log.error("Error upserting data: ", e);
        return new DBResult(false, false, "Error upserting data: " + sb);
      }
    }
    return new DBResult(true, false, "");
  }

  public static String getKey(String columnName, UpsertFlavors flavors) {
    return columnName + "_" + flavors.name();
  }

  public static boolean isList(String type) {
    return type.equalsIgnoreCase("LIST");
  }

  public static boolean isMap(String type) {
    return type.equalsIgnoreCase("MAP");
  }

  public static boolean isSet(String type) {
    return type.equalsIgnoreCase("SET");
  }

  public static boolean isCollection(String type) {
    return isList(type) || isMap(type) || isSet(type);
  }

  public static boolean isBlob(String type) {
    return type.equalsIgnoreCase("BLOB");
  }

  public static boolean isDate(String type) {
    return type.equalsIgnoreCase("DATE");
  }

  public static boolean isSupportedType(String type) {
    switch (type.toUpperCase()) {
      case "UUID":
      case "INT":
      case "INET":
      case "LIST":
      case "TIMEUUID":
      case "VARINT":
      case "BIGINT":
      case "BOOLEAN":
      case "ASCII":
      case "DATE":
      case "BLOB":
      case "FLOAT":
      case "SET":
      case "SMALLINT":
      case "TIMESTAMP":
      case "MAP":
      case "TINYINT":
      case "TEXT":
      case "TIME":
      case "DOUBLE":
      case "DECIMAL":
        return true;
      default:
        return false;
    }
  }

  public static String convertToString(Object data) {
    if (data instanceof Date) {
      return String.valueOf(((Date) data).getTime());
    }
    if (data instanceof byte[]) {
      return new String((byte[]) data);
    }
    if (data instanceof ByteBuffer) {
      return Charset.forName("UTF-8").decode((ByteBuffer) data).toString();
    }
    if (data == null) {
      return "null";
    }
    return data.toString();
  }

  public static class DBResult {

    public boolean success;
    public boolean entryPresent;
    public String errMessage;

    public DBResult(boolean success, boolean entryPresent, String message) {
      this.success = success;
      this.entryPresent = entryPresent;
      this.errMessage = message;
    }
  }
}
