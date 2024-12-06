package mvsync.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import mvsync.MVSyncSettings;
import mvsync.TableAndMVColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreparedStatementHelper {
  private static Logger log = LoggerFactory.getLogger(PreparedStatementHelper.class);

  CqlSession session;
  MVSyncSettings settings;
  TableAndMVColumns tableAndMVColumns;

  public PreparedStatementHelper(
      CqlSession session, MVSyncSettings settings, TableAndMVColumns tableAndMVColumns) {
    this.session = session;
    this.settings = settings;
    this.tableAndMVColumns = tableAndMVColumns;
  }

  public Map<String, PreparedStatement> buildUpsertPreparedStatement() {
    Map<String, PreparedStatement> upsertPreparedStatements = new HashMap<>();
    List<UpsertFlavors> upsertFlavors =
        Arrays.asList(
            UpsertFlavors.NO_TIMESTAMP_TTL,
            UpsertFlavors.TIMESTAMP,
            UpsertFlavors.TTL,
            UpsertFlavors.TIMESTAMP_TTL);
    for (UpsertFlavors upsertFlavor : upsertFlavors) {
      for (String nonPrimaryKeyColumn : tableAndMVColumns.mvNonPrimaryKeyColumns.keySet()) {
        InsertInto initialInsertStatement =
            QueryBuilder.insertInto(settings.getBaseTableKeyspaceName(), settings.getMvName());

        RegularInsert insertStatement = null;
        for (String primaryKeyColumn : tableAndMVColumns.mvPrimaryKeyColumns.keySet()) {
          if (insertStatement == null) {
            insertStatement = initialInsertStatement.value(primaryKeyColumn, QueryBuilder.bindMarker());
          } else {
            insertStatement = insertStatement.value(primaryKeyColumn, QueryBuilder.bindMarker());
          }
        }

        insertStatement = insertStatement.value(nonPrimaryKeyColumn, QueryBuilder.bindMarker());
        Insert finalInsert = insertStatement;
        if (upsertFlavor == UpsertFlavors.TIMESTAMP) {
          finalInsert = finalInsert.usingTimestamp(QueryBuilder.bindMarker());
        } else if (upsertFlavor == UpsertFlavors.TTL) {
          finalInsert = finalInsert.usingTtl(QueryBuilder.bindMarker());
        } else if (upsertFlavor == UpsertFlavors.TIMESTAMP_TTL) {
          finalInsert = finalInsert.usingTimestamp(QueryBuilder.bindMarker());
          finalInsert = finalInsert.usingTtl(QueryBuilder.bindMarker());
        }
        String query = finalInsert.toString();
        log.info(
            "INSERT Key: {}, Query: {}",
            DBOperations.getKey(nonPrimaryKeyColumn, upsertFlavor),
            query);
        upsertPreparedStatements.put(
            DBOperations.getKey(nonPrimaryKeyColumn, upsertFlavor), session.prepare(query));
      }
    }
    return upsertPreparedStatements;
  }

  public PreparedStatement buildSelectPreparedStatement() {
    Select selStatement =
        QueryBuilder.selectFrom(settings.getBaseTableKeyspaceName(), settings.getBaseTableName()).all();
    for (String column : tableAndMVColumns.mvPrimaryKeyColumns.keySet()) {
      selStatement = selStatement.whereColumn(column).isEqualTo(QueryBuilder.bindMarker());
    }

    selStatement = selStatement.allowFiltering();

    String query = selStatement.toString();
    log.info("SELECT Query: {}", query);
    return session.prepare(query);
  }

  public PreparedStatement buildDeletePreparedStatement() {
    DeleteSelection delStatement =
        QueryBuilder.deleteFrom(settings.getBaseTableKeyspaceName(), settings.getMvName());
    Delete delQuery = null;
    for (String column : tableAndMVColumns.mvPrimaryKeyColumns.keySet()) {
      if (delQuery == null) {
        delQuery = delStatement.whereColumn(column).isEqualTo(QueryBuilder.bindMarker());
      } else {
        delQuery = delQuery.whereColumn(column).isEqualTo(QueryBuilder.bindMarker());
      }
    }
    String query = delQuery.toString();
    log.info("DELETE Query: {}", query);
    return session.prepare(query);
  }
}
