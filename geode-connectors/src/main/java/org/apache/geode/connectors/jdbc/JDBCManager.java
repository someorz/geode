package org.apache.geode.connectors.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxInstance;

public class JDBCManager {

  private final JDBCConfiguration config;

  private Connection conn;

  private Statement stmt;

  JDBCManager(JDBCConfiguration config) {
    this.config = config;
  }

  private void establishConnection() {
    // Class.forName(this.config.getDriver());
    // conn = DriverManager.getConnection(this.config.getURL());
    // stmt = conn.createStatement();
  }

  public interface ColumnValue {
    String getColumnName();

    Object getValue();
  }

  public void write(Region region, Operation operation, Object key, PdxInstance value) {
    String tableName = getTableName(region);
    List<ColumnValue> columnList = getColumnToValueList(tableName, key, value);
    String query = getQueryString(tableName, columnList, operation);
    Statement statement = getQueryStatement(columnList, query);
    try {
      statement.execute(query);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private String getQueryString(String tableName, List<ColumnValue> columnList,
      Operation operation) {
    if (operation.isCreate()) {
      return getInsertQueryString(tableName, columnList);
    } else if (operation.isUpdate()) {
      return getUpdateQueryString(tableName, columnList);
    } else if (operation.isDestroy()) {
      return getDestroyQueryString(tableName, columnList);
    } else {
      throw new IllegalStateException("unsupported operation " + operation);
    }
  }

  private String getDestroyQueryString(String tableName, List<ColumnValue> columnList) {
    // TODO Auto-generated method stub
    return null;
  }

  private String getUpdateQueryString(String tableName, List<ColumnValue> columnList) {
    // TODO Auto-generated method stub
    return null;
  }

  private String getInsertQueryString(String tableName, List<ColumnValue> columnList) {
    StringBuilder columnNames = new StringBuilder("INSERT INTO " + tableName + '(');
    StringBuilder columnValues = new StringBuilder(" VALUES (");
    int columnCount = columnList.size();
    int idx = 0;
    for (ColumnValue cv : columnList) {
      idx++;
      columnNames.append(cv.getColumnName());
      columnValues.append('?');
      if (idx != columnCount) {
        columnNames.append(", ");
        columnValues.append(",");
      }
    }
    columnNames.append(")");
    columnValues.append(")");
    return columnNames.append(columnValues).toString();
  }

  private Statement getQueryStatement(List<ColumnValue> columnList, String query) {

    // TODO Auto-generated method stub
    return null;
  }

  private List<ColumnValue> getColumnToValueList(String tableName, Object key, PdxInstance value) {
    // TODO Auto-generated method stub
    return null;
  }

  private String getTableName(Region region) {
    // TODO: check config for mapping
    return region.getName();
  }



}
