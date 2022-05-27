package cn.edu.thssdb.schema;

import cn.edu.thssdb.schema.Column;
import java.util.ArrayList;
import java.util.List;

/**
 * MetaInfo is used to hold and index meta information.
 */

public class MetaInfo {

  private final String tableName;
  private final List<Column> columns;

  MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  int columnFind(String name) {
    int size = columns.size();
    for (int i = 0; i < size; ++i)
      if (columns.get(i).getColumnName().equals(name))
        return i;
    return -1;
  }

  String getColumnName(int index) {
    if (index < 0 || index >= columns.size())
      return null;
    return columns.get(index).getColumnName();
  }

  String getTableDotColumnName(int index) {
    if (index < 0 || index >= columns.size())
      return null;
    return tableName + "." + getColumnName(index);
  }

  int getColumnSize() {
    return columns.size();
  }

  String getTableName() {
    return tableName;
  }
}