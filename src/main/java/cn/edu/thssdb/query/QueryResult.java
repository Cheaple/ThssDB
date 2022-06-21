package cn.edu.thssdb.query;


import cn.edu.thssdb.common.Pair;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.MetaInfo;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.QueryResultType;

import javax.management.QueryEval;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Designed to hold general query result:
 * In SQL result, the returned answer could be QueryTable OR an error message
 * For errors, resultType = QueryResultType.MESSAGE, see Construct method.
 * For results, it will hold QueryTable.
 */

public class QueryResult {

  public final QueryResultType resultType;
  public final String errorMessage; // If it is an error.

  private List<MetaInfo> metaInfoInfos;
  private List<String> columnNames;  // 此处的列名为完整列名，如“table1.column2”

  private List<Pair<Integer, Integer>> columnList;  // <table_idx, column_idx>列表

  private List<QueryTable> queryTables;

  // WHERE子句过滤
  Integer filterTb = -1;
  Integer filterCol = -1;
  String filterOp;
  Object filterVal = null;

  public List<Row> results;

  public QueryResult(QueryTable[] queryTables) {
    this.resultType = QueryResultType.SELECT;
    this.errorMessage = null;
    // TODO
  }

  // Constructor
  public QueryResult(List<QueryTable> queryTables, List<String> columnNames) {
    this.resultType = QueryResultType.SELECT;
    this.errorMessage = null;
    // ***

    this.columnNames = columnNames;
    this.queryTables = queryTables;
    this.columnList = retrieveColumns(columnNames);
  }

  public QueryResult(String errorMessage) {
    this.resultType = QueryResultType.MESSAGE;
    this.errorMessage = errorMessage;
  }

  public void setFilter(String columnName, String valueStr, String op) {
    Pair<Integer, Integer> pair = retrieveColumn(columnName);
    this.filterTb = pair.left;
    this.filterCol = pair.right;
    this.filterOp = op;

    // 将String类型的value转化为该列的数据类型
    ColumnType valueType = queryTables.get(filterTb).getMetaInfo().getColumnType(filterCol);
    switch (valueType) {
      case INT:
        this.filterVal = Integer.valueOf(valueStr);
        break;
      case DOUBLE:
        this.filterVal = Double.valueOf(valueStr);
        break;
      case FLOAT:
        this.filterVal = Float.valueOf(valueStr);
        break;
      case LONG:
        this.filterVal = Long.valueOf(valueStr);
        break;
      case STRING:
      default:
        this.filterVal = valueStr.substring(1, valueStr.length() - 1);;
        break;
    }
  }

  private Boolean compare(Cell c1, Cell c2, String op) {
    switch (op) {
      case "<":
        return c1.compareTo(c2) == -1;
      case ">":
        return c1.compareTo(c2) == 1;
      case "<=":
        return c1.compareTo(c2) != 1;
      case ">=":
        return c1.compareTo(c2) != -1;
      case "=":
        return c1.equals(c2);
      case "<>":
        return !c1.equals(c2);
    }
    return false;
  }

  /**
   * 生成最终的查询结果
   */
  public List<Row> query() {
    QueryTable it = queryTables.get(0);
    while (it.hasNext()) {
      Row row = it.next();
      if (compare(row.getEntries().get(filterCol),
              new Cell((Comparable) filterVal), filterOp))
        results.add(row);
    }

    return results;
  }


  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    Row res = new Row();
    rows.forEach(it -> {
      res.appendEntries(it.getEntries());
    });
    return res;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    return new Row(row);
  }

  public List<String> getColumnNames() {
    return this.columnNames;
  }

  /**
   * 解析形如“table.column”的列名
   */
  private List<Pair<Integer, Integer>> retrieveColumns(List<String> columnNames) {
    List<Pair<Integer, Integer>> res = new ArrayList<>();
    columnNames.forEach(it -> {
      res.add(retrieveColumn(it));
    });
    return res;
  }

  /**
   * 解析形如“table.column”的列名
   */
  private Pair<Integer, Integer> retrieveColumn(String columnName) {
    String[] pair = columnName.split("\\.");
    int tb_idx = -1, col_idx = 0;

    if (pair.length == 1) { // <column>
      // 将<column>转化为<table_idx, column_idx>
      tb_idx = 0;
      col_idx = queryTables.get(tb_idx).getMetaInfo().columnFind(pair[0]);
    } else if (pair.length == 2) {  // <table, column>
      // 将<table, column>转化为<table_idx, column_idx>
      for (int i = 0; i < queryTables.size(); ++i) {
        if (queryTables.get(i).getName().equals(pair[0]))
          tb_idx = i;
      }
      col_idx = queryTables.get(tb_idx).getMetaInfo().columnFind(pair[0]);
    } else {  // 无法解析该列名
      throw new KeyNotExistException();
    }

    if (tb_idx == -1) {  // table不存在
      throw new TableNotExistException();
    }
    if (col_idx == -1) {  // column不存在该table中
      throw new KeyNotExistException();
    }

    return new Pair<Integer, Integer>(tb_idx, col_idx);
  }
}
