package cn.edu.thssdb.query;


import cn.edu.thssdb.common.Pair;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.QueryResultType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.thssdb.schema.Column.parseEntry;

/**
 * Designed to hold general query result:
 * In SQL result, the returned answer could be QueryTable OR an error message
 * For errors, resultType = QueryResultType.MESSAGE, see Construct method.
 * For results, it will hold QueryTable.
 */

public class QueryResult {

  public final QueryResultType resultType;
  public final String errorMessage; // If it is an error.
  private List<String> columnNames;  // 此处的列名为完整列名，如“table1.column2”
  private List<Pair<Integer, Integer>> columnList;  // <table_idx, column_idx>列表
  private List<QueryTable> queryTables;

  // JOIN ON
  Integer ifJoinOn = -1;  // -1: 单表查询; 0: 双表自然连接; 1: 双表条件连接
  List<Integer> joinLeftColumns;
  List<Integer> joinRightColumns;
  String joinOp = "=";

  // WHERE子句过滤
  Integer filterTb = -1;
  Integer filterCol = -1;
  String filterOp;
  Cell filterEntry = null;

  public List<Row> results;

  public QueryResult(QueryTable[] queryTables) {
    this.resultType = QueryResultType.SELECT;
    this.errorMessage = null;
    // TODO
  }

  // Constructor
  public QueryResult(List<QueryTable> queryTables, List<String> tbList, List<String> colList) {
    this.resultType = QueryResultType.SELECT;
    this.errorMessage = null;
    this.queryTables = queryTables;
    this.columnList = new ArrayList<>();
    this.columnNames = new ArrayList<>();

    for (int i = 0; i < tbList.size(); ++i) {
      Integer tb_idx = findTableIdx(tbList.get(i));
      Integer col_idx = findColIdx(tb_idx, colList.get(i));
      this.columnList.add(new Pair<Integer, Integer>(tb_idx, col_idx));
      if (queryTables.size() == 1)
        this.columnNames.add(colList.get(i));
      else
        this.columnNames.add(tbList.get(i) + "." + colList.get(i));
    }
  }

  public QueryResult(String errorMessage) {
    this.resultType = QueryResultType.MESSAGE;
    this.errorMessage = errorMessage;
  }

  public void setFilter(String tb, String col, String valueStr, String op) {
    this.filterTb = findTableIdx(tb);
    this.filterCol = findColIdx(this.filterTb, col);
    this.filterOp = op;
    this.filterEntry = parseEntry(valueStr, queryTables.get(filterTb).getMetaInfo().getColumn(this.filterCol));

    // 将String类型的value转化为该列的数据类型
    ColumnType valueType = queryTables.get(filterTb).getMetaInfo().getColumnType(filterCol);
    switch (valueType) {
      case INT:
        this.filterEntry = new Cell(Integer.valueOf(valueStr));
        break;
      case DOUBLE:
        this.filterEntry = new Cell(Double.valueOf(valueStr));
        break;
      case FLOAT:
        this.filterEntry = new Cell(Float.valueOf(valueStr));
        break;
      case LONG:
        this.filterEntry = new Cell(Long.valueOf(valueStr));
        break;
      case STRING:
      default:
        this.filterEntry = new Cell(valueStr.substring(1, valueStr.length() - 1));
        break;
    }
  }

  /** 条件连接 */
  public void setJoin(String col1, String col2, String op) {
    this.ifJoinOn = 1;  // 设置为条件连接
    this.joinOp = op;
    this.joinLeftColumns = new ArrayList<>();
    this.joinRightColumns = new ArrayList<>();
    this.joinLeftColumns.add(findColIdx(0, col1));
    this.joinRightColumns.add(findColIdx(1, col2));
  }

  /** 自然连接 */
  public void setJoin() {
    this.ifJoinOn = 0;  // 设置为自然连接
    // TODO: 找出两表的同名列
  }

  /**
   * 生成最终的查询结果
   */
  public List<Row> query() {
    this.results = new ArrayList<>();

    if (this.ifJoinOn == -1) { // 单表
      QueryTable it1 = queryTables.get(0);

      if (filterTb == -1) {  // 不过滤
        while (it1.hasNext()) {
          ArrayList<Cell> entryList = it1.next().getEntries();
          ArrayList<Cell> selectedEntryList = new ArrayList<>();
          this.columnList.forEach(it-> {
            selectedEntryList.add(new Cell(entryList.get(it.right)));
          });  // 选取Row中被Select的属性
          results.add(new Row(selectedEntryList));
        }
      }
      else {
        if (filterOp.equals("<")) {
          while (it1.hasNext()) {
            ArrayList<Cell> entryList = it1.next().getEntries();
            if (entryList.get(filterCol).compareTo(this.filterEntry) == -1) {
              ArrayList<Cell> selectedEntryList = new ArrayList<>();
              this.columnList.forEach(it-> {
                selectedEntryList.add(new Cell(entryList.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntryList));
            }
          }
        }
        else if (filterOp.equals(">")) {
          while (it1.hasNext()) {
            ArrayList<Cell> entryList = it1.next().getEntries();
            if (entryList.get(filterCol).compareTo(this.filterEntry) == 1) {
              ArrayList<Cell> selectedEntryList = new ArrayList<>();
              this.columnList.forEach(it-> {
                selectedEntryList.add(new Cell(entryList.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntryList));
            }
          }
        }
        else if (filterOp.equals("<=")) {
          while (it1.hasNext()) {
            ArrayList<Cell> entryList = it1.next().getEntries();
            if (entryList.get(filterCol).compareTo(this.filterEntry) != 1) {
              ArrayList<Cell> selectedEntryList = new ArrayList<>();
              this.columnList.forEach(it-> {
                selectedEntryList.add(new Cell(entryList.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntryList));
            }
          }
        }
        else if (filterOp.equals(">=")) {
          while (it1.hasNext()) {
            ArrayList<Cell> entryList = it1.next().getEntries();
            if (entryList.get(filterCol).compareTo(this.filterEntry) != -1) {
              ArrayList<Cell> selectedEntryList = new ArrayList<>();
              this.columnList.forEach(it-> {
                selectedEntryList.add(new Cell(entryList.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntryList));
            }
          }
        }
        else if (filterOp.equals("=")) {
          while (it1.hasNext()) {
            ArrayList<Cell> entryList = it1.next().getEntries();
            if (entryList.get(filterCol).equals(this.filterEntry)) {
              ArrayList<Cell> selectedEntryList = new ArrayList<>();
              this.columnList.forEach(it-> {
                selectedEntryList.add(new Cell(entryList.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntryList));
            }
          }
        }
        else if (filterOp.equals("<>")) {
          while (it1.hasNext()) {
            ArrayList<Cell> entryList = it1.next().getEntries();
            if (!entryList.get(filterCol).equals(this.filterEntry)) {
              ArrayList<Cell> selectedEntryList = new ArrayList<>();
              this.columnList.forEach(it-> {
                selectedEntryList.add(new Cell(entryList.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntryList));
            }
          }
        }
      }
    }

    else { // 双表，先WHERE过滤，再JOIN
      ArrayList<Row> leftRows = new ArrayList<>();
      ArrayList<Row> rightRows = new ArrayList<>();
      QueryTable it1 = queryTables.get(0);
      QueryTable it2 = queryTables.get(1);

      // WHERE过滤
      if (filterTb == -1) {  // 不过滤
        while (it1.hasNext()) {
          leftRows.add(new Row(it1.next()));
        }
      }
      else {
        it1 = queryTables.get(filterTb);
        it2 = queryTables.get(1 - filterTb);

        // 先导入WHERE子句所用表，并进行过滤
        if (filterOp.equals("<")) {
          while (it1.hasNext()) {
            Row row = it1.next();
            ArrayList<Cell> entryList = row.getEntries();
            if (entryList.get(filterCol).compareTo(this.filterEntry) == -1) {
              leftRows.add(new Row(row));
            }
          }
        }
        else if (filterOp.equals(">")) {
          while (it1.hasNext()) {
            Row row = it1.next();
            ArrayList<Cell> entryList = row.getEntries();
            if (entryList.get(filterCol).compareTo(this.filterEntry) == 1) {
              leftRows.add(new Row(row));
            }
          }
        }
        else if (filterOp.equals("<=")) {
          while (it1.hasNext()) {
            Row row = it1.next();
            ArrayList<Cell> entryList = row.getEntries();
            if (entryList.get(filterCol).compareTo(this.filterEntry) != 1) {
              leftRows.add(new Row(row));
            }
          }
        }
        else if (filterOp.equals(">=")) {
          while (it1.hasNext()) {
            Row row = it1.next();
            ArrayList<Cell> entryList = row.getEntries();
            if (entryList.get(filterCol).compareTo(this.filterEntry) != -1) {
              leftRows.add(new Row(row));
            }
          }
        }
        else if (filterOp.equals("=")) {
          while (it1.hasNext()) {
            Row row = it1.next();
            ArrayList<Cell> entryList = row.getEntries();
            if (entryList.get(filterCol).equals(this.filterEntry)) {
              leftRows.add(new Row(row));
            }
          }
        }
        else if (filterOp.equals("<>")) {
          while (it1.hasNext()) {
            Row row = it1.next();
            ArrayList<Cell> entryList = row.getEntries();
            if (!entryList.get(filterCol).equals(this.filterEntry)) {
              leftRows.add(new Row(row));
            }
          }
        }
      }

      while (it2.hasNext()) {
        rightRows.add(new Row(it2.next()));
      }  // 导入另一张表

      // 交换左右表
      if (filterTb == 1 ) {
        ArrayList<Row> tmp = leftRows;
        leftRows = rightRows;
        rightRows = tmp;
      }

      // JOIN连接
      if (joinOp.equals("<")) {
        for (int i = 0; i < leftRows.size(); i++) {
          ArrayList<Cell> leftEntries = leftRows.get(i).getEntries();
          for (int j = 0; j < rightRows.size(); j++) {
            ArrayList<Cell> rightEntries = rightRows.get(j).getEntries();
            Boolean ifJoin = true;
            for (int k = 0; k < this.joinLeftColumns.size(); ++k) {
              if (leftEntries.get(this.joinLeftColumns.get(k)).compareTo(rightEntries.get(this.joinRightColumns.get(k))) != -1) {
                ifJoin = false;
                break;
              }
            }
            if (ifJoin) {
              ArrayList<Cell> selectedEntries = new ArrayList<>();
              this.columnList.forEach(it-> {
                if (it.left == 0)
                  selectedEntries.add(new Cell(leftEntries.get(it.right)));
                if (it.left == 1)
                  selectedEntries.add(new Cell(rightEntries.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntries));
            }
          }
        }
      }
      else if (joinOp.equals(">")) {
        for (int i = 0; i < leftRows.size(); i++) {
          ArrayList<Cell> leftEntries = leftRows.get(i).getEntries();
          for (int j = 0; j < rightRows.size(); j++) {
            ArrayList<Cell> rightEntries = rightRows.get(j).getEntries();
            Boolean ifJoin = true;
            for (int k = 0; k < this.joinLeftColumns.size(); ++k) {
              if (leftEntries.get(this.joinLeftColumns.get(k)).compareTo(rightEntries.get(this.joinRightColumns.get(k))) != 1) {
                ifJoin = false;
                break;
              }
            }
            if (ifJoin) {
              ArrayList<Cell> selectedEntries = new ArrayList<>();
              this.columnList.forEach(it-> {
                if (it.left == 0)
                  selectedEntries.add(new Cell(leftEntries.get(it.right)));
                if (it.left == 1)
                  selectedEntries.add(new Cell(rightEntries.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntries));
            }
          }
        }
      }
      else if (joinOp.equals("<=")) {
        for (int i = 0; i < leftRows.size(); i++) {
          ArrayList<Cell> leftEntries = leftRows.get(i).getEntries();
          for (int j = 0; j < rightRows.size(); j++) {
            ArrayList<Cell> rightEntries = rightRows.get(j).getEntries();
            Boolean ifJoin = true;
            for (int k = 0; k < this.joinLeftColumns.size(); ++k) {
              if (leftEntries.get(this.joinLeftColumns.get(k)).compareTo(rightEntries.get(this.joinRightColumns.get(k))) == 1) {
                ifJoin = false;
                break;
              }
            }
            if (ifJoin) {
              ArrayList<Cell> selectedEntries = new ArrayList<>();
              this.columnList.forEach(it-> {
                if (it.left == 0)
                  selectedEntries.add(new Cell(leftEntries.get(it.right)));
                if (it.left == 1)
                  selectedEntries.add(new Cell(rightEntries.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntries));
            }
          }
        }
      }
      else if (joinOp.equals(">=")) {
        for (int i = 0; i < leftRows.size(); i++) {
          ArrayList<Cell> leftEntries = leftRows.get(i).getEntries();
          for (int j = 0; j < rightRows.size(); j++) {
            ArrayList<Cell> rightEntries = rightRows.get(j).getEntries();
            Boolean ifJoin = true;
            for (int k = 0; k < this.joinLeftColumns.size(); ++k) {
              if (leftEntries.get(this.joinLeftColumns.get(k)).compareTo(rightEntries.get(this.joinRightColumns.get(k))) == -1) {
                ifJoin = false;
                break;
              }
            }
            if (ifJoin) {
              ArrayList<Cell> selectedEntries = new ArrayList<>();
              this.columnList.forEach(it-> {
                if (it.left == 0)
                  selectedEntries.add(new Cell(leftEntries.get(it.right)));
                if (it.left == 1)
                  selectedEntries.add(new Cell(rightEntries.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntries));
            }
          }
        }
      }
      else if (joinOp.equals("=")) {
        for (int i = 0; i < leftRows.size(); i++) {
          ArrayList<Cell> leftEntries = leftRows.get(i).getEntries();
          for (int j = 0; j < rightRows.size(); j++) {
            ArrayList<Cell> rightEntries = rightRows.get(j).getEntries();
            Boolean ifJoin = true;
            for (int k = 0; k < this.joinLeftColumns.size(); ++k) {
              if (leftEntries.get(this.joinLeftColumns.get(k)).compareTo(rightEntries.get(this.joinRightColumns.get(k))) != 0) {
                ifJoin = false;
                break;
              }
            }
            if (ifJoin) {
              ArrayList<Cell> selectedEntries = new ArrayList<>();
              this.columnList.forEach(it-> {
                if (it.left == 0)
                  selectedEntries.add(new Cell(leftEntries.get(it.right)));
                if (it.left == 1)
                  selectedEntries.add(new Cell(rightEntries.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntries));
            }
          }
        }
      }
      else if (joinOp.equals("<>")) {
        for (int i = 0; i < leftRows.size(); i++) {
          ArrayList<Cell> leftEntries = leftRows.get(i).getEntries();
          for (int j = 0; j < rightRows.size(); j++) {
            ArrayList<Cell> rightEntries = rightRows.get(j).getEntries();
            Boolean ifJoin = true;
            for (int k = 0; k < this.joinLeftColumns.size(); ++k) {
              if (leftEntries.get(this.joinLeftColumns.get(k)).compareTo(rightEntries.get(this.joinRightColumns.get(k))) == 0) {
                ifJoin = false;
                break;
              }
            }
            if (ifJoin) {
              ArrayList<Cell> selectedEntries = new ArrayList<>();
              this.columnList.forEach(it-> {
                if (it.left == 0)
                  selectedEntries.add(new Cell(leftEntries.get(it.right)));
                if (it.left == 1)
                  selectedEntries.add(new Cell(rightEntries.get(it.right)));
              });  // 选取Row中被Select的属性
              results.add(new Row(selectedEntries));
            }
          }
        }
      }
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
   * 查找指定表
   * 查找失败则抛出异常
   * */
  private Integer findTableIdx(String tb) {
    for (int i = 0; i < queryTables.size(); ++i) {
      if (queryTables.get(i).getName().equals(tb))
        return i;
    }
    throw new TableNotExistException();
  }

  /**
   * 在指定表中查找列名
   * 查找失败则抛出异常
   * */
  private Integer findColIdx(Integer tb_idx, String col) {
    int res = queryTables.get(tb_idx).getMetaInfo().columnFind(col);
    if (res == -1)
      throw new KeyNotExistException();
    return res;
  }
}
