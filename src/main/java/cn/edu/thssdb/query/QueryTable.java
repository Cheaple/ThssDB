package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.MetaInfo;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterator<Row> {
  private Iterator<Row> iterator;

  private String tableName;

  private MetaInfo tableMeta;

  private Table table;

  public String getName() {
    return tableName;
  }

  public MetaInfo getMetaInfo() {
    return tableMeta;
  }

  public QueryTable(Table table) {
    // TODO
    this.tableName = table.tableName;
    this.tableMeta = new MetaInfo(table.tableName, table.columns);

    this.iterator = table.iterator();
  }

  @Override
  public boolean hasNext() {
    // TODO
    return this.iterator.hasNext();
  }

  @Override
  public Row next() {
    // TODO
    return this.iterator.next();
  }
}