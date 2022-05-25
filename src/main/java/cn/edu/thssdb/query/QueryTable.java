package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterator<Row> {

  QueryTable() {
    // TODO
  }

  @Override
  public boolean hasNext() {
    // TODO
    return true;
  }

  @Override
  public Row next() {
    // TODO
    return null;
  }
}