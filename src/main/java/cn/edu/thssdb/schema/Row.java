package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.SchemaLengthMismatchException;
import cn.edu.thssdb.exception.ValueFormatInvalidException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Cell> entries;

  public Row() {
    this.entries = new ArrayList<>();
  }

  public Row(Cell[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
  }
  public Row(ArrayList<Cell> entries) {
    this.entries = new ArrayList<>(entries);
  }
  public Row(Row row){
    this.entries = new ArrayList<>();
    for(Cell cell : row.entries)
      this.entries.add(new Cell(cell));
  }

  public ArrayList<Cell> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Cell> entries) {
    this.entries.addAll(entries);
  }

  public String toString() {
    if (entries == null)
      return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Cell e : entries)
      sj.add(e.toString());
    return sj.toString();
  }

  public ArrayList<String> toStringList() {
    ArrayList<String> valueList = new ArrayList<>();
    if (entries == null) return valueList;
    for (Cell cell : this.entries)
      valueList.add(cell.toString());
    return valueList;
  }

  public static Row parseRow(String s, ArrayList<Column> columnList){
    String[] sArray = s.split(",");
    if(sArray.length != columnList.size())
      throw new SchemaLengthMismatchException(columnList.size(), sArray.length, "when parse Row");
    ArrayList<Cell> cellList = new ArrayList<>();
    for(int i=0;i<sArray.length;i++){
      String sItem = sArray[i];
      Column column = columnList.get(i);
      try {
        cellList.add(Column.parseEntry(sItem, column));
//                System.out.println(Column.parseEntry(sItem, column).toString());
      }catch(NumberFormatException e){
//                System.out.println("        " + s);
//                System.out.println("        " + columnList.toString());
        throw new ValueFormatInvalidException("(when parse row from a String&ColumnList)");           // XXX.valueOf类型转换出错。
      }
    }
//        System.out.println("            ? "+ entryList.toString());
    return new Row(cellList);
  }

}
