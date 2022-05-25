package cn.edu.thssdb.schema;

import cn.edu.thssdb.common.Global;

import java.io.Serializable;

public class Cell implements Comparable<Cell>, Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  public Comparable value;

  public Cell(Comparable value) {
    this.value = value;
  }

  @Override
  public int compareTo(Cell e) {
    return value.compareTo(e.value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    Cell e = (Cell) obj;
    return value.equals(e.value);
  }

  public String toString() {
    return value.toString();
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  public String getValueType(){
    if(this.value == null) return Global.ENTRY_NULL;
    String valueClassString = this.value.getClass().toString();
    if(valueClassString.contains("Integer")) return "INT";
    if(valueClassString.contains("Long")) return "LONG";
    if(valueClassString.contains("Float")) return "FLOAT";
    if(valueClassString.contains("Double")) return "DOUBLE";
    if(valueClassString.contains("String")) return "STRING";
    return "UNKNOWN";
  }
}
