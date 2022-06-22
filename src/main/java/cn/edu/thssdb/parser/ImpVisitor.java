package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.SchemaLengthMismatchException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ConstraintType;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static cn.edu.thssdb.schema.Column.parseEntry;
import static cn.edu.thssdb.type.ColumnType.*;

/**
 * When use SQL sentence, e.g., "SELECT avg(A) FROM TableX;"
 * the parser will generate a grammar tree according to the rules defined in SQL.g4.
 * The corresponding terms, e.g., "select_stmt" is a root of the parser tree, given the rules
 * "select_stmt :
 *     K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
 *         K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;"
 *
 * This class "ImpVisit" is used to convert a tree rooted at e.g. "select_stmt"
 * into the collection of tuples inside the database.
 *
 * We give you a few examples to convert the tree, including create/drop/quit.
 * You need to finish the codes for parsing the other rooted trees marked TODO.
 */

public class ImpVisitor extends SQLBaseVisitor<Object> {
    private Manager manager;
    private long session;

    public ImpVisitor(Manager manager, long session) {
        super();
        this.manager = manager;
        this.session = session;
    }

    private Database GetCurrentDB() {
        Database currentDB = manager.getCurrentDatabase();
        if(currentDB == null) {
            throw new DatabaseNotExistException();
        }
        return currentDB;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) return new QueryResult(visitCreate_db_stmt(ctx.create_db_stmt()));
        if (ctx.drop_db_stmt() != null) return new QueryResult(visitDrop_db_stmt(ctx.drop_db_stmt()));
        if (ctx.use_db_stmt() != null)  return new QueryResult(visitUse_db_stmt(ctx.use_db_stmt()));
        if (ctx.create_table_stmt() != null) return new QueryResult(visitCreate_table_stmt(ctx.create_table_stmt()));
        if (ctx.drop_table_stmt() != null) return new QueryResult(visitDrop_table_stmt(ctx.drop_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        if (ctx.update_stmt() != null) return new QueryResult(visitUpdate_stmt(ctx.update_stmt()));
        if (ctx.select_stmt() != null) return visitSelect_stmt(ctx.select_stmt());
        if (ctx.quit_stmt() != null) return new QueryResult(visitQuit_stmt(ctx.quit_stmt()));
        return null;
    }

    /**
     创建数据库
     */
    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        try {
            manager.createDatabaseIfNotExists(ctx.database_name().getText().toLowerCase());
            manager.persist();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Create database " + ctx.database_name().getText() + ".";
    }

    /**
     删除数据库
     */
    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        try {
            manager.deleteDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop database " + ctx.database_name().getText() + ".";
    }

    /**
     切换数据库
     */
    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        try {
            manager.switchDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Switch to database " + ctx.database_name().getText() + ".";
    }

    /**
     删除表格
     */
    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        try {
            GetCurrentDB().drop(ctx.table_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop table " + ctx.table_name().getText() + ".";
    }

    // Below By Others; For testing

    /**
     * TODO
     创建表格
     */
    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText().toLowerCase();
            List<SQLParser.Column_defContext> columnDefItems = ctx.column_def();
            Column[] columns = new Column[columnDefItems.size()];

            for (int i = 0; i < columnDefItems.size(); ++i){
                SQLParser.Column_defContext columnDefItem =  ctx.column_def(i);
                // 获取属性名
                String columnName = columnDefItem.column_name().getText().toLowerCase();

                // 获取属性类别与最大长度
                ColumnType columnType = null;
                int columnMaxLength = 0;
                String columnTypeName = columnDefItem.type_name().getText().toLowerCase();
                switch (columnTypeName){
                    case "int":
                        columnType = INT;
                        break;
                    case "long":
                        columnType = LONG;
                        break;
                    case "float":
                        columnType = FLOAT;
                        break;
                    case "double":
                        columnType = DOUBLE;
                        break;
                    default:
                        if (columnTypeName.substring(0,6).equals("string")){
                            columnType = STRING;
                            columnMaxLength = Integer.parseInt(columnTypeName.substring(7, columnTypeName.length() - 1));
                        }
                        break;
                }

                // 获取属性约束，这里是每个属性自身的约束
                int columnPrimaryKey = 0;
                Boolean columnNotNull = false;
                for (int j = 0; j < columnDefItem.column_constraint().size(); ++j){
                    String columnConstraint = columnDefItem.column_constraint(j).getText().toLowerCase();
                    switch (columnConstraint){
                        case "notnull":
                            columnNotNull = true;
                            break;
                        case "primarykey":
                            columnPrimaryKey = 1;
                            columnNotNull = true; // 主码必须非空
                            break;
                    }
                }
                Column column = new Column( columnName, columnType, columnPrimaryKey, columnNotNull, columnMaxLength);
                columns[i] = column;
            }

            // 获取属性约束，这里是整个表的约束
            System.out.println();
            if(ctx.table_constraint() != null){
                // 对于每条约束语句
                for (int j = 0; j < ctx.table_constraint().column_name().size(); ++j){
                    String columnPrimaryName = ctx.table_constraint().column_name(j).getText().toLowerCase();
                    System.out.println(columnPrimaryName);

                    // 对于每条约束语句的主键进行修改
                    for (int i = 0; i < columnDefItems.size(); ++i){
                        if (columns[i].getColumnName().equals(columnPrimaryName)){
                            columns[i].setPrimary(1);
                            columns[i].setNotNull(true);
                        }
                    }
                }
            }
            GetCurrentDB().create(tableName, columns);
            return "Create table " + tableName + ".";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    // Above By Lingfeng

    /**
     * TODO
     表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        Table table = GetCurrentDB().get(tableName);
        if (table == null){
            throw new TableNotExistException();
        }
        while(!table.testXLock(session)){
            try {
                Thread.sleep(500);
            }catch (Exception ex){
            }
        };
        table.takeXLock(session);
        ArrayList<String> table_list = manager.x_lockDict.get(session);
        table_list.add(tableName);
        manager.x_lockDict.put(session,table_list);
        try {
            List<SQLParser.Column_nameContext> columnName = ctx.column_name();
            List<SQLParser.Value_entryContext> valueEntries = ctx.value_entry();
            ArrayList<Integer> columnIndex = new ArrayList<>();
            if (columnName.size() == 0){
                for (int i = 0; i < table.columns.size(); ++i){
                    columnIndex.add(i);
                }
            }
            else{
                for (int i = 0; i < columnName.size(); ++i){
                    int index = table.searchColumn(ctx.column_name(i).getText().toLowerCase());
                    if (index < 0){
                        throw new KeyNotExistException();
                    }
                    columnIndex.add(index);
                }
            }
            for (int j = 0; j < valueEntries.size(); ++j){
                SQLParser.Value_entryContext valueEntry = ctx.value_entry(j);
                if (valueEntry.literal_value().size() != columnIndex.size()){
                    throw new SchemaLengthMismatchException(columnIndex.size(), valueEntry.literal_value().size(), " during insert");
                }
                Cell[] cells = new Cell[table.columns.size()];
                for (int k = 0; k < columnIndex.size(); ++k){
                    cells[columnIndex.get(k)] = parseEntry(valueEntry.literal_value(k).getText(), table.columns.get(columnIndex.get(k)));
                }
                table.insert(new Row(cells));
            }

            return "Insert into table " + tableName + ".";
        } catch (Exception e) {
            table_list.remove(tableName);
            manager.x_lockDict.put(session, table_list);
            table.releaseXLock(session);
            return e.getMessage();
        }
    }

    /**
     * TODO
     表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        try {
            Table table = GetCurrentDB().get(ctx.table_name().getText().toLowerCase());

            // WHERE过滤
            // [ WHERE attrName op value ]
            // 仅包含一个比较运算op，具体为‘<’、‘>’、‘<=’、‘>=’、‘=’、‘<>’
            String filterCol = ctx.multiple_condition().condition().expression(0)
                    .comparer().column_full_name().column_name().getText().toLowerCase();
            String filterVal = ctx.multiple_condition().condition().expression(1)
                    .comparer().literal_value().getText();
            String op = ctx.multiple_condition().condition().getChild(1).getText();
            //SQLParser.ComparatorContext comparator = ctx.multiple_condition().condition().comparator();

            int col_idx = table.searchColumn(filterCol);
            if (col_idx < 0) {
                throw new KeyNotExistException();
            }
            Cell filterEntry = parseEntry(filterVal, table.columns.get(col_idx));

            ArrayList<Row> selectedRows = new ArrayList<>();
            Iterator<Row> it = table.iterator();
            if (op.equals("<")) {
                while (it.hasNext()) {
                    Row row = it.next();
                    if (row.getEntries().get(col_idx).compareTo(filterEntry) == -1)
                        selectedRows.add(row);
                }
            }
            else if (op.equals(">")) {
                while (it.hasNext()) {
                    Row row = it.next();
                    if (row.getEntries().get(col_idx).compareTo(filterEntry) == 1)
                        selectedRows.add(row);
                }
            }
            else if (op.equals("<=")) {
                while (it.hasNext()) {
                    Row row = it.next();
                    if (row.getEntries().get(col_idx).compareTo(filterEntry) != 1)
                        selectedRows.add(row);
                }
            }
            else if (op.equals(">=")) {
                while (it.hasNext()) {
                    Row row = it.next();
                    if (row.getEntries().get(col_idx).compareTo(filterEntry) != -1)
                        selectedRows.add(row);
                }
            }
            else if (op.equals("=")) {
                while (it.hasNext()) {
                    Row row = it.next();
                    if (row.getEntries().get(col_idx).compareTo(filterEntry) == 0)
                        selectedRows.add(row);
                }
            }
            else if (op.equals("<>")) {
                while (it.hasNext()) {
                    Row row = it.next();
                    if (row.getEntries().get(col_idx).compareTo(filterEntry) != 0)
                        selectedRows.add(row);
                }
            }
            selectedRows.forEach(row-> {
                table.delete(row);
            });

            return "Delete From Table " + ctx.table_name().getText() + ".";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * TODO
     表格项更新
     */
    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项查询
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        String tableName1 = "", tableName2 = "";

        QueryResult result;
        List<String> tableNameList = new ArrayList<>();
        List<String> columnNameList = new ArrayList<>();
        List<QueryTable> queryTableList = new ArrayList<>();

        // 选择表格
        if (ctx.table_query(0).K_JOIN().size() == 0) {  // 单表查询
            Table table = GetCurrentDB().get(ctx.table_query(0).table_name(0).getText().toLowerCase());
            queryTableList.add(new QueryTable(table));
        }
        else { // 双表连接
            tableName1 = ctx.table_query(0).table_name(0).getText().toLowerCase();
            tableName2 = ctx.table_query(0).table_name(1).getText().toLowerCase();
            Table table1 = GetCurrentDB().get(tableName1);
            Table table2 = GetCurrentDB().get(tableName2);
            queryTableList.add(new QueryTable(table1));
            queryTableList.add(new QueryTable(table2));
        }

        // 选择属性
        if (ctx.result_column().get(0).getText().compareTo("*") != 0) {
            ctx.result_column().forEach(it -> {
                tableNameList.add(it.column_full_name().table_name().getText().toLowerCase());
                columnNameList.add(it.column_full_name().column_name().getText().toLowerCase());
            });
        }
        else {
            // TODO: SELECT *
        }
        result = new QueryResult(queryTableList, tableNameList, columnNameList);

        // WHERE过滤
        // [ WHERE attrName op value ]
        // 仅包含一个比较运算op，具体为‘<’、‘>’、‘<=’、‘>=’、‘=’、‘<>’
        if (ctx.K_WHERE() != null) {
            String filterTable = ctx.multiple_condition().condition().expression(0)
                    .comparer().column_full_name().table_name().getText().toLowerCase();
            String filterCol = ctx.multiple_condition().condition().expression(0)
                    .comparer().column_full_name().column_name().getText().toLowerCase();
            String filterVal = ctx.multiple_condition().condition().expression(1)
                    .comparer().literal_value().getText();
            String op = ctx.multiple_condition().condition().getChild(1).getText();
            //SQLParser.ComparatorContext comparator = ctx.multiple_condition().condition().comparator();
            result.setFilter(filterTable, filterCol, filterVal, op);
        }

        // JOIN连接
        if (ctx.table_query(0).K_JOIN().size() != 0) {  // 双表
            // 判断连接条件
            if (ctx.table_query(0).K_ON() != null) {  // 条件连接
                String joinTable1 = ctx.table_query(0).multiple_condition().condition().expression(0)
                        .comparer().column_full_name().table_name().getText();
                String joinTable2 = ctx.table_query(0).multiple_condition().condition().expression(1)
                        .comparer().column_full_name().table_name().getText();
                String joinCol1 = ctx.table_query(0).multiple_condition().condition().expression(0)
                        .comparer().column_full_name().column_name().getText();
                String joinCol2 = ctx.table_query(0).multiple_condition().condition().expression(1)
                        .comparer().column_full_name().column_name().getText();
                String op = ctx.table_query(0).multiple_condition().condition().getChild(1).getText();
                //SQLParser.ComparatorContext tableComparator = ctx.table_query(0).multiple_condition().condition().comparator();

                // 判断ON子句的左右侧表顺序与JOIN子句的左右侧表顺序是相同还是相反
                if (joinTable1.equals(tableName1) && joinTable2.equals(tableName2)) {
                    result.setJoin(joinCol1, joinCol2, op);
                }
                else if (joinTable1.equals(tableName2) && joinTable2.equals(tableName1)) {
                    result.setJoin(joinCol2, joinCol1, op);
                }
                else {
                    // ON子句指定的表格不同于JOIN子句指定的表格，抛出连接条件异常
                    throw new TableNotExistException();
                }

            }
            else {  // 自然连接
                result.setJoin();
            }
        }

        result.query();

        return result;
    }

    /**
     退出
     */
    @Override
    public String visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        try {
            manager.quit();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Quit.";
    }

    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }
}
