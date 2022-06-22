package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ConstraintType;
import cn.edu.thssdb.type.ExpressionType;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static cn.edu.thssdb.schema.Column.parseEntry;

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

    /**
     * TODO
     * 创建表格
     */
    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String name = ctx.table_name().getText();
        int n = ctx.column_def().size();
        Column[] columns = new Column[n];
        int i = 0;

        // 读取各个列的定义
        for (SQLParser.Column_defContext subCtx : ctx.column_def()) {
            columns[i++] = visitColumn_def(subCtx);
        }

        // 读取表定义末端的信息--primary key
        if (ctx.table_constraint() != null) {
            String[] compositeNames = visitTable_constraint(ctx.table_constraint());

            for (String compositeName : compositeNames) {
                boolean found = false;
                for (Column c : columns) {
                    if (c.getColumnName().toLowerCase().equals(compositeName.toLowerCase())) {
                        c.setPrimary(1);
                        c.setNotNull(true);
                        found = true;
                    }
                }
                if (!found) {
                    throw new KeyNotExistException();
                }
            }
        }

        // 在当前数据库建表
        try {
            manager.getCurrentDatabase().create(name.toLowerCase(), columns);
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Created table " + name + ".";
        // return null;
    }

    /**
     * TODO
     * 表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        // table name
        String table_name = ctx.table_name().getText().toLowerCase();
        Database the_database = GetCurrentDB();
        Table the_table = the_database.get(table_name);
        if (the_table == null) {
            throw new TableNotExistException();
        }

        // column name处理
        String[] column_names = null;
        if (ctx.column_name() != null && ctx.column_name().size() != 0) {
            column_names = new String[ctx.column_name().size()];
            for (int i = 0; i < ctx.column_name().size(); i++)
                column_names[i] = ctx.column_name(i).getText().toLowerCase();
        }

        // value处理
        for (SQLParser.Value_entryContext subCtx : ctx.value_entry()) {
            String[] values = visitValue_entry(subCtx);
            System.out.println(values);
            System.out.println("\n");
            try {
                if (column_names == null) {
                    if (values == null)
                        throw new SchemaLengthMismatchException(the_table.columns.size(), 0, "");
                    int schemaLen = the_table.columns.size();
                    if (values.length > schemaLen) {
                        throw new SchemaLengthMismatchException(schemaLen, values.length, "");
                    }
                    String values_array = null;
                    ArrayList<Column> columnList = new ArrayList<>();
                    for (int i = 0; i < the_table.columns.size(); i++) {
                        values_array = values_array + "," + values[i];
                        Column column = the_table.columns.get(i);
                        if (i >= 0 && i < values.length) {
                            columnList.add(column);
                        }
                    }
                    values_array = values_array.substring(5, values_array.length());
                    Row row = Row.parseRow(values_array, columnList);
                    the_table.insert(row);
                } else {
                    if (column_names == null || values == null)
                        throw new SchemaLengthMismatchException(the_table.columns.size(), 0, "");

                    // match columns and reorder entries
                    int schemaLen = the_table.columns.size();
                    if (column_names.length > schemaLen) {
                        throw new SchemaLengthMismatchException(schemaLen, column_names.length, "");
                    } else if (values.length > schemaLen) {
                        throw new SchemaLengthMismatchException(schemaLen, values.length, "");
                    } else if (column_names.length != values.length) {
                        throw new SchemaLengthMismatchException(column_names.length, values.length, "");
                    }
                    String values_array = null;
                    ArrayList<Column> columnList = new ArrayList<>();
                    for (Column column : the_table.columns) {
                        int equal_num = 0;
                        int place = -1;
                        int end = -1;
                        for (int i = 0; i < values.length; i++) {
                            if (column_names[i].equals(column.getColumnName().toLowerCase())) {
                                place = i;
                                equal_num++;
                            }
                        }
                        if (equal_num > 1) {
                            throw new DuplicateKeyException();
                        }
                        Comparable the_cell_value = null;
                        if (equal_num == 0 || place < 0 || place >= column_names.length) {
                            columnList.add(column);
                            values_array = values_array + "," + null;
                        } else {
                            values_array = values_array + "," + values[place];
                            columnList.add(column);
                        }
                    }
                    values_array = values_array.substring(5, values_array.length());
                    Row row = Row.parseRow(values_array, columnList);
                    the_table.insert(row);
                }
            } catch (Exception e) {
                return e.toString();
            }
        }
        return "Inserted " + ctx.value_entry().size() + " rows.";
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

            int col_idx = table.findColumnIdx(filterCol);
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
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        Database the_database = GetCurrentDB();
        String table_name = ctx.table_name().getText().toLowerCase();
        String column_name = ctx.column_name().getText().toLowerCase();
        Table the_table = the_database.get(table_name);


        if (ctx.K_WHERE() == null) {
            try {
                String new_value = ctx.expression().comparer().literal_value().getText();
                int primaryIndex = the_table.getPrimaryIndex();
                int new_index = 0;
                for (int i = 0; i < the_table.columns.size(); i++) {
                    Column the_column = the_table.columns.get(i);
                    if(the_column.getColumnName().equals(column_name)){
                        new_index = i;
                    }
                }
                if (new_index < 0) {
                    throw new KeyNotExistException();
                }
                Cell cell_value = parseEntry(new_value, the_table.columns.get(new_index));
                for (Row row : the_table) {
                    ArrayList<Cell> newArray=row.getEntries();
                    newArray.set(new_index, cell_value);
                    Row new_row = new Row(newArray);
                    the_table.update(row.getEntries().get(primaryIndex), new_row);
                }
                return "The table "+table_name+" has been update";
            } catch (Exception e) {
                return e.toString();
            }
        }
        else {
            try {
                String new_value = ctx.expression().comparer().literal_value().getText();
                String w_column_name = ctx.multiple_condition().condition().expression(0).comparer().column_full_name().column_name().getText().toLowerCase();
                String w_new_value = ctx.multiple_condition().condition().expression(1).comparer().literal_value().getText();
                ArrayList<Row> be_update = new ArrayList<>();
                Iterator<Row> iterator = the_table.iterator();
                int primaryIndex = the_table.getPrimaryIndex();
                int new_index = 0;
                for (int i = 0; i < the_table.columns.size(); i++) {
                    Column the_column = the_table.columns.get(i);
                    if(the_column.getColumnName().equals(column_name)){
                        new_index = i;
                    }
                }

                int new_w_index = 0;
                for (int i = 0; i < the_table.columns.size(); i++) {
                    Column the_column = the_table.columns.get(i);
                    if(the_column.getColumnName().equals(w_column_name)){
                        new_w_index = i;
                    }
                }
                if (new_index < 0||new_w_index <0) {
                    throw new KeyNotExistException();
                }
                Cell cell_value = parseEntry(new_value, the_table.columns.get(new_index));
                Cell cell_w_alue = parseEntry(w_new_value, the_table.columns.get(new_w_index));
                if (ctx.multiple_condition().condition().comparator().EQ() != null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(new_w_index).compareTo(cell_w_alue) == 0)
                            be_update.add(row);
                    }
                } else if (ctx.multiple_condition().condition().comparator().NE() != null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(new_w_index).compareTo(cell_w_alue) != 0)
                            be_update.add(row);
                    }
                } else if (ctx.multiple_condition().condition().comparator().LE() != null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(new_w_index).compareTo(cell_w_alue) <= 0)
                            be_update.add(row);
                    }
                } else if (ctx.multiple_condition().condition().comparator().GE() != null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(new_w_index).compareTo(cell_w_alue) >= 0)
                            be_update.add(row);
                    }
                } else if (ctx.multiple_condition().condition().comparator().LT() != null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(new_w_index).compareTo(cell_w_alue) < 0)
                            be_update.add(row);
                    }
                } else if (ctx.multiple_condition().condition().comparator().GT() != null) {
                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        if (row.getEntries().get(new_w_index).compareTo(cell_w_alue) > 0)
                            be_update.add(row);
                    }
                } else {
                    return "operator doesn't exist";
                }
                for (Row row : be_update) {
                    ArrayList<Cell> newArray=row.getEntries();
                    newArray.set(new_index, cell_value);
                    Row new_row = new Row(newArray);
                    the_table.update(row.getEntries().get(primaryIndex), new_row);
                }
                return "The table "+table_name+" has been update";
            } catch (Exception e) {
                return e.toString();
            }
        }
    }

    public ExpressionType visitLiteral_value(SQLParser.Literal_valueContext ctx) {
        if (ctx.NUMERIC_LITERAL() != null) {
            return ExpressionType.NUMBER;
        }
        if (ctx.STRING_LITERAL() != null) {
            return ExpressionType.STRING;
        }
        if (ctx.K_NULL() != null) {
            return ExpressionType.NULL;
        }
        return null;
    }


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

        try {
            // 选择表格
            if (ctx.table_query(0).K_JOIN().size() == 0) {  // 单表
                Table table = GetCurrentDB().get(ctx.table_query(0).table_name(0).getText().toLowerCase());
                queryTableList.add(new QueryTable(table));
            }
            else { // 双表
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
                    if (it.column_full_name().table_name() == null)
                        tableNameList.add(ctx.table_query(0).table_name(0).getText().toLowerCase());
                    else
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
                        if (op.equals(">")) op = "<";
                        else if (op.equals("<")) op = ">";
                        else if (op.equals(">=")) op = "<=";
                        else if (op.equals("<=")) op = ">=";
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
        }
        catch (Exception e) {
            return new QueryResult(e.getMessage());
        }
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

    /**
     展示database里的表格
     */
    public String visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
        Database current_db = GetCurrentDB();
        try {
            return current_db.toString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     展示表格
     */
    public String visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        Database current_db = GetCurrentDB();
        try {
            Table table = current_db.get(tableName);
            return table.toString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }

    public Column visitColumn_def(SQLParser.Column_defContext ctx) {
        //读取当前列是否primary， not null
        boolean not_null = false;
        int primary = 0;
        for (SQLParser.Column_constraintContext subCtx : ctx.column_constraint()) {
            ConstraintType the_constraint_type = visitColumn_constraint(subCtx);
            if (the_constraint_type.equals(ConstraintType.PRIMARY)) {
                primary = 1;
                not_null = true;
            }
            else if (the_constraint_type.equals(ConstraintType.NOTNULL)) {
                not_null = true;
            }
        }

        //列名称
        String name = ctx.column_name().getText().toLowerCase();

        //列类型
        Pair<ColumnType, Integer> type = visitType_name(ctx.type_name());
        ColumnType columnType = type.getKey();

        //最大长度（仅限string）
        int maxLength = type.getValue();
        if(primary == 1){
            return new Column(name, columnType, primary, true, maxLength);
        }
        return new Column(name, columnType, primary, not_null, maxLength);
    }


    /**
     * 描述：返回列定义的限制--primary，notnull还是没有限制
     */
    public ConstraintType visitColumn_constraint(SQLParser.Column_constraintContext ctx) {
        if (ctx.K_PRIMARY() != null) {
            return ConstraintType.PRIMARY;
        }
        else if (ctx.K_NULL() != null) {
            return ConstraintType.NOTNULL;
        }
        return null;
    }


    /**
     *处理表定义的限制
     */
    public String[] visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        int n = ctx.column_name().size();
        String[] composite_names = new String[n];
        for (int i = 0; i < n; i++) {
            composite_names[i] = ctx.column_name(i).getText().toLowerCase();
        }
        return composite_names;
    }


    /**
     * 描述：处理创建列时的type，max length
     */
    public Pair<ColumnType, Integer> visitType_name(SQLParser.Type_nameContext ctx) {
        if (ctx.T_INT() != null) {
            return new Pair<>(ColumnType.INT, -1);
        }
        if (ctx.T_LONG() != null) {
            return new Pair<>(ColumnType.LONG, -1);
        }
        if (ctx.T_FLOAT() != null) {
            return new Pair<>(ColumnType.FLOAT, -1);
        }
        if (ctx.T_DOUBLE() != null) {
            return new Pair<>(ColumnType.DOUBLE, -1);
        }
        if (ctx.T_STRING() != null) {
            try {
                //仅string返回值和最大长度
                return new Pair<>(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
            } catch (Exception e) {
                throw new ValueFormatInvalidException("");
            }
        }
        return null;
    }

    /**
     * 描述：读取输入的各种值
     */
    public String[] visitValue_entry(SQLParser.Value_entryContext ctx) {
        String[] values = new String[ctx.literal_value().size()];
        for (int i = 0; i < ctx.literal_value().size(); i++) {
            values[i] = ctx.literal_value(i).getText();
        }
        return values;
    }


    /**
     * 描述：将string类型的value转换成column的类型
     * 参数：column，value
     * 返回：新的值--comparable，如果不匹配会抛出异常
     */
    private Comparable ParseValue(Column the_column, String value) {
        if (value.equals("null")) {
            if (the_column.cantBeNull()) {
                throw new NullValueException(the_column.getColumnName());
            }
            else {
                return null;
            }
        }
        switch (the_column.getColumnType()) {
            case DOUBLE:
                return Double.parseDouble(value);
            case INT:
                return Integer.parseInt(value);
            case FLOAT:
                return Float.parseFloat(value);
            case LONG:
                return Long.parseLong(value);
            case STRING:
                return value.substring(1, value.length() - 1);
        }
        return null;
    }

    /**
     * 描述：判断value是否合法，符合column规则，这里只判断null和max length
     * 参数：column，value
     * 返回：无，如果不合法会抛出异常
     */
    public void JudgeValid(Column the_column, Comparable new_value) {
        boolean not_null = the_column.cantBeNull();
        ColumnType the_type = the_column.getColumnType();
        int max_length = the_column.getMaxLength();
        if(not_null == true && new_value == null) {
            throw new NullValueException(the_column.getColumnName());
        }
        if(the_type == ColumnType.STRING && new_value != null) {
            if(max_length >= 0 && (new_value + "").length() > max_length) {
                throw new ValueExceedException(the_column.getColumnName(),(new_value + "").length(),max_length,"");
            }
        }
    }
}


