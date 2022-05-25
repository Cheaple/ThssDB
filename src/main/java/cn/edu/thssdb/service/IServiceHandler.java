package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLHandler;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetReq;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.QueryResultType;
import cn.edu.thssdb.common.Global;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;


public class IServiceHandler implements IService.Iface {
  public static Manager manager;
  public long sessionCount = 0;
  private final static String INSERT = "insert";
  private final static String UPDATE = "update";
  private final static String DELETE = "delete";
  private final static String SELECT = "select";
  private final static String[] CMD_HEADS = {INSERT, UPDATE, DELETE, SELECT};
  public static SQLHandler sqlHandler;

  public IServiceHandler() {
    super();
    manager = Manager.getInstance();
    sqlHandler = new SQLHandler(manager);
  }


  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    long session = sessionCount++;
    ConnectResp resp = new ConnectResp();
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    resp.setSessionId(session);
    return resp;
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
    // TODO
    DisconnetResp resp = new DisconnetResp();
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    ExecuteStatementResp resp = new ExecuteStatementResp();
    long session = req.getSessionId();
    if (session < 0 || session >= sessionCount) {
      Status status = new Status(Global.FAILURE_CODE);
      status.setMsg("please connect first.");
      resp.setStatus(status);
      return resp;
    }

    String command = req.statement;
    String[] statements = command.split(";");
    ArrayList<QueryResult> results = new ArrayList<>();

    for (String statement : statements) {
      statement = statement.trim();
      if (statement.length() == 0) continue;
      String cmd_head = command.split("\\s+")[0];
      ArrayList<QueryResult> queryResults;
      if ((Arrays.asList(CMD_HEADS).contains(cmd_head.toLowerCase())) && !manager.currentSessions.contains(session)) {
        sqlHandler.evaluate("begin transaction", session);
        queryResults = sqlHandler.evaluate(statement, session);
        sqlHandler.evaluate("commit", session);
      } else queryResults = sqlHandler.evaluate(statement, session);
      if (queryResults == null || queryResults.size() == 0) {
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        resp.setIsAbort(true);
        return resp;
      }
      results.addAll(queryResults);
    }
    resp.setStatus(new Status(Global.SUCCESS_CODE));

    if (results.size() == 1 && results.get(0) != null && results.get(0).resultType == QueryResultType.SELECT) {
      for (Row row : results.get(0).results) {
        ArrayList<String> the_result = row.toStringList();
        resp.addToRowList(the_result);
      }
      if (!resp.isSetRowList()) {
        resp.rowList = new ArrayList<>();
      }
      for (String column_name: results.get(0).getColumnNames()) {
        resp.addToColumnsList(column_name);
      }
    } else {
      for (QueryResult queryResult : results) {
        if (queryResult == null)
          resp.addToColumnsList("null");
        else resp.addToColumnsList(queryResult.errorMessage);
      }
    }

    return resp;
  }
}
