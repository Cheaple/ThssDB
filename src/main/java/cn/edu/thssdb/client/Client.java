package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.common.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  private static String inTransaction = "";

  private static long session = -1;

  public static void main(String[] args) {
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX + inTransaction + ">");
        String msg = SCANNER.nextLine();
        long startTime = System.currentTimeMillis();

        transport = new TSocket(host, port);
        transport.open();
        protocol = new TBinaryProtocol(transport);
        client = new IService.Client(protocol);

        switch (msg.trim()) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.QUIT:
            open = false;
            break;
          case Global.CONNECT:
            connect();
            break;
          case Global.DISCONNECT:
            disconnect();
            break;
          default:
            execute(msg.trim());
            break;
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
        transport.close();
      }
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void connect() {
    if (session >= 0) {
      println("You're already connected.");
      return;
    }
//    print("Username: ");
//    String username = SCANNER.nextLine();
//    print("Password: ");
//    String password = SCANNER.nextLine();
    ConnectReq req = new ConnectReq("1", "1");
    try {
      ConnectResp resp = client.connect(req);
      println(resp.toString());
      if (resp.getStatus().code == Global.SUCCESS_CODE)
        session = resp.getSessionId();
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void disconnect() {
    if (session < 0) {
      println("you're not connected. plz connect first.");
      return;
    }
    DisconnetReq req = new DisconnetReq(session);
    try {
      DisconnetResp resp = client.disconnect(req);
      println(resp.toString());
      if (resp.getStatus().code == Global.SUCCESS_CODE)
        session = -1;
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void execute(String msg) {
    if (session < 0) {
      println("you're not connected. plz connect first.");
      return;
    }
    ExecuteStatementReq req = new ExecuteStatementReq();
    req.setSessionId(session);
    req.setStatement(msg);
    try {
      ExecuteStatementResp resp = client.executeStatement(req);
      if (resp.getStatus().code == Global.FAILURE_CODE) {
        println("Connection Failure!");
        println(resp.getStatus().msg);
      } else {
        if (resp.isAbort) {
          println("illegal SQL statement!");
        }
        else if (resp.isSetRowList()) {
          StringBuilder column_str = new StringBuilder();
          int column_size = resp.columnsList.size();
          for (int i = 0; i < column_size; ++i) {
            column_str.append(resp.columnsList.get(i));
            if (i != column_size - 1) column_str.append(", ");
          }
          println(column_str.toString());
          println("----------------------------------------------------------------");

          for (List<String> row : resp.rowList) {
            StringBuilder row_str = new StringBuilder();
            for (int i = 0; i < column_size; ++i) {
              row_str.append(row.get(i));
              if (i != column_size - 1) row_str.append(", ");
            }
            println(row_str.toString());
          }
        } else {
          for (String column : resp.columnsList) {
            column = column.trim();
            if (column.equals("start transaction")) inTransaction = "(T)";
            else if (column.equals("commit transaction")) inTransaction = "";
            println(column);
          }
        }
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
            .argName(HELP_NAME)
            .desc("Display help information(optional)")
            .hasArg(false)
            .required(false)
            .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
            .argName(HOST_NAME)
            .desc("Host (optional, default 127.0.0.1)")
            .hasArg(false)
            .required(false)
            .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
            .argName(PORT_NAME)
            .desc("Port (optional, default 6667)")
            .hasArg(false)
            .required(false)
            .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showHelp() {
    // TODO
    println("DO IT YOURSELF");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
