package cn.edu.thssdb.common;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;

  public static String CLI_PREFIX = "ThssDB";
  public static final String SHOW_TIME = "show time;";
  public static final String QUIT = "quit;";
  public static final String CONNECT = "connect;";
  public static final String DISCONNECT = "disconnect;";


  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  public static final String ROW_EMPTY = "EMPTY_ROW";
  public static final String DATABASE_EMPTY = "EMPTY_DATABASE";
  public static final String ENTRY_NULL = "null";

  public static final String DBMS_DIR = "thssdb";
  public static final String META_SUFFIX = "_meta";

  public static final String LOG_BEGIN_TRANSACTION = "begin transaction";
  public static final String LOG_COMMIT = "commit";
}
