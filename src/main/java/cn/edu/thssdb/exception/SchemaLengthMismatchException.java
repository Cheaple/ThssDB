package cn.edu.thssdb.exception;

public class SchemaLengthMismatchException extends RuntimeException {
    private int expectedLen;
    private int realLen;
    String msg;
    public SchemaLengthMismatchException(int expectedLen, int realLen, String extraMsg)
    {
        super();
        this.expectedLen = expectedLen;
        this.realLen = realLen;
        this.msg = extraMsg;
    }

    @Override
    public String getMessage() {
        return "Exception: expected " + expectedLen + " columns, " +
                "but got " + realLen + " columns." + msg;
    }
}
