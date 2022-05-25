package cn.edu.thssdb.exception;


public class ValueExceedException extends RuntimeException {
	private String mColumnName;
	private int mRealLength;
	private int mMaxLength;
	private String extraMessage;
	
	public ValueExceedException(String column_name, int realLength, int maxLength, String extraMessage)
	{
		super();
		this.mColumnName = column_name;
		this.mRealLength = realLength;
		this.mMaxLength = maxLength;
		this.extraMessage = extraMessage;
	}
	
	@Override
	public String getMessage() {
		return "Exception: the column named " + mColumnName + "'s length" + "(" + mRealLength + ")" +
				" has exceeded its maximum" + "("+ mMaxLength + ")" + extraMessage + "!";
	}
}
