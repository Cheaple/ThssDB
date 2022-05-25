package cn.edu.thssdb.exception;

public class NullValueException extends RuntimeException {
	private String mColumnName;
	
	public NullValueException(String column_name)
	{
		super();
		this.mColumnName = column_name;
	}
	
	@Override
	public String getMessage() {
		return "Exception: the column named " + mColumnName + " should not be null!";
	}
}
