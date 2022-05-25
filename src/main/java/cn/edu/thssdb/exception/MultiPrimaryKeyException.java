package cn.edu.thssdb.exception;


public class MultiPrimaryKeyException extends RuntimeException {
	private String name;

	public MultiPrimaryKeyException(String name) {
		super();
		this.name = name;
	}
	@Override
	public String getMessage() {
		return "Exception: there is multi primary keys in table " + name + "!";
	}
}
