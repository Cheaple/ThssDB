package cn.edu.thssdb.exception;

public class ValueFormatInvalidException extends RuntimeException {

	private String extraMessage;
	public ValueFormatInvalidException(String extraMessage){
		super();
		this.extraMessage = extraMessage;
	}

	@Override
	public String getMessage() {
			return "Exception: Value format mismatched" + extraMessage + "!";
		}
}
