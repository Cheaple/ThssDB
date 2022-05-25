package cn.edu.thssdb.exception;

import cn.edu.thssdb.type.ComparerType;

public class TypeNotMatchException extends RuntimeException {

    private ComparerType type1;
    private ComparerType type2;

    public TypeNotMatchException(ComparerType type1, ComparerType type2)
    {
        super();
        this.type1 = type1;
        this.type2 = type2;
    }
    @Override
    public String getMessage() {
        String message1 = "Null";
        String message2 = "Null";
        switch(this.type1) {
            case COLUMN:
                message1 = "Column";
                break;
            case STRING:
                message1 = "String";
                break;
            case NUMBER:
                message1 = "Number";
                break;
            case NULL:
                message1 = "Null";
                break;
        }
        switch(this.type2) {
            case COLUMN:
                message2 = "Column";
                break;
            case STRING:
                message2 = "String";
                break;
            case NUMBER:
                message2 = "Number";
                break;
            case NULL:
                message2 = "Null";
                break;
        }
        return "Exception: Type 1 " + message1 + " and " + "type 2 " + message2 + " do not match!";
    }

}
