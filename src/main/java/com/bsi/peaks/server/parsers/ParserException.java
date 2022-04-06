package com.bsi.peaks.server.parsers;

/**
 * @author Shengying Pan Created by span on 2/23/17.
 */
@SuppressWarnings("javadoc")
public class ParserException extends IllegalArgumentException {
    
    private static final long serialVersionUID = 6102889875828163045L;
    
    public ParserException() {
        super();
    }
    
    public ParserException(String s) {
        super(s);
    }
    
    public ParserException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParserException(String message, Exception cause) {
        super(message, cause);
    }

    public ParserException(Throwable cause) {
        super(cause);
    }
}
