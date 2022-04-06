package com.bsi.peaks.server.handlers;

/**
 * A special case exception thrown from API handlers
 * @author Shengying Pan
 * Created by span on 1/19/17.
 */
public class HandlerException extends RuntimeException {
    private int errorCode;
    
    /**
     * Create a HandlerException with error message
     * @param errorCode associated HTTP error status code
     * @param message error message
     */
    public HandlerException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    
    /**
     * Create a HandlerException with error message and its cause
     * @param errorCode associated HTTP error status code
     * @param message error message
     * @param cause cause of the exception, a throwable
     */
    public HandlerException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    
    public int getErrorCode() {
        return errorCode;
    }
}
