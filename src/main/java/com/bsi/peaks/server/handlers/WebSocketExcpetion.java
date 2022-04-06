package com.bsi.peaks.server.handlers;

public class WebSocketExcpetion extends RuntimeException {
    private short errorCode;

    /**
     * Create a WebSocketExcpetion with error message
     * @param errorCode associated code (see https://tools.ietf.org/html/rfc6455#section-7.4)
     * @param message error message
     */
    public WebSocketExcpetion(short errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Create a WebSocketExcpetion with error message and its cause
     * @param errorCode associated code (see https://tools.ietf.org/html/rfc6455#section-7.4)
     * @param message error message
     * @param cause cause of the exception, a throwable
     */
    public WebSocketExcpetion(short errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Create a WebSocketExcpetion with error message and its cause
     * @param errorCode associated code (see https://tools.ietf.org/html/rfc6455#section-7.4)
     * @param cause cause of the exception, a throwable
     */
    public WebSocketExcpetion(short errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public static WebSocketExcpetion protocolError(String message) {
        return new WebSocketExcpetion((short) 1002, message);
    }

    public static WebSocketExcpetion protocolError(String message, Throwable cause) {
        return new WebSocketExcpetion((short) 1002, message, cause);
    }

    public static WebSocketExcpetion goingAway(String message) {
        return new WebSocketExcpetion((short) 1001, message);
    }

    public static WebSocketExcpetion goingAway(String message, Throwable cause) {
        return new WebSocketExcpetion((short) 1001, message, cause);
    }

    public static WebSocketExcpetion goingAway(Throwable cause) {
        return new WebSocketExcpetion((short) 1001, cause);
    }
}
