package com.bsi.peaks.server.service;

public class InternalServerException extends RuntimeException {
    public InternalServerException(String message){
        super(message);
    }

    public InternalServerException(Exception error) {
        super(error);
    }

    public InternalServerException(String message, Exception error){
        super(message, error);
    }

    public InternalServerException(String message, Throwable error){
        super(message, error);
    }
}
