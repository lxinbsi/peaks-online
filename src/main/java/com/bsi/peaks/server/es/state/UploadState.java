package com.bsi.peaks.server.es.state;

public enum UploadState {
    QUEUED,
    TIMEOUT,
    PENDING,
    STARTED,
    COMPLETED,
    FAILED
}
