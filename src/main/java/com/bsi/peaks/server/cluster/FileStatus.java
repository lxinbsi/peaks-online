package com.bsi.peaks.server.cluster;

import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.FilePath;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

import java.time.Instant;

@PeaksImmutable
@JsonDeserialize(builder = FileStatusBuilder.class)
public interface FileStatus {
    long fileSize();
    Instant timeStamp();
    boolean hasUploaded();
    @Value.Lazy
    default boolean enabledCopy() {
        return !copyLocation().getRemotePathName().isEmpty();
    }
    FilePath copyLocation();



    FileStatus withFileSize(long newCutOff);
    FileStatus withTimeStamp(Instant newTimeStamp);
    FileStatus withHasUploaded(boolean newHasUploaded);
}