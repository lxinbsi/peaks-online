package com.bsi.peaks.server.models;

import com.bsi.peaks.model.PeaksImmutable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.time.Instant;

@PeaksImmutable
@JsonDeserialize(builder = LicenseBuilder.class)
public interface License {
    default String user() {
        return "";
    }

    default String key() {
        return "";
    }

    default long start() {
        return Instant.now().getEpochSecond();
    }

    default long expiry() {
        return Instant.now().getEpochSecond();
    }

    default int maxThread() {
        return 0;
    }

    default int maxUser() {
        return 1;
    }

    default boolean qEnabled() {
        return false;
    }
}
