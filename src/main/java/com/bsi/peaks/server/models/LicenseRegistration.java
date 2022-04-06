package com.bsi.peaks.server.models;

import com.bsi.peaks.model.PeaksImmutable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@PeaksImmutable
@JsonDeserialize(builder = LicenseRegistrationBuilder.class)
public interface LicenseRegistration {
    String name();
    String email();
    String key();
}
