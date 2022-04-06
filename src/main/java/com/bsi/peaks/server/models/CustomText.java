package com.bsi.peaks.server.models;

import com.bsi.peaks.model.PeaksImmutable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.Serializable;

@PeaksImmutable
@JsonDeserialize(builder = CustomTextBuilder.class)
public interface CustomText extends Serializable {
    default String headMessage() { return ""; }
    default String contentMessage() { return ""; }
    default String website() { return ""; }
}
