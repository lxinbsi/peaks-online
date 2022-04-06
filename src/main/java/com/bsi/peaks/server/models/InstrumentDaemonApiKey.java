package com.bsi.peaks.server.models;

import com.bsi.peaks.model.PeaksImmutable;

import java.io.Serializable;

@PeaksImmutable
public interface InstrumentDaemonApiKey extends Serializable {
    default String apiKey() { return ""; }
}
