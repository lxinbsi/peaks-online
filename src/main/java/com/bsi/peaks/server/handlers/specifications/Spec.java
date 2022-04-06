package com.bsi.peaks.server.handlers.specifications;

public interface Spec {
    enum SpecType {
        ApplicationGraph,
        Services,
    }

    SpecType getType();
}
