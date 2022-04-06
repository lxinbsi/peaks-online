package com.bsi.peaks.server.handlers.specifications;

import com.bsi.peaks.data.graph.GraphTaskIds;
import com.bsi.peaks.model.query.Query;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.util.Map;

public interface SpecWithApplicationGraph<T extends Query, A> extends Spec {
    A applicationGraph(GraphTaskIds graphTaskIds, T query, Map<String, String> apiParameters);

    ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
        .registerModule(new Jdk8Module());

    @Override
    default SpecType getType() {
        return SpecType.ApplicationGraph;
    }
}
