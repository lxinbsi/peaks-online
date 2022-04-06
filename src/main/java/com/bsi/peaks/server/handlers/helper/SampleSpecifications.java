package com.bsi.peaks.server.handlers.helper;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.graph.DbApplicationGraph;
import com.google.protobuf.GeneratedMessageV3;
import io.vertx.core.http.HttpServerRequest;

import java.util.function.BiFunction;

public class SampleSpecifications<T extends GeneratedMessageV3> {
    private final BiFunction<DbApplicationGraph, HttpServerRequest, Source<T, NotUsed>> sourceCreator;
    private final String extensionName;

    public static <T extends GeneratedMessageV3> SampleSpecifications<T> create(
        BiFunction<DbApplicationGraph, HttpServerRequest, Source<T, NotUsed>> sourceCreator,
        String extensionName
    ){
        return new SampleSpecifications<>(sourceCreator, extensionName);
    }

    private SampleSpecifications(
        BiFunction<DbApplicationGraph, HttpServerRequest, Source<T, NotUsed>> sourceCreator,
        String extensionName
    ){
        this.sourceCreator = sourceCreator;
        this.extensionName = extensionName;
    }

    public Source<T, NotUsed> getSource(DbApplicationGraph graph, HttpServerRequest request) {
        return sourceCreator.apply(graph, request);
    }

    public String getExtensionName(){
        return extensionName;
    }
}
