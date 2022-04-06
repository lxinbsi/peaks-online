package com.bsi.peaks.server.handlers.specifications;

import akka.Done;
import akka.stream.Materializer;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.common.functional.TriFunction;
import com.bsi.peaks.data.graph.GraphTaskIds;
import com.bsi.peaks.model.filter.FilterFunction;
import com.bsi.peaks.model.query.DbSearchQuery;
import com.bsi.peaks.model.query.DenovoQuery;
import com.bsi.peaks.model.query.PtmFinderQuery;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.query.SpiderQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ExportSpecification<Q extends Query, A> implements SpecWithApplicationGraph<Q, A> {
    private static final Logger LOG = LoggerFactory.getLogger(ExportSpecification.class);

    private final String exportLabel;
    private final String mimeType;
    private final BiFunction<Q, Map<String, String>, String> extensionNameFunction;
    private final ExportProcess process;
    private final TriFunction<GraphTaskIds, Q, Map<String, String>, A> applicationGraphFactory;

    public ExportSpecification(
        String exportLabel,
        String mimeType,
        Function<Q, String> extensionNameFunction,
        ExportProcess<A> process,
        TriFunction<GraphTaskIds, Q, Map<String, String>, A> applicationGraphFactory
    ) {
        this.exportLabel = exportLabel;
        this.mimeType = mimeType;
        this.extensionNameFunction = (query, apiParameters) -> extensionNameFunction.apply(query);
        this.process = process;
        this.applicationGraphFactory = applicationGraphFactory;
    }

    public ExportSpecification(
        String exportLabel,
        String mimeType,
        BiFunction<Q, Map<String, String>, String> extensionNameFunction,
        ExportProcess<A> process,
        TriFunction<GraphTaskIds, Q, Map<String, String>, A> applicationGraphFactory
    ) {
        this.exportLabel = exportLabel;
        this.mimeType = mimeType;
        this.extensionNameFunction = extensionNameFunction;
        this.process = process;
        this.applicationGraphFactory = applicationGraphFactory;
    }

    public ExportSpecification(
        String exportLabel,
        String mimeType,
        Function<Map<String, String>, String> extensionName,
        ExportProcess<A> process,
        Function<GraphTaskIds, A> applicationGraphFactory
    ) {
        this.exportLabel = exportLabel;
        this.mimeType = mimeType;
        this.extensionNameFunction = (ignoreQuery, ignoreParameters) -> extensionName.apply(ignoreParameters);
        this.process = process;
        this.applicationGraphFactory = (taskIds, ignore, ingore2) -> applicationGraphFactory.apply(taskIds);
    }

    public ExportSpecification(
        String exportLabel,
        String mimeType,
        String extensionName,
        ExportProcess<A> process,
        Function<GraphTaskIds, A> applicationGraphFactory
    ) {
        this.exportLabel = exportLabel;
        this.mimeType = mimeType;
        this.extensionNameFunction = (ignoreQuery, ignoreParameters) -> extensionName;
        this.process = process;
        this.applicationGraphFactory = (taskIds, ignore, ingore2) -> applicationGraphFactory.apply(taskIds);
    }

    public ExportSpecification(
        String exportLabel,
        String mimeType,
        String extensionName,
        ExportProcess<A> process,
        TriFunction<GraphTaskIds, Q, Map<String, String>, A> applicationGraphFactory
    ) {
        this.exportLabel = exportLabel;
        this.mimeType = mimeType;
        this.extensionNameFunction = (ignoreQuery, ignoreParameters) -> extensionName;
        this.process = process;
        this.applicationGraphFactory = applicationGraphFactory;
    }

    public String name() {
        return extensionNameFunction.apply(null, null);
    }

    public String name(Q query, Map<String, String> apiParameters) {
        return extensionNameFunction.apply(query, apiParameters);
    }

    public String name(Q query) {
        return extensionNameFunction.apply(query, null);
    }

    public String name(Map<String, String> apiParameters) {
        return extensionNameFunction.apply(null, apiParameters);
    }

    @Override
    public A applicationGraph(GraphTaskIds graphTaskIds, Q query, Map<String, String> apiParameters) {
        return applicationGraphFactory.apply(graphTaskIds, query, apiParameters);
    }

    public <A> CompletionStage<Done> process(
        String filename,
        String hostname,
        A applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) throws Exception {
        return process.run(filename, hostname, applicationGraph, outputStream, materializer, request);
    }

    public String exportLabel() {
        return exportLabel;
    }

    public String mimeType() {
        return mimeType;
    }

    public Optional<FilterFunction[]> readFilters(Map<String, String> apiParameters) {
        final String search = apiParameters.get("search");
        final String filter = apiParameters.get("filter");

        if (filter != null) {
            try {
                if (search == null) {
                    return Optional.of(OM.readValue(filter, DenovoQuery.class).filters());
                }
                switch (search.toUpperCase()) {
                    case "DB":
                        return Optional.of(OM.readValue(filter, DbSearchQuery.class).filters());
                    case "PTM_FINDER":
                        return Optional.of(OM.readValue(filter, PtmFinderQuery.class).filters());
                    case "SPIDER":
                        return Optional.of(OM.readValue(filter, SpiderQuery.class).filters());
                    case "NO":
                    case "NONE":
                        return Optional.of(OM.readValue(filter, DenovoQuery.class).filters());
                }
            } catch (Exception e){
                LOG.error("apiParameters exception unable to parse query", e);
            }
        }

        return Optional.empty();
    }

    public WorkFlowType filterType(Map<String, String> apiParameters) {
        return ProtoHandlerSpecification.getSearchTypeIdentification(apiParameters).orElseThrow(() -> new IllegalStateException("Invalid search parameter"));
    }

    public String name(WorkFlowType query) {
        return query.name();
    }

    @FunctionalInterface
    public interface ExportProcess<A> {
        CompletionStage<Done> run(
            String filename,
            String hostname,
            A applicationGraph,
            OutputStream outputStream,
            Materializer materializer,
            Map<String, String> request
        ) throws Exception;
    }
}
