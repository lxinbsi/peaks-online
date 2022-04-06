package com.bsi.peaks.server.handlers.specifications;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.common.functional.TriFunction;
import com.bsi.peaks.data.graph.DbApplicationGraph;
import com.bsi.peaks.data.graph.GraphTaskIds;
import com.bsi.peaks.data.graph.LfqApplicationGraph;
import com.bsi.peaks.data.graph.RiqApplicationGraph;
import com.bsi.peaks.data.graph.SilacApplicationGraph;
import com.bsi.peaks.data.graph.SlApplicationGraph;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest.AnalysisQueryRequest;
import com.bsi.peaks.model.dto.FilterList;
import com.bsi.peaks.model.dto.IDFilter;
import com.bsi.peaks.model.dto.LfqProteinFeatureVectorFilter;
import com.bsi.peaks.model.dto.ReporterIonQProteinFilter;
import com.bsi.peaks.model.dto.SilacFilter;
import com.bsi.peaks.model.dto.SlFilter;
import com.bsi.peaks.model.query.IdentificationQuery;
import com.bsi.peaks.model.query.IdentificationType;
import com.bsi.peaks.model.query.LabelFreeQuery;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.query.ReporterIonQMethodQuery;
import com.bsi.peaks.model.query.SilacQuery;
import com.bsi.peaks.model.query.SlQuery;
import com.bsi.peaks.server.parsers.ParserException;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ProtoHandlerSpecification<T extends GeneratedMessageV3, Q extends Query, A> implements SpecWithApplicationGraph<Q, A> {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoHandlerSpecification.class);
    protected final Function<Map<String, String>, Optional<FilterList>> readQuery;
    protected final TriFunction<A, Q, Map<String, String>, String> extensionName;
    protected final TriFunction<GraphTaskIds, Q, Map<String, String>, A> applicationGraphFactory;
    protected final TriFunction<A, Q, Map<String, String>, Source<T, NotUsed>> graphToSource;
    protected final BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks;
    protected final Function<Map<String, String>, Optional<WorkFlowType>> getWorkFlowType;

    public static <T extends GeneratedMessageV3> ProtoHandlerSpecification<T, IdentificationQuery, DbApplicationGraph> identificationQuery(
        Function<Map<String, String>, String> extensionName,
        TriFunction<GraphTaskIds, IdentificationQuery, Map<String, String>, DbApplicationGraph> applicationGraphFactory,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return new ProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readIdFilters,
            (applicationGraph, identificationQuery, apiParameters) -> {
                if (identificationQuery.identificationType().equals(IdentificationType.NONE)) {
                    return extensionName.apply(apiParameters);
                } else {
                    return IdentificationType.displayName(identificationQuery.identificationType()).toLowerCase() + "." + extensionName.apply(apiParameters);
                }
            },
            applicationGraphFactory,
            graphToSource,
            apiParametersToGraphTasks,
            ProtoHandlerSpecification::getSearchTypeIdentification
        );
    }

    public static <T extends GeneratedMessageV3> ProtoHandlerSpecification<T, IdentificationQuery, DbApplicationGraph> identificationQueryAnySearch(
        String extensionName,
        TriFunction<GraphTaskIds, IdentificationQuery, Map<String, String>, DbApplicationGraph> applicationGraphFactory,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return new ProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readIdFilters,
            (applicationGraph, identificationQuery, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            apiParametersToGraphTasks,
            ProtoHandlerSpecification::getAnySearchType
        );
    }

    public static <T extends GeneratedMessageV3> ProtoHandlerSpecification<T, IdentificationQuery, DbApplicationGraph> identificationQuery(
        String extensionName,
        TriFunction<GraphTaskIds, IdentificationQuery, Map<String, String>, DbApplicationGraph> applicationGraphFactory,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return identificationQuery(
            (apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            apiParametersToGraphTasks
        );
    }

    public static Optional<FilterList> readLfqFilters(Map<String, String> apiParameters) {
        String filterJson = apiParameters.get("filter");
        try {
            if (filterJson == null || filterJson.isEmpty()) {
                return Optional.empty();
            }

            LfqProteinFeatureVectorFilter.Builder builder = LfqProteinFeatureVectorFilter.newBuilder();
            JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(filterJson, builder);
            return Optional.of(FilterList.newBuilder()
                .setLfqProteinFeatureVectorFilter(builder.build())
                .build());
        } catch (Exception e) {
            throw new ParserException("Lfq filter reading error.", e);
        }
    }

    public static Optional<FilterList> readRiqFilters(Map<String, String> apiParameters) {
        String filterJson = apiParameters.get("filter");
        try {
            if (filterJson == null || filterJson.isEmpty()) {
                return Optional.empty();
            }

            ReporterIonQProteinFilter.Builder builder = ReporterIonQProteinFilter.newBuilder();
            JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(filterJson, builder);
            return Optional.of(FilterList.newBuilder()
                .setReporterIonIonQProteinFilter(builder.build())
                .build());
        } catch (Exception e) {
            throw new ParserException("ReporterIonQ filter reading error.", e);
        }
    }

    public static Optional<FilterList> readIdFilters(Map<String, String> apiParameters) {
        String filterJson = apiParameters.get("filter");

        try {
            if (filterJson == null || filterJson.isEmpty()) {
                return Optional.empty();
            }

            IDFilter.Builder builder = IDFilter.newBuilder();
            JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(filterJson, builder);
            return Optional.of(FilterList.newBuilder().addIdFilters(builder.build()).build());
        } catch (Exception e) {
            throw new ParserException("Unable to read the identification filter.", e);
        }
    }

    public static Optional<FilterList> readSlFilter(Map<String, String> apiParameters) {
        String filterJson = apiParameters.get("filter");
        Optional<WorkFlowType> workFlowType = ProtoHandlerSpecification.getAnySearchType(apiParameters);

        try {
            if (filterJson == null || filterJson.isEmpty()) {
                return Optional.empty();
            }

            SlFilter.Builder slFilterBuilder = SlFilter.newBuilder();
            JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(filterJson, slFilterBuilder);

            FilterList.Builder builder = FilterList.newBuilder();
            if (workFlowType.isPresent()) {
                if (workFlowType.get() == WorkFlowType.DIA_DB_SEARCH) {
                    builder.setDiaDbFilter(slFilterBuilder.build());
                } else {
                    builder.setSlFilter(slFilterBuilder.build());
                }
            } else {
                throw new IllegalStateException("Workflow type not found when reading sl filter");
            }
            return Optional.of(builder.build());
        } catch (Exception e) {
            throw new ParserException("Unable to read the sl filter.", e);
        }
    }

    public static Optional<FilterList> readSilacFilter(Map<String, String> apiParameters) {
        String filterJson = apiParameters.get("filter");

        try {
            if (filterJson == null || filterJson.isEmpty()) {
                return Optional.empty();
            }

            SilacFilter.ProteinFilter.Builder builder = SilacFilter.ProteinFilter.newBuilder();
            JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(filterJson, builder);
            return Optional.of(FilterList.newBuilder().setSilacProteinFilter(builder.build()).build());
        } catch (Exception e) {
            throw new ParserException("Unable to read the sl filter.", e);
        }
    }

    /**
     * This causes filtering the sample ids when creating the graph taskIds. We want to avoid doing this
     */
    @Deprecated
    public static AnalysisQueryRequest.GraphTaskIds allSamplesOrSpecified(Map<String, String> apiParameters, Optional<WorkFlowType> workFlowType) {
        AnalysisQueryRequest.GraphTaskIds.Builder builder = AnalysisQueryRequest.GraphTaskIds.newBuilder();

        String sample = apiParameters.get("sample");
        if (workFlowType.isPresent()) {
            builder.setWorkflowType(workFlowType.get());
        } else {
            builder.setLastWorkflow(true);
        }
        if (sample != null && !sample.isEmpty()) {
            builder.setSampleId(sample);
        }

        return builder.build();
    }

    public static AnalysisQueryRequest.GraphTaskIds allSamples(Map<String, String> apiParameters, Optional<WorkFlowType> workFlowType) {
        AnalysisQueryRequest.GraphTaskIds.Builder builder = AnalysisQueryRequest.GraphTaskIds.newBuilder();

        if (apiParameters.containsKey("sample")) {
            LOG.info("Warning ignoring unsupported 'sample' query parameter");
        }

        if (workFlowType.isPresent()) {
            builder.setWorkflowType(workFlowType.get());
        } else {
            builder.setLastWorkflow(true);
        }

        return builder.build();
    }

    public static AnalysisQueryRequest.GraphTaskIds ignoreSample(Map<String, String> apiParameters, Optional<WorkFlowType> workFlowType) {
        AnalysisQueryRequest.GraphTaskIds.Builder builder = AnalysisQueryRequest.GraphTaskIds.newBuilder();
        if (workFlowType.isPresent()) {
            builder.setWorkflowType(workFlowType.get());
        } else {
            builder.setLastWorkflow(true);
        }
        return builder.build();
    }

    public static AnalysisQueryRequest.GraphTaskIds specifiedSample(Map<String, String> apiParameters, Optional<WorkFlowType> workFlowType) {
        AnalysisQueryRequest.GraphTaskIds.Builder builder = AnalysisQueryRequest.GraphTaskIds.newBuilder();

        String sample = apiParameters.get("sample");

        if (workFlowType.isPresent()) {
            builder.setWorkflowType(workFlowType.get());
        } else {
            builder.setLastWorkflow(true);
        }

        if (sample != null && !sample.isEmpty()) {
            builder.setSampleId(sample);
        } else {
            throw new ParserException("Unable to parse required sample");
        }

        return builder.build();
    }

    public static <T extends GeneratedMessageV3> ProtoHandlerSpecification<T, SilacQuery, SilacApplicationGraph> silacQuery(
        String extensionName,
        TriFunction<GraphTaskIds, SilacQuery, Map<String, String>, SilacApplicationGraph> applicationGraphFactory,
        TriFunction<SilacApplicationGraph, SilacQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return new ProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readSilacFilter,
            (applicationGraph, query, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            apiParametersToGraphTasks,
            apiParameters1 -> Optional.of(WorkFlowType.SILAC)
        );
    }

    public static <T extends GeneratedMessageV3> ProtoHandlerSpecification<T, LabelFreeQuery, LfqApplicationGraph> labelFreeQuery(
        String extensionName,
        TriFunction<GraphTaskIds, LabelFreeQuery, Map<String, String>, LfqApplicationGraph> applicationGraphFactory,
        TriFunction<LfqApplicationGraph, LabelFreeQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return new ProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readLfqFilters,
            (applicationGraph, query, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            apiParametersToGraphTasks,
            apiParameters -> getLfqSearchType(apiParameters)
        );
    }

    public static <T extends GeneratedMessageV3> ProtoHandlerSpecification<T, ReporterIonQMethodQuery, RiqApplicationGraph> reporterIonQuery(
        String extensionName,
        TriFunction<GraphTaskIds, ReporterIonQMethodQuery, Map<String, String>, RiqApplicationGraph> applicationGraphFactory,
        TriFunction<RiqApplicationGraph, ReporterIonQMethodQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return new ProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readRiqFilters,
            (applicationGraph, query, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            apiParametersToGraphTasks,
            ignore -> Optional.of(WorkFlowType.REPORTER_ION_Q)
        );
    }

    public static <T extends GeneratedMessageV3> ProtoHandlerSpecification<T, SlQuery, SlApplicationGraph> slQuery(
        TriFunction<GraphTaskIds, SlQuery, Map<String, String>, SlApplicationGraph> applicationGraphFactory,
        TriFunction<SlApplicationGraph, SlQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return new ProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readSlFilter,
            (applicationGraph, slQuery, apiParameters) -> "Endpoint does not support csv exporting",
            applicationGraphFactory,
            graphToSource,
            apiParametersToGraphTasks,
            ProtoHandlerSpecification::getDiaSearchType
        );
    }

    protected ProtoHandlerSpecification(
        Function<Map<String, String>, Optional<FilterList>> readQuery,
        TriFunction<A, Q, Map<String, String>, String> extensionName,
        TriFunction<GraphTaskIds, Q, Map<String, String>, A> applicationGraphFactory,
        TriFunction<A, Q, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks,
        Function<Map<String, String>, Optional<WorkFlowType>> getWorkFlowType
    ) {
        this.readQuery = readQuery;
        this.extensionName = extensionName;
        this.applicationGraphFactory = applicationGraphFactory;
        this.graphToSource = graphToSource;
        this.apiParametersToGraphTasks = apiParametersToGraphTasks;
        this.getWorkFlowType = getWorkFlowType;
    }

    public static Optional<WorkFlowType> getAnySearchType(Map<String, String> apiParameters) {
        final String search = apiParameters.get("search");
        if (search != null) {
            switch (search.toUpperCase()) {
                case "LFQ":
                    return Optional.of(WorkFlowType.LFQ);
                case "DB":
                    return Optional.of(WorkFlowType.DB_SEARCH);
                case "PTM_FINDER":
                    return Optional.of(WorkFlowType.PTM_FINDER);
                case "SPIDER":
                    return Optional.of(WorkFlowType.SPIDER);
                case "REPORTERIONQ":
                    return Optional.of(WorkFlowType.REPORTER_ION_Q);
                case "SL":
                    return Optional.of(WorkFlowType.SPECTRAL_LIBRARY);
                case "SILAC":
                    return Optional.of(WorkFlowType.SILAC);
                case "DIA_DB":
                    return Optional.of(WorkFlowType.DIA_DB_SEARCH);
                case "DENOVO":
                case "NO":
                case "NONE":
                    return Optional.of(WorkFlowType.DENOVO);
            }
            LOG.warn("Unexpected search api paramater: " + search);
        }
        return Optional.empty();
    }

    public static Optional<WorkFlowType> getDiaSearchType(Map<String, String> apiParameters) {
        final String search = apiParameters.get("search");
        if (search != null) {
            switch (search.toUpperCase()) {
                case "SL":
                    return Optional.of(WorkFlowType.SPECTRAL_LIBRARY);
                case "DIA_DB":
                    return Optional.of(WorkFlowType.DIA_DB_SEARCH);
                case "NONE":
                    throw new IllegalStateException("Dia search type unexpected search for workflow type: " + search);
            }
        }
        return Optional.empty();
    }

    public static Optional<WorkFlowType> getLfqSearchType(Map<String, String> apiParameters) {
        final String search = apiParameters.get("search");
        if (search != null) {
            switch (search.toUpperCase()) {
                case "LFQ":
                    return Optional.of(WorkFlowType.LFQ);
                default:
                    throw new IllegalStateException("Unexpected search for workflow type: " + search + " for lfq query.");
            }
        }
        return Optional.empty();
    }

    public static Optional<WorkFlowType> getSearchTypeIdentification(Map<String, String> apiParameters) {
        final String search = apiParameters.get("search");
        if (search != null) {
            switch (search.toUpperCase()) {
                case "DB":
                    return Optional.of(WorkFlowType.DB_SEARCH);
                case "PTM_FINDER":
                    return Optional.of(WorkFlowType.PTM_FINDER);
                case "SPIDER":
                    return Optional.of(WorkFlowType.SPIDER);
                case "REPORTERIONQ":
                    return Optional.of(WorkFlowType.REPORTER_ION_Q);
                case "SL":
                    return Optional.of(WorkFlowType.SPECTRAL_LIBRARY);
                case "DENOVO":
                case "NO":
                case "NONE":
                    return Optional.of(WorkFlowType.DENOVO);
            }
            LOG.warn("apiParameters exception unable to parse query");
        }
        return Optional.of(WorkFlowType.DATA_REFINEMENT);
    }

    public String extensionName(A applicationGraph, Q query, Map<String, String> apiParameters) {
        return extensionName.apply(applicationGraph, query, apiParameters);
    }

    @Override
    public A applicationGraph(GraphTaskIds graphTaskIds, Q query, Map<String, String> apiParameters) {
        return applicationGraphFactory.apply(graphTaskIds, query, apiParameters);
    }

    public AnalysisQueryRequest.GraphTaskIds graphTaskIdsQuery(Map<String, String> apiParameters, Optional<WorkFlowType> workFlowType) {
        return apiParametersToGraphTasks.apply(apiParameters, workFlowType);
    }

    public Source<T, NotUsed> source(A applicationGraph, Q query, Map<String, String> apiParameters) {
        return graphToSource.apply(applicationGraph, query, apiParameters);
    }

    public Optional<WorkFlowType> getWorkFlowType(Map<String, String> apiParameters) {
        return getWorkFlowType.apply(apiParameters);
    }
}
