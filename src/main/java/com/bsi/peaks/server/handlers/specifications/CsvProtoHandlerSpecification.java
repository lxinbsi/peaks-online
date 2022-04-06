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
import com.bsi.peaks.io.writer.dto.Csv;
import com.bsi.peaks.model.dto.FilterList;
import com.bsi.peaks.model.query.IdentificationQuery;
import com.bsi.peaks.model.query.IdentificationType;
import com.bsi.peaks.model.query.LabelFreeQuery;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.query.ReporterIonQMethodQuery;
import com.bsi.peaks.model.query.SilacQuery;
import com.bsi.peaks.model.query.SlQuery;
import com.google.protobuf.GeneratedMessageV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CsvProtoHandlerSpecification<T extends GeneratedMessageV3, Q extends Query, A> extends ProtoHandlerSpecification implements SpecWithApplicationGraph {
    private static final Logger LOG = LoggerFactory.getLogger(CsvProtoHandlerSpecification.class);

    private final TriFunction<A, Q, Map<String, String>, CompletionStage<? extends Csv<T>>> csvSupplier;

    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, IdentificationQuery, DbApplicationGraph> identificationQuery(
        BiFunction<DbApplicationGraph, Map<String, String>, String> extensionName,
        TriFunction<GraphTaskIds, IdentificationQuery, Map<String, String>, DbApplicationGraph> applicationGraphFactory,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return new CsvProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readIdFilters,
            (applicationGraph, identificationQuery, apiParameters) -> {
                if (identificationQuery.identificationType().equals(IdentificationType.NONE)) {
                    return extensionName.apply(applicationGraph, apiParameters);
                } else {
                    return IdentificationType.displayName(identificationQuery.identificationType()).toLowerCase()
                        + "." + extensionName.apply(applicationGraph, apiParameters);
                }
            },
            applicationGraphFactory,
            graphToSource,
            csvSupplier,
            apiParametersToGraphTasks,
            ProtoHandlerSpecification::getSearchTypeIdentification
        );
    }

    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, IdentificationQuery, DbApplicationGraph> identificationQuery(
        String extensionName,
        TriFunction<GraphTaskIds, IdentificationQuery, Map<String, String>, DbApplicationGraph> applicationGraphFactory,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return identificationQuery(
            (applicationGraph, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            csvSupplier,
            apiParametersToGraphTasks
        );
    }

    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, IdentificationQuery, DbApplicationGraph> identificationQuery(
        String extensionName,
        TriFunction<GraphTaskIds, IdentificationQuery, Map<String, String>, DbApplicationGraph> applicationGraphFactory,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        Function<DbApplicationGraph, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        if (csvSupplier == null) {
            throw new IllegalStateException("Csv protohandler spec must have a csv supplier");
        }

        return identificationQuery(
            (applicationGraph, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            (applicationGraph, query, apiParameters) -> csvSupplier.apply(applicationGraph),
            apiParametersToGraphTasks
        );
    }

    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, IdentificationQuery, DbApplicationGraph> identificationQuery(
        BiFunction<DbApplicationGraph, Map<String, String>, String> extensionName,
        TriFunction<GraphTaskIds, IdentificationQuery, Map<String, String>, DbApplicationGraph> applicationGraphFactory,
        TriFunction<DbApplicationGraph, IdentificationQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        Function<DbApplicationGraph, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        if (csvSupplier == null) {
            throw new IllegalStateException("Csv protohandler spec must have a csv supplier");
        }

        return identificationQuery(
            extensionName,
            applicationGraphFactory,
            graphToSource,
            (applicationGraph, query, apiParameters) -> csvSupplier.apply(applicationGraph),
            apiParametersToGraphTasks
        );
    }

    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, SlQuery, SlApplicationGraph> slQuery(
        BiFunction<SlApplicationGraph, Map<String, String>, String> extensionName,
        TriFunction<GraphTaskIds, SlQuery, Map<String, String>, SlApplicationGraph> applicationGraphFactory,
        TriFunction<SlApplicationGraph, SlQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        TriFunction<SlApplicationGraph, SlQuery, Map<String, String>, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        return new CsvProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readSlFilter,
            (applicationGraph, identificationQuery, apiParameters) -> extensionName.apply(applicationGraph, apiParameters),
            applicationGraphFactory,
            graphToSource,
            csvSupplier,
            apiParametersToGraphTasks,
            ProtoHandlerSpecification::getDiaSearchType
        );
    }

    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, SilacQuery, SilacApplicationGraph> silacQuery(
        String extensionName,
        TriFunction<GraphTaskIds, SilacQuery, Map<String, String>, SilacApplicationGraph> applicationGraphFactory,
        TriFunction<SilacApplicationGraph, SilacQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<SilacApplicationGraph, SilacQuery, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        if (csvSupplier == null) {
            throw new IllegalStateException("Csv protohandler spec must have a csv supplier");
        }

        return new CsvProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readSilacFilter,
            (applicationGraph, query, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            (applicationGraph, labelFreeQuery, apiParameters) -> csvSupplier.apply(applicationGraph, labelFreeQuery),
            apiParametersToGraphTasks,
            apiParameters1 -> Optional.of(WorkFlowType.SILAC)
        );
    }

    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, SilacQuery, SilacApplicationGraph> silacQuery(
        BiFunction<SilacApplicationGraph, Map<String, String>, String> extensionName,
        TriFunction<GraphTaskIds, SilacQuery, Map<String, String>, SilacApplicationGraph> applicationGraphFactory,
        TriFunction<SilacApplicationGraph, SilacQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<SilacApplicationGraph, SilacQuery, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        if (csvSupplier == null) {
            throw new IllegalStateException("Csv protohandler spec must have a csv supplier");
        }

        return new CsvProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readSilacFilter,
            (applicationGraph, query, apiParameters) -> extensionName.apply(applicationGraph, apiParameters),
            applicationGraphFactory,
            graphToSource,
            (applicationGraph, labelFreeQuery, apiParameters) -> csvSupplier.apply(applicationGraph, labelFreeQuery),
            apiParametersToGraphTasks,
            apiParameters1 -> Optional.of(WorkFlowType.SILAC)
        );
    }


    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, LabelFreeQuery, LfqApplicationGraph> labelFreeQuery(
        String extensionName,
        TriFunction<GraphTaskIds, LabelFreeQuery, Map<String, String>, LfqApplicationGraph> applicationGraphFactory,
        TriFunction<LfqApplicationGraph, LabelFreeQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        BiFunction<LfqApplicationGraph, LabelFreeQuery, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        if (csvSupplier == null) {
            throw new IllegalStateException("Csv protohandler spec must have a csv supplier");
        }

        return new CsvProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readLfqFilters,
            (applicationGraph, query, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            (applicationGraph, labelFreeQuery, apiParameters) -> csvSupplier.apply(applicationGraph, labelFreeQuery),
            apiParametersToGraphTasks,
            apiParameters -> getLfqSearchType(apiParameters)
        );
    }

    public static <T extends GeneratedMessageV3> CsvProtoHandlerSpecification<T, ReporterIonQMethodQuery, RiqApplicationGraph> reportIonQuery(
        String extensionName,
        TriFunction<GraphTaskIds, ReporterIonQMethodQuery, Map<String, String>, RiqApplicationGraph> applicationGraphFactory,
        TriFunction<RiqApplicationGraph, ReporterIonQMethodQuery, Map<String, String>, Source<T, NotUsed>> graphToSource,
        TriFunction<RiqApplicationGraph, ReporterIonQMethodQuery, Map<String, String>, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks
    ) {
        if (csvSupplier == null) {
            throw new IllegalStateException("Csv protohandler spec must have a csv supplier");
        }

        return new CsvProtoHandlerSpecification<>(
            ProtoHandlerSpecification::readRiqFilters,
            (applicationGraph, query, apiParameters) -> extensionName,
            applicationGraphFactory,
            graphToSource,
            csvSupplier,
            apiParametersToGraphTasks,
            ignore -> Optional.of(WorkFlowType.REPORTER_ION_Q)
        );
    }

    private CsvProtoHandlerSpecification(
        Function<Map<String, String>, Optional<FilterList>> readQuery,
        TriFunction<A, Q, Map<String, String>, String> extensionName,
        TriFunction<GraphTaskIds, Q, Map<String, String>, A> applicationGraphFactory,
        TriFunction<A, Q, Map<String, String>, Source<T, NotUsed>> graphToSource,
        TriFunction<A, Q, Map<String, String>, CompletionStage<? extends Csv<T>>> csvSupplier,
        BiFunction<Map<String, String>, Optional<WorkFlowType>, AnalysisQueryRequest.GraphTaskIds> apiParametersToGraphTasks,
        Function<Map<String, String>, Optional<WorkFlowType>> getWorkFlowType
    ) {
        super(readQuery, extensionName, applicationGraphFactory, graphToSource, apiParametersToGraphTasks, getWorkFlowType);
        this.csvSupplier = csvSupplier;
    }

    public CompletionStage<? extends Csv<T>> csv(A applicationGraph, Q query, Map<String, String> apiParameters) {
        return csvSupplier.apply(applicationGraph, query, apiParameters);
    }

    public boolean supportsCsv() {
        return true;
    }

}
