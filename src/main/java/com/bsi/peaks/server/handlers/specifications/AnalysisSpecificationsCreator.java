package com.bsi.peaks.server.handlers.specifications;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.analysis.dto.ReporterIonQStep;
import com.bsi.peaks.data.graph.AnalysisInfoApplicationGraph;
import com.bsi.peaks.data.graph.LfqApplicationGraph;
import com.bsi.peaks.data.graph.QuantificationNormalizationGraph;
import com.bsi.peaks.data.graph.SampleFractions;
import com.bsi.peaks.data.graph.SilacApplicationGraph;
import com.bsi.peaks.io.writer.csv.riq.RiqNormalizationCsv;
import com.bsi.peaks.io.writer.csv.riq.RiqPeptideVectorCsv;
import com.bsi.peaks.io.writer.csv.riq.RiqProteinPeptideCsv;
import com.bsi.peaks.io.writer.csv.riq.RiqProteinVectorCsv;
import com.bsi.peaks.io.writer.csv.riq.RiqPsmVectorCsv;
import com.bsi.peaks.io.writer.csv.riq.RiqSummaryCsv;
import com.bsi.peaks.io.writer.csv.silac.SilacFeatureVectorDtoCsv;
import com.bsi.peaks.io.writer.csv.silac.SilacPeptideFeatureVectorCsv;
import com.bsi.peaks.io.writer.csv.silac.SilacProteinFeatureVectorCsv;
import com.bsi.peaks.io.writer.csv.silac.SilacProteinPeptideFeactureVectorCsv;
import com.bsi.peaks.io.writer.dto.DbDtoSources;
import com.bsi.peaks.io.writer.dto.DenovoResultCsv;
import com.bsi.peaks.io.writer.dto.GenericDtoFlows;
import com.bsi.peaks.io.writer.dto.IDSummaryCsv;
import com.bsi.peaks.io.writer.dto.LabelFreeCsv.FeatureVectorCsv;
import com.bsi.peaks.io.writer.dto.LabelFreeCsv.InternalFeatureVectorCsv;
import com.bsi.peaks.io.writer.dto.LabelFreeCsv.PeptideFeatureVectorCsv;
import com.bsi.peaks.io.writer.dto.LabelFreeCsv.ProteinFeatureVectorCsv;
import com.bsi.peaks.io.writer.dto.LabelFreeCsv.ProteinPeptideFeatureVectorCsv;
import com.bsi.peaks.io.writer.dto.LabelFreeCsv.ProteinWithPeptideFeatureVectorCsv;
import com.bsi.peaks.io.writer.dto.LfqDtoSources;
import com.bsi.peaks.io.writer.dto.PeptideCsv;
import com.bsi.peaks.io.writer.dto.ProteinCsv;
import com.bsi.peaks.io.writer.dto.ProteinPeptideCsv;
import com.bsi.peaks.io.writer.dto.PtmProfileCsv;
import com.bsi.peaks.io.writer.dto.RiqDtoSources;
import com.bsi.peaks.io.writer.dto.SilacDtoSources;
import com.bsi.peaks.io.writer.dto.SilacSummaryCsv;
import com.bsi.peaks.io.writer.dto.SlDtoSources;
import com.bsi.peaks.io.writer.dto.denovo.DenovoSummaryCsv;
import com.bsi.peaks.model.core.quantification.labelfree.LabelFreeGroup;
import com.bsi.peaks.model.dto.PruningFilterList;
import com.bsi.peaks.model.dto.peptide.DiaPeptideManualInput;
import com.bsi.peaks.model.dto.peptide.IonType;
import com.bsi.peaks.model.dto.peptide.Peptide;
import com.bsi.peaks.model.dto.peptide.Protein;
import com.bsi.peaks.model.dto.peptide.RiqPeptideVectors;
import com.bsi.peaks.model.dto.peptide.SilacProteinPeptideFeatureVector;
import com.bsi.peaks.model.filter.PeptidePrunerFactoryBuilder;
import com.bsi.peaks.model.filter.ProteinFeatureVectorFilter;
import com.bsi.peaks.model.filter.ProteinFeatureVectorFilterBuilder;
import com.bsi.peaks.model.filter.ProteinPrunerFactoryBuilder;
import com.bsi.peaks.model.filter.RiqProteinVectorFilter;
import com.bsi.peaks.model.parameters.query.LabelFreeQueryParameters;
import com.bsi.peaks.model.parameters.query.LabelFreeQueryParametersBuilder;
import com.bsi.peaks.model.query.DenovoViewFilter;
import com.bsi.peaks.model.query.LabelFreeQuery;
import com.bsi.peaks.model.query.LabelFreeQueryBuilder;
import com.bsi.peaks.model.query.PeptideFeatureVectorViewFilter;
import com.bsi.peaks.model.query.PeptideViewFilter;
import com.bsi.peaks.model.query.ProteinFeatureVectorViewFilter;
import com.bsi.peaks.model.query.ProteinViewFilter;
import com.bsi.peaks.model.query.ReporterIonQMethodQuery;
import com.bsi.peaks.model.query.ReporterIonQMethodQueryBuilder;
import com.bsi.peaks.model.query.RiqPeptideViewFilter;
import com.bsi.peaks.model.query.RiqPsmViewFilter;
import com.bsi.peaks.model.query.RiqPsmViewFilterBuilder;
import com.bsi.peaks.model.query.SilacPeptideViewFilter;
import com.bsi.peaks.model.query.SilacProteinViewFilter;
import com.bsi.peaks.model.query.SilacQuery;
import com.bsi.peaks.model.query.ViewFilter;
import com.bsi.peaks.server.handlers.HandlerException;
import com.bsi.peaks.server.handlers.ProtoHandler;
import com.bsi.peaks.server.handlers.helper.ApplicationGraphFactory;
import com.bsi.peaks.server.handlers.helper.ExportHelpers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.bsi.peaks.server.handlers.helper.ApplicationGraphFactory.SAMPLE_ID_API_PARAMETERS;
import static java.util.concurrent.CompletableFuture.completedFuture;

public interface AnalysisSpecificationsCreator {
    Logger LOG = LoggerFactory.getLogger(AnalysisSpecificationsCreator.class);

    ApplicationGraphFactory applicationGraphFactory();
    DtoSourceFactory dtoSourceFactory();
    ObjectMapper om();

    default ProtoHandlerSpecification silacPeptideDisplay() {
        return ProtoHandlerSpecification.silacQuery(
            "peptideDisplayXic",
            applicationGraphFactory()::createFromSilacQuery,
            (applicationGraph, query, request) -> {
                final UUID fractionId = UUID.fromString(request.get("fractionId"));
                final String peptideWithAsterisk = request.get("peptide");
                final UUID featureVectorId = UUID.fromString(request.get("featureVectorId"));
                return dtoSourceFactory().silac(applicationGraph)
                    .featureVectorSource(fractionId, peptideWithAsterisk, featureVectorId)
                    .orElse(Source.lazily(() -> Source.failed(new IllegalStateException("Failed to create silac Xic for peptide" + peptideWithAsterisk))));
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification silacStatisticsOfFilteredResult() {
        return CsvProtoHandlerSpecification.silacQuery(
            "silac.filteredStatistics",
            applicationGraphFactory()::createFromSilacQuery,
            (applicationGraph, query, request) -> dtoSourceFactory().silac(applicationGraph)
                .statisticsOfFilteredResult()
                .orElse(Source.lazily(() -> Source.failed(new IllegalStateException("Failed to create Silac Filtered Statistics.")))),
            (applicationGraph, query) -> completedFuture(SilacSummaryCsv.newSilacSummaryCsv(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification lfqStatisticsOfFilteredResult() {
        return ProtoHandlerSpecification.labelFreeQuery(
            "filteredStatistics",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> dtoSourceFactory().lfq(applicationGraph)
                .lfqStatisticsOfFilteredResult()
                .orElse(Source.lazily(() -> Source.failed(new IllegalStateException("Failed to create Lfq Filtered Statistics.")))),
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification riqSummaryStatistics() {
        return ProtoHandlerSpecification.reporterIonQuery(
            "riq.summaryStatistics",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> dtoSourceFactory().riq(applicationGraph)
                .riqSummaryStatistics()
                .orElse(Source.lazily(() -> Source.failed(new IllegalStateException("Failed to create ReporterIonQ Summary Statistics.")))),
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification statisticsOfFilteredResult() {
        return CsvProtoHandlerSpecification.identificationQuery(
            "filteredStatistics",
            applicationGraphFactory()::createFromIdentificationQueryIncludeDecoy,
            (applicationGraph, query, request) -> {
                return dtoSourceFactory().db(applicationGraph)
                    .statisticsOfFilteredResult()
                    .orElse(Source.lazily(() -> Source.failed(new IllegalStateException("Failed to create Identification Filtered Statistics."))));
            },
            (graph) -> completedFuture(IDSummaryCsv.newIDSummaryCsv(graph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification sampleScanStats() {
        return ProtoHandlerSpecification.identificationQuery(
            "sampleScanStats",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                return dtoSourceFactory().db(applicationGraph).sampleScanStats();
            },
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default CsvProtoHandlerSpecification proteinPeptideHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (graph, param) -> exportWithSampleName("protein-peptides", graph, param),
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                try {
                    final String protein = request.get("protein");
                    Predicate<Protein> viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinViewFilter.class)
                        .predicate();
                    final boolean topPsm = topPsm(request);
                    final Optional<UUID> specifiedSampleId = applicationGraph.specifiedSampleId();
                    if (protein == null) {
                        if (specifiedSampleId.isPresent()) {
                            return dtoSources.dbSearchProteinSourceBySample(specifiedSampleId.get())
                                .filter(viewFilter::test)
                                .via(GenericDtoFlows.proteinSorterFlow())
                                .via(dtoSources.proteinPeptideWithDenovoCandidateFlow(specifiedSampleId.get(), topPsm));
                        } else {
                            return dtoSources.dbSearchProteinSource()
                                .filter(viewFilter::test)
                                .via(GenericDtoFlows.proteinSorterFlow())
                                .via(dtoSources.proteinPeptideFlow(topPsm));
                        }
                    } else if (specifiedSampleId.isPresent()) {
                        final UUID proteinId = UUID.fromString(protein);
                        final UUID sampleId = specifiedSampleId.get();
                        return dtoSources.dbSearchProteinSourceBySample(proteinId, sampleId)
                            .filter(viewFilter::test)
                            .via(GenericDtoFlows.proteinSorterFlow())
                            .via(dtoSources.proteinPeptideWithDenovoCandidateFlow(specifiedSampleId.get(), topPsm));
                    } else {
                        final UUID proteinId = UUID.fromString(protein);
                        return dtoSources.dbSearchProteinSource(proteinId)
                            .filter(viewFilter::test)
                            .via(GenericDtoFlows.proteinSorterFlow())
                            .via(dtoSources.proteinPeptideFlow(topPsm));
                    }
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, query, request) -> ProteinPeptideCsv.newProteinPeptideCsv(applicationGraph),
            ProtoHandlerSpecification::allSamplesOrSpecified
        );

    }

    default ProtoHandlerSpecification samplePeptideHandler() {
        return ProtoHandlerSpecification.identificationQuery(
            "sample-peptide-frequency",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                try {
                    final String protein = request.get("protein");
                    final UUID proteinId = UUID.fromString(protein);
                    return dtoSources.samplePeptideFequence(proteinId);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification riqSamplePeptideHandler() {
        return ProtoHandlerSpecification.reporterIonQuery(
            "sample-peptide-frequency",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                RiqDtoSources dtoSources = dtoSourceFactory().riq(applicationGraph);
                try {
                    final String protein = request.get("protein");
                    final UUID proteinId = UUID.fromString(protein);
                    return dtoSources.riqChannelPeptideSums(proteinId);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification proteinHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (applicationGraph, request) -> exportWithSampleName("proteins", applicationGraph, request),
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                try {
                    final String protein = request.get("protein");
                    Predicate<Protein> viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinViewFilter.class)
                        .predicate();
                    Source<Protein, NotUsed> source;
                    if (protein != null) {
                        final UUID proteinId = UUID.fromString(protein);
                        source = dtoSources.dbSearchProteinSource(proteinId);
                    } else if (applicationGraph.specifiedSampleId().isPresent()) {
                        source = dtoSources.dbSearchProteinSourceBySample(applicationGraph.specifiedSampleId().get());
                    } else {
                        source = dtoSources.dbSearchProteinSource();
                    }

                    source = source.filter(viewFilter::test);
                    if (request.get("pruner") != null ) {
                        Function<Protein, Protein> pruner = new ProteinPrunerFactoryBuilder()
                            .pruner(pruningfilterList(request).getProteinPruner())
                            .addAllAllOrderAnalysisSampleIds(Lists.transform(applicationGraph.sampleFractions(), SampleFractions::sampleId))
                            .build()
                            .buildPruner();

                        source = source.map(pruner::apply);
                    }
                    return source
                        .via(GenericDtoFlows.proteinSorterFlow());
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, query, request) -> completedFuture(
                ProteinCsv.newProteinCsv(applicationGraph)
            ),
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default CsvProtoHandlerSpecification peptideHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (applicationGraph, request) -> {
                String file = topPsm(request) ? "peptides" : "psms";
                return exportWithSampleName(file, applicationGraph, request);
            },
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                try {
                    final boolean topPsm = topPsm(request);
                    PeptideViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, PeptideViewFilter.class);
                    DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                    final String peptide = request.get("peptide");
                    Source<Peptide, NotUsed> source;
                    if (peptide != null) {
                        source = dtoSources.dbSearchPeptideSource(peptide, topPsm);
                    } else if (applicationGraph.specifiedSampleId().isPresent()) {
                        final UUID sampleId = applicationGraph.specifiedSampleId().get();
                        source = dtoSources.dbSearchPeptideSource(sampleId, topPsm);
                    } else {
                        source = dtoSources.dbSearchPeptideSource(topPsm);
                    }

                    source = source.filter(viewFilter::predicate);
                    if (request.get("pruner") != null ) {
                        Function<Peptide, Peptide> pruner = new PeptidePrunerFactoryBuilder()
                            .pruner(pruningfilterList(request).getPeptidePruner())
                            .addAllAllOrderAnalysisSampleIds(Lists.transform(applicationGraph.sampleFractions(), SampleFractions::sampleId))
                            .build()
                            .buildPruner();

                        source = source.map(pruner::apply);
                    }

                    return source;
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, query, request) -> {
                if (topPsm(request)) {
                    return PeptideCsv.newPeptideCsv(applicationGraph);
                } else {
                    return PeptideCsv.newDbPsmCsv(applicationGraph);
                }
            },
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default CsvProtoHandlerSpecification ptmProfileHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (applicationGraph, request) -> "ptmprofile",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                try {
                    DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                    return dtoSources.ptmProfile();
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, query, request) -> CompletableFuture.completedFuture(PtmProfileCsv.newPtmProfileCsv(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification slPsmHandler() {
        return CsvProtoHandlerSpecification.slQuery(
            (graph, apiParameters) -> getWorkflowFileName(apiParameters, exportWithSampleName("psm", graph, apiParameters)),
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, api) -> {
                PeptideViewFilter viewFilter = ExportHelpers.getViewFilter(om(), api, PeptideViewFilter.class);
                SlDtoSources dtoSources = dtoSourceFactory().sl(graph);
                return dtoSources.slPsms().filter(viewFilter::predicate);
            },
            (graph, query, api) -> PeptideCsv.newSlPsmCsv(graph),
            ProtoHandlerSpecification::ignoreSample
        );
    }

    default ProtoHandlerSpecification slDiaOnTheFlyHandler() {
        return ProtoHandlerSpecification.slQuery(
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, api) -> {
                String onTheFlyStr = api.get("diaPeptideManualInput");
                DiaPeptideManualInput.Builder manualInputBuilder = DiaPeptideManualInput.newBuilder();
                DiaPeptideManualInput manualInput;
                try {
                    JsonFormat.parser()
                        .ignoringUnknownFields()
                        .merge(onTheFlyStr, manualInputBuilder);
                    manualInput = manualInputBuilder.build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                UUID fractionId = fractionId(api);

                SlDtoSources dtoSources = dtoSourceFactory().sl(graph);
                return dtoSources.manualSlPeptide(fractionId, manualInput);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default ProtoHandlerSpecification timsCcsDiaPeptideManualInput() {
        return ProtoHandlerSpecification.slQuery(
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, api) -> {
                String onTheFlyStr = api.get("diaPeptideManualInput");
                DiaPeptideManualInput.Builder manualInputBuilder = DiaPeptideManualInput.newBuilder();
                DiaPeptideManualInput manualInput;
                try {
                    JsonFormat.parser()
                        .ignoringUnknownFields()
                        .merge(onTheFlyStr, manualInputBuilder);
                    manualInput = manualInputBuilder.build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                UUID fractionId = fractionId(api);
                int ms2FrameId = Integer.parseInt(api.get("ms2FrameId"));

                SlDtoSources dtoSources = dtoSourceFactory().sl(graph);
                return dtoSources.timsScansManualPeptide(fractionId, manualInput, ms2FrameId);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default ProtoHandlerSpecification timsScanPeptideDisplayHandler() {
        return ProtoHandlerSpecification.slQuery(
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, api) -> {
                String peptideSeq = api.get("peptide");
                UUID psmId = UUID.fromString(api.get("psmId"));
                UUID fractionId = fractionId(api);
                int ms2FrameId = Integer.parseInt(api.get("ms2FrameId"));

                SlDtoSources dtoSources = dtoSourceFactory().sl(graph);
                return dtoSources.timsSlPeptideScanDisplay(fractionId, peptideSeq, psmId, ms2FrameId);
            },
            ProtoHandlerSpecification::ignoreSample
        );
    }

    default CsvProtoHandlerSpecification slProteinHandler() {
        return CsvProtoHandlerSpecification.slQuery(
                (applicationGraph, request) -> getWorkflowFileName(request, exportWithSampleName("proteins", applicationGraph, request)),
                applicationGraphFactory()::createFromSlQueryNoDecoy,
                (applicationGraph, query, request) -> {
                    SlDtoSources dtoSources = dtoSourceFactory().sl(applicationGraph);
                    final String sample = request.get(SAMPLE_ID_API_PARAMETERS);
                    Source<Protein, NotUsed> source;
                    if (sample != null) {
                        source = dtoSources.slProteinSource(UUID.fromString(sample));
                    } else {
                        source = dtoSources.slProteinSource();
                    }
                    try {
                        Predicate<Protein> viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinViewFilter.class)
                            .predicate();
                        if (request.get("pruner") != null ) {
                            Function<Protein, Protein> pruner = new ProteinPrunerFactoryBuilder()
                                    .pruner(pruningfilterList(request).getProteinPruner())
                                    .addAllAllOrderAnalysisSampleIds(Lists.transform(applicationGraph.sampleFractions(), SampleFractions::sampleId))
                                    .build()
                                    .buildPruner();

                            source = source.map(pruner::apply);
                        }

                        return source
                            .filter(viewFilter::test)
                            .via(GenericDtoFlows.proteinSorterFlow());
                    } catch (Exception e) {
                        return Source.failed(e);
                    }
                },
                (applicationGraph, query, request) -> completedFuture(
                    ProteinCsv.newProteinCsv(applicationGraph)
                ),
                ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default CsvProtoHandlerSpecification slPeptideHandler() {
        return CsvProtoHandlerSpecification.slQuery(
            (graph, apiParameters) -> getWorkflowFileName(apiParameters, exportWithSampleName("peptide", graph, apiParameters)),
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, api) -> {
                SlDtoSources dtoSources = dtoSourceFactory().sl(graph);
                PeptideViewFilter viewFilter = ExportHelpers.getViewFilter(om(), api, PeptideViewFilter.class);
                Source<Peptide, NotUsed> source = graph.specifiedSampleId()
                    .map(dtoSources::slPeptide) // with sampleId
                    .orElseGet(dtoSources::slPeptide); // withoutSampleId
                source = source.filter(viewFilter::predicate);
                if (api.get("pruner") != null ) {
                    Function<Peptide, Peptide> pruner = new PeptidePrunerFactoryBuilder()
                            .pruner(pruningfilterList(api).getPeptidePruner())
                            .addAllAllOrderAnalysisSampleIds(Lists.transform(graph.sampleFractions(), SampleFractions::sampleId))
                            .build()
                            .buildPruner();

                    source = source.map(pruner::apply);
                }
                return source;
            },
            (graph, query, api) -> PeptideCsv.newSlPeptideCsv(graph),
            ProtoHandlerSpecification::ignoreSample
        );
    }

    default CsvProtoHandlerSpecification slSummary() {
        return CsvProtoHandlerSpecification.slQuery(
                (graph, apiParameters) -> getWorkflowFileName(apiParameters, "summary"),
                applicationGraphFactory()::createFromSlQueryNoDecoy,
                (applicationGraph, query, request) ->
                    dtoSourceFactory().sl(applicationGraph)
                        .slSummary()
                        .orElse(Source.lazily(() ->
                                Source.failed(new IllegalStateException("Failed to create SL Summary Statistics.")))),
                (applicationGraph, query, request) -> completedFuture(IDSummaryCsv.newSLSummaryCsv(applicationGraph)),
                ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification slSampleScanStats() {
        return ProtoHandlerSpecification.slQuery(
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, api) -> dtoSourceFactory().sl(graph).sampleScanStats(),
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification slPeptideDisplay() {
        return ProtoHandlerSpecification.slQuery(
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, api) -> {
                String peptideSeq = api.get("peptide");
                UUID psmId = UUID.fromString(api.get("psmId"));
                UUID fId = fractionId(api);
                return dtoSourceFactory().sl(graph).slPeptideDisplay(fId, peptideSeq, psmId);
            },
            ProtoHandlerSpecification::ignoreSample
        );
    }

    default CsvProtoHandlerSpecification slProteinPeptideHandler() {
        return CsvProtoHandlerSpecification.slQuery(
            (graph, api) -> getWorkflowFileName(api, exportWithSampleName("supportPeptides", graph, api)),
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, api) -> {
                final String protein = api.get("protein");

                Predicate<Protein> viewFilter = ExportHelpers.getViewFilter(om(), api, ProteinViewFilter.class)
                    .predicate();
                final Optional<UUID> proteinId = Optional.ofNullable(protein).map(UUID::fromString);

                SlDtoSources dtoSources = dtoSourceFactory().sl(graph);
                Source<Protein, NotUsed> source;
                if (proteinId.isPresent()) {
                    source = dtoSources.slProteinSource(proteinId.get(), graph.specifiedSampleId());
                } else if (graph.specifiedSampleId().isPresent()) {
                    source = dtoSources.slProteinSource(graph.specifiedSampleId().get());
                } else {
                    source = dtoSources.slProteinSource();
                }
                return source
                    .filter(viewFilter::test)
                    .via(GenericDtoFlows.proteinSorterFlow())
                    .via(dtoSources.slProteinPeptidesFlow(graph.specifiedSampleId()));
            },
            (applicationGraph, query, request) -> ProteinPeptideCsv.newProteinPeptideCsv(applicationGraph),
            ProtoHandlerSpecification::ignoreSample
        );
    }

    default ProtoHandlerSpecification slPeptideMenu() {
        return ProtoHandlerSpecification.slQuery(
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (graph, query, request) -> {
                try {
                    SlDtoSources dtoSources = dtoSourceFactory().sl(graph);
                    final String peptide = request.get("peptide");
                    return graph.specifiedSampleId()
                        .map(sampleId -> dtoSources.slPeptideMenu(sampleId, peptide))
                        .orElseGet(() -> dtoSources.slPeptideMenu(peptide));
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::ignoreSample
        );
    }


    default ProtoHandlerSpecification slSamplePeptideHandler() {
        return ProtoHandlerSpecification.slQuery(
            applicationGraphFactory()::createFromSlQueryNoDecoy,
            (applicationGraph, query, request) -> {
                SlDtoSources dtoSources = dtoSourceFactory().sl(applicationGraph);
                try {
                    final String protein = request.get("protein");
                    final UUID proteinId = UUID.fromString(protein);
                    return dtoSources.samplePeptideFequence(proteinId);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification allProteinHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (applicationGraph, request) -> "proteins",
            applicationGraphFactory()::createFromIdentificationQueryIncludeDecoy,
            (applicationGraph, query, request) -> {
                DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                try {
                    return dtoSources.dbSearchProteinWithDecoySource();
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, query, request) -> completedFuture(
                ProteinCsv.newProteinCsv(applicationGraph, ProteinCsv.columnsWithDecoy())
            ),
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }


    default ProtoHandlerSpecification peptideMenuHandler() {
        return ProtoHandlerSpecification.identificationQuery(
            "peptidesMenu",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                try {
                    DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                    final String peptide = request.get("peptide");
                    return applicationGraph.specifiedSampleId()
                        .map(sampleId -> dtoSources.peptideMenu(sampleId, peptide))
                        .orElseGet(() -> dtoSources.peptideMenu(peptide));
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default ProtoHandlerSpecification riqPeptideMenuHandler() {
        return ProtoHandlerSpecification.reporterIonQuery(
            "riq.peptidesMenu",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                try {
                    RiqDtoSources dtoSources = dtoSourceFactory().riq(applicationGraph);
                    final String peptide = request.get("peptide");
                    return applicationGraph.specifiedSampleId()
                        .map(sampleId -> dtoSources.peptideMenu(sampleId, peptide))
                        .orElseGet(() -> dtoSources.peptideMenu(peptide));
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default CsvProtoHandlerSpecification denovoSummaryHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            "denovo.statistics",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> dtoSourceFactory().db(applicationGraph).denovoSummaries(),
            (graph) -> completedFuture(DenovoSummaryCsv.newIDSummaryCsv(graph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification denovoHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (applicationGraph, request) -> {
                String sampleName = applicationGraph.specifiedSampleId()
                    .map(sampleId -> applicationGraph.sample(sampleId).sample().concat("."))
                    .orElse("");
                return sampleName.concat("denovo");
            },
            applicationGraphFactory()::createFromIdentificationQueryTopDenovo,
            (applicationGraph, query, request) -> {
                try {
                    final UUID sampleId = applicationGraph.specifiedSampleId().get();
                    final DenovoViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, DenovoViewFilter.class);
                    return dtoSourceFactory().db(applicationGraph).denovoResults(sampleId).filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, ignore, request) -> {
                final double confidence = tagConfidence(request).orElse(0);
                return DenovoResultCsv.newDenovoResultCSV(applicationGraph, confidence);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default CsvProtoHandlerSpecification denovoAllHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (applicationGraph, request) -> {
                String sampleName = applicationGraph.specifiedSampleId()
                    .map(sampleId -> applicationGraph.sample(sampleId).sample().concat("."))
                    .orElse("");
                return sampleName.concat("denovoAllCandidates");
            },
            applicationGraphFactory()::createFromIdentificationQueryAllDenovo,
            (applicationGraph, query, request) -> {
                try {
                    final UUID sampleId = applicationGraph.specifiedSampleId().get();
                    final DenovoViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, DenovoViewFilter.class);
                    return dtoSourceFactory().db(applicationGraph).denovoResults(sampleId).filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, ignore, request) -> {
                final double confidence = tagConfidence(request).orElse(0);
                return DenovoResultCsv.newDenovoResultCSV(applicationGraph, confidence);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default ProtoHandlerSpecification denovoOnlyMenuHandler() {
        return ProtoHandlerSpecification.identificationQuery(
            "denovo-only.menu",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, qyery, request) -> {
                final DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                final UUID sampleId = applicationGraph.specifiedSampleId().get();
                final UUID proteinId = UUID.fromString(request.get("protein"));
                final int start = Integer.parseInt(request.get("start"));
                final int end = Integer.parseInt(request.get("end"));
                return dtoSources.supportDenovoOnlyTags(sampleId, proteinId, start, end);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default CsvProtoHandlerSpecification denovoOnlyHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (applicationGraph, request) -> {
                String sampleName = applicationGraph.specifiedSampleId()
                    .map(sampleId -> applicationGraph.sample(sampleId).sample().concat("."))
                    .orElse("");
                String searchType = request.getOrDefault("search", "WorkFlow Unknown");
                return searchType + "." + sampleName + "denovoOnly";
            },
            applicationGraphFactory()::createFromIdentificationQueryTopDenovo,
            (applicationGraph, query, request) -> {
                try {
                    final UUID sampleId = applicationGraph.specifiedSampleId().get();
                    final DenovoViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, DenovoViewFilter.class);
                    return dtoSourceFactory().db(applicationGraph).denovoOnlyResults(sampleId)
                        .filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, ignore, request) -> {
                final double confidence = tagConfidence(request).orElse(0);
                return DenovoResultCsv.newDenovoResultCSV(applicationGraph, confidence);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default CsvProtoHandlerSpecification denovoOnlyAllHandler() {
        return CsvProtoHandlerSpecification.identificationQuery(
            (applicationGraph, request) -> {
                String sampleName = applicationGraph.specifiedSampleId()
                    .map(sampleId -> applicationGraph.sample(sampleId).sample().concat("."))
                    .orElse("");
                String searchType = request.getOrDefault("search", "WorkFlow Unknown");
                return searchType + "." + sampleName + "denovoOnlyAllCandidates";
            },
            applicationGraphFactory()::createFromIdentificationQueryAllDenovo,
            (applicationGraph, query, request) -> {
                try {
                    final UUID sampleId = applicationGraph.specifiedSampleId().get();
                    final DenovoViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, DenovoViewFilter.class);
                    return dtoSourceFactory().db(applicationGraph).denovoOnlyResults(sampleId)
                        .filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, ignore, request) -> {
                final double confidence = tagConfidence(request).orElse(0);
                return DenovoResultCsv.newDenovoResultCSV(applicationGraph, confidence);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default ProtoHandlerSpecification peptideIonMatchHandler() {
        return ProtoHandlerSpecification.identificationQuery(
            "peptide.ionmatch",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, qyery, request) -> {
                final DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                final String peptide = request.get("peptide");
                if (peptide == null) {
                    return applicationGraph.peptides()
                        .flatMapMerge(4, dtoSources::ionMatchSource);
                } else {
                    return Source.fromCompletionStage(applicationGraph.peptide(peptide))
                        .flatMapConcat(dtoSources::ionMatchSource);
                }
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification denovoIonMatchHandler() {
        return ProtoHandlerSpecification.identificationQuery(
            "denovo.ionmatch",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                final UUID fractionId = fractionId(request);
                final int scannum = scannum(request);
                return dtoSourceFactory().db(applicationGraph).ionMatchSource(fractionId, scannum);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification psmIonMatchHandler() {
        return ProtoHandlerSpecification.identificationQuery(
            "psm.ionmatch",
            applicationGraphFactory()::createFromIdentificationQuery,
            (applicationGraph, query, request) -> {
                final UUID fractionId = fractionId(request);
                final DbDtoSources dtoSources = dtoSourceFactory().db(applicationGraph);
                UUID psmId = psmId(request).orElseThrow(() -> new HandlerException(400, "Missing PSM ID"));
                // To avoid edge case rounding/filter errors, return result unfiltered. This can be caused from proto changing float values. Can be avoided if we use double.
                return Source.fromCompletionStage(applicationGraph.fraction(fractionId).psmUnfiltered(psmId)) // TODO Should return 404 if empty
                    .flatMapConcat(dtoSources::ionMatchSource);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification diaDenovoPeptideDisplay() {
        return ProtoHandlerSpecification.identificationQuery(
            "denovo.psm.ionmatch",
            applicationGraphFactory()::createFromIdentificationQuery,
            (graph, query, api) -> {
                UUID sampleId = graph.specifiedSampleId().get();
                final int candidateId = Integer.parseInt(api.get("candidateId"));
                final int featureId = Integer.parseInt(api.get("featureId"));
                final int scanNum = Integer.parseInt(api.get("scanNum"));
                UUID fId = fractionId(api);
                IonType[] ionTypes = ionsTypes(api);
                return dtoSourceFactory().db(graph).diaDenovoPeptideDisplay(fId, sampleId, candidateId, scanNum, featureId, ionTypes);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default ProtoHandlerSpecification diaDenovoTimsScanPeptideDisplay() {
        return ProtoHandlerSpecification.identificationQuery(
            "denovo.psm.ionmatch",
            applicationGraphFactory()::createFromIdentificationQuery,
            (graph, query, api) -> {
                UUID sampleId = graph.specifiedSampleId().get();
                final int candidateId = Integer.parseInt(api.get("candidateId"));
                final int featureId = Integer.parseInt(api.get("featureId"));
                final int scanNum = Integer.parseInt(api.get("scanNum"));
                final int ms2FrameId = Integer.parseInt(api.get("ms2FrameId"));
                UUID fId = fractionId(api);
                return dtoSourceFactory().db(graph).diaTimsScanDenovoPeptideDisplay(fId, sampleId, candidateId, scanNum, featureId, ms2FrameId);
            },
            ProtoHandlerSpecification::specifiedSample
        );
    }

    default CsvProtoHandlerSpecification lfqMissingIds() {
        return CsvProtoHandlerSpecification.labelFreeQuery(
            "lfq.features",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    return dtoSources.internalAbbreviatedFeatureVector(query.labelFreeQueryParameters());
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, labelFreeQuery) -> CompletableFuture.completedFuture(InternalFeatureVectorCsv.create(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification lfqPsmIonMatchHandler() {
        return ProtoHandlerSpecification.labelFreeQuery(
            "psm.ionmatch",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                final UUID fractionId = fractionId(request);
                final LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                UUID psmId = psmId(request).orElseThrow(() -> new HandlerException(400, "Missing PSM ID"));
                // To avoid edge case rounding/filter errors, return result unfiltered. This can be caused from proto changing float values. Can be avoided if we use double.
                return dtoSources.ionMatchSource(fractionId, psmId);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification riqPsmIonMatchHandler() {
        return ProtoHandlerSpecification.reporterIonQuery(
            "psm.ionmatch",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                final UUID fractionId = fractionId(request);
                final RiqDtoSources dtoSources = dtoSourceFactory().riq(applicationGraph);
                UUID psmId = psmId(request).orElseThrow(() -> new HandlerException(400, "Missing PSM ID"));
                // To avoid edge case rounding/filter errors, return result unfiltered. This can be caused from proto changing float values. Can be avoided if we use double.
                return dtoSources.ionMatchSource(fractionId, psmId);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification silacPsmIonMatchHandler() {
        return ProtoHandlerSpecification.silacQuery(
            "psm.ionmatch",
            applicationGraphFactory()::createFromSilacQuery,
            (applicationGraph, query, request) -> {
                final UUID fractionId = fractionId(request);
                final SilacDtoSources dtoSources = dtoSourceFactory().silac(applicationGraph);
                UUID psmId = psmId(request).orElseThrow(() -> new HandlerException(400, "Missing PSM ID"));
                // To avoid edge case rounding/filter errors, return result unfiltered. This can be caused from proto changing float values. Can be avoided if we use double.
                return dtoSources.ionMatchSource(fractionId, psmId);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification lfqFeatureHandler() {
        return CsvProtoHandlerSpecification.labelFreeQuery(
            "lfq.features",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    return dtoSources.featureVectorSource(labelFreeQueryParameters(query.labelFreeQueryParameters(), applicationGraph));
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, labelFreeQuery) -> {
                final LabelFreeQueryParameters labelFreeQueryParameters = labelFreeQuery.labelFreeQueryParameters();
                return FeatureVectorCsv.newCsv(applicationGraph, labelFreeQueryParameters);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification lfqProteinHandler() {
        return CsvProtoHandlerSpecification.labelFreeQuery(
            "lfq.proteins",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    final ProteinFeatureVectorViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinFeatureVectorViewFilter.class);
                    return dtoSources.proteinFeatureVectorSource(labelFreeQueryParameters(query.labelFreeQueryParameters(), applicationGraph))
                        .filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, labelFreeQuery) -> {
                final LabelFreeQueryParameters labelFreeQueryParameters = labelFreeQuery.labelFreeQueryParameters();
                return ProteinFeatureVectorCsv.newProteinFeatureVectorCsv(applicationGraph, labelFreeQueryParameters);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification riqProteinHandler() {
        return CsvProtoHandlerSpecification.reportIonQuery(
            "riq.proteins",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                RiqDtoSources dtoSources = dtoSourceFactory().riq(applicationGraph);
                try {
                    final ProteinFeatureVectorViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinFeatureVectorViewFilter.class);
                    return applicationGraph.specifiedSampleId()
                        .map(dtoSources::riqProteinFeatureVectorSourceBySample)
                        .orElseGet(dtoSources::riqProteinFeatureVectorSource)
                        .filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, riqQuery, request) -> RiqProteinVectorCsv.riqProteinVectorCsv(applicationGraph),
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default CsvProtoHandlerSpecification silacPeptideMenu() {
        //TODO make non-csv
        return CsvProtoHandlerSpecification.silacQuery(
            "silac.peptide.menu",
            applicationGraphFactory()::createFromSilacQuery,
            (applicationGraph, query, request) -> {
                SilacDtoSources dtoSources = dtoSourceFactory().silac(applicationGraph);
                final String peptideSequence = request.get("peptide");
                try {
                    return dtoSources.featureVectorMenu(peptideSequence);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, labelFreeQuery) -> null,
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification silacProteinHandler() {
        return CsvProtoHandlerSpecification.silacQuery(
            "silac.proteins",
            applicationGraphFactory()::createFromSilacQuery,
            (applicationGraph, query, request) -> {
                SilacDtoSources dtoSources = dtoSourceFactory().silac(applicationGraph);
                try {
                    final SilacProteinViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, SilacProteinViewFilter.class);
                    return dtoSources.proteinFeatureVectorSource()
                        .filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, query) -> completedFuture(SilacProteinFeatureVectorCsv.allSamples(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification lfqProteinPeptidesHandler() {
        return CsvProtoHandlerSpecification.labelFreeQuery(
            "lfq.protein-peptides",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    final String protein = request.get("protein");
                    final ProteinFeatureVectorViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinFeatureVectorViewFilter.class);
                    final LabelFreeQueryParameters labelFreeQueryParameters = labelFreeQueryParameters(query.labelFreeQueryParameters(), applicationGraph);
                    if (protein == null) {
                        return dtoSources.supportPeptideFeatureVectorSource(labelFreeQueryParameters)
                            .filter(p -> viewFilter.predicate(p.getProteinFeatureVector()));
                    } else {
                        final UUID proteinId = UUID.fromString(protein);
                        return dtoSources.supportPeptideFeatureVectorSource(labelFreeQueryParameters, proteinId);
                    }
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, labelFreeQuery) -> {
                final LabelFreeQueryParameters labelFreeQueryParameters = labelFreeQuery.labelFreeQueryParameters();
                return ProteinPeptideFeatureVectorCsv.newProteinPeptideFeatureVectorCsv(applicationGraph, labelFreeQueryParameters);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification riqProteinPeptidesHandler() {
        return CsvProtoHandlerSpecification.reportIonQuery(
            "riq.protein-peptides",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                RiqDtoSources dtoSources = dtoSourceFactory().riq(applicationGraph);
                try {
                    final ProteinFeatureVectorViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinFeatureVectorViewFilter.class);
                    final String protein = request.get("protein");
                    if (protein == null) {
                        return applicationGraph.specifiedSampleId()
                            .map(dtoSources::riqSupportPeptideFeatureVectorSourceBySample)
                            .orElseGet(dtoSources::riqSupportPeptideFeatureVectorSource)
                            .filter(p -> viewFilter.predicate(p.getProteinFeatureVector()));
                    } else {
                        final UUID proteinId = UUID.fromString(protein);
                        return applicationGraph.specifiedSampleId()
                            .map(sampleId -> dtoSources.riqSupportPeptideFeatureVectorSourceBySample(sampleId, proteinId))
                            .orElseGet(() -> dtoSources.riqSupportPeptideFeatureVectorSource(proteinId));
                    }
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, riqQuery, request) -> {
                final String protein = request.get("protein");
                if (protein != null) {
                    return null;
                }
                return CompletableFuture.completedFuture(RiqProteinPeptideCsv.create(applicationGraph));
            },
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default CsvProtoHandlerSpecification<SilacProteinPeptideFeatureVector, SilacQuery, SilacApplicationGraph> silacProteinPeptidesHandler() {
        return CsvProtoHandlerSpecification.silacQuery(
            "silac.protein-peptides",
            applicationGraphFactory()::createFromSilacQuery,
            (applicationGraph, query, request) -> {
                SilacDtoSources dtoSources = dtoSourceFactory().silac(applicationGraph);
                try {
                    final String protein = request.get("protein");
                    final SilacProteinViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, SilacProteinViewFilter.class);
                    if (protein == null) {
                        return dtoSources.supportPeptideFeatureVector()
                            .filter(p -> viewFilter.predicate(p.getProteinFeatureVector()));
                    } else {
                        final UUID proteinId = UUID.fromString(protein);
                        return dtoSources.supportPeptideFeatureVector(proteinId);
                    }
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, query) -> completedFuture(SilacProteinPeptideFeactureVectorCsv.allSamples(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification lfqProteinWithPeptidesHandler() {
        return CsvProtoHandlerSpecification.labelFreeQuery(
            "lfq.protein-with-peptides",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    final ProteinFeatureVectorViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinFeatureVectorViewFilter.class);
                    final LabelFreeQueryParameters labelFreeQueryParameters = labelFreeQueryParameters(query.labelFreeQueryParameters(), applicationGraph);
                    return dtoSources.supportPeptideFeatureVectorSource(labelFreeQueryParameters)
                        .filter(p -> viewFilter.predicate(p.getProteinFeatureVector()));
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, labelFreeQuery) -> {
                final LabelFreeQueryParameters labelFreeQueryParameters = labelFreeQuery.labelFreeQueryParameters();
                return ProteinWithPeptideFeatureVectorCsv.newProteinWithPeptideFeatureVectorCsv(applicationGraph, labelFreeQueryParameters);
            },
            ProtoHandlerSpecification::allSamples
        );
    }


    default CsvProtoHandlerSpecification lfqPeptideHandler() {
        return CsvProtoHandlerSpecification.labelFreeQuery(
            "lfq.peptides",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    PeptideFeatureVectorViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, PeptideFeatureVectorViewFilter.class);
                    return dtoSources.peptideFeatureVectorSource(labelFreeQueryParameters(query.labelFreeQueryParameters(), applicationGraph))
                        .filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, labelFreeQuery) -> {
                final LabelFreeQueryParameters labelFreeQueryParameters = labelFreeQuery.labelFreeQueryParameters();
                return PeptideFeatureVectorCsv.newPeptideFeatureVectorCsv(applicationGraph, labelFreeQueryParameters);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification riqPeptideHandler() {
        return CsvProtoHandlerSpecification.reportIonQuery(
            "riq.peptides",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                try {
                    RiqPeptideViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, RiqPeptideViewFilter.class);
                    final RiqDtoSources dtoSources = dtoSourceFactory().riq(applicationGraph);
                    return applicationGraph.specifiedSampleId()
                        .map(dtoSources::riqPeptideFeatureVectorSource)
                        .orElseGet(dtoSources::riqPeptideFeatureVectorSource)
                        .filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, riqQuery, request) -> {
                final String sample = request.get("sample");
                if (sample != null) {
                    final UUID sampleId = UUID.fromString(sample);
                    return completedFuture(RiqPeptideVectorCsv.peptideSingleSample(applicationGraph, sampleId));
                } else {
                    return completedFuture(RiqPeptideVectorCsv.peptideAllSamples(applicationGraph));
                }
            },
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default CsvProtoHandlerSpecification silacPeptideHandler() {
        return CsvProtoHandlerSpecification.silacQuery(
            "silac.peptides",
            applicationGraphFactory()::createFromSilacQuery,
            (applicationGraph, query, request) -> {
                SilacDtoSources dtoSources = dtoSourceFactory().silac(applicationGraph);
                try {
                    SilacPeptideViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, SilacPeptideViewFilter.class);
                    return dtoSources.peptideFeatureVectorSource()
                        .filter(viewFilter::predicate);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, query) -> completedFuture(SilacPeptideFeatureVectorCsv.allSamples(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification silacFeatureVectorHandler() {
        return CsvProtoHandlerSpecification.silacQuery(
            (graph, param) -> exportWithSampleName("silac.featurevectors", graph, param),
            applicationGraphFactory()::createFromSilacQuery,
            (applicationGraph, query, request) -> {
                final UUID sampleId = UUID.fromString(request.get("sample"));
                return dtoSourceFactory().silac(applicationGraph).featureVector(sampleId);
            },
            (applicationGraph, query) -> completedFuture(SilacFeatureVectorDtoCsv.allSamples(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification riqPsmHandler() {
        return CsvProtoHandlerSpecification.reportIonQuery(
            "riq.psm",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                try {
                    RiqPeptideViewFilter peptideViewFilter = ExportHelpers.getViewFilter(om(), request, RiqPeptideViewFilter.class);
                    RiqPsmViewFilter viewFilter = new RiqPsmViewFilterBuilder()
                        .peptideViewFilter(peptideViewFilter)
                        .build();
                    final RiqDtoSources dtoSources = dtoSourceFactory().riq(applicationGraph);
                    final Source<RiqPeptideVectors, NotUsed> source;
                    return applicationGraph.specifiedSampleId()
                        .map(dtoSources::riqPsmFeatureVectorSource)
                        .orElseGet(dtoSources::riqPsmFeatureVectorSource)
                        .filter(viewFilter::predicate)
                        .mapConcat(RiqPeptideVectors::getRiqPeptideVectorsList);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            (applicationGraph, riqQuery, request) -> completedFuture(RiqPsmVectorCsv.psm(applicationGraph)),
            ProtoHandlerSpecification::allSamplesOrSpecified
        );
    }

    default ProtoHandlerSpecification lfqPeptideFeatureHandler() {
        return ProtoHandlerSpecification.labelFreeQuery(
            "lfq.peptide-feature",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    final String featureId = request.get("featureId");
                    final boolean alignRT = request.get("alignRT") != null;
                    return dtoSources.peptideFeatureSource(labelFreeQueryParameters(query.labelFreeQueryParameters(), applicationGraph), featureId, alignRT);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification lfqSlPeptideFeatureDisplayHandler() {
        return ProtoHandlerSpecification.labelFreeQuery(
            "lfq.peptide-feature",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    UUID fvId = UUID.fromString(request.get("featureId"));
                    return dtoSources.slLfqPeptideDisplay(fvId);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification lfqSlPeptideFvDisplay() {
        return ProtoHandlerSpecification.labelFreeQuery(
            "lfq.peptide-feature-display",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    UUID fvId = UUID.fromString(request.get("featureVectorId"));
                    final boolean alignRT = Boolean.parseBoolean(request.get("alignRT"));
                    return dtoSources.slLfqPeptideXicDisplay(labelFreeQueryParameters(query.labelFreeQueryParameters(), applicationGraph), fvId, alignRT);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification lfqSlTimsScanPeptideFeatureDisplayHandler() {
        return ProtoHandlerSpecification.labelFreeQuery(
            "lfq.peptide-feature",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    UUID fvId = UUID.fromString(request.get("featureId"));
                    int ms2FrameId = Integer.parseInt(request.get("ms2FrameId"));
                    return dtoSources.slLfqTimsScanPeptideDisplay(fvId, ms2FrameId);
                } catch (Exception e) {
                    return Source.failed(e);
                }
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification lfqFeatureVectorMenuHandler() {
        return ProtoHandlerSpecification.labelFreeQuery(
            "lfq.feature-vector-menu",
            applicationGraphFactory()::createFromLabelFreeQuery,
            ((applicationGraph, labelFreeQuery, request) -> {
                LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                try {
                    final String peptide = request.get("peptide");
                    return dtoSources.featureVectorMenu(peptide, labelFreeQueryParameters(labelFreeQuery.labelFreeQueryParameters(), applicationGraph));
                } catch (Exception e) {
                    return Source.failed(e);
                }
            }),
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification lfqProteinFoldChange() {
        return ProtoHandlerSpecification.labelFreeQuery(
            "lfq.proteinFoldChanges",
            (graphTaskIds, query, paramaters) -> {
                final LabelFreeQuery modifiedQuery = new LabelFreeQueryBuilder()
                    .from(query)
                    .proteinFeatureVectorFilter(new ProteinFeatureVectorFilterBuilder()
                        .from(ProteinFeatureVectorFilter.PASS_ALL_VALID_USE_FDR)
                        .useSignificance(query.proteinFeatureVectorFilter().useSignificance())
                        .build()
                    )
                    .build();
                return applicationGraphFactory().createFromLabelFreeQuery(graphTaskIds, modifiedQuery, paramaters);
            },
            (applicationGraph, query, request) -> {
                final LfqDtoSources dtoSources = dtoSourceFactory().lfq(applicationGraph);
                return dtoSources.proteinFoldChangeSource(labelFreeQueryParameters(query.labelFreeQueryParameters(), applicationGraph), query.proteinFeatureVectorFilter()::predicate);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification lfqNormalizationFactorHandler() {
        return CsvProtoHandlerSpecification.labelFreeQuery(
            "lfq.normalization-factors",
            applicationGraphFactory()::createFromLabelFreeQuery,
            (applicationGraph, query, request) -> dtoSourceFactory().lfq(applicationGraph).lfqNormalizationFactors(),
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification riqProteinFoldChange() {
        return ProtoHandlerSpecification.reporterIonQuery(
            "riq.proteinFoldChanges",
            (graphTaskIds, query, paramaters) -> {
                final ReporterIonQMethodQuery modifiedQuery = new ReporterIonQMethodQueryBuilder()
                    .from(query)
                    .proteinVectorFilter(RiqProteinVectorFilter.PASS_ALL_FILTER)
                    .build();
                return applicationGraphFactory().createFromReporterIonQMethodQuery(graphTaskIds, modifiedQuery, paramaters);
            },
            (applicationGraph, query, request) -> {
                return dtoSourceFactory().riq(applicationGraph).riqProteinFoldChangeSource(query.proteinVectorFilter()::predicate);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification riqRawScanHandler() {
        return ProtoHandlerSpecification.reporterIonQuery(
            "rawscan",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                final UUID fractionId = UUID.fromString(request.get("fid"));
                final int scannum = Integer.parseInt(request.get("scannum"));
                return dtoSourceFactory().riq(applicationGraph)
                    .riqRawScan(fractionId, scannum);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default ProtoHandlerSpecification riqScanReporterIonQResultsHandler() {
        return ProtoHandlerSpecification.reporterIonQuery(
            "riq.scan-ion-results",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> {
                final UUID fractionId = UUID.fromString(request.get("fid"));
                final int scannum = Integer.parseInt(request.get("scannum"));
                return dtoSourceFactory().riq(applicationGraph)
                    .reporterIonQResultByRefinedScannum(fractionId, scannum);
            },
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification riqNormalizationFactorHandler() {
        return CsvProtoHandlerSpecification.reportIonQuery(
            "riq.normalization-factors",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (applicationGraph, query, request) -> dtoSourceFactory().riq(applicationGraph).riqNormalizationFactors(),
            (applicationGraph, riqQuery, request) -> CompletableFuture.completedFuture(new RiqNormalizationCsv(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default CsvProtoHandlerSpecification riqFilterHandler() {
        return CsvProtoHandlerSpecification.reportIonQuery(
            "riq.filters",
            applicationGraphFactory()::createFromReporterIonQMethodQuery,
            (graph, query, request) -> Source.single(ReporterIonQStep.newBuilder()
                .setReporterIonQParameters(graph.reporterIonQParameters())
                .setFilterSummarization(graph.reporterIonQSummaryFilterTaskParameters())
                .build()
            ),
            (applicationGraph, riqQuery, request) -> CompletableFuture.completedFuture(RiqSummaryCsv.create(applicationGraph)),
            ProtoHandlerSpecification::allSamples
        );
    }

    default Optional<UUID> psmId(Map<String, String> request) {
        return Optional.ofNullable(request.get("psmId")).map(UUID::fromString);
    }

    default int scannum(Map<String, String> request) {
        return Integer.parseInt(request.get("scannum"));
    }

    default OptionalDouble tagConfidence(Map<String, String> request) {
        final String tagConfidence = request.get("confidence");
        if (tagConfidence == null) {
            return OptionalDouble.empty();
        } else {
            return OptionalDouble.of(Double.parseDouble(tagConfidence));
        }
    }

    default UUID fractionId(Map<String, String> request) {
        String fraction = request.get("fractionId");
        return fraction == null ? null : UUID.fromString(fraction);
    }

    default UUID sample(Map<String, String> request) {
        String sample = request.get("sample");
        return sample == null ? null : UUID.fromString(sample);
    }

    default IonType[] ionsTypes(Map<String, String> request) {
        String ions = request.get("ions");
        if (ions == null || ions.isEmpty()) return new IonType[0];

        IonType[] ret = Arrays.stream(ions.substring(1, ions.length() - 1).split(","))
            .map(String::trim)
            .mapToInt(Integer::parseInt)
            .mapToObj(IonType::forNumber)
            .toArray(IonType[]::new);
        return ret;
    }

    default boolean topPsm(Map<String, String> request) {
        String topPsm = request.get("topPsm");
        if (topPsm == null) {
            return false;
        }
        switch (Integer.parseInt(topPsm)) {
            case 1:
                return true;
            default:
                throw new IllegalArgumentException("Does not support top psm other than 1");
        }
    }

    default PruningFilterList pruningfilterList(Map<String, String> request) {
        String query = request.get("pruner");
        try {
            if (query == null) {
                query = "{}"; //empty object use default values
            }

            PruningFilterList.Builder builder = PruningFilterList.newBuilder();
            JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(query, builder);
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    default String exportWithSampleName(String defaultName, AnalysisInfoApplicationGraph applicationGraph, Map<String, String> request) {
        if (request.containsKey(SAMPLE_ID_API_PARAMETERS)) {
            UUID sampleId = UUID.fromString(request.get(SAMPLE_ID_API_PARAMETERS));
            return applicationGraph.sampleFraction(sampleId).name() + "." + defaultName;
        }
        return defaultName;
    }

    default String getWorkflowFileName(Map<String, String> apiParameters, String fileName) {
        String searchType = apiParameters.getOrDefault("search", "WorkFlow Unknown");
        return searchType + "." + fileName;
    }

    default LabelFreeQueryParameters labelFreeQueryParameters(
        LabelFreeQueryParameters labelFreeQueryParameters,
        LfqApplicationGraph applicationGraph
    ) {
        QuantificationNormalizationGraph normalizationGraph = applicationGraph.quantificationNormalizationGraph();
        final List<Float> normsAreas = normalizationGraph.lfqNormalizationFactors().stream()
            .map(p -> p.second()).collect(Collectors.toList());
        float[] normAreas = Floats.toArray(normsAreas);

        List<LabelFreeGroup> groups = labelFreeQueryParameters.groups();

        return new LabelFreeQueryParametersBuilder()
            .from(labelFreeQueryParameters)
            .normalizationFactors(normAreas)
            .groups(groups)
            .build();
    }
}
