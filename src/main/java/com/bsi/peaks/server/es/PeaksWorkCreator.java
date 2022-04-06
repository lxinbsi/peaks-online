package com.bsi.peaks.server.es;

import akka.japi.Pair;
import com.bsi.peaks.algorithm.basic.protein.Enzyme;
import com.bsi.peaks.algorithm.module.peaksdb.decoy.DecoyInfo;
import com.bsi.peaks.algorithm.module.peaksdb.decoy.ProteinDecoyInfo;
import com.bsi.peaks.analysis.parameterModels.dto.SilacParameters;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.CommonTasksFactory;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.TaskParameters;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.internal.task.DenovoOnlyTagSearchTaskParameters;
import com.bsi.peaks.internal.task.DenovoSummarizeTaskParameters;
import com.bsi.peaks.internal.task.DiaDataRefineParameters;
import com.bsi.peaks.internal.task.DiaDataRefineTaskParameters;
import com.bsi.peaks.internal.task.DiaDbPreSearchParameters;
import com.bsi.peaks.internal.task.DiaDbSearchParameters;
import com.bsi.peaks.internal.task.DiaDbSearchSharedParameters;
import com.bsi.peaks.internal.task.DiaDbSearchTaskParameters;
import com.bsi.peaks.internal.task.DiaFeatureExtractorParameters;
import com.bsi.peaks.internal.task.DiaFeatureExtractorTaskParameters;
import com.bsi.peaks.internal.task.DiaLfqSummarizerTaskParameters;
import com.bsi.peaks.internal.task.DiaPreSearchTaskParameters;
import com.bsi.peaks.internal.task.DiaRetentionTimeAlignmentTaskParameters;
import com.bsi.peaks.internal.task.FDBProteinCandidateTaskParameters;
import com.bsi.peaks.internal.task.IdentificaitonFilter;
import com.bsi.peaks.internal.task.IdentificationSummaryFilterTaskParameters;
import com.bsi.peaks.internal.task.LfqSummaryFilterParameters;
import com.bsi.peaks.internal.task.PtmFinderSearchParameters;
import com.bsi.peaks.internal.task.PtmFinderSearchTaskParameters;
import com.bsi.peaks.internal.task.PtmFinderSummarizeTaskParameters;
import com.bsi.peaks.internal.task.ReporterIonQBatchCalculationParameters;
import com.bsi.peaks.internal.task.ReporterIonQBatchCalculationTaskParameters;
import com.bsi.peaks.internal.task.ReporterIonQNormalizationTaskParameters;
import com.bsi.peaks.internal.task.ReporterIonQSummaryFilterTaskParameters;
import com.bsi.peaks.internal.task.SilacConservedParameters;
import com.bsi.peaks.internal.task.SilacFeatureAlignmentParameters;
import com.bsi.peaks.internal.task.SilacFeatureAlignmentTaskParameters;
import com.bsi.peaks.internal.task.SilacFilterSummarizationParameters;
import com.bsi.peaks.internal.task.SilacFilterSummarizationTaskParamaters;
import com.bsi.peaks.internal.task.SilacRTAlignmentTaskParameters;
import com.bsi.peaks.internal.task.SilacRtAlignmentParameters;
import com.bsi.peaks.internal.task.SilacSearchParameters;
import com.bsi.peaks.internal.task.SilacSearchTaskParameters;
import com.bsi.peaks.internal.task.SilacSummarizationConservedParameters;
import com.bsi.peaks.internal.task.SilacSummarizeParameters;
import com.bsi.peaks.internal.task.SilacSummarizeTaskParameters;
import com.bsi.peaks.internal.task.SlFilterSummarizationParameters;
import com.bsi.peaks.internal.task.SlFilterSummarizationTaskParameters;
import com.bsi.peaks.internal.task.SlPeptideSummarizationParameters;
import com.bsi.peaks.internal.task.SlPeptideSummarizationTaskParameters;
import com.bsi.peaks.internal.task.SlSearchParameters;
import com.bsi.peaks.internal.task.SlSearchTaskParameters;
import com.bsi.peaks.io.writer.dto.DtoConversion;
import com.bsi.peaks.messages.service.SlFilterSummarizationResult;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksParameters;
import com.bsi.peaks.model.core.ms.fraction.FractionAttributes;
import com.bsi.peaks.model.core.quantification.labelfree.LabelFreeGroupBuilder;
import com.bsi.peaks.model.dto.FdrType;
import com.bsi.peaks.model.dto.IDFilter;
import com.bsi.peaks.model.dto.LfqFilter;
import com.bsi.peaks.model.dto.NormalizationMethodType;
import com.bsi.peaks.model.dto.ReporterIonQExperimentSettings;
import com.bsi.peaks.model.dto.ReporterIonQFilterSummarization;
import com.bsi.peaks.model.dto.ReporterIonQSpectrumFilter;
import com.bsi.peaks.model.dto.SlFilter;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.filter.PeptideFilter;
import com.bsi.peaks.model.filter.ProteinHitFilter;
import com.bsi.peaks.model.filter.PsmFilter;
import com.bsi.peaks.model.filter.SlPeptideFilter;
import com.bsi.peaks.model.filter.SlProteinHitFilter;
import com.bsi.peaks.model.parameters.DBSearchConservedParameters;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.model.parameters.DBSearchParametersBuilder;
import com.bsi.peaks.model.parameters.DataRefineParameters;
import com.bsi.peaks.model.parameters.DenovoParameters;
import com.bsi.peaks.model.parameters.DenovoParametersBuilder;
import com.bsi.peaks.model.parameters.FeatureDetectionParameters;
import com.bsi.peaks.model.parameters.IDBaseParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationConservedParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParametersBuilder;
import com.bsi.peaks.model.parameters.PtmFinderConservedParameters;
import com.bsi.peaks.model.parameters.PtmFinderParameters;
import com.bsi.peaks.model.parameters.RetentionTimeAlignmentParameters;
import com.bsi.peaks.model.parameters.SpiderParameters;
import com.bsi.peaks.model.parameters.TagSearchParameters;
import com.bsi.peaks.model.parameters.TagSearchParametersBuilder;
import com.bsi.peaks.model.proto.AcquisitionMethod;
import com.bsi.peaks.model.system.OrderedSampleFractions;
import com.bsi.peaks.model.system.OrderedSampleFractionsList;
import com.bsi.peaks.model.system.Task;
import com.bsi.peaks.server.es.state.AnalysisTaskInformation;
import com.bsi.peaks.server.es.tasks.Helper;
import com.bsi.peaks.server.parsers.ParametersPopulater;
import com.bsi.peaks.server.parsers.ParserException;
import com.bsi.peaks.service.common.DecoyInfoWrapper;
import com.bsi.peaks.service.common.EnzymeAdaptor;
import com.bsi.peaks.service.messages.tasks.DBPreSearchTaskBuilder;
import com.bsi.peaks.service.messages.tasks.DBSearchSummarizeTask;
import com.bsi.peaks.service.messages.tasks.DBSearchSummarizeTaskBuilder;
import com.bsi.peaks.service.messages.tasks.DBSearchTaskBuilder;
import com.bsi.peaks.service.messages.tasks.DataRefineTaskBuilder;
import com.bsi.peaks.service.messages.tasks.DenovoTaskBuilder;
import com.bsi.peaks.service.messages.tasks.FeatureAlignmentTaskBuilder;
import com.bsi.peaks.service.messages.tasks.FeatureDetectionTaskBuilder;
import com.bsi.peaks.service.messages.tasks.LabelFreeSummarizeTaskBuilder;
import com.bsi.peaks.service.messages.tasks.RetentionTimeAlignmentTaskBuilder;
import com.bsi.peaks.service.messages.tasks.SpiderSummarizeTask;
import com.bsi.peaks.service.messages.tasks.SpiderSummarizeTaskBuilder;
import com.bsi.peaks.service.messages.tasks.SpiderTaskBuilder;
import com.bsi.peaks.service.messages.tasks.TagSearchSummarizeTaskBuilder;
import com.bsi.peaks.service.messages.tasks.TagSearchTaskBuilder;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbPreSearchService;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbSearchService;
import com.bsi.peaks.service.services.dbsearch.DbIdentificationSummaryFilterService;
import com.bsi.peaks.service.services.dbsearch.DeNovoOnlyTagSearchService;
import com.bsi.peaks.service.services.dbsearch.FDBProteinCandidateFromFastaService;
import com.bsi.peaks.service.services.denovo.DenovoSummarizeService;
import com.bsi.peaks.service.services.labelfreequantification.LfqSummaryFilterService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaFeatureExtractorService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaLfqSummarizeService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaRetentionTimeAlignmentService;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQNormalizationService;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQService;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQSummarizeService;
import com.bsi.peaks.service.services.preprocess.dia.DiaDataRefineService;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderService;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderSummarizeService;
import com.bsi.peaks.service.services.silac.SilacFeatureAlignService;
import com.bsi.peaks.service.services.silac.SilacFeatureVectorSearchService;
import com.bsi.peaks.service.services.silac.SilacFilterSummarizationService;
import com.bsi.peaks.service.services.silac.SilacPeptideSummarizationService;
import com.bsi.peaks.service.services.silac.SilacRtAlignmentService;
import com.bsi.peaks.service.services.spectralLibrary.SlFilterSummarizationService;
import com.bsi.peaks.service.services.spectralLibrary.SlPeptideSummarizationService;
import com.bsi.peaks.service.services.spectralLibrary.SlSearchService;
import com.bsi.peaks.service.services.spectralLibrary.dda.DdaSlSearchService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.google.protobuf.util.JsonFormat;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.bsi.peaks.event.CommonTasksFactory.taskDescriptionAnalysisDataIndependentLevel;
import static com.bsi.peaks.event.CommonTasksFactory.taskDescriptionAnalysisLevel;
import static com.bsi.peaks.event.CommonTasksFactory.taskDescriptionFractionLevel;
import static com.bsi.peaks.event.CommonTasksFactory.taskDescriptionIndexFractionLevel;
import static com.bsi.peaks.event.CommonTasksFactory.taskDescriptionSampleLevel;
import static com.bsi.peaks.model.ModelConversion.uuidToByteString;
import static com.bsi.peaks.model.ModelConversion.uuidsToByteString;
import static com.bsi.peaks.model.dto.StepType.DB_FILTER_SUMMARIZATION;
import static com.bsi.peaks.model.dto.StepType.DIA_DATA_REFINE;
import static com.bsi.peaks.model.dto.StepType.DIA_DB_FILTER_SUMMARIZE;
import static com.bsi.peaks.model.dto.StepType.DIA_DB_PRE_SEARCH;
import static com.bsi.peaks.model.dto.StepType.DIA_DB_SEARCH;
import static com.bsi.peaks.model.dto.StepType.DIA_LFQ_FILTER_SUMMARIZATION;
import static com.bsi.peaks.model.dto.StepType.SILAC_FILTER_SUMMARIZE;
import static com.bsi.peaks.model.dto.StepType.SL_FILTER_SUMMARIZATION;

public class PeaksWorkCreator implements AnalysisWorkCreator {

    private static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new ProtobufModule())
        .registerModule(new Jdk8Module());

    private static final Logger LOG = LoggerFactory.getLogger(PeaksWorkCreator.class);

    private final Config localconfig;
    private final ApplicationStorageFactory storageFactory;
    private final ParametersPopulater parametersPopulater;
    private final LoadingCache<FDRKey, Float> fdrCache;
    @Inject
    public PeaksWorkCreator(
        Config localconfig,
        ApplicationStorageFactory storageFactory,
        ParametersPopulater parametersPopulater
    ) {
        this.localconfig = localconfig;
        this.storageFactory = storageFactory;
        this.parametersPopulater = parametersPopulater;
        this.fdrCache = CacheBuilder.newBuilder()
            .softValues()
            .maximumSize(100)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(CacheLoader.from(this::calculatefdrToPValue));
    }

    private static class FDRKey {
        String keyspace;
        UUID searchSummarizeTaskId;
        float fdrThreshold;
        FdrType fdrType;
        boolean isDIA;

        @Override
        public boolean equals(Object o) {
            if (o==null) return false;
            if (o==this) return true;
            if (!(o instanceof FDRKey)) return false;
            FDRKey key = (FDRKey) o;
            return key.keyspace.equals(this.keyspace) && key.searchSummarizeTaskId.equals(this.searchSummarizeTaskId)
                && key.fdrThreshold ==this.fdrThreshold && key.isDIA == this.isDIA && key.fdrType.equals(this.fdrType);
        }

        @Override
        public int hashCode() {
            int result;
            result = keyspace.hashCode();
            result = 31 * result + searchSummarizeTaskId.hashCode();
            result = 31 * result + fdrType.hashCode();
            result = 31 * result + Float.hashCode(fdrThreshold);
            result = 31 * result + Boolean.hashCode(isDIA);
            return result;

        }
    }

    @Override
    public List<TaskDescription> workFrom(AnalysisTaskInformation analysis) {
        try {
            final UUID projectId = analysis.projectId();
            final List<OrderedSampleFractions> samples = analysis.samples();

            final Map<StepType, Object> stepParameters = analysis.stepParameters();
            final boolean hasPtm = analysis.hasPtm();
            final boolean hasSpider = analysis.hasSpider();
            final boolean hasDb = analysis.hasDb();
            final boolean hasDenovo = analysis.hasDenovo();
            final boolean hasIdentification = hasPtm || hasSpider || hasDb;
            final boolean hasFastDb = hasDb && !hasDenovo;
            final boolean hasSlSearchStep = analysis.hasSlSearch();
            final boolean hasDiaDb = analysis.hasDiaDb();
            final boolean hasDiaDenovo = analysis.hasDiaDenovo();
            final boolean hasSlProteinInference = analysis.hasSlProteinInference();
            final AcquisitionMethod acquisitionMethod = hasSlSearchStep || hasDiaDb || hasDiaDenovo ?
                AcquisitionMethod.DIA : AcquisitionMethod.DDA;


            final List<UUID> analysisSampleIds = samples.stream()
                .map(OrderedSampleFractions::sampleId)
                .collect(Collectors.toList());

            final List<UUID> analysisFractionIds = samples.stream()
                .flatMap(sample -> sample.fractionIds().stream())
                .collect(Collectors.toList());

            final Map<UUID, UUID> fractionIdToSampleIdMap = samples.stream()
                .flatMap(sample -> sample.fractionIds().stream().map(fId -> Pair.create(fId, sample.sampleId())))
                .collect(Collectors.toMap(Pair::first, Pair::second));

            final Map<UUID, UUID> dataLoadingStepFractionIdToTaskId = analysis.dataLoadingStepFractionIdToTaskId();
            final Map<UUID, UUID> dataRefineStepFractionIdToTaskId = analysis.dataRefineStepFractionIdToTaskId();
            final Map<UUID, UUID> denovoStepFractionIdToTaskId = analysis.denovoStepFractionIdToTaskId();
            final Map<UUID, UUID> tagSearchStepFractionIdToTaskId = analysis.tagSearchStepFractionIdToTaskId();
            final Map<UUID, UUID> tagSummarizeStepSampleIdToTaskId = analysis.tagSummarizeStepSampleIdToTaskId();
            final Map<UUID, UUID> featureDetectionStepFractionIdToTaskId = analysis.featureDetectionStepFractionIdToTaskId();
            final Map<UUID, UUID> dbPreSearchStepFractionIdToTaskId = analysis.dbPreSearchStepFractionIdToTaskId();
            final Map<UUID, UUID> dbSearchStepFractionIdToTaskId = analysis.dbSearchStepFractionIdToTaskId();
            final Map<UUID, UUID> ptmSearchStepFractionIdToTaskId = analysis.ptmSearchStepFractionIdToTaskId();
            final Map<UUID, UUID> spiderSearchStepFractionIdToTaskId = analysis.spiderSearchStepFractionIdToTaskId();
            final Map<UUID, UUID> reporterIonQStepFractionIdToTaskId = analysis.reporterIonQStepFractionIdToTaskId();
            final Map<Integer, UUID> retentionTimeAlignmentFractionIndexToTaskId = analysis.retentionTimeAlignmentFractionIndexToTaskId();
            final Map<Integer, UUID> diaLfqRtAlignmentFractionIndexToTaskId = analysis.diaLfqRtAlignmentFractionIndexToTaskId();
            final Map<UUID, UUID> diaFeatureExtractorFractionIndexToTaskId = analysis.diaFeatureExtractorFractionIndexToTaskId();
            final Map<UUID, UUID> featureAlignmentFractionIdToTaskId = analysis.featureAlignmentFractionIndexToTaskId();
            final Map<UUID, UUID> reporterIonQNormalizationSampleIdToTaskId = analysis.reporterIonQNormalizationSampleIdToTaskId();
            final Map<Integer, UUID> lfqSummarizeFractionIndexToTaskId = analysis.lfqSummarizeFractionIndexToTaskId();
            final Map<Integer, UUID> diaLfqSummarizeFractionIndexToTaskId = analysis.diaLfqSummarizeFractionIndexToTaskId();
            final Map<UUID, UUID> slSearchStepFractionIdToTaskId = stepParameters.containsKey(StepType.SL_SEARCH) ?
                analysis.slSearchStepFractionIdToTaskId() : analysis.ddaSlSearchStepFractionIdToTaskId();
            final Map<Integer, UUID> silacRtAlignmentFractionIndexToTaskId = analysis.silacRtAlignmentFractionIndexToTaskId();
            final Map<UUID, UUID> diaDbSearchStepFractionIdToTaskId = analysis.diaDbSearchStepFractionIdToTaskId();
            final Map<UUID, UUID> diaDataRefineStepFractionIdToTaskId = analysis.diaDenovoDataRefineStepFractionIdToTaskId();
            final Map<UUID, UUID> dbDiaDataRefineStepFractionIdToTaskId = analysis.diaDbDataRefineStepFractionIdToTaskId();
            final Map<UUID, UUID> silacFeatureAlignmentFractionIdToTaskId = analysis.silacFeatureAlignmentFractionIndexToTaskId();
            final Map<UUID, UUID> silacFeatureVectorSearchFractionIdToTaskId = analysis.silacFeatureVectorSearchTaskId();
            Map<UUID, UUID> sampleIdToDenovoFilterTaskId = analysis.sampleIdToDenovoFilterTaskId();

            final Optional<UUID> dbSearchSummarizeTaskId = analysis.dbSearchSummarizeTaskId();
            final Optional<UUID> ptmSearchSummarizeTaskId = analysis.ptmSearchSummarizeTaskId();
            final Optional<UUID> spiderSearchSummarizeTaskId = analysis.spiderSearchSummarizeTaskId();
            final Optional<UUID> peptideLibraryPrepareTaskId = analysis.peptideLibraryPrepareTaskId();
            final Optional<UUID> fDbProteinCandidateFromFastaTaskId = analysis.fDbProteinCandidateFromFastaTaskId();
            final Optional<UUID> dbFilterSummarizeTaskId = analysis.dbFilterSummarizationTaskId();
            final Optional<UUID> ptmFinderFilterSummarizeTaskId = analysis.ptmFilterSummarizationTaskId();
            final Optional<UUID> spiderFilterSummarizeTaskId = analysis.spiderFilterSummarizationTaskId();
            final Optional<UUID> slPeptideSummarizeTaskId = analysis.slPeptideSummarizationTaskId();
            final Optional<UUID> diaDbPeptideSummarizeTaskId = analysis.diaDbPeptideSummarizationTaskId();
            final Optional<UUID> diaDbFilterSummarizeTaskId = analysis.diaDbFilterSummarizationTaskId();
            final Optional<UUID> slFilterSummarizationTaskId = analysis.slFilterSummarizationTaskId();
            final Optional<UUID> silacPeptideSummarizeTaskId = analysis.silacPeptideSummarizeTaskId();
            final Optional<UUID> silacFilterSummarizationTaskId = analysis.silacFilterSummarizationTaskId();
            final Optional<UUID> diaDbPreSearchTaskId = analysis.diaDbPreSearchTaskId();

            final List<TaskDescription> tasks = new ArrayList<>();
            steploop:
            for (WorkflowStep step : analysis.steps()) {
                final StepType stepType = step.getStepType();

                // Skip steps where all tasks are done.
                if (analysis.stepQueued(stepType)) {
                    continue;
                }

                switch (stepType) {
                    case FEATURE_DETECTION: {
                        final FeatureDetectionParameters featureDetectionParameters = (FeatureDetectionParameters) Helper.workflowStepParameters(step);
                        tasks.addAll(fractionLevelTask(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) {
                                return Optional.empty();
                            }
                            return Optional.of(new FeatureDetectionTaskBuilder()
                                .fractionId(fractionId)
                                .parameters(featureDetectionParameters)
                                .dataLoadingTaskId(dataLoadingTaskId)
                                .build()
                            );
                        }));
                        break;
                    }
                    case DATA_REFINE: {
                        final DataRefineParameters dataRefineParameters = (DataRefineParameters) Helper.workflowStepParameters(step);
                        tasks.addAll(fractionLevelTask(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            final UUID featureDetectionTaskId = featureDetectionStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null || featureDetectionTaskId == null) {
                                return Optional.empty();
                            }
                            return Optional.of(new DataRefineTaskBuilder()
                                .fractionId(fractionId)
                                .parameters(dataRefineParameters)
                                .dataLoadingTaskId(dataLoadingTaskId)
                                .featureTaskId(featureDetectionTaskId)
                                .build()
                            );
                        }));
                        break;
                    }
                    case DIA_DB_DATA_REFINE:
                    case DIA_DATA_REFINE: {
                        boolean checkForDb = stepType.equals(DIA_DATA_REFINE) && hasDiaDb;
                        final Optional<Float> slpsmPValue;
                        final Optional<Float> slPeptidePValue;
                        if (hasSlSearchStep) {
                            if (!slPeptideSummarizeTaskId.isPresent()) continue; //has sl but peptide summarize not done
                            WorkflowStep filterSummarizationWorkflowStep = analysis.workflowStep().get(SL_FILTER_SUMMARIZATION);
                            SlFilterSummarizationParameters slFilterSummarizationParameters = filterSummarizationWorkflowStep.getSlFilterSummarizationParameters();
                            IdentificaitonFilter filter = slFilterSummarizationParameters.getFilters();
                            slpsmPValue = Optional.of(PsmFilter.fromDto(filter, fdrValue -> fdrToPValue(analysis.keyspace(), slPeptideSummarizeTaskId.get(), fdrValue, FdrType.PSM, true))
                                .minPValue());
                            slPeptidePValue = Optional.of(SlPeptideFilter.fromProto(
                                filterSummarizationWorkflowStep.getSlFilterSummarizationParameters().getFilters().getSlPeptideFilter(),
                                fdrValue -> fdrToPValue(analysis.keyspace(), slPeptideSummarizeTaskId.get(), fdrValue, FdrType.PEPTIDE, true)
                            ).minPValue());
                        } else {
                            slpsmPValue = Optional.empty();
                            slPeptidePValue = Optional.empty();
                        }

                        final Optional<Float> diaDbPsmPValue;
                        final Optional<Float> diaDbPeptidePValue;
                        if (checkForDb) {
                            if (!diaDbPeptideSummarizeTaskId.isPresent()) continue; //has dia db but peptide summarize not done
                            WorkflowStep filterSummarizationWorkflowStep = analysis.workflowStep().get(DIA_DB_FILTER_SUMMARIZE);
                            SlFilterSummarizationParameters slFilterSummarizationParameters = filterSummarizationWorkflowStep.getSlFilterSummarizationParameters();
                            IdentificaitonFilter filter = slFilterSummarizationParameters.getFilters();
                            diaDbPsmPValue = Optional.of(PsmFilter.fromDto(filter, fdrValue -> fdrToPValue(analysis.keyspace(), diaDbPeptideSummarizeTaskId.get(), fdrValue, FdrType.PSM, true))
                                .minPValue());
                            diaDbPeptidePValue = Optional.of(SlPeptideFilter.fromProto(
                                filterSummarizationWorkflowStep.getSlFilterSummarizationParameters().getFilters().getSlPeptideFilter(),
                                fdrValue -> fdrToPValue(analysis.keyspace(), diaDbPeptideSummarizeTaskId.get(), fdrValue, FdrType.PEPTIDE, true)
                            ).minPValue());
                        } else {
                            diaDbPsmPValue = Optional.empty();
                            diaDbPeptidePValue = Optional.empty();
                        }

                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            UUID featureDetectionTaskId = featureDetectionStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) return Optional.empty();
                            if (featureDetectionTaskId == null) return Optional.empty();

                            DiaDataRefineTaskParameters.Builder taskParameterBuilder = DiaDataRefineTaskParameters.newBuilder();
                            final DiaDataRefineTaskParameters.OptionalFilterStep.Builder slStep;
                            if (hasSlSearchStep) {
                                UUID slSearchStep = slSearchStepFractionIdToTaskId.get(fractionId);
                                if (slSearchStep == null) return Optional.empty();
                                slStep = DiaDataRefineTaskParameters.OptionalFilterStep.newBuilder()
                                    .setPsmTaskId(ModelConversion.uuidToByteString(slSearchStep))
                                    .setPeptideTaskId(ModelConversion.uuidToByteString(slPeptideSummarizeTaskId.get()))
                                    .setPresent(true)
                                    .setMinPsmFilter(slpsmPValue.get())
                                    .setMinPeptideFilter(slPeptidePValue.get());
                            } else {
                                slStep = DiaDataRefineTaskParameters.OptionalFilterStep.newBuilder()
                                    .setPresent(false);
                            }

                            final DiaDataRefineTaskParameters.OptionalFilterStep.Builder idStep;
                            if (checkForDb) {
                                UUID diaDbStep = diaDbSearchStepFractionIdToTaskId.get(fractionId);
                                if (diaDbStep == null) return Optional.empty();
                                idStep = DiaDataRefineTaskParameters.OptionalFilterStep.newBuilder()
                                    .setPsmTaskId(ModelConversion.uuidToByteString(diaDbStep))
                                    .setPeptideTaskId(ModelConversion.uuidToByteString(diaDbPeptideSummarizeTaskId.get()))
                                    .setPresent(true)
                                    .setMinPsmFilter(diaDbPsmPValue.get())
                                    .setMinPeptideFilter(diaDbPeptidePValue.get());
                                taskParameterBuilder
                                    .setDbDataRefineTaskId(ModelConversion.uuidToByteString(dbDiaDataRefineStepFractionIdToTaskId.get(fractionId)));
                            } else {
                                idStep = DiaDataRefineTaskParameters.OptionalFilterStep.newBuilder()
                                    .setPresent(false);
                            }

                            DiaDataRefineParameters searchParameters = (DiaDataRefineParameters) Helper.workflowStepParameters(step);
                            taskParameterBuilder
                                .setFractionId(ModelConversion.uuidToByteString(fractionId))
                                .setFeatureDetectionTaskId(ModelConversion.uuidToByteString(featureDetectionTaskId))
                                .setDataLoadingTaskId(ModelConversion.uuidToByteString(dataLoadingTaskId))
                                .setDiaDataRefineParameters(searchParameters)
                                .setSlFilter(slStep)
                                .setIdentificationFilter(idStep)
                                .setType(stepType);

                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(DiaDataRefineService.NAME)
                                .setDiaDataRefineTaskParameters(taskParameterBuilder.build())
                                .build()
                            );
                        }));
                        break;
                    }
                    case DENOVO: {
                        final Supplier<DenovoParameters> denovoParameters = Suppliers.memoize(() -> {
                            final DenovoParameters originalDenovoParameters = (DenovoParameters) Helper.workflowStepParameters(step);
                            final DenovoParameters populated = parametersPopulater.populateDeNovoParameters(originalDenovoParameters);
                            return new DenovoParametersBuilder().from(originalDenovoParameters)
                                .fixedModification(populated.fixedModification())
                                .variableModification(populated.variableModification())
                                .build();
                        });
                        tasks.addAll(fractionLevelTask(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) return Optional.empty();

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (!hasDiaDenovo && dataRefineTaskId == null) return Optional.empty();

                            final UUID diaDenovoTaskId = diaDataRefineStepFractionIdToTaskId.get(fractionId);
                            if (hasDiaDenovo && diaDenovoTaskId == null) return Optional.empty();

                            return Optional.of(new DenovoTaskBuilder()
                                .fractionId(fractionId)
                                .parameters(denovoParameters.get())
                                .dataLoadingTaskId(dataLoadingTaskId)
                                .dataRefineTaskId(hasDiaDenovo ? diaDenovoTaskId : dataRefineTaskId)
                                .build()
                            );
                        }));
                        break;
                    }
                    case FDB_PROTEIN_CANDIDATE_FROM_FASTA: {
                        if (hasDenovo) throw new IllegalStateException("Invalid task FastDB protein candidate from fasta should not have DeNovo.");
                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setService(FDBProteinCandidateFromFastaService.NAME)
                            .setFastDBProteinCandidateParameters(FDBProteinCandidateTaskParameters.newBuilder()
                                .addAllTargetDatabaseSources(analysis.targetDatabaseSources())
                                .addAllContaminantDatabaseSources(analysis.contaminantDatabaseSources())
                                .build()
                            )
                            .build();
                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, Collections.emptyList(), Collections.emptyList()));
                        break;
                    }
                    case DB_TAG_SEARCH: {
                        final Supplier<TagSearchParameters> tagSearchParameters = Suppliers.memoize(() -> {
                            final TagSearchParameters originalTagSearchParameters = (TagSearchParameters) Helper.workflowStepParameters(step);
                            final IDBaseParameters populated = parametersPopulater.populateDBParameters(originalTagSearchParameters);
                            return new TagSearchParametersBuilder().from(originalTagSearchParameters)
                                .fixedModification(populated.fixedModification())
                                .variableModification(populated.variableModification())
                                .build();
                        });
                        tasks.addAll(fractionLevelTask(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) return Optional.empty();

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataRefineTaskId == null) return Optional.empty();

                            final UUID denovoTaskId = denovoStepFractionIdToTaskId.get(fractionId);
                            // denovo task id can be null
                            if (denovoTaskId == null && !peptideLibraryPrepareTaskId.isPresent()) return Optional.empty();

                            return Optional.of(new TagSearchTaskBuilder()
                                .fractionId(fractionId)
                                .workflowId(projectId)
                                .dataLoadingTaskId(dataLoadingTaskId)
                                .dataRefineTaskId(dataRefineTaskId)
                                .denovoTaskId(denovoTaskId)
                                .peptideLibraryPrepareTaskId(peptideLibraryPrepareTaskId.orElse(null))
                                .addAllTargetDatabaseSources(analysis.targetDatabaseSources())
                                .addAllContaminantDatabaseSources(analysis.contaminantDatabaseSources())
                                .parameters(tagSearchParameters.get())
                                .build()
                            );
                        }));
                        break;
                    }
                    case DB_TAG_SUMMARIZE: {
                        final Supplier<TagSearchParameters> tagSearchParameters = Suppliers.memoize(() -> {
                            final TagSearchParameters originalDbParameters = (TagSearchParameters) Helper.workflowStepParameters(step);
                            final IDBaseParameters populated = parametersPopulater.populateDBParameters(originalDbParameters);
                            return new TagSearchParametersBuilder().from(originalDbParameters)
                                .fixedModification(populated.fixedModification())
                                .variableModification(populated.variableModification())
                                .build();
                        });
                        tasks.addAll(sampleLevelTask(analysis, stepType, sampleFractions -> {
                            final TagSearchSummarizeTaskBuilder builder = new TagSearchSummarizeTaskBuilder();
                            for (UUID fractionId : sampleFractions.fractionIds()) {
                                UUID tagSearcgTaskId = tagSearchStepFractionIdToTaskId.get(fractionId);
                                if (tagSearcgTaskId == null) {
                                    return Optional.empty();
                                }
                                builder.putFractionTagSearchTaskId(fractionId, tagSearcgTaskId);
                            }
                            return Optional.of(builder
                                .workflowId(projectId)
                                .parameters(tagSearchParameters.get())
                                .addAllTargetDatabaseSources(analysis.targetDatabaseSources())
                                .addAllContaminantDatabaseSources(analysis.contaminantDatabaseSources())
                                .build()
                            );
                        }));
                        break;
                    }
                    case DB_PRE_SEARCH: {
                        final Supplier<DBSearchParameters> dbParameters = Suppliers.memoize(
                            () -> dbSearchParameters((DBSearchParameters) Helper.workflowStepParameters(step))
                        );

                        final UUID fDbTaskId = fDbProteinCandidateFromFastaTaskId.orElse(null);
                        tasks.addAll(fractionLevelTask(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) return Optional.empty();

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataRefineTaskId == null) return Optional.empty();

                            final UUID denovoTaskId = denovoStepFractionIdToTaskId.get(fractionId);
                            if (hasDenovo && denovoTaskId == null) return Optional.empty();

                            if (!hasDenovo && fDbTaskId == null) return Optional.empty();

                            final UUID tagSearchTaskId = tagSearchStepFractionIdToTaskId.get(fractionId);
                            if (hasDenovo && tagSearchTaskId == null) return Optional.empty();

                            final UUID tagSummarizeId = tagSummarizeStepSampleIdToTaskId.get(sampleId);
                            if (hasDenovo && tagSummarizeId == null) return Optional.empty();

                            return Optional.of(new DBPreSearchTaskBuilder()
                                .fractionId(fractionId)
                                .workflowId(projectId)
                                .dataLoadingTaskId(dataLoadingTaskId)
                                .refinedTaskId(dataRefineTaskId)
                                .denovoTaskId(denovoTaskId)
                                .tagSearchBatchTaskId(tagSearchTaskId)
                                .tagSearchSummarizeTaskID(tagSummarizeId)
                                .fDbProteinCandidateFromFastaTaskId(fDbTaskId)
                                .parameters(dbParameters.get())
                                .build()
                            );
                        }));
                        break;
                    }
                    case DB_BATCH_SEARCH: {
                        final Supplier<DBSearchParameters> dbParameters = Suppliers.memoize(
                            () -> dbSearchParameters((DBSearchParameters)Helper.workflowStepParameters(step))
                        );
                        final UUID fDbTaskId = fDbProteinCandidateFromFastaTaskId.orElse(null);
                        tasks.addAll(fractionLevelTask(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) return Optional.empty();

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataRefineTaskId == null) return Optional.empty();

                            final UUID featureDetectionTaskId = featureDetectionStepFractionIdToTaskId.get(fractionId);
                            if (featureDetectionTaskId == null) return Optional.empty();

                            final UUID tagSummarizeId = tagSummarizeStepSampleIdToTaskId.get(sampleId);
                            if (hasDenovo && tagSummarizeId == null) return Optional.empty();
                            if (!hasDenovo && fDbTaskId == null) return Optional.empty();

                            final UUID preSearchTaskId = dbPreSearchStepFractionIdToTaskId.get(fractionId);
                            final UUID denovoTaskId = denovoStepFractionIdToTaskId.get(fractionId);
                            // pre search and denovo tasks can both be null
                            if (!peptideLibraryPrepareTaskId.isPresent()) {
                                if (preSearchTaskId == null || (hasDenovo && denovoTaskId == null)) {
                                    return Optional.empty();
                                }
                            }

                            return Optional.of(new DBSearchTaskBuilder()
                                .fractionId(fractionId)
                                .dataLoadingTaskId(dataLoadingTaskId)
                                .dataRefineTaskId(dataRefineTaskId)
                                .denovoTaskId(denovoTaskId)
                                .tagSearchSummarizeTaskId(tagSummarizeId)
                                .preSearchTaskId(preSearchTaskId)
                                .fdbSearchTaskId(fDbTaskId)
                                .parameters(dbParameters.get())
                                .build()
                            );
                        }));
                        break;
                    }
                    case DB_SUMMARIZE: {
                        if (!dbSearchStepFractionIdToTaskId.keySet().containsAll(analysisFractionIds)) continue;
                        if (!hasDenovo && !fDbProteinCandidateFromFastaTaskId.isPresent()) continue;
                        if (hasDenovo && !tagSummarizeStepSampleIdToTaskId.keySet().containsAll(analysisSampleIds)) continue;

                        final DBSearchParameters dbParameters = dbSearchParameters((DBSearchParameters) Helper.workflowStepParameters(step));
                        final DBSearchSummarizeTaskBuilder builder = new DBSearchSummarizeTaskBuilder();
                        if (hasDenovo) {
                            for (UUID sampleId : analysisSampleIds) {
                                builder.addTagSearchSummarizeTaskIds(tagSummarizeStepSampleIdToTaskId.get(sampleId));
                            }
                        } else {
                            builder.fDbProteinCandidateFromFastaTaskId(fDbProteinCandidateFromFastaTaskId.get());
                        }
                        final DBSearchSummarizeTask dbSearchSummarizeTaskBuilder = builder
                            .workflowId(projectId)
                            .fractionDbSearchTaskIds(dbSearchStepFractionIdToTaskId)
                            .parameters(dbParameters)
                            .sampleFractions(samples)
                            .build();
                        tasks.add(taskDescriptionAnalysisLevel(dbSearchSummarizeTaskBuilder, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case PTM_FINDER_BATCH_SEARCH: {
                        if (hasFastDb) throw new IllegalStateException("Invalid task cannot run PTM Finder on Quick DB");

                        final Supplier<PtmFinderParameters> ptmFinderParameters = Suppliers.memoize(
                            () -> parametersPopulater.populatePTMParameters((PtmFinderParameters) Helper.workflowStepParameters(step))
                        );

                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            if (!dbSearchSummarizeTaskId.isPresent()) return Optional.empty();

                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) return Optional.empty();

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataRefineTaskId == null) return Optional.empty();

                            final UUID featureDetectionTaskId = featureDetectionStepFractionIdToTaskId.get(fractionId);
                            if (featureDetectionTaskId == null) return Optional.empty();

                            final UUID denovoTaskId = denovoStepFractionIdToTaskId.get(fractionId);
                            if (denovoTaskId == null) return Optional.empty();

                            final UUID tagSummarizeId = tagSummarizeStepSampleIdToTaskId.get(sampleId);
                            if (tagSummarizeId == null) return Optional.empty();

                            final UUID preSearchTaskId = dbPreSearchStepFractionIdToTaskId.get(fractionId);
                            if (preSearchTaskId == null) return Optional.empty();

                            final UUID dbSearchTaskId = dbSearchStepFractionIdToTaskId.get(fractionId);
                            if (dbSearchTaskId == null) return Optional.empty();

                            if (!dbFilterSummarizeTaskId.isPresent()) return Optional.empty();

                            final PtmFinderSearchParameters paramaters = ptmFinderParameters.get().searchParameters();
                            final PtmFinderSearchTaskParameters taskParameters = PtmFinderSearchTaskParameters.newBuilder()
                                .setFractionId(fractionId.toString())
                                .setDataLoadingTaskId(dataLoadingTaskId.toString())
                                .setRefinedTaskId(dataRefineTaskId.toString())
                                .setDenovoTaskId(denovoTaskId.toString())
                                .setTagSearchSummarizeTaskId(tagSummarizeId.toString())
                                .setPreSearchTaskId(preSearchTaskId.toString())
                                .setDbSearchTaskId(dbSearchTaskId.toString())
                                .setDbFilterSummarizationTaskId(dbFilterSummarizeTaskId.get().toString())
                                .setParamaters(paramaters)
                                .build();

                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(PtmFinderService.NAME)
                                .setPtmFinderSearchTaskParameters(taskParameters)
                                .build());
                        }));
                        break;
                    }
                    case PTM_FINDER_SUMMARIZE: {
                        if (!dbSearchSummarizeTaskId.isPresent()) continue;

                        if (!tagSummarizeStepSampleIdToTaskId.keySet().containsAll(analysisSampleIds)) {
                            continue;
                        }
                        if (!dbSearchStepFractionIdToTaskId.keySet().containsAll(analysisFractionIds)) {
                            continue;
                        }
                        if (!ptmSearchStepFractionIdToTaskId.keySet().containsAll(analysisFractionIds)) {
                            continue;
                        }

                        final PtmFinderParameters parameters = parametersPopulater.populatePTMParameters((PtmFinderParameters) Helper.workflowStepParameters(step));

                        final PtmFinderSummarizeTaskParameters.Builder builder = PtmFinderSummarizeTaskParameters.newBuilder()
                            .setDbSearchSummarizeTaskId(dbSearchSummarizeTaskId.get().toString());

                        for (UUID sampleId : analysisSampleIds) {
                            builder.addTagSearchSummarizeTaskIds(tagSummarizeStepSampleIdToTaskId.get(sampleId).toString());
                        }

                        for (UUID fractionId : analysisFractionIds) {
                            final String fractionIdString = fractionId.toString();
                            builder.putFractionDbSearchTaskIds(fractionIdString, dbSearchStepFractionIdToTaskId.get(fractionId).toString());
                            builder.putFractionPtmSearchTaskIds(fractionIdString, ptmSearchStepFractionIdToTaskId.get(fractionId).toString());
                        }
                        builder
                            .setOrderedSampleFractions(OrderedSampleFractions.proto(samples))
                            .setTopProteinGroups(parameters.ptmFinderConservedParameter().topProteinGroups());

                        final TaskParameters ptmFinderSummarizeTask = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setService(PtmFinderSummarizeService.NAME)
                            .setPtmFinderSummarizeTaskParameters(builder)
                            .build();

                        tasks.add(taskDescriptionAnalysisLevel(ptmFinderSummarizeTask, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case SPIDER_BATCH_SEARCH: {
                        if (hasFastDb) throw new IllegalStateException("Invalid task cannot run PTM Finder on Quick DB");

                        final Supplier<SpiderParameters> spiderParameters = Suppliers.memoize(
                            () -> parametersPopulater.populateSpiderParameters((SpiderParameters) Helper.workflowStepParameters(step))
                        );
                        tasks.addAll(fractionLevelTask(analysis, stepType, (sampleId, fractionId) -> {
                            if (!dbSearchSummarizeTaskId.isPresent()) return Optional.empty();
                            if (hasPtm && !ptmSearchSummarizeTaskId.isPresent()) return Optional.empty();

                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) return Optional.empty();

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataRefineTaskId == null) return Optional.empty();

                            final UUID featureDetectionTaskId = featureDetectionStepFractionIdToTaskId.get(fractionId);
                            if (featureDetectionTaskId == null) return Optional.empty();

                            final UUID denovoTaskId = denovoStepFractionIdToTaskId.get(fractionId);
                            if (denovoTaskId == null) return Optional.empty();

                            final UUID tagSummarizeId = tagSummarizeStepSampleIdToTaskId.get(sampleId);
                            if (tagSummarizeId == null) return Optional.empty();

                            final UUID preSearchTaskId = dbPreSearchStepFractionIdToTaskId.get(fractionId);
                            if (preSearchTaskId == null) return Optional.empty();

                            final UUID dbSearchTaskId = dbSearchStepFractionIdToTaskId.get(fractionId);
                            if (dbSearchTaskId == null) return Optional.empty();

                            if (!dbFilterSummarizeTaskId.isPresent()) return Optional.empty();
                            if (hasPtm && !ptmFinderFilterSummarizeTaskId.isPresent()) return Optional.empty();

                            return Optional.of(new SpiderTaskBuilder()
                                .fractionId(fractionId)
                                .dataLoadingTaskId(dataLoadingTaskId)
                                .refinedTaskId(dataRefineTaskId)
                                .denovoTaskId(denovoTaskId)
                                .tagSearchSummarizeTaskId(tagSummarizeId)
                                .preSearchTaskId(preSearchTaskId)
                                .dbSearchTaskId(dbSearchTaskId)
                                .dbSearchSummarizeTaskId(dbSearchSummarizeTaskId.get())
                                .ptmFinderSummarizeTaskId(ptmSearchSummarizeTaskId.orElse(null))
                                .filterSummarizationTaskId(ptmSearchSummarizeTaskId.orElse(dbSearchSummarizeTaskId.get()))
                                .parameters(spiderParameters.get())
                                .build()
                            );
                        }));
                        break;
                    }
                    case SPIDER_SUMMARIZE: {
                        if (!spiderSearchStepFractionIdToTaskId.keySet().containsAll(analysisFractionIds)) continue;
                        if (!tagSummarizeStepSampleIdToTaskId.keySet().containsAll(analysisSampleIds)) continue;

                        final Map<UUID, UUID> fractionSearchTaskIds;
                        final UUID searchSummarizeTaskId;
                        if (hasPtm) {
                            if (!ptmSearchSummarizeTaskId.isPresent()) continue;
                            searchSummarizeTaskId = ptmSearchSummarizeTaskId.get();
                            fractionSearchTaskIds = Maps.asMap(dbSearchStepFractionIdToTaskId.keySet(), fractionId -> searchSummarizeTaskId);
                        } else {
                            fractionSearchTaskIds = dbSearchStepFractionIdToTaskId;
                        }
                        if (!dbSearchSummarizeTaskId.isPresent()) continue;
                        if (!fractionSearchTaskIds.keySet().containsAll(analysisFractionIds)) continue;

                        final SpiderParameters parameters = parametersPopulater.populateSpiderParameters((SpiderParameters) Helper.workflowStepParameters(step));
                        final SpiderSummarizeTaskBuilder builder = new SpiderSummarizeTaskBuilder();

                        for (UUID sampleId : analysisSampleIds) {
                            builder.addTagSearchSummarizeTaskIds(tagSummarizeStepSampleIdToTaskId.get(sampleId));
                        }

                        final SpiderSummarizeTask spiderSummarizeTask = builder
                            .workflowId(projectId)
                            .fractionSearchTaskIds(fractionSearchTaskIds)
                            .fractionSpiderTaskIds(spiderSearchStepFractionIdToTaskId)
                            .dbSearchSummarizeTaskId(dbSearchSummarizeTaskId.get())
                            .ptmSearchSummarizeTaskId(ptmSearchSummarizeTaskId.orElse(null))
                            .parameters(parameters)
                            .sampleFractions(samples)
                            .build();
                        tasks.add(taskDescriptionAnalysisLevel(spiderSummarizeTask, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case DB_DENOVO_ONLY_TAG_SEARCH: {
                        if (!dbFilterSummarizeTaskId.isPresent()) continue;
                        if (!dbSearchSummarizeTaskId.isPresent()) continue;
                        tasks.addAll(sampleLevelTaskParamaters(analysis, stepType, (sampleFractionIds -> {
                            UUID sampleId = sampleFractionIds.sampleId();
                            if (sampleIdToDenovoFilterTaskId.get(sampleId) == null) return Optional.empty();
                            return Optional.of(TaskParameters.newBuilder()
                                .setService(DeNovoOnlyTagSearchService.NAME)
                                .setProjectId(projectId.toString())
                                .setDenovoOnlyTagSearchTaskParameters(DenovoOnlyTagSearchTaskParameters.newBuilder()
                                    .setDenovoSummarizationTaskId(sampleIdToDenovoFilterTaskId.get(sampleId).toString())
                                    .setFilterSummarizationTaskd(dbFilterSummarizeTaskId.get().toString())
                                    .setSampleId(sampleFractionIds.sampleId().toString())
                                    .setSummarizationTaskId(dbSearchSummarizeTaskId.get().toString())
                                    .setDenovoOnlyTagSearchParameter(step.getDenovoOnlyTagSearchParameters())
                                    .build())
                                .build());
                        })));
                        break;
                    }
                    case PTM_FINDER_DENOVO_ONLY_TAG_SEARCH: {
                        if (!ptmFinderFilterSummarizeTaskId.isPresent()) continue;
                        if (!ptmSearchSummarizeTaskId.isPresent()) continue;
                        tasks.addAll(sampleLevelTaskParamaters(analysis, stepType, (sampleFractionIds -> {
                            UUID sampleId = sampleFractionIds.sampleId();
                            if (sampleIdToDenovoFilterTaskId.get(sampleId) == null) return Optional.empty();
                            return Optional.of(TaskParameters.newBuilder()
                                .setService(DeNovoOnlyTagSearchService.NAME)
                                .setProjectId(projectId.toString())
                                .setDenovoOnlyTagSearchTaskParameters(DenovoOnlyTagSearchTaskParameters.newBuilder()
                                    .setDenovoSummarizationTaskId(sampleIdToDenovoFilterTaskId.get(sampleId).toString())
                                    .setFilterSummarizationTaskd(ptmFinderFilterSummarizeTaskId.get().toString())
                                    .setSampleId(sampleFractionIds.sampleId().toString())
                                    .setSummarizationTaskId(ptmSearchSummarizeTaskId.get().toString())
                                    .setDenovoOnlyTagSearchParameter(step.getDenovoOnlyTagSearchParameters())
                                    .build())
                                .build());
                        })));
                        break;
                    }
                    case SPIDER_DENOVO_ONLY_TAG_SEARCH: {
                        if (!spiderFilterSummarizeTaskId.isPresent()) continue;
                        if (!spiderSearchSummarizeTaskId.isPresent()) continue;
                        tasks.addAll(sampleLevelTaskParamaters(analysis, stepType, (sampleFractionIds -> {
                            UUID sampleId = sampleFractionIds.sampleId();
                            if (sampleIdToDenovoFilterTaskId.get(sampleId) == null) return Optional.empty();
                            return Optional.of(TaskParameters.newBuilder()
                                .setService(DeNovoOnlyTagSearchService.NAME)
                                .setProjectId(projectId.toString())
                                .setDenovoOnlyTagSearchTaskParameters(DenovoOnlyTagSearchTaskParameters.newBuilder()
                                    .setDenovoSummarizationTaskId(sampleIdToDenovoFilterTaskId.get(sampleId).toString())
                                    .setFilterSummarizationTaskd(spiderFilterSummarizeTaskId.get().toString())
                                    .setSampleId(sampleFractionIds.sampleId().toString())
                                    .setSummarizationTaskId(spiderSearchSummarizeTaskId.get().toString())
                                    .setDenovoOnlyTagSearchParameter(step.getDenovoOnlyTagSearchParameters())
                                    .build())
                                .build());
                        })));
                        break;
                    }
                    case REPORTER_ION_Q_BATCH_CALCULATION: {
                        final Supplier<ReporterIonQBatchCalculationParameters> parametersSupplier = Suppliers.memoize(() -> {
                            final ReporterIonQBatchCalculationParameters parameters = (ReporterIonQBatchCalculationParameters) Helper.workflowStepParameters(step);
                            return parametersPopulater.populateRepoterIonQParameters(parameters);
                        });

                        if (hasDb && !dbSearchSummarizeTaskId.isPresent()) continue;
                        if (hasPtm && !ptmSearchSummarizeTaskId.isPresent()) continue;
                        if (hasSpider && !spiderSearchSummarizeTaskId.isPresent()) continue;

                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null || dataRefineTaskId == null) return Optional.empty();

                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(ReporterIonQService.NAME)
                                .setReporterIonQBatchCalculationTaskParameters(ReporterIonQBatchCalculationTaskParameters
                                    .newBuilder()
                                    .setReporterIonQBatchCalculationParameters(parametersSupplier.get())
                                    .setFractionId(fractionId.toString())
                                    .setDataLoadingTaskId(dataLoadingTaskId.toString())
                                    .setDataRefineTaskId(dataRefineTaskId.toString())
                                    .build()
                                )
                                .build());
                        }));
                        break;
                    }
                    case REPORTER_ION_Q_NORMALIZATION: {
                        final Optional<String> searchSummarizationTaskId = ((hasSpider) ? spiderSearchSummarizeTaskId : (hasPtm) ? ptmSearchSummarizeTaskId : dbSearchSummarizeTaskId).map(UUID::toString);
                        if (!searchSummarizationTaskId.isPresent()) continue;

                        final Optional<String> filterSummarizationTaskId = ((hasSpider) ? spiderFilterSummarizeTaskId : (hasPtm) ? ptmFinderFilterSummarizeTaskId : dbFilterSummarizeTaskId).map(UUID::toString);
                        if (!filterSummarizationTaskId.isPresent()) continue;

                        final ReporterIonQBatchCalculationParameters reporterIonQBatchCalculationParameters = (ReporterIonQBatchCalculationParameters) stepParameters.get(StepType.REPORTER_ION_Q_BATCH_CALCULATION);
                        final ReporterIonQFilterSummarization reporterIonQFilterSummarization = (ReporterIonQFilterSummarization) stepParameters.get(StepType.REPORTER_ION_Q_FILTER_SUMMARIZATION);
                        final float pValue = getPValueOnRiqComplete(analysis, FdrType.PSM, analysis.hasSlSearch());

                        tasks.addAll(sampleLevelTaskParamaters(analysis, stepType, sampleFractions -> {
                            final ReporterIonQNormalizationTaskParameters.Builder builder = ReporterIonQNormalizationTaskParameters.newBuilder();
                            for (UUID fractionId : sampleFractions.fractionIds()) {
                                final UUID calculationTaskId = reporterIonQStepFractionIdToTaskId.get(fractionId);
                                if (calculationTaskId == null) {
                                    return Optional.empty();
                                }
                                final String fractionIdString = fractionId.toString();
                                builder.putFractionIdToRiqCalculationTaskSearchTaskId(fractionIdString, calculationTaskId.toString());
                                if (hasSpider || hasPtm) {
                                    builder.putFractionIdToSearchTaskId(fractionIdString, searchSummarizationTaskId.get());
                                } else {
                                    builder.putFractionIdToSearchTaskId(fractionIdString, dbSearchStepFractionIdToTaskId.get(fractionId).toString());
                                }
                            }
                            final NormalizationMethodType normalizationMethod =
                                reporterIonQFilterSummarization.getExperimentSettings().getReporterQNormalization()
                                    .getNormalizationMethod();
                            final List<ReporterIonQExperimentSettings.ExpectedRatios> expectedRatios;
                            switch (normalizationMethod) {
                                case AUTO_NORMALIZATION:
                                case NO_NORMALIZATION:
                                    expectedRatios = new ArrayList<>();
                                    break;
                                case MANUAL_NORMALIZATION:
                                    expectedRatios = reporterIonQFilterSummarization.getExperimentSettings().getReporterQNormalization().getManualExpectedRatiosList();
                                    break;
                                case SPIKE_NORMALIZATION:
                                    expectedRatios = reporterIonQFilterSummarization.getExperimentSettings().getReporterQNormalization().getSpikedExpectedRatiosList();
                                    break;
                                default:
                                    expectedRatios = null;
                            }
                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(ReporterIonQNormalizationService.NAME)
                                .setRepoterIonQNormalizationParameters(builder
                                    .setConservedParameters(reporterIonQBatchCalculationParameters.getConservedParameters())
                                    .setParameters(reporterIonQBatchCalculationParameters.getParameters())
                                    .setIdentificationSummarizeTaskId(searchSummarizationTaskId.get())
                                    .setMinPsmPValueThreshold(pValue)
                                    .setSampleId(sampleFractions.sampleId().toString())
                                    .addAllFractionId(sampleFractions.fractionIds().stream().map(UUID::toString)::iterator)
                                    .setNormalizationMethod(normalizationMethod)
                                    .addAllSpikedProteinHits(reporterIonQFilterSummarization.getExperimentSettings().getReporterQNormalization().getSpikedProteinHitIdListList())
                                    .addAllExpectedRatio(expectedRatios)
                                    .setFilterSummarizationTaskId(filterSummarizationTaskId.get())
                                    .build())
                                .build());
                        }));
                        break;
                    }
                    case REPORTER_ION_Q_FILTER_SUMMARIZATION: {
                        final String searchSummarizationTaskId;
                        if (hasSpider) {
                            if (!spiderSearchSummarizeTaskId.isPresent()) continue;
                            searchSummarizationTaskId = spiderSearchSummarizeTaskId.get().toString();
                        } else if (hasPtm) {
                            if (!ptmSearchSummarizeTaskId.isPresent()) continue;
                            searchSummarizationTaskId = ptmSearchSummarizeTaskId.get().toString();
                        } else {
                            if (!dbSearchSummarizeTaskId.isPresent()) continue;
                            searchSummarizationTaskId = dbSearchSummarizeTaskId.get().toString();
                        }

                        final String filterSummarizeTaskId;
                        if (hasSpider) {
                            if (!spiderFilterSummarizeTaskId.isPresent()) continue;
                            filterSummarizeTaskId = spiderSearchSummarizeTaskId.get().toString();
                        } else if (hasPtm) {
                            if (!ptmFinderFilterSummarizeTaskId.isPresent()) continue;
                            filterSummarizeTaskId = ptmFinderFilterSummarizeTaskId.get().toString();
                        } else if (hasDb) {
                            if (!dbFilterSummarizeTaskId.isPresent()) continue;
                            filterSummarizeTaskId = dbFilterSummarizeTaskId.get().toString();
                        } else {
                            if (!slFilterSummarizationTaskId.isPresent()) continue;
                            filterSummarizeTaskId = slFilterSummarizationTaskId.get().toString();
                        }

                        final ReporterIonQBatchCalculationParameters reporterIonQBatchCalculationParameters = (ReporterIonQBatchCalculationParameters) stepParameters.get(StepType.REPORTER_ION_Q_BATCH_CALCULATION);
                        final ReporterIonQFilterSummarization reporterIonQFilterSummarization = (ReporterIonQFilterSummarization) Helper.workflowStepParameters(step);
                        final ReporterIonQExperimentSettings experimentSettings = reporterIonQFilterSummarization.getExperimentSettings();

                        final List<UUID> sampleIds;
                        switch (experimentSettings.getSelectedExperimentCase()) {
                            case ALLEXPERIMENTS:
                                sampleIds = analysisSampleIds;
                                break;
                            case SAMPLE:
                                UUID sampleId = UUID.fromString(experimentSettings.getSample().getId());
                                sampleIds = Collections.singletonList(sampleId);
                                break;
                            default:
                                throw new IllegalArgumentException("Unexpected " + experimentSettings.getSelectedExperimentCase());
                        }

                        final ReporterIonQSummaryFilterTaskParameters.Builder builder = ReporterIonQSummaryFilterTaskParameters.newBuilder();
                        for (OrderedSampleFractions sampleFractions : samples) {
                            UUID sampleId = sampleFractions.sampleId();
                            if (!sampleIds.contains(sampleId)) {
                                continue;
                            }
                            List<UUID> fractionIds = sampleFractions.fractionIds();
                            for (UUID fractionId : fractionIds) {
                                final UUID reporterIonQTaskId = reporterIonQStepFractionIdToTaskId.get(fractionId);
                                final UUID psmTaskId = reporterIonQNormalizationSampleIdToTaskId.get(sampleId);
                                if (reporterIonQTaskId == null || psmTaskId == null) {
                                    continue steploop;
                                }
                                final String fractionIdString = fractionId.toString();
                                builder.putFractionIdToDataloadingTaskId(fractionIdString, dataLoadingStepFractionIdToTaskId.get(fractionId).toString());
                                builder.putFractionIdToPsmTaskId(fractionIdString, psmTaskId.toString());
                            }
                            builder.addSamples(CommonTasksFactory.sampleLevelTask(sampleId, fractionIds));
                        }

                        final ReporterIonQFilterSummarization filterSummarizationCorrected =
                            ReporterIonQFilterSummarization.newBuilder().mergeFrom(reporterIonQFilterSummarization)
                                .setSpectrumFilter(ReporterIonQSpectrumFilter.newBuilder()
                                    .mergeFrom(reporterIonQFilterSummarization.getSpectrumFilter())
                                    .setMinPValue(getPValueOnRiqComplete(analysis, FdrType.PSM, analysis.hasSlSearch())).build()).build();

                        builder
                            .setReporterIonQFilterSummarization(filterSummarizationCorrected)
                            .setSearchSummarizationTaskId(searchSummarizationTaskId)
                            .addAllFastaHeaderParser(analysis.fastaHeaderParsers())
                            .setMethod(reporterIonQBatchCalculationParameters.getParameters().getMethod())
                            .setIdentificationSummarizeTaskId(filterSummarizeTaskId)
                            .setMinPeptidePValueThreshold(getPValueOnRiqComplete(analysis, FdrType.PEPTIDE, analysis.hasSlSearch()));

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setReporterIonQSummaryFilterParameters(builder)
                            .setService(ReporterIonQSummarizeService.NAME)
                            .build();

                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case LFQ_RETENTION_TIME_ALIGNMENT: {
                        final RetentionTimeAlignmentParameters parameters = (RetentionTimeAlignmentParameters) Helper.workflowStepParameters(step);
                        final Set<UUID> samplesToInclude = parameters.groups().stream().flatMap(g -> g.sampleIds().stream())
                            .collect(Collectors.toSet());
                        final Map<UUID, UUID> fractionSearchTaskIds;
                        final Map<UUID, UUID> fractionPreDBSearchTaskIds;

                        final UUID searchSummariationTaskId;
                        if (hasSpider) {
                            if (!spiderSearchSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = spiderSearchSummarizeTaskId.get();
                            fractionSearchTaskIds = Maps.asMap(dbSearchStepFractionIdToTaskId.keySet(), fractionId -> searchSummariationTaskId);
                        } else if (hasPtm) {
                            if (!ptmSearchSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = ptmSearchSummarizeTaskId.get();
                            fractionSearchTaskIds = Maps.asMap(dbSearchStepFractionIdToTaskId.keySet(), fractionId -> searchSummariationTaskId);
                        } else if (hasDb) {
                            if (!dbSearchSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = dbSearchSummarizeTaskId.get();
                            fractionSearchTaskIds = dbSearchStepFractionIdToTaskId;
                        } else if(hasDiaDb) {
                            if (!diaDbPeptideSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = diaDbPeptideSummarizeTaskId.get();
                            fractionSearchTaskIds = diaDbSearchStepFractionIdToTaskId;
                        } else if (hasSlProteinInference) {
                            if (!slPeptideSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = slPeptideSummarizeTaskId.get();
                            fractionSearchTaskIds = slSearchStepFractionIdToTaskId;
                        } else {
                            continue;
                        }
                        final PsmFilter psmFilter;
                        final WorkflowStep filterSummarizationWorkflowStep;
                        if (hasDb) { //dda
                            filterSummarizationWorkflowStep = analysis.workflowStep().get(DB_FILTER_SUMMARIZATION);
                            IdentificaitonFilter dbFilterSummarizationParameters = filterSummarizationWorkflowStep.getDbFilterSummarizationParameters();
                            psmFilter = PsmFilter.fromDto(dbFilterSummarizationParameters, fdrValue -> fdrToPValue(analysis.keyspace(), searchSummariationTaskId, fdrValue, FdrType.PSM, false));
                            fractionPreDBSearchTaskIds = dbPreSearchStepFractionIdToTaskId;
                        } else { //dia
                            if (hasDiaDb) {
                                filterSummarizationWorkflowStep = analysis.workflowStep().get(DIA_DB_FILTER_SUMMARIZE);
                            } else {
                                filterSummarizationWorkflowStep = analysis.workflowStep().get(SL_FILTER_SUMMARIZATION);
                            }                            SlFilterSummarizationParameters slFilterSummarizationParameters = filterSummarizationWorkflowStep.getSlFilterSummarizationParameters();
                            IdentificaitonFilter filter = slFilterSummarizationParameters.getFilters();
                            psmFilter = PsmFilter.fromDto(filter, fdrValue -> fdrToPValue(analysis.keyspace(), searchSummariationTaskId, fdrValue, FdrType.PSM, true));
                            fractionPreDBSearchTaskIds = new HashMap<>();
                        }

                        final Map<UUID, UUID> featureTaskIdToFractionTaskId;
                        if (hasSlProteinInference || hasDiaDb) {
                            featureTaskIdToFractionTaskId = featureDetectionStepFractionIdToTaskId;
                        } else{
                            featureTaskIdToFractionTaskId = dataRefineStepFractionIdToTaskId;
                        }
                        List<OrderedSampleFractions> sampleFractionsToInclude = analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .collect(Collectors.toList());
                        List<UUID> fractionIdsForTransfer = samples.stream()
                            .filter(sample -> parameters.sampleIdsForIdTransfer().contains(sample.sampleId()))
                            .flatMap(sample -> sample.fractionIds().stream())
                            .collect(Collectors.toList());
                        tasks.addAll(indexedFractionLevelTask(analysis, stepType, sampleFractionsToInclude, (fractionIndex, fractionIds) -> {
                            fractionIds = fractionIds.stream().filter(f -> samplesToInclude.contains(fractionIdToSampleIdMap.get(f))).collect(Collectors.toList());
                            List<UUID> fractionIdsWithTransfers = Streams.concat(fractionIdsForTransfer.stream(), fractionIds.stream())
                                .collect(Collectors.toList());
                            if (!featureTaskIdToFractionTaskId.keySet().containsAll(fractionIds)) return Optional.empty();
                            if (!fractionSearchTaskIds.keySet().containsAll(fractionIds)) return Optional.empty();
                            if (hasDb && !fractionPreDBSearchTaskIds.keySet().containsAll(fractionIds)) return Optional.empty();

                            RetentionTimeAlignmentTaskBuilder retentionTimeAlignmentTaskBuilder = new RetentionTimeAlignmentTaskBuilder();
                            if(hasDb) {
                                retentionTimeAlignmentTaskBuilder.ddaPeptideFilter(PeptideFilter.fromProto(
                                    filterSummarizationWorkflowStep.getDbFilterSummarizationParameters().getIdPeptideFilter(),
                                    fdrValue -> fdrToPValue(analysis.keyspace(), searchSummariationTaskId, fdrValue, FdrType.PEPTIDE, false)
                                ));
                            } else {
                                retentionTimeAlignmentTaskBuilder.diaPeptideFilter(SlPeptideFilter.fromProto(
                                    filterSummarizationWorkflowStep.getSlFilterSummarizationParameters().getFilters().getSlPeptideFilter(),
                                    fdrValue -> fdrToPValue(analysis.keyspace(), searchSummariationTaskId, fdrValue, FdrType.PEPTIDE, true)
                                ));
                            }

                            return Optional.of(retentionTimeAlignmentTaskBuilder
                                .fractionIds(fractionIdsWithTransfers)
                                .workflowId(projectId)
                                .idSummarizeTaskId(searchSummariationTaskId)
                                .fracIdToDataRefineTaskId(Maps.filterKeys(featureTaskIdToFractionTaskId, fractionIdsWithTransfers::contains))
                                .fractionIdToPsmRepositoryTaskId(Maps.filterKeys(fractionSearchTaskIds, fractionIdsWithTransfers::contains))
                                .psmFilter(psmFilter)
                                .parameters(parameters)
                                .acquisitionMethod(acquisitionMethod)
                                .fractionIdsForTransfer(fractionIdsForTransfer)
                                .fractionIdToPreDBSearchTaskId(fractionPreDBSearchTaskIds)
                                .build()
                            );
                        }));
                        break;
                    }
                    case LFQ_FEATURE_ALIGNMENT: {
                        final LabelFreeQuantificationParameters parameters = (LabelFreeQuantificationParameters) Helper.workflowStepParameters(step);
                        final Map<UUID, UUID> fractionSearchTaskIds;

                        final UUID searchSummariationTaskId;
                        if (hasSpider) {
                            if (!spiderSearchSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = spiderSearchSummarizeTaskId.get();
                            fractionSearchTaskIds = Maps.asMap(dbSearchStepFractionIdToTaskId.keySet(), fractionId -> searchSummariationTaskId);
                        } else if (hasPtm) {
                            if (!ptmSearchSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = ptmSearchSummarizeTaskId.get();
                            fractionSearchTaskIds = Maps.asMap(dbSearchStepFractionIdToTaskId.keySet(), fractionId -> searchSummariationTaskId);
                        } else if (hasDb) {
                            if (!dbSearchSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = dbSearchSummarizeTaskId.get();
                            fractionSearchTaskIds = dbSearchStepFractionIdToTaskId;
                        } else if (hasDiaDb) {
                            if (!diaDbPeptideSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = diaDbPeptideSummarizeTaskId.get();
                            fractionSearchTaskIds = diaDbSearchStepFractionIdToTaskId;
                        } else if (hasSlProteinInference) {
                            if (!slPeptideSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = slPeptideSummarizeTaskId.get();
                            fractionSearchTaskIds = slSearchStepFractionIdToTaskId;
                        } else {
                            continue;
                        }

                        final PsmFilter psmFilter;
                        final WorkflowStep filterSummarizationWorkflowStep;
                        if (hasDb) { //dda
                            filterSummarizationWorkflowStep = analysis.workflowStep().get(DB_FILTER_SUMMARIZATION);
                            IdentificaitonFilter dbFilterSummarizationParameters = filterSummarizationWorkflowStep.getDbFilterSummarizationParameters();
                            psmFilter = PsmFilter.fromDto(dbFilterSummarizationParameters, fdrValue -> fdrToPValue(analysis.keyspace(), searchSummariationTaskId, fdrValue, FdrType.PSM, false));
                        } else { //dia
                            if (hasDiaDb) {
                                filterSummarizationWorkflowStep = analysis.workflowStep().get(DIA_DB_FILTER_SUMMARIZE);
                            } else {
                                filterSummarizationWorkflowStep = analysis.workflowStep().get(SL_FILTER_SUMMARIZATION);
                            }
                            SlFilterSummarizationParameters slFilterSummarizationParameters = filterSummarizationWorkflowStep.getSlFilterSummarizationParameters();
                            IdentificaitonFilter filter = slFilterSummarizationParameters.getFilters();
                            psmFilter = PsmFilter.fromDto(filter, fdrValue -> fdrToPValue(analysis.keyspace(), searchSummariationTaskId, fdrValue, FdrType.PSM, true));
                        }
                        WorkflowStep rtStep = analysis.workflowStep().get(StepType.LFQ_RETENTION_TIME_ALIGNMENT);
                        final RetentionTimeAlignmentParameters rtParameters = (RetentionTimeAlignmentParameters) Helper.workflowStepParameters(rtStep);
                        List<UUID> fractionIdsForTransfer = samples.stream()
                            .filter(sample -> rtParameters.sampleIdsForIdTransfer().contains(sample.sampleId()))
                            .flatMap(sample -> sample.fractionIds().stream())
                            .collect(Collectors.toList());
                        final Set<UUID> samplesToInclude = Streams.concat(
                                parameters.groups().stream().flatMap(g -> g.sampleIds().stream()),
                                rtParameters.sampleIdsForIdTransfer().stream()
                            )
                            .collect(Collectors.toSet());

                        final Map<UUID, UUID> fractionIdToDataRefineTaskId;
                        if (hasSlProteinInference || hasDiaDb) {
                            fractionIdToDataRefineTaskId = featureDetectionStepFractionIdToTaskId;
                        } else{
                            fractionIdToDataRefineTaskId = dataRefineStepFractionIdToTaskId;
                        }

                        Map<UUID, Integer> fractionIdToIndex = new HashMap<>();
                        analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .forEach(sampleFractions -> {
                                final List<UUID> fractionIds = sampleFractions.fractionIds();
                                for (int i = 0; i < fractionIds.size(); i++) {
                                    fractionIdToIndex.put(fractionIds.get(i), i);
                                }
                            });

                        tasks.addAll(fractionLevelTask(analysis, stepType, (sampleId, fractionId) -> {
                            final Integer fractionIndex;
                            if (fractionIdsForTransfer.contains(fractionId)) {
                                //We don't do rt align on fractions for transfer, but they will have a unit predictor
                                //  in all rt alignment tasks. Therefore we can use the first index.
                                fractionIndex = 0;
                            } else {
                                fractionIndex = fractionIdToIndex.get(fractionId);
                            }
                            final UUID retentionTimeAlignmentTaskId = retentionTimeAlignmentFractionIndexToTaskId.get(fractionIndex);

                            if (retentionTimeAlignmentTaskId == null) return Optional.empty();
                            if (!fractionIdToDataRefineTaskId.containsKey(fractionId)) return Optional.empty();
                            if (!fractionSearchTaskIds.containsKey(fractionId)) return Optional.empty();
                            if (hasDb && !dbPreSearchStepFractionIdToTaskId.containsKey(fractionId)) return Optional.empty();

                            FeatureAlignmentTaskBuilder featureAlignmentTaskBuilder = new FeatureAlignmentTaskBuilder();
                            if (hasDb) {
                                featureAlignmentTaskBuilder.ddaPeptideFilter(PeptideFilter.fromProto(
                                    filterSummarizationWorkflowStep.getDbFilterSummarizationParameters().getIdPeptideFilter(),
                                    fdrValue -> fdrToPValue(analysis.keyspace(), searchSummariationTaskId, fdrValue, FdrType.PEPTIDE, false)
                                ))
                                    .preDBSearchTaskId(dbPreSearchStepFractionIdToTaskId.get(fractionId));
                            } else {
                                featureAlignmentTaskBuilder.diaPeptideFilter(SlPeptideFilter.fromProto(
                                    filterSummarizationWorkflowStep.getSlFilterSummarizationParameters().getFilters().getSlPeptideFilter(),
                                    fdrValue -> fdrToPValue(analysis.keyspace(), searchSummariationTaskId, fdrValue, FdrType.PEPTIDE, true)
                                ));
                            }
                            //clear some fields to reduce the message size
                            LabelFreeQuantificationParametersBuilder parametersBuilder = new LabelFreeQuantificationParametersBuilder();
                            LabelFreeQuantificationParameters currentParam = parametersBuilder.from(parameters).groups(
                                ImmutableList.of(new LabelFreeGroupBuilder().groupName("").addSampleIds(sampleId).hexColour("").build())
                            ).build();
                            List<OrderedSampleFractions> currentSample = new ArrayList<>();
                            for (OrderedSampleFractions s : samples) {
                                if (s.sampleId().equals(sampleId)) {
                                    currentSample.add(s);
                                }
                            }
                            return Optional.of(featureAlignmentTaskBuilder
                                .workflowId(projectId)
                                .retentionTimeAlignmentTaskId(retentionTimeAlignmentTaskId)
                                .idSummarizeTaskId(searchSummariationTaskId)
                                .fractionId(fractionId)
                                .dataLoadingTaskId(dataLoadingStepFractionIdToTaskId.get(fractionId))
                                .dataRefineTaskId(fractionIdToDataRefineTaskId.get(fractionId))
                                .searchTaskId(fractionSearchTaskIds.get(fractionId))
                                .parameters(currentParam)
                                .psmFilter(psmFilter)
                                .peptideSummarizationTaskId(searchSummariationTaskId)
                                .allOrderedSampleFractions(currentSample)
                                .acquisitionMethod(acquisitionMethod)
                                .isFractionAssistedTransfer(fractionIdsForTransfer.size() > 0)
                                .build()
                            );
                        }));
                        break;
                    }
                    case LFQ_SUMMARIZE: {
                        if (hasDb) {
                            if (!dbFilterSummarizeTaskId.isPresent()) continue;
                        } else if (hasDiaDb) {
                            if (!diaDbFilterSummarizeTaskId.isPresent()) continue;
                        } else if (hasSlProteinInference) {
                            if (!slFilterSummarizationTaskId.isPresent()) continue;
                        } else {
                            throw new IllegalStateException("LFQ step not properly supported");
                        }

                        final LabelFreeQuantificationParameters parameters = (LabelFreeQuantificationParameters) Helper.workflowStepParameters(step);
                        final Set<UUID> samplesToInclude = parameters.groups().stream().flatMap(g -> g.sampleIds().stream())
                            .collect(Collectors.toSet());
                        List<OrderedSampleFractions> sampleFractionsToInclude = analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .collect(Collectors.toList());
                        WorkflowStep rtStep = analysis.workflowStep().get(StepType.LFQ_RETENTION_TIME_ALIGNMENT);
                        final RetentionTimeAlignmentParameters rtParameters = (RetentionTimeAlignmentParameters) Helper.workflowStepParameters(rtStep);
                        List<UUID> fractionIdsForTransfer = samples.stream()
                            .filter(sample -> rtParameters.sampleIdsForIdTransfer().contains(sample.sampleId()))
                            .flatMap(sample -> sample.fractionIds().stream())
                            .collect(Collectors.toList());
                        tasks.addAll(indexedFractionLevelTask(analysis, stepType, sampleFractionsToInclude, (fractionIndex, fractionIds) -> {
                            final UUID retentionTimeAlignmentTaskId = retentionTimeAlignmentFractionIndexToTaskId.get(fractionIndex);
                            fractionIds = fractionIds.stream().filter(f -> samplesToInclude.contains(fractionIdToSampleIdMap.get(f))).collect(Collectors.toList());
                            List<UUID> fractionIdsWithTransfers = Streams.concat(fractionIdsForTransfer.stream(), fractionIds.stream())
                                .collect(Collectors.toList());
                            if (retentionTimeAlignmentTaskId == null) return Optional.empty();
                            for (UUID fractionId : fractionIdsWithTransfers) {
                                if (featureAlignmentFractionIdToTaskId.get(fractionId) == null) {
                                    return Optional.empty();
                                }
                            }

                            Map<UUID, UUID> fractionIdToFeatureAlignmentId = Maps.asMap(new HashSet<>(fractionIdsWithTransfers), fractionId -> featureAlignmentFractionIdToTaskId.get(fractionId));
                            final LabelFreeSummarizeTaskBuilder builder;
                            if (acquisitionMethod == AcquisitionMethod.DDA) {
                                ProteinDecoyInfo proteinDecoyInfo = proteinDecoyInfo(analysis.keyspace(), dbFilterSummarizeTaskId.get());
                                IDFilter.ProteinHitFilter dbProteinHitFilter = analysis.workflowStep().get(DB_FILTER_SUMMARIZATION).getDbProteinHitFilter();
                                ProteinHitFilter proteinHitFilter = ProteinHitFilter.fromDto(dbProteinHitFilter,
                                        fdr -> DecoyInfoWrapper.fdrToProScoreCountBased(proteinDecoyInfo, fdr, dbProteinHitFilter.getMinUniquePeptides()),
                                        pvalue -> DecoyInfoWrapper.proScoreToFdrCountBased(proteinDecoyInfo, pvalue, dbProteinHitFilter.getMinUniquePeptides())
                                );
                                builder = new LabelFreeSummarizeTaskBuilder()
                                    .proteinHitTaskId(dbFilterSummarizeTaskId.get())
                                    .proteinHitFilter(
                                            proteinHitFilter
                                    )
                                    .acquisitionMethod(acquisitionMethod);
                            } else if (acquisitionMethod == AcquisitionMethod.DIA) {
                                final SlFilter.ProteinHitFilter slFilter;
                                final UUID summarizeTaskId;
                                if (hasDiaDb) {
                                    slFilter = analysis.workflowStep().get(DIA_DB_FILTER_SUMMARIZE).getSlFilter();
                                    summarizeTaskId = diaDbFilterSummarizeTaskId.get();
                                } else {
                                    slFilter = analysis.workflowStep().get(SL_FILTER_SUMMARIZATION).getSlFilter();
                                    summarizeTaskId = slFilterSummarizationTaskId.get();
                                }

                                ProteinDecoyInfo proteinDecoyInfo = proteinDecoyInfo(analysis.keyspace(), summarizeTaskId);
                                SlProteinHitFilter slProteinHitFilter = SlProteinHitFilter.fromProto(slFilter,
                                        fdr -> DecoyInfoWrapper.fdrToProScore((ProteinDecoyInfo) proteinDecoyInfo, fdr, slFilter.getMinUniquePeptides()),
                                        pvalue -> DecoyInfoWrapper.proScoreToFdr((ProteinDecoyInfo) proteinDecoyInfo, pvalue, slFilter.getMinUniquePeptides())
                                );

                                builder = new LabelFreeSummarizeTaskBuilder()
                                    .proteinHitTaskId(summarizeTaskId)
                                    .slProteinHitFilter(slProteinHitFilter)
                                    .acquisitionMethod(acquisitionMethod);
                            } else {
                                throw new IllegalStateException("LFQ step not properly supported");
                            }

                            return Optional.of(builder
                                .workflowId(projectId)
                                .fractionIds(fractionIdsWithTransfers)
                                .rtAlignmentTaskId(retentionTimeAlignmentTaskId)
                                .fractionIdToFeatureAlignmentId(fractionIdToFeatureAlignmentId)
                                .parameters(parameters)
                                .fractionsForIdTransfer(fractionIdsForTransfer)
                                .build()
                            );
                        }));
                        break;
                    }
                    case DENOVO_FILTER: {
                        tasks.addAll(sampleLevelTaskParamaters(analysis, stepType, sampleFractions -> {
                            DenovoSummarizeTaskParameters.Builder taskParametersBuilder = DenovoSummarizeTaskParameters.newBuilder();
                            List<UUID> fractionIds = sampleFractions.fractionIds();
                            for(UUID fractionId : fractionIds) {
                                final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                                final UUID denovoTaskId = denovoStepFractionIdToTaskId.get(fractionId);
                                final UUID diaDenovoTaskId = diaDataRefineStepFractionIdToTaskId.get(fractionId);
                                if (denovoTaskId == null) return Optional.empty();
                                if (!hasDiaDenovo && dataRefineTaskId == null) return Optional.empty();
                                if (hasDiaDenovo && diaDenovoTaskId == null) return Optional.empty();

                                final String fractionIdString = fractionId.toString();
                                taskParametersBuilder.putDataRefineFractionIdToTaskId(fractionIdString, hasDiaDenovo ? diaDenovoTaskId.toString() : dataRefineTaskId.toString());
                                taskParametersBuilder.putDenovoStepFractionIdToTaskId(fractionIdString, denovoTaskId.toString());
                            }


                            taskParametersBuilder.setSample(CommonTasksFactory.sampleLevelTask(sampleFractions.sampleId(), sampleFractions.fractionIds()));
                            TaskParameters taskParameters = TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setDenovoSummarizeTaskParameters(taskParametersBuilder.build())
                                .setService(DenovoSummarizeService.NAME)
                                .build();

                            return Optional.of(taskParameters);
                        }));
                        break;
                    }
                    case DB_FILTER_SUMMARIZATION: {
                        if (!dbSearchSummarizeTaskId.isPresent()) continue;
                        if (hasDenovo && sampleIdToDenovoFilterTaskId.size() < samples.size()) continue;

                        IdentificationSummaryFilterTaskParameters.Builder taskParametersBuilder = IdentificationSummaryFilterTaskParameters.newBuilder();
                        IdentificaitonFilter filter = (IdentificaitonFilter) Helper.workflowStepParameters(step);

                        for (UUID fractionId : analysisFractionIds) {
                            final String fractionIdString = fractionId.toString();

                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) throw new IllegalStateException("dbSearchSummarizeTaskId exists, but dataLoadingTaskId missing");
                            taskParametersBuilder.putDataLoadingFractionIdToTaskId(fractionIdString, dataLoadingTaskId.toString());

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataRefineTaskId == null) throw new IllegalStateException("dbSearchSummarizeTaskId exists, but dataRefineTaskId missing");
                            taskParametersBuilder.putDataRefineFractionIdToTaskId(fractionIdString, dataRefineTaskId.toString());

                            final UUID preSreachTaskId = dbPreSearchStepFractionIdToTaskId.get(fractionId);
                            if (preSreachTaskId == null) throw new IllegalStateException("dbSearchSummarizeTaskId exists, but preSreachTaskId missing");
                            taskParametersBuilder.putFractionIdToPreDBSearchTaskId(fractionIdString, preSreachTaskId.toString());

                            final UUID searchTaskId = dbSearchStepFractionIdToTaskId.get(fractionId);
                            if (searchTaskId == null) throw new IllegalStateException("dbSearchSummarizeTaskId exists, but searchTaskId missing");
                            taskParametersBuilder.putFractionIdToPsmTaskId(fractionIdString, searchTaskId.toString());
                        }

                        DBSearchParameters parameters = (DBSearchParameters) stepParameters.get(StepType.DB_SUMMARIZE);

                        for (UUID sampleId : sampleIdToDenovoFilterTaskId.keySet()) {
                            taskParametersBuilder.putSampleIdToDenovoSummarizeTaskId(sampleId.toString(), sampleIdToDenovoFilterTaskId.get(sampleId).toString());
                            taskParametersBuilder.putSampleIdToTagSearchSummarizeTaskId(sampleId.toString(), tagSummarizeStepSampleIdToTaskId.get(sampleId).toString());
                        }
                        int keepTopProteinGroups = localconfig.getConfig(PeaksParameters.ROOT + DBSearchConservedParameters.SECTION)
                            .getInt("topProteinGroups");


                        final float psmPValue = psmPValueFromIdentificationFilter(analysis.keyspace(), filter, dbSearchSummarizeTaskId.get(), false);
                        final float peptidePValue = peptidePValueFromIdentificationFilter(analysis.keyspace(), filter, dbSearchSummarizeTaskId.get(), false);

                        taskParametersBuilder
                            .setMinPsmPValue(psmPValue)
                            .setMinPeptidePValue(peptidePValue)
                            .setEnzyme(DtoConversion.toProtoEnzyme(parameters.enzyme()))
                            .setMinMutationIonIntensity(0)
                            .addAllSamples(samples.stream().map(i -> CommonTasksFactory.sampleLevelTask(i.sampleId(), i.fractionIds()))::iterator)
                            .setSummarizeTaskId(dbSearchSummarizeTaskId.get().toString())
                            .addAllFastaHeaderParser(analysis.fastaHeaderParsers())
                            .setMinDenovoAlc(filter.getMinDenovoAlc())
                            .setKeepTopProteinGroup(keepTopProteinGroups)
                            .setFdrType(filter.getFdrType());

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setIdentificationSummaryFilterTaskParameters(taskParametersBuilder)
                            .setService(DbIdentificationSummaryFilterService.NAME)
                            .build();
                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case PTM_FINDER_FILTER_SUMMARIZATION: {
                        if (!ptmSearchSummarizeTaskId.isPresent()) continue;
                        if (hasDenovo && sampleIdToDenovoFilterTaskId.size() < samples.size()) continue;
                        IdentificationSummaryFilterTaskParameters.Builder taskParametersBuilder = IdentificationSummaryFilterTaskParameters.newBuilder();
                        IdentificaitonFilter filter = (IdentificaitonFilter) Helper.workflowStepParameters(step);

                        for (UUID fractionId : analysisFractionIds) {
                            final String fractionIdString = fractionId.toString();

                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) throw new IllegalStateException("ptmSearchSummarizeTaskId exists, but dataLoadingTaskId missing");
                            taskParametersBuilder.putDataLoadingFractionIdToTaskId(fractionIdString, dataLoadingTaskId.toString());

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataRefineTaskId == null) throw new IllegalStateException("ptmSearchSummarizeTaskId exists, but dataRefineTaskId missing");
                            taskParametersBuilder.putDataRefineFractionIdToTaskId(fractionIdString, dataRefineTaskId.toString());

                            final UUID preSreachTaskId = dbPreSearchStepFractionIdToTaskId.get(fractionId);
                            if (preSreachTaskId == null) throw new IllegalStateException("ptmSearchSummarizeTaskId exists, but preSreachTaskId missing");
                            taskParametersBuilder.putFractionIdToPreDBSearchTaskId(fractionIdString, preSreachTaskId.toString());

                            taskParametersBuilder.putFractionIdToPsmTaskId(fractionIdString, ptmSearchSummarizeTaskId.get().toString());
                        }

                        PtmFinderParameters parameters = (PtmFinderParameters) stepParameters.get(StepType.PTM_FINDER_SUMMARIZE);

                        for (UUID sampleId : sampleIdToDenovoFilterTaskId.keySet()) {
                            taskParametersBuilder.putSampleIdToDenovoSummarizeTaskId(sampleId.toString(), sampleIdToDenovoFilterTaskId.get(sampleId).toString());
                            taskParametersBuilder.putSampleIdToTagSearchSummarizeTaskId(sampleId.toString(), tagSummarizeStepSampleIdToTaskId.get(sampleId).toString());
                        }
                        int keepTopProteinGroups = localconfig.getConfig(PeaksParameters.ROOT + PtmFinderConservedParameters.SECTION)
                            .getInt("topProteinGroups");

                        final float psmPValue = psmPValueFromIdentificationFilter(analysis.keyspace(), filter, ptmSearchSummarizeTaskId.get(), false);
                        final float peptidePValue = peptidePValueFromIdentificationFilter(analysis.keyspace(), filter, ptmSearchSummarizeTaskId.get(), false);
                        taskParametersBuilder
                            .setMinPsmPValue(psmPValue)
                            .setEnzyme(DtoConversion.toProtoEnzyme(parameters.enzyme()))
                            .setMinMutationIonIntensity(0)
                            .addAllSamples(samples.stream().map(i -> CommonTasksFactory.sampleLevelTask(i.sampleId(), i.fractionIds()))::iterator)
                            .setSummarizeTaskId(ptmSearchSummarizeTaskId.get().toString())
                            .addAllFastaHeaderParser(analysis.fastaHeaderParsers())
                            .setMinDenovoAlc(filter.getMinDenovoAlc())
                            .setMinPeptidePValue(peptidePValue)
                            .setKeepTopProteinGroup(keepTopProteinGroups)
                            .setFdrType(filter.getFdrType());

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setIdentificationSummaryFilterTaskParameters(taskParametersBuilder)
                            .setService(DbIdentificationSummaryFilterService .NAME)
                            .build();
                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;

                    }
                    case SPIDER_FILTER_SUMMARIZATION: {
                        if (!spiderSearchSummarizeTaskId.isPresent()) continue;
                        if (hasDenovo && sampleIdToDenovoFilterTaskId.size() < samples.size()) continue;
                        IdentificationSummaryFilterTaskParameters.Builder taskParametersBuilder = IdentificationSummaryFilterTaskParameters.newBuilder();
                        IdentificaitonFilter filter = (IdentificaitonFilter) Helper.workflowStepParameters(step);

                        for (UUID fractionId : analysisFractionIds) {
                            final String fractionIdString = fractionId.toString();

                            final UUID dataLoadingTaskId = dataLoadingStepFractionIdToTaskId.get(fractionId);
                            if (dataLoadingTaskId == null) throw new IllegalStateException("spiderSearchSummarizeTaskId exists, but dataLoadingTaskId missing");
                            taskParametersBuilder.putDataLoadingFractionIdToTaskId(fractionIdString, dataLoadingTaskId.toString());

                            final UUID dataRefineTaskId = dataRefineStepFractionIdToTaskId.get(fractionId);
                            if (dataRefineTaskId == null) throw new IllegalStateException("spiderSearchSummarizeTaskId exists, but dataRefineTaskId missing");
                            taskParametersBuilder.putDataRefineFractionIdToTaskId(fractionIdString, dataRefineTaskId.toString());

                            final UUID preSreachTaskId = dbPreSearchStepFractionIdToTaskId.get(fractionId);
                            if (preSreachTaskId == null) throw new IllegalStateException("spiderSearchSummarizeTaskId exists, but preSreachTaskId missing");
                            taskParametersBuilder.putFractionIdToPreDBSearchTaskId(fractionIdString, preSreachTaskId.toString());

                            taskParametersBuilder.putFractionIdToPsmTaskId(fractionIdString, spiderSearchSummarizeTaskId.get().toString());
                        }

                        SpiderParameters parameters = (SpiderParameters) stepParameters.get(StepType.SPIDER_SUMMARIZE);

                        for (UUID sampleId : sampleIdToDenovoFilterTaskId.keySet()) {
                            taskParametersBuilder.putSampleIdToDenovoSummarizeTaskId(sampleId.toString(), sampleIdToDenovoFilterTaskId.get(sampleId).toString());
                            taskParametersBuilder.putSampleIdToTagSearchSummarizeTaskId(sampleId.toString(), tagSummarizeStepSampleIdToTaskId.get(sampleId).toString());
                        }
                        int keepTopProteinGroups = localconfig.getConfig(PeaksParameters.ROOT + PtmFinderConservedParameters.SECTION)
                            .getInt("topProteinGroups");

                        final float psmPValue = psmPValueFromIdentificationFilter(analysis.keyspace(), filter, spiderSearchSummarizeTaskId.get(), false);
                        final float peptidePValue = peptidePValueFromIdentificationFilter(analysis.keyspace(), filter, spiderSearchSummarizeTaskId.get(), false);

                        taskParametersBuilder
                            .setMinPsmPValue(psmPValue)
                            .setMinPeptidePValue(peptidePValue)
                            .setEnzyme(DtoConversion.toProtoEnzyme(parameters.enzyme()))
                            .setMinMutationIonIntensity(filter.getMinMutationIonIntensity())
                            .addAllSamples(samples.stream().map(i -> CommonTasksFactory.sampleLevelTask(i.sampleId(), i.fractionIds()))::iterator)
                            .setSummarizeTaskId(spiderSearchSummarizeTaskId.get().toString())
                            .addAllFastaHeaderParser(analysis.fastaHeaderParsers())
                            .setMinDenovoAlc(filter.getMinDenovoAlc())
                            .setKeepTopProteinGroup(keepTopProteinGroups)
                            .setFdrType(filter.getFdrType());

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setIdentificationSummaryFilterTaskParameters(taskParametersBuilder)
                            .setService(DbIdentificationSummaryFilterService.NAME)
                            .build();
                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case LFQ_FILTER_SUMMARIZATION: {
                        final int fractionsPerSample = analysis.analysis().calculateFractionIndexLevelTaskCount(analysis, StepType.LFQ_RETENTION_TIME_ALIGNMENT);
                        if (lfqSummarizeFractionIndexToTaskId.size() < fractionsPerSample) continue;
                        LabelFreeQuantificationConservedParameters labelFreeQuantificationConservedParameters = PeaksParameters.fromConfig(
                            localconfig.getConfig(LabelFreeQuantificationConservedParameters.ROOT + LabelFreeQuantificationConservedParameters.SECTION),
                            LabelFreeQuantificationConservedParameters.class
                        );

                        LfqFilter filter = (LfqFilter) Helper.workflowStepParameters(step);

                        final LfqSummaryFilterParameters.Builder builder = LfqSummaryFilterParameters.newBuilder();
                        if (hasSpider) {
                            if (!spiderFilterSummarizeTaskId.isPresent()) continue;
                            builder.setProteinHitTaskId(ModelConversion.uuidToByteString(spiderFilterSummarizeTaskId.get()));
                        } else if (hasPtm) {
                            if (!ptmFinderFilterSummarizeTaskId.isPresent()) continue;
                            builder.setProteinHitTaskId(ModelConversion.uuidToByteString(ptmFinderFilterSummarizeTaskId.get()));
                        } else if (hasDb){
                            if (!dbFilterSummarizeTaskId.isPresent()) continue;
                            builder.setProteinHitTaskId(ModelConversion.uuidToByteString(dbFilterSummarizeTaskId.get()));
                        } else if (hasDiaDb) {
                            if (!diaDbFilterSummarizeTaskId.isPresent()) continue;
                            builder.setProteinHitTaskId(ModelConversion.uuidToByteString(diaDbFilterSummarizeTaskId.get()));
                        } else if(hasSlProteinInference) {
                            if (!slFilterSummarizationTaskId.isPresent()) continue;
                            builder.setProteinHitTaskId(ModelConversion.uuidToByteString(slFilterSummarizationTaskId.get()));
                        }

                        Map<String, String> fracIdToDataLoading = dataLoadingStepFractionIdToTaskId.entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));

                        final Set<UUID> samplesToInclude = filter.getLabelFreeQueryParameters().getGroupsList().stream()
                            .flatMap(g -> g.getSampleIndexesList().stream())
                            .map(sampleIdx -> analysis.samples().get(sampleIdx).sampleId())
                            .collect(Collectors.toSet());
                        final Set<UUID> fractionsToInclude = fractionIdToSampleIdMap.entrySet().stream()
                            .filter(e -> samplesToInclude.contains(e.getValue()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
                        List<OrderedSampleFractions> sampleFractionsToInclude = analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .collect(Collectors.toList());
                        HashSet<UUID> fractionIds = new HashSet<>();
                        retentionTimeAlignmentFractionIndexToTaskId.keySet().forEach(key -> {
                            for(OrderedSampleFractions sample: sampleFractionsToInclude) {
                                fractionIds.add(sample.fractionIds().get(key));
                            }
                        });

                        // rt alignment has not finished, so continue
                        if (fractionIds.size() != fractionsToInclude.size()) {
                            continue;
                        }

                        final HashSet<UUID> rtAlignTaskIds = new HashSet<>(retentionTimeAlignmentFractionIndexToTaskId.values());

                        builder
                            .setLfqFilter(filter)
                            .addAllLfqSummarizeTaskIds(Iterables.transform(lfqSummarizeFractionIndexToTaskId.values(), UUID::toString))
                            //should be all samples even those excluded from LFQ results
                            .addAllSamples(Iterables.transform(samples, i -> CommonTasksFactory.sampleLevelTask(i.sampleId(), i.fractionIds())))
                            .setMaxRatio(labelFreeQuantificationConservedParameters.maxRatio())
                            .setAttachIdentification(hasIdentification || hasSlProteinInference)
                            .putAllFractionIdToDataLoadTaskId(fracIdToDataLoading)
                            .addAllFastaHeaderParsers(analysis.fastaHeaderParsers())
                            .setCvFilterThresholdIQRRatio(labelFreeQuantificationConservedParameters.cvFilterThresholdIQRRatio())
                            .setRemoveOutlierIQRRatio(labelFreeQuantificationConservedParameters.removeOutlierIQRRatio())
                            .setAcquisitionMethod(acquisitionMethod)
                            .setRtAlignTaskIds(uuidsToByteString(rtAlignTaskIds));

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setLfqSummaryFilterParameters(builder)
                            .setService(LfqSummaryFilterService.NAME)
                            .build();

                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case DDA_SL_SEARCH:{
                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            if (featureDetectionStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            if (dataLoadingStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            if (dataRefineStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();

                            SlSearchTaskParameters taskParameters = SlSearchTaskParameters.newBuilder()
                                .setSlSearchParameters((SlSearchParameters) stepParameters.get(StepType.DDA_SL_SEARCH))
                                .setDataLoadingTaskId(ModelConversion.uuidToByteString(dataLoadingStepFractionIdToTaskId.get(fractionId)))
                                .setFeatureDetectionTaskId(uuidsToByteString(featureDetectionStepFractionIdToTaskId.get(fractionId)))
                                .setDdaDataRefinementTaskId(uuidsToByteString(dataRefineStepFractionIdToTaskId.get(fractionId)))
                                .setFractionId(uuidsToByteString(fractionId))
                                .setSaveTenPercentOnly(true)
                                .build();

                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(DdaSlSearchService.NAME)
                                .setSlSearchTaskParameters(taskParameters)
                                .build());
                        }));
                        break;
                    }
                    case SL_SEARCH: {
                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            if (featureDetectionStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            if (dataLoadingStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();

                            SlSearchTaskParameters taskParameters = SlSearchTaskParameters.newBuilder()
                                .setSlSearchParameters((SlSearchParameters) stepParameters.get(StepType.SL_SEARCH))
                                .setDataLoadingTaskId(ModelConversion.uuidToByteString(dataLoadingStepFractionIdToTaskId.get(fractionId)))
                                .setFeatureDetectionTaskId(uuidsToByteString(featureDetectionStepFractionIdToTaskId.get(fractionId)))
                                .setFractionId(uuidsToByteString(fractionId))
                                .setSaveTenPercentOnly(true)
                                .build();

                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(SlSearchService.NAME)
                                .setSlSearchTaskParameters(taskParameters)
                                .build());
                        }));
                        break;
                    }
                    case DIA_DB_SEARCH: {
                        if (!diaDbPreSearchTaskId.isPresent()) continue;
                        DiaDbSearchParameters diaDbSearchParameters = (DiaDbSearchParameters) stepParameters.get(DIA_DB_SEARCH);
                        final Optional<Float> slpsmPValue;
                        final Optional<Float> slPeptidePValue;
                        if (hasSlSearchStep) {
                            if (!slPeptideSummarizeTaskId.isPresent()) continue; //has sl but peptide summarize not done
                            WorkflowStep filterSummarizationWorkflowStep = analysis.workflowStep().get(SL_FILTER_SUMMARIZATION);
                            SlFilterSummarizationParameters slFilterSummarizationParameters = filterSummarizationWorkflowStep.getSlFilterSummarizationParameters();
                            IdentificaitonFilter filter = slFilterSummarizationParameters.getFilters();
                            slpsmPValue = Optional.of(PsmFilter.fromDto(filter, fdrValue -> fdrToPValue(analysis.keyspace(), slPeptideSummarizeTaskId.get(), fdrValue, FdrType.PSM, true))
                                .minPValue());
                            slPeptidePValue = Optional.of(SlPeptideFilter.fromProto(
                                filterSummarizationWorkflowStep.getSlFilterSummarizationParameters().getFilters().getSlPeptideFilter(),
                                fdrValue -> fdrToPValue(analysis.keyspace(), slPeptideSummarizeTaskId.get(), fdrValue, FdrType.PEPTIDE, true)
                            ).minPValue());
                        } else {
                            slpsmPValue = Optional.empty();
                            slPeptidePValue = Optional.empty();
                        }
                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            if (hasSlSearchStep && (!slFilterSummarizationTaskId.isPresent() || !slPeptideSummarizeTaskId.isPresent()))
                                return Optional.empty();
                            if (dataLoadingStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            if (featureDetectionStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            if (dbDiaDataRefineStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();

                            DiaDbSearchTaskParameters.Builder builder = DiaDbSearchTaskParameters.newBuilder();
                            if (hasSlSearchStep) {
                                builder
                                    .setSlPsmPValue(slpsmPValue.orElse(0f))
                                    .setSlPeptidePValue(slPeptidePValue.orElse(0f))
                                    .setSlFilterSummarizationTaskId(uuidsToByteString(slFilterSummarizationTaskId.get()))
                                    .setSlPeptideFilterSummarizationTaskId(uuidsToByteString(slPeptideSummarizeTaskId.get()))
                                    .setSlSearchTaskId(uuidsToByteString(slSearchStepFractionIdToTaskId.get(fractionId)));
                            }
                            DiaDbSearchTaskParameters diaDbSearchTaskParameters =
                                parametersPopulater.populateDiaDbSearchTaskParameters(builder, diaDbSearchParameters)
                                    .setDiaDbSearchParameters(diaDbSearchParameters)
                                    .setFractionId(uuidsToByteString(fractionId))
                                    .setFeatureDetectionTaskId(uuidsToByteString(featureDetectionStepFractionIdToTaskId.get(fractionId)))
                                    .setDataLoadingTaskId(uuidToByteString(dataLoadingStepFractionIdToTaskId.get(fractionId)))
                                    .setActivationMethod(analysis.getActivationMethodFromSample(sampleId))
                                    .addAllTargetDatabaseSources(analysis.targetDatabaseSources())
                                    .addAllContaminantDatabaseSources(analysis.contaminantDatabaseSources())
                                    .setDiaDataRefineTaskId(uuidsToByteString(dbDiaDataRefineStepFractionIdToTaskId.get(fractionId)))
                                    .setGeneratedLibraryId(uuidsToByteString(diaDbPreSearchTaskId.get()))
                                    .build();
                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(DiaDbSearchService.NAME)
                                .setDiaDbSearchTaskParameters(diaDbSearchTaskParameters)
                                .build());
                        }));
                        break;
                    }
                    case DIA_DB_PRE_SEARCH: {
                        if (hasSlSearchStep && (!slFilterSummarizationTaskId.isPresent() || !slPeptideSummarizeTaskId.isPresent())) continue;
                        if (dataLoadingStepFractionIdToTaskId.size() != fractionIdToSampleIdMap.size()) continue;
                        if (featureDetectionStepFractionIdToTaskId.size() != fractionIdToSampleIdMap.size()) continue;
                        DiaDbPreSearchParameters diaDbPreSearchParameters = (DiaDbPreSearchParameters) stepParameters.get(DIA_DB_PRE_SEARCH);
                        final Optional<Float> slpsmPValue;
                        final Optional<Float> slPeptidePValue;
                        if (hasSlSearchStep) {
                            if (!slPeptideSummarizeTaskId.isPresent()) continue; //has sl but peptide summarize not done
                            WorkflowStep filterSummarizationWorkflowStep = analysis.workflowStep().get(SL_FILTER_SUMMARIZATION);
                            SlFilterSummarizationParameters slFilterSummarizationParameters = filterSummarizationWorkflowStep.getSlFilterSummarizationParameters();
                            IdentificaitonFilter filter = slFilterSummarizationParameters.getFilters();
                            slpsmPValue = Optional.of(PsmFilter.fromDto(filter, fdrValue -> fdrToPValue(analysis.keyspace(), slPeptideSummarizeTaskId.get(), fdrValue, FdrType.PSM, true))
                                .minPValue());
                            slPeptidePValue = Optional.of(SlPeptideFilter.fromProto(
                                filterSummarizationWorkflowStep.getSlFilterSummarizationParameters().getFilters().getSlPeptideFilter(),
                                fdrValue -> fdrToPValue(analysis.keyspace(), slPeptideSummarizeTaskId.get(), fdrValue, FdrType.PEPTIDE, true)
                            ).minPValue());
                        } else {
                            slpsmPValue = Optional.empty();
                            slPeptidePValue = Optional.empty();
                        }

                        DiaPreSearchTaskParameters.Builder builder = DiaPreSearchTaskParameters.newBuilder();
//                        if (hasSlSearchStep) {
//                            builder
//                                .setSlPsmPValue(slpsmPValue.orElse(0f))
//                                .setSlPeptidePValue(slPeptidePValue.orElse(0f))
//                                .setSlFilterSummarizationTaskId(uuidsToByteString(slFilterSummarizationTaskId.get()))
//                                .setSlPeptideFilterSummarizationTaskId(uuidsToByteString(slPeptideSummarizeTaskId.get()))
//                                .setSlSearchTaskId(uuidsToByteString(slSearchStepFractionIdToTaskId.get(fractionId)));
//                        }
                        OrderedSampleFractions sampleFractions = analysis.samples().get(0);
                        UUID fractionId = sampleFractions.fractionIds().get(0);
                        FractionAttributes fractionAttributes =
                            storageFactory.getStorage(analysis.keyspace()).getFractionRepository()
                            .getAttributesById(fractionId)
                            .toCompletableFuture().join().get();

                        DiaPreSearchTaskParameters diaPreSearchTaskParameters =
                            parametersPopulater.populateDiaDbPreSearchTaskParameters(builder, diaDbPreSearchParameters)
                                .setActivationMethod(analysis.getActivationMethodFromSample(sampleFractions.sampleId()))
                                .setDiaDbPreSearchParameters(diaDbPreSearchParameters)
                                .addAllTargetDatabaseSources(analysis.targetDatabaseSources())
                                .addAllContaminantDatabaseSources(analysis.contaminantDatabaseSources())
                                .setIsTimsTof(fractionAttributes.hasIonMobility())
                                .setEnzymeFromFraction(fractionAttributes.enzyme().proto())
                                .build();

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setService(DiaDbPreSearchService.NAME)
                            .setDiaPreSearchTaskParameters(diaPreSearchTaskParameters)
                            .build();
                        tasks.add(taskDescriptionAnalysisDataIndependentLevel(taskParameters, stepType));
                        break;
                    }
                    case SL_PEPTIDE_SUMMARIZATION:
                    case DIA_DB_PEPTIDE_SUMMARIZE:{
                        if (stepType == StepType.SL_PEPTIDE_SUMMARIZATION && slSearchStepFractionIdToTaskId.size() != fractionIdToSampleIdMap.size()) continue;
                        if (stepType == StepType.DIA_DB_PEPTIDE_SUMMARIZE) {
                            if (diaDbSearchStepFractionIdToTaskId.size() != fractionIdToSampleIdMap.size()) {
                                continue;
                            }
                        }

                        SlPeptideSummarizationParameters summarizationParameters = (SlPeptideSummarizationParameters) stepParameters.get(stepType);

                        SlPeptideSummarizationTaskParameters.Builder builder = SlPeptideSummarizationTaskParameters.newBuilder();

                        List<UUID> slPsmTaskIds;

                        if (stepType == StepType.SL_PEPTIDE_SUMMARIZATION) {
                            builder.setSlPeptideSummarizationParameters(summarizationParameters);
                            slPsmTaskIds = analysisFractionIds.stream()
                                .map(slSearchStepFractionIdToTaskId::get)
                                .collect(Collectors.toList());
                        } else {
                            SlPeptideSummarizationParameters summarizationParametersWithDiaDbLibraryGenerationLibId =
                                SlPeptideSummarizationParameters.newBuilder().mergeFrom(summarizationParameters)
                                    .build();
                            slPeptideSummarizeTaskId.ifPresent(uuid -> builder.setPreviousSlPeptideSummarizationTaskId(ModelConversion.uuidToByteString(uuid)));
                            builder
                                .setSlPeptideSummarizationParameters(summarizationParametersWithDiaDbLibraryGenerationLibId);
                            slPsmTaskIds = analysisFractionIds.stream()
                                .map(diaDbSearchStepFractionIdToTaskId::get)
                                .collect(Collectors.toList());
                        }

                        builder
                            .setSampleFractions(OrderedSampleFractions.proto(samples))
                            .setSlSearchTaskIds(uuidsToByteString(slPsmTaskIds))
                            .addAllTargetDatabaseSources(analysis.targetDatabaseSources())
                            .addAllContaminantDatabaseSources(analysis.contaminantDatabaseSources())
                            .setStepType(stepType);

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setService(SlPeptideSummarizationService.NAME)
                            .setSlPeptideSummarizationTaskParameters(builder)
                            .build();

                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case SL_FILTER_SUMMARIZATION:
                    case DIA_DB_FILTER_SUMMARIZE:{
                        if (stepType == StepType.SL_FILTER_SUMMARIZATION && !slPeptideSummarizeTaskId.isPresent()) continue;
                        if (stepType == StepType.DIA_DB_FILTER_SUMMARIZE && !diaDbPeptideSummarizeTaskId.isPresent()) continue;
                        SlFilterSummarizationParameters slFilterSummarizationParameters = (SlFilterSummarizationParameters) stepParameters.get(stepType);

                        if (stepType == StepType.SL_FILTER_SUMMARIZATION && slSearchStepFractionIdToTaskId.size() != fractionIdToSampleIdMap.size()) continue;
                        if (stepType == StepType.DIA_DB_FILTER_SUMMARIZE && diaDbSearchStepFractionIdToTaskId.size() != fractionIdToSampleIdMap.size()) continue;
                        List<UUID> slPsmTaskIds;
                        UUID peptideSummarizeId;

                        if (stepType == StepType.SL_FILTER_SUMMARIZATION) {
                            peptideSummarizeId = slPeptideSummarizeTaskId.get();
                            slPsmTaskIds = analysisFractionIds.stream()
                                .map(slSearchStepFractionIdToTaskId::get)
                                .collect(Collectors.toList());
                        } else {
                            peptideSummarizeId = diaDbPeptideSummarizeTaskId.get();
                            slPsmTaskIds = analysisFractionIds.stream()
                                .map(diaDbSearchStepFractionIdToTaskId::get)
                                .collect(Collectors.toList());
                        }

                        SlFilterSummarizationTaskParameters.Builder parametersBuilder = SlFilterSummarizationTaskParameters.newBuilder()
                            .setSlPeptideSummarizationTaskId(ModelConversion.uuidToByteString(peptideSummarizeId))
                            .setSlFilterSummarizationParameters(slFilterSummarizationParameters)
                            .setSampleFractions(OrderedSampleFractions.proto(samples))
                            .setSlSearchTaskIds(uuidsToByteString(slPsmTaskIds))
                            .addAllFastaHeaderParsers(analysis.fastaHeaderParsers());

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setService(SlFilterSummarizationService.NAME)
                            .setSlFilterSummarizationTaskParameters(parametersBuilder)
                            .build();

                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case SILAC_RT_ALIGNMENT: {
                        final SilacRtAlignmentParameters silacRtAlignmentParameters = (SilacRtAlignmentParameters) Helper.workflowStepParameters(step);
                        List<UUID> samplesToInclude = samples.stream().map(orderedSampleFraction -> orderedSampleFraction.sampleId()).collect(Collectors.toList());
                        SilacParameters silacParameters = analysis.workflowStep().get(SILAC_FILTER_SUMMARIZE).getSilacFilterSummarizationParamaters()
                            .getSilacParameters();
                        final Map<UUID, UUID> fractionSearchTaskIds;
                        final Map<UUID, UUID> fractionPreDBSearchTaskIds;
                        if (hasDb) {
                            if (!dbSearchSummarizeTaskId.isPresent()) continue;
                            fractionSearchTaskIds = dbSearchStepFractionIdToTaskId;
                            fractionPreDBSearchTaskIds = dbPreSearchStepFractionIdToTaskId;
                        } else if (hasSlProteinInference) {
                            if (!slPeptideSummarizeTaskId.isPresent()) continue;
                            fractionSearchTaskIds = slSearchStepFractionIdToTaskId;
                            fractionPreDBSearchTaskIds = new HashMap<>();
                        } else {
                            continue;
                        }

                        final Map<UUID, UUID> featureTaskIdToFractionTaskId;
                        if (hasSlProteinInference) {
                            featureTaskIdToFractionTaskId = featureDetectionStepFractionIdToTaskId;
                        } else{
                            featureTaskIdToFractionTaskId = dataRefineStepFractionIdToTaskId;
                        }
                        List<OrderedSampleFractions> analysisSamples = analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .collect(Collectors.toList());

                        Set<UUID> sampleIdsForTransfer = silacParameters.getTransferOnlyGroup().getSamplesList().stream()
                            .map(s -> UUID.fromString(s.getSampleId()))
                            .collect(Collectors.toSet());

                        Set<UUID> fractionIdsForTransfer = analysisSamples.stream()
                            .filter(s -> sampleIdsForTransfer.contains(s.sampleId()))
                            .flatMap(s -> s.fractionIds().stream())
                            .collect(Collectors.toSet());

                        List<OrderedSampleFractions> sampleFractionsToInclude = analysisSamples.stream()
                            .filter(s -> !sampleIdsForTransfer.contains(s.sampleId()))
                            .collect(Collectors.toList());

                        IdentificaitonFilter filter;
                        if (!hasDb) {
                            final SlFilterSummarizationParameters slFilterSummarizationParameters = (SlFilterSummarizationParameters) stepParameters.get(SL_FILTER_SUMMARIZATION);
                            filter = slFilterSummarizationParameters.getFilters();
                        } else {
                            filter = (IdentificaitonFilter) stepParameters.get(StepType.DB_FILTER_SUMMARIZATION);
                        }

                        final float psmPValue = psmPValueFromIdentificationFilter(analysis.keyspace(), filter, dbSearchSummarizeTaskId.get(), false);
                        final float peptidePValue = peptidePValueFromIdentificationFilter(analysis.keyspace(), filter, dbSearchSummarizeTaskId.get(), false);

                        tasks.addAll(indexedFractionLevelTaskProto(analysis, stepType, sampleFractionsToInclude, (fractionIndex, fractionIds) -> {
                            List<UUID> fractionIdWithTransfers = Streams.concat(fractionIds.stream(), fractionIdsForTransfer.stream()).collect(Collectors.toList());
                            final Optional<UUID> optPeptideTaskId = analysis.hasSlSearch() ? slPeptideSummarizeTaskId : dbSearchSummarizeTaskId;
                            if (!featureTaskIdToFractionTaskId.keySet().containsAll(fractionIdWithTransfers)) return Optional.empty();
                            if (!fractionSearchTaskIds.keySet().containsAll(fractionIdWithTransfers)) return Optional.empty();
                            if (!optPeptideTaskId.isPresent()) return Optional.empty();
                            if (hasDb && !fractionPreDBSearchTaskIds.keySet().containsAll(fractionIdWithTransfers)) return Optional.empty();
                            List<UUID> featureTaskIds = fractionIdWithTransfers.stream().map(featureTaskIdToFractionTaskId::get).collect(Collectors.toList());
                            List<UUID> psmTaskIds = fractionIdWithTransfers.stream().map(fractionSearchTaskIds::get).collect(Collectors.toList());
                            SilacRTAlignmentTaskParameters.Builder taskParamBuilder = SilacRTAlignmentTaskParameters.newBuilder();
                            if (hasDb) {
                                List<UUID> preDBSearchTaskIds = fractionIdWithTransfers.stream().map(fractionPreDBSearchTaskIds::get).collect(Collectors.toList());
                                taskParamBuilder.setPreDBSearchTaskIds(ModelConversion.uuidsToByteString(preDBSearchTaskIds));
                            }
                            SilacRTAlignmentTaskParameters.Builder rtTaskBuilder = taskParamBuilder
                                .setSilacRtAlignmentParameters(silacRtAlignmentParameters)
                                .setFractionIds(ModelConversion.uuidsToByteString(fractionIdWithTransfers))
                                .setDataRefineTaskIds(ModelConversion.uuidsToByteString(featureTaskIds))
                                .setPsmTaskIds(ModelConversion.uuidsToByteString(psmTaskIds))
                                .setMinPsmPValue(psmPValue)
                                .setMinPeptidePValue(peptidePValue)
                                .setIdSummarizeTaskId(ModelConversion.uuidToByteString(optPeptideTaskId.get()))
                                .setFractionsForIdTransfer(ModelConversion.uuidsToByteString(fractionIdsForTransfer));

                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(SilacRtAlignmentService.NAME)
                                .setSilacRTAlignmentTaskParameters(rtTaskBuilder.build())
                                .build());
                        }));
                        break;
                    }
                    case SILAC_FEATURE_ALIGNMENT: {
                        final SilacFeatureAlignmentParameters silacFeatureAlignmentParameters =
                            (SilacFeatureAlignmentParameters) Helper.workflowStepParameters(step);
                        final Map<UUID, UUID> fractionSearchTaskIds;

                        final UUID searchSummariationTaskId;
                        if (hasDb) {
                            if (!dbSearchSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = dbSearchSummarizeTaskId.get();
                            fractionSearchTaskIds = dbSearchStepFractionIdToTaskId;
                        } else if (hasSlProteinInference) {
                            if (!slPeptideSummarizeTaskId.isPresent()) continue;
                            searchSummariationTaskId = slPeptideSummarizeTaskId.get();
                            fractionSearchTaskIds = slSearchStepFractionIdToTaskId;
                        } else {
                            continue;
                        }

                        final Set<UUID> samplesToInclude = samples.stream().map(orderedSampleFractions -> orderedSampleFractions.sampleId()).collect(Collectors.toSet());

                        final Map<UUID, UUID> fractionIdToDataRefineTaskId;
                        if (hasSlProteinInference) {
                            fractionIdToDataRefineTaskId = featureDetectionStepFractionIdToTaskId;
                        } else{
                            fractionIdToDataRefineTaskId = dataRefineStepFractionIdToTaskId;
                        }

                        Map<UUID, Integer> fractionIdToIndex = new HashMap<>();
                        analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .forEach(sampleFractions -> {
                                final List<UUID> fractionIds = sampleFractions.fractionIds();
                                for (int i = 0; i < fractionIds.size(); i++) {
                                    fractionIdToIndex.put(fractionIds.get(i), i);
                                }
                            });

                        IdentificaitonFilter filter;
                        if (!hasDb) {
                            final SlFilterSummarizationParameters slFilterSummarizationParameters = (SlFilterSummarizationParameters) stepParameters.get(SL_FILTER_SUMMARIZATION);
                            filter = slFilterSummarizationParameters.getFilters();
                        } else {
                            filter = (IdentificaitonFilter) stepParameters.get(StepType.DB_FILTER_SUMMARIZATION);
                        }
                        final float psmPValue = psmPValueFromIdentificationFilter(analysis.keyspace(), filter, dbSearchSummarizeTaskId.get(), false);
                        final float peptidePValue = peptidePValueFromIdentificationFilter(analysis.keyspace(), filter, dbSearchSummarizeTaskId.get(), false);

                        final Set<UUID> sampleIdsInSilac = silacSamplesInGroup(analysis).streamSampleIds().collect(Collectors.toSet());

                        SilacParameters silacParameters = analysis.workflowStep().get(SILAC_FILTER_SUMMARIZE).getSilacFilterSummarizationParamaters()
                            .getSilacParameters();
                        final boolean isFractionAssisted = silacParameters.getTransferOnlyGroup().getSamplesCount() > 0;

                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID retentionTimeAlignmentTaskId = silacRtAlignmentFractionIndexToTaskId.get(fractionIdToIndex.get(fractionId));

                            if (!sampleIdsInSilac.contains(sampleId)) return Optional.empty();
                            if (retentionTimeAlignmentTaskId == null) return Optional.empty();
                            if (!fractionIdToDataRefineTaskId.containsKey(fractionId)) return Optional.empty();
                            if (!fractionSearchTaskIds.containsKey(fractionId)) return Optional.empty();
                            if (hasDb && !dbPreSearchStepFractionIdToTaskId.containsKey(fractionId)) return Optional.empty();

                            List<OrderedSampleFractions> currentSample = new ArrayList<>();
                            for (OrderedSampleFractions s : samples) {
                                if (s.sampleId().equals(sampleId)) {
                                    currentSample.add(s);
                                }
                            }
                            SilacFeatureAlignmentTaskParameters.Builder taskParameters = SilacFeatureAlignmentTaskParameters.newBuilder();

                            if (hasDb) {
                                taskParameters.setPreDBSearchTaskId(ModelConversion.uuidToByteString(dbPreSearchStepFractionIdToTaskId.get(fractionId)));
                            }

                            taskParameters
                                .setSilacFeatureAlignmentParameters(silacFeatureAlignmentParameters)
                                .setRtAlignmentTaskId(ModelConversion.uuidToByteString(retentionTimeAlignmentTaskId))
                                .setFractionTaskId(ModelConversion.uuidToByteString(fractionId))
                                .setDataLoadingTaskId(ModelConversion.uuidToByteString(dataLoadingStepFractionIdToTaskId.get(fractionId)))
                                .setDataRefineTaskId(ModelConversion.uuidToByteString(fractionIdToDataRefineTaskId.get(fractionId)))
                                .setSearchTaskId(ModelConversion.uuidToByteString(fractionSearchTaskIds.get(fractionId)))
                                .setPeptideSummarizeTaskId(ModelConversion.uuidToByteString(searchSummariationTaskId))
                                .setAllOrderedSampleFractions(OrderedSampleFractions.proto(currentSample))
                                .setMinPeptidePValue(peptidePValue)
                                .setMinPsmPValue(psmPValue)
                                .setIsFractionAssistedTransfer(isFractionAssisted);

                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(SilacFeatureAlignService.NAME)
                                .setSilacFeatureAlignmentTaskParameters(taskParameters.build())
                                .build());
                        }));
                        break;
                    }
                    case SILAC_FEATURE_VECTOR_SEARCH: {
                        final SilacSearchParameters silacSearchParameters = (SilacSearchParameters) Helper.workflowStepParameters(step);
                        final boolean isSL = analysis.hasSlSearch();

                        final Set<UUID> samplesToInclude = samples.stream().map(orderedSampleFractions -> orderedSampleFractions.sampleId()).collect(Collectors.toSet());

                        Map<UUID, Integer> fractionIdToIndex = new HashMap<>();
                        analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .forEach(sampleFractions -> {
                                final List<UUID> fractionIds = sampleFractions.fractionIds();
                                for (int i = 0; i < fractionIds.size(); i++) {
                                    fractionIdToIndex.put(fractionIds.get(i), i);
                                }
                            });
                        final Set<UUID> sampleIdsInSilac = silacSamplesInGroup(analysis).streamSampleIds().collect(Collectors.toSet());
                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            final Optional<UUID> optPeptideTaskId = isSL ? slPeptideSummarizeTaskId : dbSearchSummarizeTaskId;
                            final UUID psmTaskId = isSL ? slSearchStepFractionIdToTaskId.get(fractionId) : dbSearchStepFractionIdToTaskId.get(fractionId);

                            if (!sampleIdsInSilac.contains(sampleId)) return Optional.empty();
                            if (psmTaskId == null) return Optional.empty();
                            if (!optPeptideTaskId.isPresent()) return Optional.empty();
                            if (dataLoadingStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            if (dataRefineStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            if (featureDetectionStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            boolean enableMatchBetweenRuns = silacSearchParameters.getEnableMatchBetweenRuns();
                            if (enableMatchBetweenRuns) {
                                if (silacRtAlignmentFractionIndexToTaskId.get(fractionIdToIndex.get(fractionId)) == null) return Optional.empty();
                                if (silacFeatureAlignmentFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            }

                            final UUID peptideTaskId = optPeptideTaskId.get();

                            IdentificaitonFilter filter;
                            if (isSL) {
                                final SlFilterSummarizationParameters slFilterSummarizationParameters = (SlFilterSummarizationParameters) stepParameters.get(SL_FILTER_SUMMARIZATION);
                                filter = slFilterSummarizationParameters.getFilters();
                            } else {
                                filter = (IdentificaitonFilter) stepParameters.get(StepType.DB_FILTER_SUMMARIZATION);
                            }

                            final float psmPValue = psmPValueFromIdentificationFilter(analysis.keyspace(), filter, peptideTaskId, isSL);
                            final float peptidePValue = peptidePValueFromIdentificationFilter(analysis.keyspace(), filter, peptideTaskId, isSL);

                            SilacSearchTaskParameters.Builder builder = SilacSearchTaskParameters.newBuilder();

                            if (enableMatchBetweenRuns) {
                                builder
                                    .setRtAlignmentTaskId(ModelConversion.uuidToByteString(silacRtAlignmentFractionIndexToTaskId.get(fractionIdToIndex.get(fractionId))))
                                    .setFeatureAlignTaskId(ModelConversion.uuidToByteString(silacFeatureAlignmentFractionIdToTaskId.get(fractionId)));
                            }

                            builder
                                .setSilacSearchParameters(silacSearchParameters)
                                .setFractionId(ModelConversion.uuidToByteString(fractionId))
                                .setDataLoadingTaskId(ModelConversion.uuidToByteString(dataLoadingStepFractionIdToTaskId.get(fractionId)))
                                .setPsmTaskId(ModelConversion.uuidToByteString(psmTaskId))
                                .setPsmPValue(psmPValue)
                                .setPeptidePValue(peptidePValue)
                                .setFeatureDetectionTaskId(ModelConversion.uuidToByteString(featureDetectionStepFractionIdToTaskId.get(fractionId)))
                                .addAllSamples(samples.stream().map(i -> CommonTasksFactory.sampleLevelTask(i.sampleId(), i.fractionIds()))::iterator)
                                .setPeptideTaskId(ModelConversion.uuidToByteString(peptideTaskId))
                                .setDataRefineTaskId(ModelConversion.uuidToByteString(dataRefineStepFractionIdToTaskId.get(fractionId)))
                                .build();

                            return Optional.of(TaskParameters.newBuilder()
                                .setProjectId(projectId.toString())
                                .setService(SilacFeatureVectorSearchService.NAME)
                                .setSilacSeacrhTaskParameters(builder)
                                .build());
                        }));
                        break;
                    }
                    case SILAC_PEPTIDE_SUMMARIZE: {
                        //verify all fractions in silac grouping a complete
                        boolean allFractionsInSilacFinished = true;
                        OrderedSampleFractionsList samplesInSilac = silacSamplesInGroup(analysis);
                        for (OrderedSampleFractions sampleInSilac : samplesInSilac) {
                            for(UUID fractionId : sampleInSilac.fractionIds()) {
                                if (!silacFeatureVectorSearchFractionIdToTaskId.containsKey(fractionId)) {
                                    allFractionsInSilacFinished = false;
                                    break;
                                }
                            }
                        }
                        if (!allFractionsInSilacFinished) continue;

                        SilacSummarizeTaskParameters.Builder taskParameterBuilder = SilacSummarizeTaskParameters.newBuilder();

                        if (hasDb) {
                            if (!dbFilterSummarizeTaskId.isPresent()) continue;
                            taskParameterBuilder
                                .setProteinHitTaskId(ModelConversion.uuidToByteString(dbFilterSummarizeTaskId.get()));
                        } else if(hasSlProteinInference) {
                            if (!slFilterSummarizationTaskId.isPresent()) continue;
                            taskParameterBuilder
                                .setProteinHitTaskId(ModelConversion.uuidToByteString(slFilterSummarizationTaskId.get()));
                        }

                        String conservedJSON = localconfig.getConfig("peaks.parameters.SilacSummarizationConservedParameters").root()
                            .render(ConfigRenderOptions.concise());
                        SilacSummarizationConservedParameters.Builder conservedBuilder = SilacSummarizationConservedParameters.newBuilder();
                        try {
                            JsonFormat.parser().ignoringUnknownFields().merge(conservedJSON, conservedBuilder);
                        } catch (IOException e) {
                            throw new ParserException("Unable to load conserved parameters for silac");
                        }

                        final SilacSummarizeParameters silacSummarizeParameters = (SilacSummarizeParameters) Helper.workflowStepParameters(step);
                        taskParameterBuilder
                            .setSilacSearchTaskIds(uuidsToByteString(Lists.transform(Lists.newArrayList(samplesInSilac.fractionIds()), silacFeatureVectorSearchFractionIdToTaskId::get)))
                            .setOrderedSampleFractions(OrderedSampleFractions.proto(samplesInSilac))
                            .setSilacSummarizationConservedParameters(conservedBuilder)
                            .setSilacSummarizationParameters(silacSummarizeParameters);

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setService(SilacPeptideSummarizationService.NAME)
                            .setSilacSummarizeTaskParameters(taskParameterBuilder.build())
                            .build();

                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case SILAC_FILTER_SUMMARIZE: {
                        //verify all fractions in silac grouping a complete
                        boolean allFractionsInSilacFinished = true;
                        OrderedSampleFractionsList samplesInSilac = silacSamplesInGroup(analysis);
                        for (OrderedSampleFractions sampleInSilac : samplesInSilac) {
                            for(UUID fractionId : sampleInSilac.fractionIds()) {
                                if (!silacFeatureVectorSearchFractionIdToTaskId.containsKey(fractionId)) {
                                    allFractionsInSilacFinished = false;
                                    break;
                                }
                            }
                        }
                        if (!allFractionsInSilacFinished) continue;
                        if (!silacPeptideSummarizeTaskId.isPresent()) continue;

                        SilacFilterSummarizationTaskParamaters.Builder taskParameterBuilder = SilacFilterSummarizationTaskParamaters.newBuilder();
                        if (hasDb) {
                            if (!dbFilterSummarizeTaskId.isPresent()) continue;

                            IDFilter.ProteinHitFilter dbProteinHitFilter = analysis.workflowStep().get(DB_FILTER_SUMMARIZATION).getDbProteinHitFilter();

                            taskParameterBuilder
                                .setProteinHitTaskId(ModelConversion.uuidToByteString(dbFilterSummarizeTaskId.get()))
                                .setDdaProteinFilter(dbProteinHitFilter);
                        } else if(hasSlProteinInference) {
                            if (!slFilterSummarizationTaskId.isPresent()) continue;
                            SlFilter.ProteinHitFilter slFilter = analysis.workflowStep().get(SL_FILTER_SUMMARIZATION).getSlFilter();
                            taskParameterBuilder
                                .setProteinHitTaskId(ModelConversion.uuidToByteString(slFilterSummarizationTaskId.get()))
                                .setDiaProteinFilter(slFilter);
                        }

                        final SilacFilterSummarizationParameters silacFilterSummarizationParameters = (SilacFilterSummarizationParameters) Helper.workflowStepParameters(step);

                        String conservedJSON = localconfig.getConfig("peaks.parameters.SilacConservedParameters").root()
                            .render(ConfigRenderOptions.concise());
                        SilacConservedParameters.Builder conservedBuilder = SilacConservedParameters.newBuilder();
                        try {
                            JsonFormat.parser().ignoringUnknownFields().merge(conservedJSON, conservedBuilder);
                        } catch (IOException e) {
                            throw new ParserException("Unable to load conserved parameters for silac");
                        }

                        final SilacFilterSummarizationTaskParamaters.Builder parametersBuilder = taskParameterBuilder
                            .setOrderedSampleFractions(OrderedSampleFractions.proto(samplesInSilac))
                            .setSilacSearchTaskIds(uuidsToByteString(Lists.transform(Lists.newArrayList(samplesInSilac.fractionIds()), silacFeatureVectorSearchFractionIdToTaskId::get)))
                            .setSilacFilterSummarizationParameters(silacFilterSummarizationParameters)
                            .setSilacConservedParameters(conservedBuilder)
                            .addAllFastaHeaderParsers(analysis.fastaHeaderParsers())
                            .setSilacSummarizationTaskId(ModelConversion.uuidsToByteString(silacPeptideSummarizeTaskId.get()));

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setService(SilacFilterSummarizationService.NAME)
                            .setSilacFilterSummarizationTaskParamaters(parametersBuilder)
                            .build();

                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    }
                    case DIA_LFQ_RETENTION_TIME_ALIGNMENT: {
                        if (hasSlSearchStep && !slPeptideSummarizeTaskId.isPresent()) continue;
                        if (hasSlSearchStep && !slFilterSummarizationTaskId.isPresent()) continue;
                        if (hasDiaDb && !diaDbFilterSummarizeTaskId.isPresent()) continue;

                        LfqFilter filter = (LfqFilter) Helper.workflowStepParameters(analysis.workflowStep().get(DIA_LFQ_FILTER_SUMMARIZATION));

                        final Set<UUID> samplesToInclude = filter.getLabelFreeQueryParameters().getGroupsList().stream()
                            .flatMap(g -> g.getSampleIndexesList().stream())
                            .map(sampleIdx -> analysis.samples().get(sampleIdx).sampleId())
                            .collect(Collectors.toSet());
                        List<OrderedSampleFractions> sampleFractionsToInclude = analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .collect(Collectors.toList());
                        tasks.addAll(indexedFractionLevelTaskProto(analysis, stepType, sampleFractionsToInclude, (fractionIndex, fractionIds) -> {
                            final DiaRetentionTimeAlignmentTaskParameters.OptionalSLSearch.Builder optionalSlSearchBuilder = DiaRetentionTimeAlignmentTaskParameters
                                .OptionalSLSearch.newBuilder()
                                .setPresent(hasSlSearchStep);
                            final DiaRetentionTimeAlignmentTaskParameters.OptionalDbSearch.Builder optionalDbSearchBuilder = DiaRetentionTimeAlignmentTaskParameters
                                .OptionalDbSearch.newBuilder()
                                .setPresent(hasDiaDb);
                            if (hasDiaDb) {
                                long searchesDone = fractionIds.stream().map(diaDbSearchStepFractionIdToTaskId::get)
                                    .filter(id -> id != null)
                                    .count();
                                if (searchesDone != fractionIds.size()) return Optional.empty();
                                SlFilterSummarizationResult result = analysis.diaFilterSummarizationResult().orElseThrow(() -> new IllegalStateException("No DiaDb filter result"));
                                optionalDbSearchBuilder
                                    .setMinPsmFilter((float) result.getMinPsmPValue())
                                    .setMinPeptideFilter((float) result.getMinPeptidePValue());
                            }
                            if (hasSlSearchStep) {
                                long searchesDone = fractionIds.stream().map(slSearchStepFractionIdToTaskId::get)
                                    .filter(id -> id != null)
                                    .count();
                                if (searchesDone != fractionIds.size()) return Optional.empty();
                                SlFilterSummarizationResult result = analysis.slFilterSummarizationResult().orElseThrow(() -> new IllegalStateException("No Sl filter result"));
                                optionalSlSearchBuilder
                                    .setMinPsmFilter((float) result.getMinPsmPValue())
                                    .setMinPeptideFilter((float)  result.getMinPeptidePValue())
                                    .setPeptideTaskId(ModelConversion.uuidToByteString(slPeptideSummarizeTaskId.orElseThrow(() -> new IllegalStateException("No Peptide filter step found"))));
                            }
                            final List<UUID> lastPsmSearchTaskIds;
                            if (hasDiaDb) {
                                lastPsmSearchTaskIds = fractionIds.stream().map(diaDbSearchStepFractionIdToTaskId::get).collect(Collectors.toList());
                            } else {
                                lastPsmSearchTaskIds = fractionIds.stream().map(slSearchStepFractionIdToTaskId::get).collect(Collectors.toList());
                            }
                            DiaRetentionTimeAlignmentTaskParameters.Builder taskParameters = DiaRetentionTimeAlignmentTaskParameters.newBuilder()
                                .setFractionIds(ModelConversion.uuidsToByteString(fractionIds))
                                .setLastPsmSearchTaskIds(ModelConversion.uuidsToByteString(lastPsmSearchTaskIds))
                                .setSlSearch(optionalSlSearchBuilder)
                                .setDiaDbSearch(optionalDbSearchBuilder)
                                .setFractionIndex(fractionIndex);
                            return Optional.of(
                                TaskParameters.newBuilder()
                                    .setProjectId(projectId.toString())
                                    .setService(DiaRetentionTimeAlignmentService.NAME)
                                    .setDiaRetentionTimeAlignmentTaskParameters(taskParameters)
                                    .build()
                            );
                        }));
                        break;
                    }
                    case DIA_LFQ_FEATURE_EXTRACTION: {
                        OrderedSampleFractionsList sampleFractionsList = new OrderedSampleFractionsList(analysis.samples());

                        LfqFilter filter = (LfqFilter) Helper.workflowStepParameters(analysis.workflowStep().get(DIA_LFQ_FILTER_SUMMARIZATION));

                        final List<UUID> samplesToInclude = filter.getLabelFreeQueryParameters().getGroupsList().stream()
                            .flatMap(g -> g.getSampleIndexesList().stream())
                            .map(sampleIdx -> analysis.samples().get(sampleIdx).sampleId())
                            .collect(Collectors.toList());

                        Map<UUID, Integer> fractionIdToIndex = new HashMap<>();
                        analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .forEach(sampleFractions -> {
                                final List<UUID> fractionIds = sampleFractions.fractionIds();
                                for (int i = 0; i < fractionIds.size(); i++) {
                                    fractionIdToIndex.put(fractionIds.get(i), i);
                                }
                            });
                        DiaFeatureExtractorParameters extractorParameters = (DiaFeatureExtractorParameters) Helper.workflowStepParameters(step);
                        tasks.addAll(fractionLevelTaskParameters(analysis, stepType, (sampleId, fractionId) -> {
                            final UUID lastSearchTaskId;
                            if (hasDiaDb) {
                                lastSearchTaskId = diaDbSearchStepFractionIdToTaskId.get(fractionId);
                            } else {
                                lastSearchTaskId = slSearchStepFractionIdToTaskId.get(fractionId);
                            }
                            Integer fractionIndex = fractionIdToIndex.get(fractionId);
                            if (fractionIndex == null) return Optional.empty(); //unused fraction index
                            final UUID rtAlignmentTask = diaLfqRtAlignmentFractionIndexToTaskId.get(fractionIndex);

                            //validate previous tasks are complete
                            if (dataLoadingStepFractionIdToTaskId.get(fractionId) == null) return Optional.empty();
                            if (lastSearchTaskId == null) return Optional.empty();
                            if (rtAlignmentTask == null) return Optional.empty();
                            DiaFeatureExtractorTaskParameters.Builder taskParameters = DiaFeatureExtractorTaskParameters.newBuilder();
                            final int sampleIdx = samplesToInclude.indexOf(sampleId);
                            taskParameters
                                .setFractionId(ModelConversion.uuidsToByteString(fractionId))
                                .setDataLoadingScanId(ModelConversion.uuidToByteString(dataLoadingStepFractionIdToTaskId.get(fractionId)))
                                .setFirstSearchTaskId(ModelConversion.uuidToByteString(lastSearchTaskId))
                                .setRetentionAlignmentTaskId(ModelConversion.uuidToByteString(rtAlignmentTask))
                                .setDiaFeatureExtractorParameters(extractorParameters)
                                .setSampleIdx(sampleIdx)
                                .setFractionIdx(fractionIndex);
                            return Optional.of(
                                TaskParameters.newBuilder()
                                    .setProjectId(projectId.toString())
                                    .setService(DiaFeatureExtractorService.NAME)
                                    .setDiaFeatureExtractorTaskParameters(taskParameters)
                                    .build()
                            );
                        }));
                        break;
                    }
                    case DIA_LFQ_SUMMARIZE: {
                        //TODO use samples selected
                        //TODO order
                        final UUID lastProteinHitTaskId;
                        final UUID summarizeTaskId;
                        final SlFilter.ProteinHitFilter slFilter;
                        if (hasDiaDb) {
                            if (!diaDbPeptideSummarizeTaskId.isPresent()) continue;
                            if (!diaDbFilterSummarizeTaskId.isPresent()) continue;
                            lastProteinHitTaskId = diaDbPeptideSummarizeTaskId.get();
                            slFilter = analysis.workflowStep().get(DIA_DB_FILTER_SUMMARIZE).getSlFilter();
                            summarizeTaskId = diaDbFilterSummarizeTaskId.get();

                        } else {
                            if (!slPeptideSummarizeTaskId.isPresent()) continue;
                            if (!slFilterSummarizationTaskId.isPresent()) continue;
                            lastProteinHitTaskId = slPeptideSummarizeTaskId.get();
                            slFilter = analysis.workflowStep().get(SL_FILTER_SUMMARIZATION).getSlFilter();
                            summarizeTaskId = slFilterSummarizationTaskId.get();
                        }

                        ProteinDecoyInfo proteinDecoyInfo = proteinDecoyInfo(analysis.keyspace(), summarizeTaskId);
                        SlProteinHitFilter slProteinHitFilter = SlProteinHitFilter.fromProto(slFilter,
                            fdr -> DecoyInfoWrapper.fdrToProScore(proteinDecoyInfo, fdr, slFilter.getMinUniquePeptides()),
                            pvalue -> DecoyInfoWrapper.proScoreToFdr(proteinDecoyInfo, pvalue, slFilter.getMinUniquePeptides())
                        );

                        LfqFilter filter = (LfqFilter) Helper.workflowStepParameters(analysis.workflowStep().get(DIA_LFQ_FILTER_SUMMARIZATION));

                        final Set<UUID> samplesToInclude = filter.getLabelFreeQueryParameters().getGroupsList().stream()
                            .flatMap(g -> g.getSampleIndexesList().stream())
                            .map(sampleIdx -> analysis.samples().get(sampleIdx).sampleId())
                            .collect(Collectors.toSet());
                        List<OrderedSampleFractions> sampleFractionsToInclude = analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .collect(Collectors.toList());

                        tasks.addAll(indexedFractionLevelTaskProto(analysis, stepType, sampleFractionsToInclude, (fractionIndex, fractionIds) -> {
                            UUID rtAlignmentTaskId = diaLfqRtAlignmentFractionIndexToTaskId.getOrDefault(fractionIndex, null);
                            if (rtAlignmentTaskId == null) return Optional.empty();

                            //TODO use a used sample only
                            List<UUID> featureExtractorTaskIds = new ArrayList<>();
                            for (UUID fid : fractionIds) {
                                UUID featureTaskId = diaFeatureExtractorFractionIndexToTaskId.getOrDefault(fid, null);
                                if (featureTaskId == null) {
                                    return Optional.empty();
                                } else {
                                    featureExtractorTaskIds.add(featureTaskId);
                                }
                            }

                            DiaLfqSummarizerTaskParameters taskParameters = DiaLfqSummarizerTaskParameters.newBuilder()
                                .setSamples(OrderedSampleFractions.proto(analysis.samples()))
                                .setRetentionTimeAlignmentTaskId(ModelConversion.uuidToByteString(rtAlignmentTaskId))
                                .setFeatureExtractorTaskIds(ModelConversion.uuidsToByteString(featureExtractorTaskIds))
                                .setLastProteinHitTaskId(ModelConversion.uuidsToByteString(lastProteinHitTaskId))
                                .setProteinHitMinPValue(slProteinHitFilter.minPValue())
                                .setProteinHitMinUniquePeptides(slProteinHitFilter.minUniquePeptides())
                                .build();

                            return Optional.of(
                                TaskParameters.newBuilder()
                                    .setProjectId(projectId.toString())
                                    .setService(DiaLfqSummarizeService.NAME)
                                    .setDiaLfqSummarizerTaskParameters(taskParameters)
                                    .build()
                            );
                        }));

                        break;
                    }
                    case DIA_LFQ_FILTER_SUMMARIZATION:
                        final int fractionsPerSample = analysis.analysis().calculateFractionIndexLevelTaskCount(analysis, StepType.DIA_LFQ_RETENTION_TIME_ALIGNMENT);
                        if (diaLfqSummarizeFractionIndexToTaskId.size() < fractionsPerSample) continue;
                        LabelFreeQuantificationConservedParameters labelFreeQuantificationConservedParameters = PeaksParameters.fromConfig(
                            localconfig.getConfig(LabelFreeQuantificationConservedParameters.ROOT + LabelFreeQuantificationConservedParameters.SECTION),
                            LabelFreeQuantificationConservedParameters.class
                        );

                        LfqFilter filter = (LfqFilter) Helper.workflowStepParameters(step);

                        final LfqSummaryFilterParameters.Builder builder = LfqSummaryFilterParameters.newBuilder();
                        if (hasDiaDb) {
                            if (!diaDbFilterSummarizeTaskId.isPresent()) continue;
                            builder.setProteinHitTaskId(ModelConversion.uuidToByteString(diaDbFilterSummarizeTaskId.get()));
                        } else if(hasSlProteinInference) {
                            if (!slFilterSummarizationTaskId.isPresent()) continue;
                            builder.setProteinHitTaskId(ModelConversion.uuidToByteString(slFilterSummarizationTaskId.get()));
                        }

                        Map<String, String> fracIdToDataLoading = dataLoadingStepFractionIdToTaskId.entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));

                        final Set<UUID> samplesToInclude = filter.getLabelFreeQueryParameters().getGroupsList().stream()
                            .flatMap(g -> g.getSampleIndexesList().stream())
                            .map(sampleIdx -> analysis.samples().get(sampleIdx).sampleId())
                            .collect(Collectors.toSet());
                        final Set<UUID> fractionsToInclude = fractionIdToSampleIdMap.entrySet().stream()
                            .filter(e -> samplesToInclude.contains(e.getValue()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
                        List<OrderedSampleFractions> sampleFractionsToInclude = analysis.samples().stream()
                            .filter(s -> samplesToInclude.contains(s.sampleId()))
                            .collect(Collectors.toList());
                        HashSet<UUID> fractionIds = new HashSet<>();
                        diaLfqRtAlignmentFractionIndexToTaskId.keySet().forEach(key -> {
                            for(OrderedSampleFractions sample: sampleFractionsToInclude) {
                                fractionIds.add(sample.fractionIds().get(key));
                            }
                        });

                        // rt alignment has not finished, so continue
                        if (fractionIds.size() != fractionsToInclude.size()) {
                            continue;
                        }

                        List<UUID> orderedRtAlignmentTaskIds = diaLfqRtAlignmentFractionIndexToTaskId.entrySet().stream()
                            .sorted(Comparator.comparing(Map.Entry::getKey))
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList());

                        builder
                            .setLfqFilter(filter)
                            .addAllLfqSummarizeTaskIds(Iterables.transform(diaLfqSummarizeFractionIndexToTaskId.values(), UUID::toString))
                            //should be all samples even those excluded from LFQ results
                            .addAllSamples(Iterables.transform(samples, i -> CommonTasksFactory.sampleLevelTask(i.sampleId(), i.fractionIds())))
                            .setMaxRatio(labelFreeQuantificationConservedParameters.maxRatio())
                            .setAttachIdentification(hasDiaDb || hasSlProteinInference)
                            .putAllFractionIdToDataLoadTaskId(fracIdToDataLoading)
                            .addAllFastaHeaderParsers(analysis.fastaHeaderParsers())
                            .setCvFilterThresholdIQRRatio(labelFreeQuantificationConservedParameters.cvFilterThresholdIQRRatio())
                            .setRemoveOutlierIQRRatio(labelFreeQuantificationConservedParameters.removeOutlierIQRRatio())
                            .setAcquisitionMethod(acquisitionMethod)
                            .setRtAlignTaskIds(uuidsToByteString(orderedRtAlignmentTaskIds));

                        TaskParameters taskParameters = TaskParameters.newBuilder()
                            .setProjectId(projectId.toString())
                            .setLfqSummaryFilterParameters(builder)
                            .setService(LfqSummaryFilterService.NAME)
                            .build();

                        tasks.add(taskDescriptionAnalysisLevel(taskParameters, stepType, analysisSampleIds, analysisFractionIds));
                        break;
                    default:
                        throw new IllegalArgumentException("Step type " + stepType + " not supported on analysis " + analysis.analysisId());
                }
            }
            return tasks.stream().map(t -> t.toBuilder().setResourceOverride(analysis.resourceOverride()).build()).collect(Collectors.toList());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    private float fdrToPValue(FDRKey key) {
        try {
            return this.fdrCache.get(key);
        } catch (ExecutionException e) {
            return 0;
        }
    }


    private float calculatefdrToPValue(FDRKey key) {
        try {
            String keyspace = key.keyspace;
            UUID searchSummarizeTaskId = key.searchSummarizeTaskId;
            float fdrThreshold= key.fdrThreshold;
            FdrType fdrType = key.fdrType;
            ApplicationStorage storage = storageFactory.getStorage(keyspace);

            final CompletionStage<Optional<DecoyInfo>> decoyInfoFromDecoyInfo;
            switch (fdrType) {
                case PSM:
                    decoyInfoFromDecoyInfo = storage.getPsmDecoyInfoRepository().load(searchSummarizeTaskId);
                    break;
                case PEPTIDE:
                    decoyInfoFromDecoyInfo = storage.getPeptideDecoyInfoRepository().load(searchSummarizeTaskId);
                    break;
                default:
                    throw new IllegalStateException("Invalid fdr type " + fdrType);
            }

            CompletionStage<Optional<DecoyInfo>> decoyInfoFromTaskOutput = storage
                .getTaskOutputRepository()
                .load(searchSummarizeTaskId, com.bsi.peaks.model.proto.DecoyInfo::parseFrom)
                .thenApply(o -> o.map(DecoyInfoWrapper::decoyInfoFromProto));

            DecoyInfo decoyInfo = decoyInfoFromDecoyInfo.thenCompose(o -> {
                if (o.isPresent()) return CompletableFuture.completedFuture(o);
                return decoyInfoFromTaskOutput;
            })
                .thenApply(optional -> optional.orElseThrow(NoSuchElementException::new))
                .toCompletableFuture().join();

            return DecoyInfoWrapper.fdrToPValue(decoyInfo, fdrThreshold);
        } catch (Throwable e) {
            LOG.error("An error occurred in retrieving DecoyInfo.", e);
        }

        return 0;
    }



    private float fdrToPValue(String keyspace, UUID searchSummarizeTaskId, float fdrThreshold, FdrType fdrType, boolean isDIA) {
        FDRKey key = new FDRKey();
        key.keyspace = keyspace;
        key.searchSummarizeTaskId = searchSummarizeTaskId;
        key.fdrThreshold = fdrThreshold;
        key.fdrType = fdrType;
        key.isDIA = isDIA;
        return fdrToPValue(key);
    }

    private ProteinDecoyInfo proteinDecoyInfo(String keyspace, UUID filterSummarizeTaskId) {
        try {
            ApplicationStorage storage = storageFactory.getStorage(keyspace);
            DecoyInfo decoyInfo = storage.getProteinDecoyInfoRepository().loadArchive(filterSummarizeTaskId)
                .thenApply(o -> o.map(DecoyInfoWrapper::proteinDecoyInfoFromProto))
                .thenApply(optional -> optional.orElseThrow(NoSuchElementException::new))
                .toCompletableFuture().join();

            return (ProteinDecoyInfo) decoyInfo;
        } catch (Throwable e) {
            throw new IllegalStateException("Unable to find protein decoy info for taskId: " + filterSummarizeTaskId);
        }
    }

    private List<TaskDescription> fractionLevelTask(
        AnalysisTaskInformation analysisTaskIds,
        StepType stepType,
        BiFunction<UUID, UUID, Optional<? extends Task>> fractionIdToTask
    ) throws JsonProcessingException {
        final Set<UUID> queuedFractionIds = analysisTaskIds.stepQueuedFractionIds(stepType);
        List<TaskDescription> tasks = new ArrayList<>();
        for (OrderedSampleFractions sampleFractions : analysisTaskIds.samples()) {
            final UUID sampleId = sampleFractions.sampleId();
            for (UUID fractionId : sampleFractions.fractionIds()) {
                if (queuedFractionIds.contains(fractionId)) {
                    continue;
                }
                final Optional<? extends Task> optionalTask = fractionIdToTask.apply(sampleId, fractionId);
                if (optionalTask.isPresent()) {
                    Task task = optionalTask.get();
                    tasks.add(taskDescriptionFractionLevel(task, stepType, sampleId, fractionId));
                }
            }
        }
        return tasks;
    }

    private List<TaskDescription> fractionLevelTaskParameters(
        AnalysisTaskInformation analysisTaskIds,
        StepType stepType,
        BiFunction<UUID, UUID, Optional<TaskParameters>> fractionIdToTask
    ) throws JsonProcessingException {
        final Set<UUID> queuedFractionIds = analysisTaskIds.stepQueuedFractionIds(stepType);
        List<TaskDescription> tasks = new ArrayList<>();
        for (OrderedSampleFractions sampleFractions : analysisTaskIds.samples()) {
            final UUID sampleId = sampleFractions.sampleId();
            for (UUID fractionId : sampleFractions.fractionIds()) {
                if (queuedFractionIds.contains(fractionId)) {
                    continue;
                }
                final Optional<TaskParameters> optionalTask = fractionIdToTask.apply(sampleId, fractionId);
                if (optionalTask.isPresent()) {
                    TaskParameters task = optionalTask.get();
                    TaskDescription description = taskDescriptionFractionLevel(task, stepType, sampleId, fractionId);
                    tasks.add(description);
                }
            }
        }
        return tasks;
    }

    private List<TaskDescription> sampleLevelTask(
        AnalysisTaskInformation analysisTaskIds,
        StepType stepType,
        Function<OrderedSampleFractions, Optional<? extends Task>> sampleFractionToTask
    ) throws JsonProcessingException {
        final Set<UUID> queuedFractionIds = analysisTaskIds.stepQueuedFractionIds(stepType);
        List<TaskDescription> tasks = new ArrayList<>();
        for (OrderedSampleFractions sampleFractions : analysisTaskIds.samples()) {
            if (queuedFractionIds.contains(sampleFractions.fractionIds().get(0))) {
                continue;
            }
            final UUID sampleId = sampleFractions.sampleId();
            final Optional<? extends Task> optionalTask = sampleFractionToTask.apply(sampleFractions);
            if (optionalTask.isPresent()) {
                Task task = optionalTask.get();
                tasks.add(taskDescriptionSampleLevel(task, stepType, sampleId, sampleFractions.fractionIds()));
            }
        }
        return tasks;
    }

    private List<TaskDescription> sampleLevelTaskParamaters(
        AnalysisTaskInformation analysisTaskIds,
        StepType stepType,
        Function<OrderedSampleFractions, Optional<TaskParameters>> sampleFractionToTask
    ) throws JsonProcessingException {
        final Set<UUID> queuedFractionIds = analysisTaskIds.stepQueuedFractionIds(stepType);
        List<TaskDescription> tasks = new ArrayList<>();
        for (OrderedSampleFractions sampleFractions : analysisTaskIds.samples()) {
            if (queuedFractionIds.contains(sampleFractions.fractionIds().get(0))) {
                continue;
            }
            final UUID sampleId = sampleFractions.sampleId();
            final Optional<TaskParameters> optionalTask = sampleFractionToTask.apply(sampleFractions);
            if (optionalTask.isPresent()) {
                TaskParameters task = optionalTask.get();
                tasks.add(taskDescriptionSampleLevel(task, stepType, sampleId, sampleFractions.fractionIds()));
            }
        }
        return tasks;
    }

    private List<TaskDescription> indexedFractionLevelTask(
        AnalysisTaskInformation analysisTaskIds,
        StepType stepType,
        List<OrderedSampleFractions> samples,
        BiFunction<Integer, List<UUID>, Optional<? extends Task>> indexedFractionLevelToTask
    ) throws JsonProcessingException {
        final int fractionsPerSample = samples.get(0).fractionIds().size();

        final UUID[][] sampleFractionIds = new UUID[fractionsPerSample][samples.size()];
        final Iterator<OrderedSampleFractions> sampleFractionsIterator = samples.iterator();
        for (int sampleIndex = 0; sampleFractionsIterator.hasNext(); sampleIndex++) {
            final Iterator<UUID> fractionIdIterator = sampleFractionsIterator.next().fractionIds().iterator();
            for (int fractionIndex = 0; fractionIdIterator.hasNext(); fractionIndex++) {
                final UUID fractionId = fractionIdIterator.next();
                sampleFractionIds[fractionIndex][sampleIndex] = fractionId;
            }
        }

        final Set<UUID> queuedFractionIds = analysisTaskIds.stepQueuedFractionIds(stepType);
        List<TaskDescription> tasks = new ArrayList<>();
        for (int i = 0; i < fractionsPerSample; i++) {
            final List<UUID> fractionIds = ImmutableList.copyOf(sampleFractionIds[i]);
            if (queuedFractionIds.contains(sampleFractionIds[i][0])) {
                continue;
            }
            final Optional<? extends Task> optionalTask = indexedFractionLevelToTask.apply(i, fractionIds);
            if (optionalTask.isPresent()) {
                Task task = optionalTask.get();
                tasks.add(taskDescriptionIndexFractionLevel(task, stepType, i, Lists.transform(samples, OrderedSampleFractions::sampleId), fractionIds));
            }
        }
        return tasks;
    }

    private List<TaskDescription> indexedFractionLevelTaskProto(
        AnalysisTaskInformation analysisTaskIds,
        StepType stepType,
        List<OrderedSampleFractions> samples,
        BiFunction<Integer, List<UUID>, Optional<TaskParameters>> indexedFractionLevelToTask
    ) throws JsonProcessingException {
        final int fractionsPerSample = samples.get(0).fractionIds().size();

        final UUID[][] sampleFractionIds = new UUID[fractionsPerSample][samples.size()];
        final Iterator<OrderedSampleFractions> sampleFractionsIterator = samples.iterator();
        for (int sampleIndex = 0; sampleFractionsIterator.hasNext(); sampleIndex++) {
            final Iterator<UUID> fractionIdIterator = sampleFractionsIterator.next().fractionIds().iterator();
            for (int fractionIndex = 0; fractionIdIterator.hasNext(); fractionIndex++) {
                final UUID fractionId = fractionIdIterator.next();
                sampleFractionIds[fractionIndex][sampleIndex] = fractionId;
            }
        }

        final Set<UUID> queuedFractionIds = analysisTaskIds.stepQueuedFractionIds(stepType);
        List<TaskDescription> tasks = new ArrayList<>();
        for (int i = 0; i < fractionsPerSample; i++) {
            final List<UUID> fractionIds = ImmutableList.copyOf(sampleFractionIds[i]);
            if (queuedFractionIds.contains(sampleFractionIds[i][0])) {
                continue;
            }
            Optional<TaskParameters> optionalTask = indexedFractionLevelToTask.apply(i, fractionIds);
            if (optionalTask.isPresent()) {
                TaskParameters taskParameters = optionalTask.get();
                tasks.add(taskDescriptionIndexFractionLevel(taskParameters, stepType, i, Lists.transform(samples, OrderedSampleFractions::sampleId), fractionIds));
            }
        }
        return tasks;
    }


    private DBSearchParameters dbSearchParameters(DBSearchParameters originalDbParameters) {
        final IDBaseParameters populated = parametersPopulater.populateDBParameters(originalDbParameters);
        return new DBSearchParametersBuilder().from(originalDbParameters)
            .fixedModification(populated.fixedModification())
            .variableModification(populated.variableModification())
            .build();
    }

    private float psmPValueFromIdentificationFilter(String keyspace, IdentificaitonFilter filter, UUID summarizationTaskId, boolean isDIA) {
        if (filter.getPsmFilterTypeCase() == IdentificaitonFilter.PsmFilterTypeCase.MINPVALUE) {
            return filter.getMinPValue();
        } else {
            return fdrToPValue(keyspace, summarizationTaskId, filter.getMaxFdr(), FdrType.PSM, isDIA);
        }
    }

    private float peptidePValueFromIdentificationFilter(String keyspace, IdentificaitonFilter filter, UUID summarizationTaskId, boolean isDIA) {
        if (filter.hasSlPeptideFilter()) {
            final SlFilter.PeptideFilter slPeptideFilter = filter.getSlPeptideFilter();
            if (slPeptideFilter.getUseFdr()) {
                return fdrToPValue(keyspace, summarizationTaskId, (float) slPeptideFilter.getMaxFDR(), FdrType.PEPTIDE, isDIA);
            } else {
                return (float) slPeptideFilter.getMinPValue();
            }
        } else if (filter.hasIdPeptideFilter()) {
            final IDFilter.PeptideFilter idPeptideFilter = filter.getIdPeptideFilter();
            if (idPeptideFilter.getUseFdr()) {
                return fdrToPValue(keyspace, summarizationTaskId, (float) idPeptideFilter.getMaxFDR(), FdrType.PEPTIDE, isDIA);
            } else {
                return (float) idPeptideFilter.getMinPValue();
            }
        } else {
            return 0f;
        }
    }

    private float getPValueOnRiqComplete(AnalysisTaskInformation analysisTaskIds, FdrType fdrType, boolean isDIA) {
        ReporterIonQFilterSummarization summarizationParameters = analysisTaskIds.stepParametersReporterIonQFilterSummarization();
        ReporterIonQSpectrumFilter spectrumFilter = summarizationParameters.getSpectrumFilter();

        final UUID searchSummariationTaskId;
        if (analysisTaskIds.hasSpider()) {
            searchSummariationTaskId = analysisTaskIds.spiderSearchSummarizeTaskId().orElseThrow(IllegalStateException::new);
        } else if (analysisTaskIds.hasPtm()) {
            searchSummariationTaskId = analysisTaskIds.ptmSearchSummarizeTaskId().orElseThrow(IllegalStateException::new);
        } else if (analysisTaskIds.hasDb()) {
            searchSummariationTaskId = analysisTaskIds.dbSearchSummarizeTaskId().orElseThrow(IllegalStateException::new);
        } else {
            throw new IllegalStateException("Not Identification step cannot calculate pValue/fdr Spectrum filter value.");
        }

        switch (spectrumFilter.getFilterTypeCase()) {
            case MINPVALUE:
                return spectrumFilter.getMinPValue();
            case MAXFDR:
                return fdrToPValue(analysisTaskIds.keyspace(), searchSummariationTaskId, spectrumFilter.getMaxFDR(), fdrType, isDIA);
            default:
                throw new IllegalStateException("Filter had invalid pvalue type.");
        }
    }

    private OrderedSampleFractionsList silacSamplesInGroup(AnalysisTaskInformation analysis) {
        List<OrderedSampleFractions> allSamples = analysis.samples();
        WorkflowStep silacFilterStep = analysis.steps().stream()
            .filter(step -> step.getStepType().equals(SILAC_FILTER_SUMMARIZE))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Unable to find Silac filter summarization step in the analysis task parameters"));

        List<SilacParameters.Group> groups = silacFilterStep.getSilacFilterSummarizationParamaters().getSilacParameters().getGroupsList();
        Set<UUID> sampleIdsInSilac = groups.stream()
            .flatMap(group -> group.getSamplesList().stream())
            .map(SilacParameters.Sample::getSampleId)
            .map(UUID::fromString)
            .collect(Collectors.toSet());
        ;
        return new OrderedSampleFractionsList(allSamples.stream()
            .filter(sample -> sampleIdsInSilac.contains(sample.sampleId()))
            .collect(Collectors.toList()));
    }
}
