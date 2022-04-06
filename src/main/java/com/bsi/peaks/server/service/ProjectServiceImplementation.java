package com.bsi.peaks.server.service;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.analysis.dto.Analysis;
import com.bsi.peaks.analysis.analysis.dto.AnalysisParameters;
import com.bsi.peaks.analysis.analysis.dto.DbSearchStep;
import com.bsi.peaks.analysis.analysis.dto.SlStep;
import com.bsi.peaks.analysis.parameterModels.dto.AnalysisAcquisitionMethod;
import com.bsi.peaks.analysis.parameterModels.dto.SilacParameters;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.graph.DbApplicationGraph;
import com.bsi.peaks.data.graph.DbApplicationGraphImplementation;
import com.bsi.peaks.data.graph.GraphTaskIds;
import com.bsi.peaks.data.model.vm.EnzymeVM;
import com.bsi.peaks.data.model.vm.InstrumentVM;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.UserQueryFactory;
import com.bsi.peaks.event.projectAggregate.AnalysisInformation;
import com.bsi.peaks.event.projectAggregate.Fraction;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateCommand;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest.AnalysisQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState;
import com.bsi.peaks.event.projectAggregate.ProjectInformation;
import com.bsi.peaks.event.projectAggregate.ProjectPermission;
import com.bsi.peaks.event.projectAggregate.RemoteSample;
import com.bsi.peaks.event.projectAggregate.SampleActivationMethod;
import com.bsi.peaks.event.projectAggregate.SampleEnzyme;
import com.bsi.peaks.event.projectAggregate.SampleInformation;
import com.bsi.peaks.event.projectAggregate.SampleParameters;
import com.bsi.peaks.event.projectAggregate.WorkflowStepFilter;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.event.user.UserState;
import com.bsi.peaks.internal.task.*;
import com.bsi.peaks.io.writer.dto.DbDtoSources;
import com.bsi.peaks.io.writer.dto.DbDtoSourcesViaDbGraph;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.dto.ActivationMethod;
import com.bsi.peaks.model.dto.LfqFilter;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.dto.ProjectAnalysisInfos;
import com.bsi.peaks.model.dto.ReporterIonQFilterSummarization;
import com.bsi.peaks.model.dto.Sample;
import com.bsi.peaks.model.dto.SampleIdList;
import com.bsi.peaks.model.dto.SampleSubmission;
import com.bsi.peaks.model.dto.SilacFilter;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.filter.FilterFunction;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParameters;
import com.bsi.peaks.model.parameters.PtmFinderParameters;
import com.bsi.peaks.model.parameters.RetentionTimeAlignmentParameters;
import com.bsi.peaks.model.parameters.SpiderParameters;
import com.bsi.peaks.model.parameters.TagSearchParameters;
import com.bsi.peaks.model.proto.Enzyme;
import com.bsi.peaks.model.proto.SimplifiedFastaSequenceDatabaseInfo;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.model.system.steps.DataLoadingStepBuilder;
import com.bsi.peaks.model.system.steps.DataRefineStepBuilder;
import com.bsi.peaks.model.system.steps.DbBatchSearchStepBuilder;
import com.bsi.peaks.model.system.steps.DbPreSearchStepBuilder;
import com.bsi.peaks.model.system.steps.DbSummarizeStepBuilder;
import com.bsi.peaks.model.system.steps.DbTagSearchStepBuilder;
import com.bsi.peaks.model.system.steps.DbTagSummarizeStepBuilder;
import com.bsi.peaks.model.system.steps.DenovoStep;
import com.bsi.peaks.model.system.steps.DenovoStepBuilder;
import com.bsi.peaks.model.system.steps.FeatureDetectionStepBuilder;
import com.bsi.peaks.model.system.steps.LfqFeatureAlignmentStep;
import com.bsi.peaks.model.system.steps.LfqFeatureAlignmentStepBuilder;
import com.bsi.peaks.model.system.steps.LfqSummarizeStep;
import com.bsi.peaks.model.system.steps.LfqSummarizeStepBuilder;
import com.bsi.peaks.model.system.steps.PtmFinderBatchStep;
import com.bsi.peaks.model.system.steps.PtmFinderBatchStepBuilder;
import com.bsi.peaks.model.system.steps.PtmFinderSummarizeStep;
import com.bsi.peaks.model.system.steps.PtmFinderSummarizeStepBuilder;
import com.bsi.peaks.model.system.steps.RetentionTimeAlignmentStep;
import com.bsi.peaks.model.system.steps.RetentionTimeAlignmentStepBuilder;
import com.bsi.peaks.model.system.steps.SpiderBatchStep;
import com.bsi.peaks.model.system.steps.SpiderBatchStepBuilder;
import com.bsi.peaks.model.system.steps.SpiderSummarizeStep;
import com.bsi.peaks.model.system.steps.SpiderSummarizeStepBuilder;
import com.bsi.peaks.model.system.steps.Step;
import com.bsi.peaks.model.system.steps.StepHelper;
import com.bsi.peaks.server.cluster.Master;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.bsi.peaks.server.es.communication.UserManagerCommunication;
import com.bsi.peaks.server.es.project.ProjectView;
import com.bsi.peaks.server.es.tasks.Helper;
import com.bsi.peaks.server.handlers.HandlerException;
import com.bsi.peaks.server.parsers.ParametersParser;
import com.bsi.peaks.server.parsers.ParserException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import static com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType.*;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ProjectServiceImplementation implements ProjectService {

    private static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new Jdk8Module());

    private static final Logger LOG = LoggerFactory.getLogger(ProjectServiceImplementation.class);
    final Materializer materializer;
    private final ActorRef masterProxy;
    private final UserManagerCommunication userManagerCommunication;
    private final ParametersParser parametersParser;
    private final ProjectView projectView;
    private final ApplicationStorageFactory storageFactory;
    private final ProjectAggregateCommunication projectAggregateCommunication;
    private final InstrumentDaemonService instrumentDaemonService;
    private final Map<String, CompletionStage<Done>> deletingKeyspaces = new ConcurrentHashMap<>();

    @Inject
    public ProjectServiceImplementation(
        final ActorSystem system,
        @Named(Master.PROXY_NAME) final ActorRef masterProxy,
        final UserManagerCommunication userManagerCommunication,
        final ParametersParser parametersParser,
        final ProjectView projectView,
        final ProjectAggregateCommunication projectAggregateCommunication,
        final InstrumentDaemonService instrumentDaemonService,
        final ApplicationStorageFactory storageFactory
    ) {
        this.materializer = ActorMaterializer.create(system);
        this.masterProxy = masterProxy;
        this.projectAggregateCommunication = projectAggregateCommunication;
        this.instrumentDaemonService = instrumentDaemonService;
        this.userManagerCommunication = userManagerCommunication;
        this.parametersParser = parametersParser;
        this.projectView = projectView;
        this.storageFactory = storageFactory;
    }

    public Pair<List<SimplifiedFastaSequenceDatabaseInfo>, List<WorkflowStep>> parseAnalysisSteps(Analysis analysis, User creator) {
        List<WorkflowStep> steps = new ArrayList<>();
        List<SimplifiedFastaSequenceDatabaseInfo> databases = Collections.emptyList();

        try {
            steps.add(workflowStep(new FeatureDetectionStepBuilder()
                .parameters(parametersParser.getFeatureDetectionParameters())
                .build()));
            final boolean isDia;
            if (analysis.getAnalysisAcquisitionMethod() == AnalysisAcquisitionMethod.DIA) {
                isDia = true;
            } else {
                isDia = false;
            }
            if (!isDia) { //DIA does not have data refine step
                steps.add(workflowStep(new DataRefineStepBuilder()
                    .parameters(parametersParser.getDataRefineParameters())
                    .build()));
            }
            for (WorkFlowType type : analysis.getWorkFlowsEnabledList()) {
                boolean denovoDisabled = !analysis.getWorkFlowsEnabledList().contains(WorkFlowType.DENOVO);
                switch (type) {
                    case DB_SEARCH: {
                        DBSearchParameters dbSearchParameters = parametersParser.getDbSearchParameters(creator, analysis);
                        if (dbSearchParameters.maxVariablePtmPerPeptide() > 10) {
                            throw new IllegalStateException("Max Varaible PTM must be < 11.");
                        }
                        databases = parametersParser.getDatabases(creator, analysis.getDbSearchStep().getDbSearchParameters());
                        if (denovoDisabled) {
                            steps.add(StepHelper.fdbProteinCandidateFromFastaStep(FDBProteinCandidateParameters.getDefaultInstance()));
                            steps.add(workflowStep(new DbPreSearchStepBuilder().parameters(dbSearchParameters).build()));
                            steps.add(workflowStep(new DbBatchSearchStepBuilder().parameters(dbSearchParameters).build()));
                            steps.add(workflowStep(new DbSummarizeStepBuilder().parameters(dbSearchParameters).build()));
                        } else {
                            TagSearchParameters tagSearchParameters = parametersParser.getTagSearchParameters(creator, analysis);
                            // regular workflow
                            steps.add(workflowStep(new DbTagSearchStepBuilder().parameters(tagSearchParameters).build()));
                            steps.add(workflowStep(new DbTagSummarizeStepBuilder().parameters(tagSearchParameters).build()));
                            steps.add(workflowStep(new DbPreSearchStepBuilder().parameters(dbSearchParameters).build()));
                            steps.add(workflowStep(new DbBatchSearchStepBuilder().parameters(dbSearchParameters).build()));
                            steps.add(workflowStep(new DbSummarizeStepBuilder().parameters(dbSearchParameters).build()));
                        }
                        IdentificaitonFilter dbFilter = parametersParser.getDbFilterSummarizationParameters(analysis);
                        steps.add(StepHelper.dbFilterProteinSummarizationStep(analysis, dbFilter));
                        if (!denovoDisabled)
                            steps.add(StepHelper.denovoOnlyTagSearchStep(analysis, parametersParser.getDbDenovoOnlyTagSearchParameters(analysis), type));
                        break;
                    }
                    case DENOVO: {
                        if (analysis.getAnalysisAcquisitionMethod().equals(AnalysisAcquisitionMethod.DIA)) {
                            steps.add(StepHelper.diaDataRefineStep(parametersParser.diadataRefineParameters(analysis, type)));
                        }
                        DenovoStep denovoStep = new DenovoStepBuilder().parameters(parametersParser.denovoParameters(creator, analysis)).build();
                        DenovoFilter denovoFilter = parametersParser.getDenovoFilterSummarizationParameters(analysis);
                        steps.add(workflowStep(denovoStep));
                        steps.add(StepHelper.denovoFilterStep(denovoFilter));
                        break;
                    }
                    case PTM_FINDER: {
                        if (denovoDisabled) {
                            // sanity check
                            throw new IllegalStateException("Must have denovo enabled to perform ptm finder");
                        }
                        PtmFinderParameters parameters = parametersParser.getPtmFinderParameters(creator, analysis);
                        PtmFinderBatchStep ptmFinderBatchStep = new PtmFinderBatchStepBuilder().parameters(parameters).build();
                        PtmFinderSummarizeStep ptmFinderSummarizeStep =
                            new PtmFinderSummarizeStepBuilder().parameters(parameters).build();
                        IdentificaitonFilter ptmFilter = parametersParser.getPtmFilterSummarizationParameters(analysis);
                        steps.add(workflowStep(ptmFinderBatchStep));
                        steps.add(workflowStep(ptmFinderSummarizeStep));
                        steps.add(StepHelper.ptmFilterProteinSummarizationStep(analysis, ptmFilter));
                        steps.add(StepHelper.denovoOnlyTagSearchStep(analysis, parametersParser.getPtmFinderDenovoOnlyTagSearchParameters(analysis), type));
                        break;
                    }
                    case SPIDER: {
                        if (denovoDisabled) {
                            // sanity check
                            throw new IllegalStateException("Must have denovo enabled to perform spider");
                        }
                        SpiderParameters parameters = parametersParser.getSpiderParameters(creator, analysis);
                        SpiderBatchStep spiderBatchStep = new SpiderBatchStepBuilder().parameters(parameters).build();
                        SpiderSummarizeStep spiderSummarizeStep = new SpiderSummarizeStepBuilder().parameters(parameters).build();
                        IdentificaitonFilter spiderFilter = parametersParser.getSpiderFilterSummarizationParameters(analysis);
                        steps.add(workflowStep(spiderBatchStep));
                        steps.add(workflowStep(spiderSummarizeStep));
                        steps.add(StepHelper.spiderFilterProteinSummarizationStep(analysis, spiderFilter));
                        steps.add(StepHelper.denovoOnlyTagSearchStep(analysis, parametersParser.getSpiderDenovoOnlyTagSearchParameters(analysis), type));
                        break;
                    }
                    case LFQ_FRACTION_ID_TRANSFER:
                    case LFQ: {
                        if (analysis.getAnalysisAcquisitionMethod().equals(AnalysisAcquisitionMethod.DDA)) {
                            LabelFreeQuantificationParameters lfqParameters = parametersParser.getLfqParameters(analysis);
                            RetentionTimeAlignmentParameters rtAlignmentParameters = parametersParser.getRtAlignmentParameters(analysis);
                            RetentionTimeAlignmentStep retentionTimeAlignmentStep =
                                new RetentionTimeAlignmentStepBuilder().parameters(rtAlignmentParameters).build();
                            LfqFeatureAlignmentStep lfqFeatureAlignmentStep =
                                new LfqFeatureAlignmentStepBuilder().parameters(lfqParameters).build();
                            LfqSummarizeStep lfqSummarizeStep = new LfqSummarizeStepBuilder().parameters(lfqParameters).build();
                            LfqFilter lfqFilter = parametersParser.getLfqFilter(analysis);
                            steps.add(workflowStep(retentionTimeAlignmentStep));
                            steps.add(workflowStep(lfqFeatureAlignmentStep));
                            steps.add(workflowStep(lfqSummarizeStep));
                            steps.add(StepHelper.lfqFilterSummarizationStep(analysis, lfqFilter));
                        } else if (analysis.getAnalysisAcquisitionMethod().equals(AnalysisAcquisitionMethod.DIA)) {
                            DiaFeatureExtractorParameters featureExtractorParameters = parametersParser.getDiaFeatureExtractorParameters(analysis);
                            LfqFilter lfqFilter = parametersParser.getLfqFilter(analysis);
                            steps.add(StepHelper.diaLfqRtAlignmentStep());
                            steps.add(StepHelper.diaLfqFeatureExtractionStep(featureExtractorParameters));
                            steps.add(StepHelper.diaLfqSummarizationStep());
                            // same step, but change the type
                            steps.add(StepHelper.lfqFilterSummarizationStep(analysis, lfqFilter).toBuilder()
                                .setStepType(StepType.DIA_LFQ_FILTER_SUMMARIZATION).build()
                            );
                        }
                        break;
                    }
                    case REPORTER_ION_Q: {
                        ReporterIonQBatchCalculationParameters qParameters = parametersParser.getReporterIonQParameters(creator, analysis);
                        ReporterIonQFilterSummarization filterSummarization = parametersParser.getReporterIonQFilterSummarization(analysis);
                        steps.add(StepHelper.reporterIonQBatchCalculationStep(qParameters));
                        steps.add(StepHelper.reporterIonQNormalizationStep());
                        steps.add(StepHelper.reporterIonQSummarizationStep(analysis, filterSummarization));
                        break;
                    }
                    case SPECTRAL_LIBRARY: {
                        SlSearchParameters slSearchParameters = parametersParser.getSlSearchParameters(creator, analysis);
                        SlStep slStep = analysis.getSlStep();
                        SlPeptideSummarizationParameters slPeptideSummarizationParameters = parametersParser.getSlPeptideSummarizationParameters(creator, analysis, Optional.of(slSearchParameters));
                        SlFilterSummarizationParameters slFilterSummarizationParameters = parametersParser.getSlFilterSummarizationParameters(creator, slStep);
                        databases = parametersParser.getDatabases(creator, slStep.getSlProteinInferenceStepParameters());
                        if (isDia){
                            steps.add(StepHelper.slSearchStep(slSearchParameters));
                        } else {
                            steps.add(StepHelper.ddaLibrarySearchStep(slSearchParameters));
                        }
                        steps.add(StepHelper.slPeptideSummarizationStep(slPeptideSummarizationParameters));
                        steps.add(StepHelper.slFilterSummarizationStep(analysis, slFilterSummarizationParameters));
                        break;
                    }
                    case DIA_DB_SEARCH: {
                        //get library info
                        final Optional<UUID> libId;
                        Optional<Pair<UUID, Integer>> libraryIdAndEntryCountPair = parametersParser.getLibraryIdAndEntryCountFromAnalysis(creator, analysis);
                        if (libraryIdAndEntryCountPair.isPresent()) {
                            libId = Optional.of(libraryIdAndEntryCountPair.get().first());
                        } else {
                            libId = Optional.empty();
                        }

                        //make parameters
                        final SlPeptideSummarizationParameters slPeptideSummarizationParameters;
                        final SlFilterSummarizationParameters slFilterSummarizationParameters;
                        if (analysis.getWorkFlowsEnabledList().contains(WorkFlowType.SPECTRAL_LIBRARY)) {
                            SlSearchParameters slSearchParameters = parametersParser.getSlSearchParameters(creator, analysis);
                            SlStep slStep = analysis.getSlStep();
                            slPeptideSummarizationParameters = parametersParser.getSlPeptideSummarizationParameters(creator, analysis, Optional.of(slSearchParameters));
                            slFilterSummarizationParameters = parametersParser.getSlFilterSummarizationParameters(creator, slStep);
                        } else {
                            DbSearchStep dbSearchStep = analysis.getDbSearchStep();
                            slPeptideSummarizationParameters = parametersParser.getSlPeptideSummarizationParameters(creator, analysis, Optional.empty());
                            slFilterSummarizationParameters = parametersParser.getSlFilterSummarizationParameters(creator, dbSearchStep);
                        }

                        DiaDbSearchParameters diaDbSearchParameters = parametersParser.getDiaDbSearchParameters(creator, analysis, libId);
                        DiaDbPreSearchParameters diaDbPreSearchParameters = parametersParser.getDiaDbPreSearchParameters(creator, analysis, libId);

                        databases = parametersParser.getDatabases(creator, analysis.getDbSearchStep().getDbSearchParameters());
                        //make steps
                        steps.add(StepHelper.diaDbDataRefineStep(parametersParser.diadataRefineParameters(analysis, type)));
                        steps.add(StepHelper.diaDbPreSearchStep(diaDbPreSearchParameters));
                        steps.add(StepHelper.diaDbSearchStep(diaDbSearchParameters));
                        steps.add(StepHelper.diaDbPeptideSummarizationStep(slPeptideSummarizationParameters));
                        steps.add(StepHelper.diaDbFilterSummarizationStep(analysis, slFilterSummarizationParameters));
                        break;
                    }
                    case SILAC_FRACTION_ID_TRANSFER:
                    case SILAC: {
                        final SilacParameters silacParameters = parametersParser.getSilacParameters(creator, analysis, type);
                        final SilacFilter silacFilter = analysis.getSilacStep().getSilacFilter();
                        boolean enableMatchBetweenRuns = silacParameters.getEnableMatchBetweenRuns();
                        if (enableMatchBetweenRuns) {
                            SilacParameters.MatchBetweenRuns matchBetweenRunsParams = silacParameters.getMatchBetweenRunsParams();
                            steps.add(StepHelper.silacRtAlignmentStep(matchBetweenRunsParams, isDia));
                            steps.add(StepHelper.silacFeatureAlignmentStep(matchBetweenRunsParams, isDia));
                        }
                        steps.add(StepHelper.silacFeatureVectorSearchStep(silacParameters, isDia, enableMatchBetweenRuns));
                        steps.add(StepHelper.silacPeptideSummarizeStep(silacParameters));
                        steps.add(StepHelper.silacFilterSummarizeStep(silacParameters, silacFilter));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Invalid analysis parameters", e);
            throw new InvalidParameterException("Invalid analysis parameter cause " + e.getMessage());
        }

        return Pair.create(databases, steps);
    }

    @Override
    public CompletionStage<UUID> createProject(Project project, User creator) {
        String targetKeyspace = storageFactory.getKeyspace(
            Project.newBuilder()
                .setCreatedTimestamp(CommonFactory.convert(CommonFactory.nowTimeStamp()).getEpochSecond())
                .build()
        );
        if (deletingKeyspaces.containsKey(targetKeyspace)) {
            return Futures.failedCompletionStage(new IllegalStateException(
                "The project's associated keyspace is currently being dropped, wait for the drop to complete to proceed."));
        }

        UUID projectId = UUID.randomUUID();
        ProjectAggregateCommandFactory projectCommandFactory = ProjectAggregateCommandFactory.project(projectId, creator.userId());

        final ProjectInformation projectInformation = ProjectInformation.newBuilder()
            .setDescription(project.getDescription())
            .setName(project.getName())
            .build();

        final ProjectPermission projectPermission = ProjectPermission.newBuilder()
            .setOwnerId(ModelConversion.uuidToByteString(creator.userId()))
            .setShared(false)
            .build();

        final ProjectAggregateCommand createCommand = projectCommandFactory.create(
            projectInformation,
            projectPermission
        );

        return projectAggregateCommunication.command(createCommand).thenApply(ignore -> projectId);
    }

    @Override
    public CompletionStage<Done> deleteProject(UUID projectId, User user, boolean forceDeleteWithoutKeyspace) {
        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId());
        return projectAggregateCommunication.command(commandFactory.delete(forceDeleteWithoutKeyspace))
            .exceptionally(throwable -> {
                if (throwable instanceof NoSuchElementException) {
                    return Done.getInstance();
                }
                throw Throwables.propagate(throwable);
            });
    }

    @Override
    public CompletionStage<Done> deleteProjectsInKeyspace(User user, String keyspace) {
        return deletingKeyspaces.computeIfAbsent(keyspace, ignore -> projectsInKeyspace(user, keyspace)
            .mapAsync(storageFactory.getParallelism(), project -> deleteProject(UUID.fromString(project.getId()), user, true))
            .runWith(Sink.ignore(), materializer)
            .whenComplete((done, error) -> deletingKeyspaces.remove(keyspace))
        );
    }

    @Override
    public CompletionStage<Done> updateProject(UUID projectId, User user, ProjectInformation projectInformation) {
        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId());
        return projectAggregateCommunication.command(commandFactory.setInformation(projectInformation));
    }

    @Override
    public CompletionStage<Done> updateProject(UUID projectId, User user, ProjectPermission projectPermission) {
        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId());
        return projectAggregateCommunication.command(commandFactory.setPermission(projectPermission));
    }

    @Override
    public CompletionStage<Done> deleteAnalysis(UUID projectId, UUID analysisId, User user) {
        if (!projectView.writePermission(user.userId(), projectId)) {
            return Futures.failedCompletionStage(new HandlerException(403, "User does not have permission to delete analysis"));
        }
        final ProjectAggregateCommandFactory.AnalysisCommandFactory commandFactory = ProjectAggregateCommandFactory
            .project(projectId, user.userId())
            .analysis(analysisId);
        return projectAggregateCommunication.command(commandFactory.delete())
            .exceptionally(throwable -> {
                if (throwable instanceof NoSuchElementException) {
                    return Done.getInstance();
                }
                throw Throwables.propagate(throwable);
            });
    }

    @Override
    public CompletionStage<Done> cancelAnalysis(UUID projectId, UUID analysisId, User user) {
        if (!projectView.writePermission(user.userId(), projectId)) {
            return Futures.failedCompletionStage(new HandlerException(403, "User does not have permission to cancel analysis"));
        }
        final ProjectAggregateCommandFactory.AnalysisCommandFactory commandFactory = ProjectAggregateCommandFactory
            .project(projectId, user.userId())
            .analysis(analysisId);
        return projectAggregateCommunication.command(commandFactory.cancel())
            .exceptionally(throwable -> {
                if (throwable instanceof NoSuchElementException) {
                    return Done.getInstance();
                }
                throw Throwables.propagate(throwable);
            });
    }

    @Override
    public CompletionStage<Done> updateSample(UUID projectId, UUID sampleId, User user, SampleInformation sampleInformation) {
        ProjectAggregateCommandFactory.SampleCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId()).sample(sampleId);
        return projectAggregateCommunication.command(commandFactory.setInformation(sampleInformation));
    }

    @Override
    public CompletionStage<Done> updateSampleEnzyme(UUID projectId, UUID sampleId, User user, String enzyme) {
        Optional<Enzyme> enzymeProto = Optional.empty();
        try {
            enzymeProto = Optional.of(DtoAdaptors.convertEnzymeProto((EnzymeVM) parametersParser.fetchEnzyme(user, enzyme)));
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        if (!enzymeProto.isPresent()) {
            return completedFuture(Done.getInstance());
        }
        final SampleEnzyme sampleEnzyme = SampleEnzyme.newBuilder().setEnzyme(enzymeProto.get()).build();
        ProjectAggregateCommandFactory.SampleCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId()).sample(sampleId);
        return projectAggregateCommunication.command(commandFactory.setEnzyme(sampleEnzyme));
    }

    @Override
    public CompletionStage<Done> updateSampleActivationMethod(UUID projectId, UUID sampleId, User user, String activationMethodName) {
        final ActivationMethod activationMethod;
        if (activationMethodName.equalsIgnoreCase(ActivationMethod.CID.name())) {
            activationMethod =  ActivationMethod.CID;
        } else if (activationMethodName.equalsIgnoreCase(ActivationMethod.HCD.name())) {
            activationMethod =  ActivationMethod.HCD;
        } else if (activationMethodName.equalsIgnoreCase(ActivationMethod.ETD.name())) {
            activationMethod =  ActivationMethod.ETD;
        } else if (activationMethodName.equalsIgnoreCase(ActivationMethod.MIX.name())) {
            activationMethod =  ActivationMethod.MIX;
        } else if (activationMethodName.equalsIgnoreCase(ActivationMethod.PQD.name())) {
            activationMethod =  ActivationMethod.PQD;
        } else if (activationMethodName.equalsIgnoreCase(ActivationMethod.IRMPD.name())) {
            activationMethod =  ActivationMethod.IRMPD;
        } else if (activationMethodName.equalsIgnoreCase(ActivationMethod.ETHCD.name())) {
            activationMethod =  ActivationMethod.ETHCD;
        } else {
            activationMethod = ActivationMethod.UNDEFINED;
        }
        ProjectAggregateCommandFactory.SampleCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId()).sample(sampleId);
        final SampleActivationMethod sampleActivationMethod = SampleActivationMethod.newBuilder().setActivationMethod(activationMethod).build();
        return projectAggregateCommunication.command(commandFactory.setActivationMethod(sampleActivationMethod));
    }

    @Override
    public CompletionStage<Done> updatedFractionName(UUID projectId, UUID sampleId, UUID fractionId, User user, String newName) {
        ProjectAggregateCommandFactory.SampleCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId()).sample(sampleId);
        return projectAggregateCommunication.command(commandFactory.updateFractionName(sampleId, fractionId, newName));
    }

    @Override
    public CompletionStage<Done> deleteSample(UUID projectId, UUID sampleId, User user) {
        ProjectAggregateCommandFactory.SampleCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId()).sample(sampleId);
        return projectAggregateCommunication.commandWithAck(commandFactory.delete())
            .thenApply(ack -> {
                switch (ack.getAckStatus()) {
                    case ACCEPT:
                        return Done.getInstance();
                    case REJECT:
                        throw new HandlerException(400, ack.getAckStatusDescription());
                    default:
                        throw new IllegalStateException("Unexpected ack status" + ack.getAckStatus());
                }
            });
    }

    @Override
    public CompletionStage<Done> deleteSamplesByStatus(UUID projectId, ProjectAggregateState.SampleState.SampleStatus status, User user) {
        ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory
            .project(projectId, user.userId());
        return projectAggregateCommunication.commandWithAck(commandFactory.deleteSamplesBatch(status))
            .thenApply(ack -> {
                switch (ack.getAckStatus()) {
                    case ACCEPT:
                        return Done.getInstance();
                    case REJECT:
                        throw new HandlerException(400, ack.getAckStatusDescription());
                    default:
                        throw new IllegalStateException("Unexpected ack status" + ack.getAckStatus());
                }
            });
    }

    @Override
    public CompletionStage<Done> uploadSampleNotification(UUID projectId, UUID user, ProjectAggregateCommand uploadNotification) {
        return projectAggregateCommunication.command(uploadNotification);
    }

    @Override
    public CompletionStage<Done> queueRemoteUploads(UUID projectId, UUID userId, List<RemoteSample> remoteSamples) {
        final ProjectAggregateCommand command = ProjectAggregateCommandFactory.project(projectId, userId).queueRemoteUploads(remoteSamples);
        return projectAggregateCommunication.command(command);
    }

    @Override
    public CompletionStage<Done> restartFailedUploads(UUID projectId, User user) {
        return projectAggregateCommunication.command(ProjectAggregateCommandFactory.project(projectId, user.userId()).restartFailedUploads());
    }

    @Override
    public CompletionStage<Done> setFilter(UUID projectId, UUID analysisId, WorkflowStepFilter filter, User user) {
        final ProjectAggregateCommandFactory.AnalysisCommandFactory commandFactory = ProjectAggregateCommandFactory
            .project(projectId, user.userId())
            .analysis(analysisId);
        return projectAggregateCommunication.command(commandFactory.userSetFilter(filter))
            .exceptionally(throwable -> {
                if (throwable instanceof NoSuchElementException) {
                    return Done.getInstance();
                }
                throw Throwables.propagate(throwable);
            });
    }

    @Override
    public CompletionStage<SampleIdList> createSamples(UUID projectId, SampleSubmission samples, User creator) {
        ProjectAggregateCommandFactory projectCommandFactory = ProjectAggregateCommandFactory
            .project(projectId, creator.userId());

        List<String> sampleIds = new ArrayList<>();
        List<ProjectAggregateCommand.CreateSample> createSamples = new ArrayList<>();
        for (Sample sample : samples.getSampleList()) {
            final UUID sampleId = UUID.randomUUID();
            final ProjectAggregateCommandFactory.SampleCommandFactory commandFactory = projectCommandFactory
                .sample(sampleId);
            sampleIds.add(sampleId.toString());
            createSamples.add(buildCreateSampleCommand(sample, creator, commandFactory));
        }

        ProjectAggregateCommand samplesBatchCommand = projectCommandFactory.createSamplesBatch(createSamples);

        return projectAggregateCommunication.command(samplesBatchCommand)
            .thenApply(ignore -> SampleIdList.newBuilder().addAllId(sampleIds).build());
    }

    @Override
    public CompletionStage<Done> createDaemonFractionsInfo(UUID projectId, List<UUID> fractionIds, int timeOut, User creator) {
        ProjectAggregateCommandFactory projectCommandFactory = ProjectAggregateCommandFactory
            .project(projectId, creator.userId());

        ProjectAggregateCommand daemonFractionsBatchCommand =
            projectCommandFactory.createDaemonFractionsBatch(fractionIds, timeOut < 0 ? Integer.MAX_VALUE : timeOut);

        return projectAggregateCommunication.command(daemonFractionsBatchCommand);
    }

    private ProjectAggregateCommand.CreateSample buildCreateSampleCommand(Sample sample, User creator, ProjectAggregateCommandFactory.SampleCommandFactory commandFactory) {
        final SampleInformation sampleInformation = SampleInformation.newBuilder()
            .setName(sample.getName())
            .build();
        final SampleParameters sampleParameters = SampleParameters.newBuilder()
            .setEnzyme(DtoAdaptors.convertEnzymeProto((EnzymeVM) parametersParser.fetchEnzyme(creator, sample.getEnzyme())))
            .setInstrument(DtoAdaptors.convertInstrumentProto((InstrumentVM) parametersParser.fetchInstrument(creator, sample.getInstrument())))
            .setActivationMethod(sample.getActivationMethod())
            .setAcquisitionMethod(sample.getAcquisitionMethod())
            .build();
        final Iterable<Fraction> fractions = Iterables.transform(
            sample.getFractionsList(),
            fraction -> {
                final String fractionIdString = fraction.getId();
                final String sourceFile = fraction.getSourceFile();
                if (Strings.isNullOrEmpty(fractionIdString)) {
                    return Fraction.newBuilder()
                        .setSrcFile(sourceFile)
                        .setFractionId(ModelConversion.uuidToByteString(UUID.randomUUID()))
                        .build();
                } else {
                    final UUID fractionId = UUID.fromString(fractionIdString);
                    return Fraction.newBuilder()
                        .setFractionId(ModelConversion.uuidToByteString(fractionId))
                        .setSrcFile(sourceFile)
                        .build();
                }
            }
        );
        final List<WorkflowStep> steps;
        try {
            steps = ImmutableList.of(
                WorkflowStep.newBuilder()
                    .setStepType(StepType.DATA_LOADING)
                    .setStepParametersJson(OM.writeValueAsString(new DataLoadingStepBuilder().build()))
                    .build()
            );
        } catch (Exception e) {
            throw new InvalidParameterException("Invalid sample parameters");
        }
        return commandFactory.create(
            sampleInformation,
            sampleParameters,
            fractions,
            steps,
            3
        ).getCreateSamplesBatch().getCreateSamples(0);
    }

    @Override
    public CompletionStage<UUID> createAnalysis(UUID projectId, Analysis analysis, User creator) {
        final UUID userId = creator.userId();
        final UUID analysisId = UUID.randomUUID();

        final ProjectAggregateCommandFactory.AnalysisCommandFactory commandFactory = ProjectAggregateCommandFactory
            .project(projectId, userId)
            .analysis(analysisId);
        ProjectAggregateCommand createCommand;
        try {
            final AnalysisInformation analysisInformation = AnalysisInformation.newBuilder()
                .setName(analysis.getName())
                .setDescription(analysis.getDescription())
                .build();
            final List<UUID> sampleIds = Lists.transform(analysis.getSampleIdsList(), UUID::fromString);
            Pair<List<SimplifiedFastaSequenceDatabaseInfo>, List<WorkflowStep>> pair = parseAnalysisSteps(analysis, creator);
            List<SimplifiedFastaSequenceDatabaseInfo> databases = pair.first();
            List<WorkflowStep> steps = pair.second();
            createCommand = commandFactory.create(
                analysisInformation,
                sampleIds,
                steps,
                databases,
                analysis.getPriority()
            );
        } catch (Exception e) {
            return Futures.failedCompletionStage(new ParserException("Unable to create analysis: " + e.getMessage(), e));
        }

        return projectAggregateCommunication.command(createCommand)
            .thenApply(ignore -> analysisId);
    }

    @Override
    public CompletionStage<Done> updateAnalysis(UUID projectId, UUID analysisId, User user, AnalysisInformation analysisInformation) {
        final ProjectAggregateCommandFactory.AnalysisCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, user.userId()).analysis(analysisId);
        return projectAggregateCommunication.command(commandFactory.setInformation(analysisInformation));
    }

    @NotNull
    private WorkflowStep workflowStep(Step step) throws JsonProcessingException {
        return WorkflowStep.newBuilder()
            .setId(step.id().toString())
            .setStepTypeValue(step.type().value)
            .setStepParametersJson(OM.writeValueAsString(step))
            .build();
    }

    @Override
    public Source<Project, NotUsed> projects(User user) {
        return Source.from(projectView.currentProjectIds(user.userId()))
            .mapAsyncUnordered(2, projectId -> projectAggregateCommunication
                .queryProjectOptional(ProjectAggregateCommandFactory.project(projectId, user.userId()).queryProject(false, false))
            )
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(ProjectAggregateQueryResponse.ProjectQueryResponse::getProjectDto)
            .map(ProjectAggregateQueryResponse.ProjectQueryResponse.ProjectDto::getProject);
    }

    @Override
    public Source<ProjectAnalysisInfos, NotUsed> projectAnalysisInfos(User user) {
        return Source.from(projectView.currentProjectIds(user.userId()))
            .mapAsyncUnordered(2, projectId -> projectAggregateCommunication
                .queryProjectOptional(ProjectAggregateCommandFactory.project(projectId, user.userId()).queryProjectAnalysisInfoDtos())
            )
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(ProjectAggregateQueryResponse.ProjectQueryResponse::getProjectAnalysisInfos);
    }


    public CompletionStage<Boolean> projectExist(UUID projectId) {
        return projectAggregateCommunication.query(
            ProjectAggregateCommandFactory.project(projectId, User.admin().userId())
                .queryProject(false, false)
        ).thenApply(response -> {
            if (response.hasFailure()) {
                final ProjectAggregateQueryResponse.QueryFailure failure = response.getFailure();
                switch (failure.getReason()) {
                    case NOT_EXIST:
                        return false;
                    case NOT_FOUND:
                        return true; // Not yet completely deleted
                    default:
                        throw new IllegalStateException(failure.getReason() + " " + failure.getDescription());
                }
            } else {
                return true;
            }
        });
    }

    @Override
    public CompletionStage<Project> project(UUID projectId, User user) {
        CompletionStage<Map<String, UserState>> futureUserMap = userManagerCommunication
            .query(UserQueryFactory.actingAsUserId(user.userId().toString()).requestUserList())
            .thenApply(userQueryResponse -> userQueryResponse.getListUsers().getUsersMap());
        CompletionStage<Project> futureProject = projectAggregateCommunication
            .queryProject(ProjectAggregateCommandFactory.project(projectId, user.userId()).queryProject(false, false))
            .thenApply(p -> p.getProjectDto().getProject());
        return futureProject.thenCombine(futureUserMap, (project, userMap) ->
            Optional.of(project.toBuilder()
                .setOwnerUsername(userMap.get(project.getOwnerUserId()).getUserInformation().getUserName())
                .build()
            ))
            .exceptionally(this::handleNoSuchElementExceptionAsOptionalEmpty)
            .thenApply(optionalProject -> optionalProject.orElseThrow(() -> new HandlerException(404, "Project not found")));
    }

    @Override
    public Source<Analysis, NotUsed> projectAnalysis(UUID projectId, User user) {
        final ProjectAggregateQueryRequest query = ProjectAggregateCommandFactory
            .project(projectId, user.userId())
            .queryProject(false, true);
        return Source.fromCompletionStage(projectAggregateCommunication.queryProject(query))
            .mapConcat(queryResponse -> queryResponse.getProjectDto().getAnalysisList());
    }

    @Override
    public Source<Sample, NotUsed> projectSamples(UUID projectId, User user) {
        return Source.fromCompletionStage(projectAggregateCommunication.queryProject(ProjectAggregateCommandFactory
                .project(projectId, user.userId())
                .queryProject(true, false)))
            .mapConcat(response -> response.getProjectDto().getSampleList());
    }

    @Override
    public CompletionStage<GraphTaskIds> sampleGraphTaskIds(UUID projectId, UUID sampleId, User user) {
        return projectAggregateCommunication.querySample(ProjectAggregateCommandFactory.project(projectId, user.userId())
                .sample(sampleId)
                .queryGraphTaskIds()
            ).thenApply(ProjectAggregateQueryResponse.SampleQueryResponse::getGraphTaskIds)
            .thenCompose(graphTaskIds -> {
                try {
                    return completedFuture(GraphTaskIds.from(graphTaskIds));
                } catch (Throwable throwable) {
                    throw Throwables.propagate(throwable);
                }
            });
    }

    @Override
    public CompletionStage<GraphTaskIds> analysisGraphTaskIds(UUID projectId, UUID analysis, WorkFlowType type, User user) {
        return projectAggregateCommunication.queryAnalysis(ProjectAggregateCommandFactory.project(projectId, user.userId())
            .analysis(analysis)
            .graphTaskIds(AnalysisQueryRequest.GraphTaskIds.newBuilder()
                .setWorkflowType(type)
                .build())
        ).thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse::getGraphTaskIds)
            .thenCompose(graphTaskIds -> {
                try {
                    return completedFuture(GraphTaskIds.from(graphTaskIds));
                } catch (Throwable throwable) {
                    throw Throwables.propagate(throwable);
                }
            });
    }

    @Override
    public CompletionStage<GraphTaskIds> analysisFractionGraphTaskIds(UUID projectId, UUID analysis, UUID sampleId, UUID fractionId, WorkFlowType type, User user) {
        return projectAggregateCommunication.queryAnalysis(ProjectAggregateCommandFactory.project(projectId, user.userId())
            .analysis(analysis)
            .graphTaskIds(AnalysisQueryRequest.GraphTaskIds.newBuilder()
                .setSampleId(sampleId.toString())
                .setFractionId(fractionId.toString())
                .setWorkflowType(type)
                .build())
        ).thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse::getGraphTaskIds)
            .thenCompose(graphTaskIds -> {
                try {
                    return completedFuture(GraphTaskIds.from(graphTaskIds));
                } catch (Throwable throwable) {
                    throw Throwables.propagate(throwable);
                }
            });
    }

    @Override
    public CompletionStage<GraphTaskIds> analysisGraphTaskIdsLastWorkflowType(UUID projectId, UUID analysisId, User user) {
        return projectAggregateCommunication.queryAnalysis(ProjectAggregateCommandFactory.project(projectId, user.userId())
            .analysis(analysisId)
            .graphTaskIds(
                AnalysisQueryRequest.GraphTaskIds.newBuilder()
                    .setLastWorkflow(true)
                    .build()
            )
        ).thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse::getGraphTaskIds)
            .thenCompose(graphTaskIds -> {
                try {
                    return completedFuture(GraphTaskIds.from(graphTaskIds));
                } catch (Throwable throwable) {
                    throw Throwables.propagate(throwable);
                }
            });
    }

    public CompletionStage<GraphTaskIds> analysisGraphTaskIds(UUID projectId, UUID analysisId, User user, AnalysisQueryRequest.GraphTaskIds request) {
        return projectAggregateCommunication.queryAnalysis(ProjectAggregateCommandFactory.project(projectId, user.userId())
            .analysis(analysisId)
            .graphTaskIds(request)
        ).thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse::getGraphTaskIds)
            .thenCompose(response -> {
                try {
                    return completedFuture(GraphTaskIds.from(response));
                } catch (Throwable throwable) {
                    throw Throwables.propagate(throwable);
                }
            });
    }

    @Override
    public CompletableFuture<FilterFunction[]> analysisFiltersByWorkflowType(UUID projectId, UUID analysisId, User user, WorkFlowType workFlowType) {
        return projectAggregateCommunication.queryAnalysis(
            ProjectAggregateCommandFactory.project(projectId, user.userId())
                .analysis(analysisId)
                .filters(AnalysisQueryRequest.Filters.newBuilder()
                    .setWorkflowType(workFlowType)
                    .build())
            )
            .thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse::getFilterQueries)
            .thenApply(queryResponse -> {
                try {
                    List<Query> queries = new ArrayList<>();
                    for (int i = 0; i < queryResponse.getQueryJsonCount(); i++) {
                        queries.add(Helper.convertQuery(queryResponse.getQueryJson(i), queryResponse.getWorkflowType(i)));
                    }
                    return queries.stream()
                        .flatMap(q -> Arrays.stream(q.filters()))
                        .toArray(s -> new FilterFunction[s]);
                } catch (Throwable e) {
                    throw Throwables.propagate(e);
                }
            }).toCompletableFuture();
    }

    @Override
    public CompletionStage<Map<WorkFlowType, Query>> analysisQueriesAll(UUID projectId, UUID analysisId, User user) {
        return projectAggregateCommunication.queryAnalysis(
            ProjectAggregateCommandFactory.project(projectId, user.userId())
                .analysis(analysisId)
                .filters(AnalysisQueryRequest.Filters.newBuilder()
                    .setAllQueries(true)
                    .build())
            )
            .thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse::getFilterQueries)
            .thenApply(queryResponse -> {
                try {
                    Map<WorkFlowType, Query> queries = new HashMap<>();
                    for (int i = 0; i < queryResponse.getWorkflowTypeCount(); i++) {
                        WorkFlowType workflowType = queryResponse.getWorkflowType(i);
                        queries.put(workflowType, Helper.convertQuery(queryResponse.getQueryJson(i), workflowType));
                    }
                    return queries;
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
    }

    @Override
    public CompletionStage<Query> analysisQuery(UUID projectId, UUID analysisId, User user, Optional<WorkFlowType> workFlowType) {
        AnalysisQueryRequest.Filters.Builder queryBuilder = AnalysisQueryRequest.Filters.newBuilder();
        if (workFlowType.isPresent()) {
            queryBuilder.setWorkflowType(workFlowType.get());
        } else {
            queryBuilder.setLastWorkflow(true);
        }

        return projectAggregateCommunication.queryAnalysis(
            ProjectAggregateCommandFactory.project(projectId, user.userId())
                .analysis(analysisId)
                .filters(queryBuilder.build())
            )
            .thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse::getFilterQueries)
            .thenApply(queryResponse -> {
                if (queryResponse.getQueryJsonList().size() != 1) {
                    throw new IllegalStateException("Internal Server Error, unable to get query");
                }
                try{
                    Query query = Helper.convertQuery(queryResponse.getQueryJson(0), queryResponse.getWorkflowType(0));
                    return query;
                } catch (Throwable e) {
                    throw Throwables.propagate(e);
                }
            });
    }

    @Override
    public CompletionStage<Sample> sample(UUID projectId, UUID sampleId, User user) {
        return projectAggregateCommunication
            .querySample(ProjectAggregateCommandFactory.project(projectId, user.userId())
                .sample(sampleId)
                .queryGraphTaskIds()
            )
            .thenApply(ProjectAggregateQueryResponse.SampleQueryResponse::getGraphTaskIds)
            .thenCompose(graphTaskIdsProto -> {
                GraphTaskIds graphTaskIds = null;
                try {
                    graphTaskIds = GraphTaskIds.from(graphTaskIdsProto);
                } catch (Throwable e) {
                    throw Throwables.propagate(e);
                }
                ApplicationStorage storage = storageFactory.getStorage(graphTaskIds.keyspace());
                DbApplicationGraph applicationGraph = new DbApplicationGraphImplementation(storage, graphTaskIds, Optional.empty());
                DbDtoSources dtoSources = new DbDtoSourcesViaDbGraph(applicationGraph);
                return dtoSources.sample(sampleId)
                    .thenApply(optionalSample -> optionalSample.orElseThrow(() -> new HandlerException(404, "Sample not found")));
            });
    }

    @Override
    public CompletionStage<Analysis> analysis(UUID projectId, UUID analysisId, User user) {
        return projectAggregateCommunication
            .queryAnalysis(ProjectAggregateCommandFactory.project(projectId, user.userId())
                .analysis(analysisId)
                .queryAnalysis(false))
            .thenApply(response -> response.getAnalysisDto().getAnalysis())
            .thenApply(Optional::of)
            .exceptionally(this::handleNoSuchElementExceptionAsOptionalEmpty)
            .thenApply(optionalAnalysis -> optionalAnalysis.orElseThrow(() -> new HandlerException(404, "Analysis not found")));
    }

    @Override
    public CompletionStage<AnalysisParameters> analysisParameters(UUID projectId, UUID analysisId, User user) {
        return projectAggregateCommunication
            .queryAnalysis(ProjectAggregateCommandFactory.project(projectId, user.userId())
                .analysis(analysisId)
                .queryAnalysisParametersDto()
            )
            .thenApply(response -> response.getAnalysisParametersDto().getAnalysisParameters())
            .thenApply(Optional::of)
            .exceptionally(this::handleNoSuchElementExceptionAsOptionalEmpty)
            .thenApply(optionalAnalysis -> optionalAnalysis.orElseThrow(() -> new HandlerException(404, "Analysis not found")));
    }

    private <T> Optional<T> handleNoSuchElementExceptionAsOptionalEmpty(Throwable e) {
        final Throwable rootCause = Throwables.getRootCause(e);
        if (rootCause instanceof NoSuchElementException) {
            return Optional.empty();
        }
        throw Throwables.propagate(e);
    }

    @Override
    public Source<Progress, NotUsed> currentProjectProgress(UUID projectId, User user) {
        CompletionStage<ProjectAggregateQueryResponse.ProgressList> future = projectAggregateCommunication
            .queryProject(ProjectAggregateCommandFactory.project(projectId, user.userId()).queryProgress(true, true))
            .thenApply(ProjectAggregateQueryResponse.ProjectQueryResponse::getProgressList);
        return Source.fromCompletionStage(future)
            .mapConcat(ProjectAggregateQueryResponse.ProgressList::getProgressList);

    }

    @Override
    public Source<Progress, NotUsed> currentAllAnalysisProgress(User user) {
        throw new IllegalStateException("Querying all analysis progress is no longer supported");
    }

    @Override
    public Source<Progress, NotUsed> analysisProgress(UUID projectId, UUID analysisId, User user) {
        CompletionStage<ProjectAggregateQueryResponse.ProgressList> future = projectAggregateCommunication
            .queryProject(ProjectAggregateCommandFactory.project(projectId, user.userId())
                .queryProgress(false, true, analysisId, null)
            )
            .thenApply(ProjectAggregateQueryResponse.ProjectQueryResponse::getProgressList);
        return Source.fromCompletionStage(future)
            .mapConcat(ProjectAggregateQueryResponse.ProgressList::getProgressList);
    }

    @Override
    public Source<Progress, NotUsed> sampleProgress(UUID projectId, UUID sampleId, User user) {
        final CompletionStage<ProjectAggregateQueryResponse.ProgressList> future = projectAggregateCommunication
            .queryProject(ProjectAggregateCommandFactory.project(projectId, user.userId())
                .queryProgress(true, false, null, sampleId)
            )
            .thenApply(ProjectAggregateQueryResponse.ProjectQueryResponse::getProgressList);
        return Source.fromCompletionStage(future)
            .mapConcat(ProjectAggregateQueryResponse.ProgressList::getProgressList);
    }

    @Override
    public Source<Progress, NotUsed> daemonSamplesCopyProgress(UUID projectId, User user) {
        return Source.fromSourceCompletionStage(
            instrumentDaemonService.queryCopySample(user.userId(), projectId)
                .thenApply(copyProgresses -> {
                    if (!copyProgresses.getIsDataCopy()) {
                        return Source.empty();
                    } else {
                        List<Progress.State> copyingProgressesList = copyProgresses.getCopyingProgressesList();
                        List<UUID> fractionIds = Lists.newArrayList(ModelConversion.uuidsFrom(copyProgresses.getFractionIds()));
                        List<Progress> progresses = new ArrayList<>();
                        for (int i = 0; i < fractionIds.size(); i++) {
                            progresses.add(Progress.newBuilder()
                                .setId(fractionIds.get(i).toString())
                                .setState(copyingProgressesList.get(i))
                                .build());
                        }
                        return Source.from(progresses);
                    }
            })).mapMaterializedValue(ignore -> NotUsed.getInstance());
    }
}
