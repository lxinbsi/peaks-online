package com.bsi.peaks.server.es.project;

import akka.dispatch.Futures;
import akka.japi.Pair;
import akka.stream.javadsl.Source;
import com.bsi.peaks.algorithm.module.peaksdb.decoy.DecoyInfo;
import com.bsi.peaks.algorithm.module.peaksdb.decoy.ProteinDecoyInfo;
import com.bsi.peaks.analysis.analysis.dto.Analysis;
import com.bsi.peaks.analysis.analysis.dto.AnalysisInfo;
import com.bsi.peaks.analysis.analysis.dto.AnalysisParameters;
import com.bsi.peaks.analysis.analysis.dto.DbSearchStep;
import com.bsi.peaks.analysis.analysis.dto.IdentificaitonFilterSummarization;
import com.bsi.peaks.analysis.analysis.dto.LfqStep;
import com.bsi.peaks.analysis.analysis.dto.PtmFinderStep;
import com.bsi.peaks.analysis.analysis.dto.SlStep;
import com.bsi.peaks.analysis.analysis.dto.SpiderStep;
import com.bsi.peaks.analysis.parameterModels.dto.*;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.internal.task.*;
import com.bsi.peaks.io.writer.dto.DtoConversion;
import com.bsi.peaks.messages.service.LfqSummarizationFilterResult;
import com.bsi.peaks.messages.service.SlFilterSummarizationResult;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.applicationgraph.GraphTaskIds;
import com.bsi.peaks.model.applicationgraph.SampleFractions;
import com.bsi.peaks.model.dto.*;
import com.bsi.peaks.model.filter.DenovoCandidateFilter;
import com.bsi.peaks.model.filter.DenovoCandidateFilterBuilder;
import com.bsi.peaks.model.filter.PeptideFilter;
import com.bsi.peaks.model.filter.PsmFilter;
import com.bsi.peaks.model.filter.RiqPeptideFilter;
import com.bsi.peaks.model.filter.SLPsmFilterBuilder;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.model.parameters.DataRefineParameters;
import com.bsi.peaks.model.parameters.DenovoParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParametersBuilder;
import com.bsi.peaks.model.parameters.RetentionTimeAlignmentParameters;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.model.query.DbSearchQuery;
import com.bsi.peaks.model.query.DenovoQuery;
import com.bsi.peaks.model.query.LabelFreeQuery;
import com.bsi.peaks.model.query.PtmFinderQuery;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.query.ReporterIonQMethodQuery;
import com.bsi.peaks.model.query.SilacQuery;
import com.bsi.peaks.model.query.SlQuery;
import com.bsi.peaks.model.query.SpiderQuery;
import com.bsi.peaks.model.system.OrderedSampleFractions;
import com.bsi.peaks.model.system.OrderedSampleFractionsBuilder;
import com.bsi.peaks.model.system.OrderedSampleFractionsList;
import com.bsi.peaks.model.system.levels.LevelTask;
import com.bsi.peaks.server.dto.DtoAdaptorHelpers;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.state.AnalysisTaskInformation;
import com.bsi.peaks.server.es.state.ProjectAggregateState;
import com.bsi.peaks.server.es.state.ProjectAnalysisState;
import com.bsi.peaks.server.es.state.ProjectFractionState;
import com.bsi.peaks.server.es.state.ProjectSampleState;
import com.bsi.peaks.server.es.state.ProjectTaskPlanState;
import com.bsi.peaks.server.es.state.ProjectTaskState;
import com.bsi.peaks.server.es.tasks.Helper;
import com.bsi.peaks.server.service.CassandraMonitorService;
import com.bsi.peaks.service.common.DecoyInfoWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.pcollections.PCollectionsModule;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static akka.dispatch.Futures.failedCompletionStage;
import static com.bsi.peaks.model.ModelConversion.uuidFrom;
import static com.bsi.peaks.server.dto.DtoAdaptors.dataRefinementStep;
import static com.bsi.peaks.server.dto.DtoAdaptors.dbSearchParameters;
import static com.bsi.peaks.server.dto.DtoAdaptors.denovoParameters;
import static com.bsi.peaks.server.dto.DtoAdaptors.featureDetectionStep;
import static com.bsi.peaks.server.dto.DtoAdaptors.spiderParameters;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ProjectAggregateQuery {

    private final UUID projectId;
    private final ProjectAggregateQueryConfig config;
    private final ProjectAggregateSampleQuery sampleQuery;
    private final CassandraMonitorService cassandraMonitorService;
    private static final UUID ADMIN_USERID = UUID.fromString(UserManager.ADMIN_USERID);

    public ProjectAggregateQuery(ProjectAggregateQueryConfig config, CassandraMonitorService cassandraMonitorService) {
        this.config = config;
        this.projectId = config.projectId();
        this.sampleQuery = new ProjectAggregateSampleQuery(config);
        this.cassandraMonitorService = cassandraMonitorService;
    }

    protected static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new Jdk8Module())
        .registerModule(new ProtobufModule())
        .registerModule(new JavaTimeModule())
        .registerModule(new PCollectionsModule());

    public CompletionStage<ProjectAggregateQueryResponse.Builder> queryProject(ProjectAggregateQueryRequest.ProjectQueryRequest queryRequest, ProjectAggregateState data) throws IOException {
        final ProjectAggregateQueryResponse.ProjectQueryResponse.Builder projectBuilder = ProjectAggregateQueryResponse.ProjectQueryResponse.newBuilder();
        switch (queryRequest.getQueryCase()) {
            case PROGRESS:
                queryProjectProgress(data, queryRequest.getProgress(), projectBuilder.getProgressListBuilder());
                break;
            case PROJECTDTO:
                return queryProjectDto(data, queryRequest.getProjectDto())
                    .thenApply(x -> {
                        final ProjectAggregateQueryResponse.Builder builder = ProjectAggregateQueryResponse.newBuilder();
                        builder.getProjectQueryResponseBuilder().setProjectDto(x);
                        return builder;
                    });
            case SAMPLEFRACTIONS:
                sampleQuery.querySampleFractions(data, projectBuilder.getSampleFractionsListBuilder());
                break;
            case STATE:
                projectBuilder.setStateJson(OM.writeValueAsString(data));
                break;
            case TASKLISTING:
                final ProjectAggregateQueryResponse.TaskListing.Builder taskListingBuilder = projectBuilder.getTaskListingBuilder();
                data.taskPlanIdToTaskPlan().values().forEach(taskPlan -> taskListingBuilder.addTasks(projectTaskListing(data, taskPlan)));
                break;
            case PROJECTANALYSISINFODTOS:
                final ProjectAnalysisInfos.Builder projectAnalysisInfosBuilder = projectBuilder.getProjectAnalysisInfosBuilder();
                projectAnalysisInfos(data, projectAnalysisInfosBuilder);
                break;

            default:
                return Futures.failedCompletionStage(new IllegalArgumentException("Unexpected project query " + queryRequest.getQueryCase()));
        }
        return completedFuture(ProjectAggregateQueryResponse.newBuilder()
            .setProjectQueryResponse(projectBuilder)
        );
    }

    private void projectAnalysisInfos(ProjectAggregateState data, ProjectAnalysisInfos.Builder builder) {
        builder.setName(data.name())
            .setId(projectId.toString());
        Comparator<Map.Entry<UUID, ProjectAnalysisState>> lastUpdatedComparator = Comparator.comparing(e -> e.getValue().lastUpdatedTimeStamp());
        data.analysisIdToAnalysis().entrySet().stream()
            .sorted(lastUpdatedComparator)
            .forEach(entry -> {
                final UUID analysisId = entry.getKey();
                final ProjectAnalysisState analysisData = entry.getValue();
                final Set<WorkFlowType> workflowTypes = analysisData.steps()
                    .stream()
                    .map(step -> ModelConversion.workflowType(step.getStepType()))
                    .collect(Collectors.toSet());
                builder.addAnalysisInfos(AnalysisInfo.newBuilder()
                    .setName(analysisData.name())
                    .setId(analysisId.toString())
                    .setState(analysisData.status().progressState())
                    .addAllWorkFlowsEnabled(workflowTypes)
                    .setAnalysisAcquisitionMethod(analysisData.analysisAcquisitionMethod()));
            });
    }

    private ProjectTaskListing.Builder projectTaskListing(ProjectAggregateState data, ProjectTaskPlanState taskPlan) {
        final UUID taskPlanId = taskPlan.taskPlanId();
        final Map<UUID, ProjectSampleState> sampleIdToSample = data.sampleIdToSample();
        final StepType stepType = taskPlan.stepType();
        final LevelTask level = taskPlan.level();
        final Set<UUID> fractionIds = new HashSet<>();
        fractionIds.addAll(level.fractionIds());

        final ProjectTaskListing.Builder taskListing = ProjectTaskListing.newBuilder()
            .setType(stepType);

        for (UUID sampleId : level.sampleIds()) {
            taskListing.addSampleId(sampleId.toString());
            final ProjectSampleState sample = sampleIdToSample.get(sampleId);
            if (sample == null) {
                taskListing.addSampleName("");
            } else {
                taskListing.addSampleName(sample.name());
                final Map<UUID, ProjectFractionState> fractionStateMap = sample.fractionState();
                for (UUID fractionId : sample.fractionIds()) {
                    if (fractionIds.remove(fractionId)) {
                        taskListing.addFractionId(fractionId.toString());
                        taskListing.addFractionName(fractionStateMap.get(fractionId).srcFile());
                    }
                }
            }
        }
        for (UUID fractionId : fractionIds) {
            taskListing.addFractionId(fractionId.toString());
            taskListing.addFractionName("");
        }
        for (ProjectAnalysisState analysis : data.analysisIdToAnalysis().values()) {
            final Set<UUID> taskPlanIds = analysis.stepTaskPlanId().get(stepType);
            if (taskPlanIds == null || !taskPlanIds.contains(taskPlanId)) continue;
            taskListing
                .addAnalysisId(analysis.analysisId().toString())
                .addAnalysisName(analysis.name());
        }

        Optional<ProjectTaskState> lookupTaskState = data.taskState(taskPlanId);
        lookupTaskState.ifPresent(task -> {
            switch(task.progress()) {
                case QUEUED:
                    taskListing.setState(Progress.State.QUEUED);
                    break;
                case IN_PROGRESS:
                    taskListing.setState(Progress.State.PROGRESS);
                    break;
                case DONE:
                    taskListing.setState(Progress.State.DONE);
                    break;
                case FAILED:
                    taskListing.setState(Progress.State.FAILED);
                    break;
                case STOPPING:
                case CANCELLED:
                    taskListing.setState(Progress.State.CANCELLED);
                    break;
            }
            switch(task.dataState()) {
                case DELETING:
                case DELETED:
                    taskListing.setDeleted(true);
                    break;
                default:
                    taskListing.setDeleted(false);
                    break;
            }
            taskListing
                .setTaskId(task.taskId().toString())
                .setQueuedTimeStamp(task.queuedTimestamp().getEpochSecond());

            final Instant startTimestamp = task.startTimestamp();
            if (startTimestamp != null) taskListing.setStartedTimeStamp(startTimestamp.getEpochSecond());

            final Instant doneTimestamp = task.completedTimestamp();
            if (doneTimestamp != null) taskListing.setDoneTimeStamp(doneTimestamp.getEpochSecond());

            final UUID workerId = task.workerId();
            if (workerId != null) taskListing.setWorkerId(workerId.toString());

            final String workerIpAddress = task.workerIpAddress();
            if (workerIpAddress != null) taskListing.setWorkerIpAddress(workerIpAddress);
        });

        return taskListing;
    }


    private void queryProjectProgress(
        ProjectAggregateState data,
        ProjectAggregateQueryRequest.ProjectQueryRequest.Progress progress,
        ProjectAggregateQueryResponse.ProgressList.Builder progressListBuilder
    ) {
        final Collection<UUID> analysisIds;
        Collection<UUID> sampleIds;
        {
            final String analysisIdString = progress.getAnalysisId();
            final String sampleIdString = progress.getSampleId();
            if (!Strings.isNullOrEmpty(analysisIdString)) {
                final UUID analysisId = UUID.fromString(analysisIdString);
                final ProjectAnalysisState analysisState = data.analysisIdToAnalysis().get(analysisId);
                if (analysisState == null) {
                    throw new IllegalStateException("AnalysisId " + analysisIdString + " does not exist");
                }
                analysisIds = Collections.singletonList(analysisId);
                sampleIds = Lists.transform(analysisState.samples(), OrderedSampleFractions::sampleId);
            } else {
                analysisIds = data.analysisIdToAnalysis().keySet();
                sampleIds = data.sampleIdToSample().keySet();
            }
            if (!Strings.isNullOrEmpty(sampleIdString)) {
                final UUID sampleId = UUID.fromString(sampleIdString);
                sampleIds = sampleIds.stream().filter(sid -> sid.equals(sampleId)).collect(Collectors.toList());
                if (sampleIds.size() != 1) {
                    throw new IllegalStateException("SampleId " + sampleId + " does not exist");
                }
            }
        }
        Boolean isFull = cassandraMonitorService.checkFull(ADMIN_USERID).toCompletableFuture().join();
        if (progress.getIncludeAnalyses()) {
            for (UUID analysisId : analysisIds) {
                analysisProgress(data, progressListBuilder, analysisId, isFull);
            }
        }
        if (progress.getIncludeSamples()) {
            for (UUID sampleId : sampleIds) {
                sampleQuery.progress(data, progressListBuilder, sampleId, isFull);
            }
        }

        progressListBuilder.addProgressBuilder()
            .setId(projectId.toString())
            .setState(projectProgress(data));
    }

    private void analysisProgress(
        ProjectAggregateState data,
        ProjectAggregateQueryResponse.ProgressList.Builder progressListBuilder,
        UUID analysisId,
        boolean isCassandraFull
    ) {
        final ProjectAnalysisState analysis = data.analysisIdToAnalysis().get(analysisId);
        final Progress.State progressState = analysis.status().progressState();
        final Map<StepType, PSet<UUID>> stepTaskPlanId = analysis.stepTaskPlanId();
        final List<WorkflowStep> steps = analysis.steps();
        final AnalysisTaskInformation analysisTaskInformation = AnalysisTaskInformation.create(config.keyspace(), data, projectId, analysisId);

        progressListBuilder.addProgress(
            Progress.newBuilder()
                .setId(analysisId.toString())
                .setState(progressState)
                .build()
        );

        steps.stream()
            .map(step -> {
                final StepType type = step.getStepType();
                final Set<UUID> taskPlanIds = stepTaskPlanId.get(type);
                final int taskCount;
                final Iterable<com.bsi.peaks.model.system.Progress> progressList;
                if (taskPlanIds == null || taskPlanIds.isEmpty()) {
                    progressList = Collections.emptyList();
                } else {
                    progressList = taskPlanIds.stream()
                        .map(data::taskState)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(ProjectTaskState::progress)
                        .collect(Collectors.toList());
                    return Pair.create(type, progressList);
                }
                return Pair.create(type, progressList);
            })
            .collect(Collectors.groupingBy(
                p -> ModelConversion.largeStep(p.first()),
                Collectors.reducing(
                    Pair.create(0, Collections.<com.bsi.peaks.model.system.Progress>emptyList()),
                    p -> Pair.create(analysis.totalTasks(analysisTaskInformation, p.first()), p.second()),
                    (a, b) -> Pair.create(a.first() + b.first(), Iterables.concat(a.second(), b.second()))
                )
            ))
            .forEach((largeStep, p) -> {
                final UUID id = UUID.fromString(Iterables.find(steps, step -> step.getStepType() == largeStep).getId());
                final Integer totalTasks = p.first();
                final float[] progressCount = DtoAdaptorHelpers.progressCount(p.second());
                progressListBuilder.addProgress(DtoAdaptorHelpers.progress(id, progressCount, totalTasks, isCassandraFull));
            });
    }

    @NotNull
    public Progress.State projectProgress(ProjectAggregateState data) {
        switch (data.status()) {
            case DELETING:
                return Progress.State.DELETING;
        }
        for (ProjectSampleState sample : data.sampleIdToSample().values()) {
            switch (sample.status()) {
                case CREATED:
                case PROCESSING:
                    return Progress.State.PROGRESS;
            }
        }
        final Collection<ProjectAnalysisState> analyses = data.analysisIdToAnalysis().values();
        for (ProjectAnalysisState analysis : analyses) {
            switch (analysis.status()) {
                case CREATED:
                case PROCESSING:
                    return Progress.State.PROGRESS;
            }
        }
        return analyses.stream()
            .sorted(Comparator.comparing(ProjectAnalysisState::createdTimeStamp).reversed())
            .map(ProjectAnalysisState::status)
            .map(status -> {
                switch(status) {
                    case PENDING:
                    case CREATED:
                    case PROCESSING:
                        return Progress.State.PROGRESS;
                    case DONE:
                        return Progress.State.DONE;
                    case FAILED:
                        return Progress.State.FAILED;
                    case CANCELLED:
                        return Progress.State.CANCELLED;
                    default:
                        return Progress.State.UNDEFINED;
                }})
            .filter(progressState -> progressState != Progress.State.UNDEFINED)
            .findFirst()
            .orElse(Progress.State.DONE);
    }

    private CompletionStage<ProjectAggregateQueryResponse.ProjectQueryResponse.ProjectDto.Builder> queryProjectDto(
        ProjectAggregateState data,
        ProjectAggregateQueryRequest.ProjectQueryRequest.ProjectDto projectDto
    ) {
        final CompletionStage<ProjectAggregateQueryResponse.ProjectQueryResponse.ProjectDto.Builder> futureProjectDtoBuilder;
        if (projectDto.getIncludeSampleList()) {
            futureProjectDtoBuilder = Source.from(data.orderSamples())
                .mapAsync(config.parallelism(), sampleQuery::sampleDto)
                .runFold(
                    ProjectAggregateQueryResponse.ProjectQueryResponse.ProjectDto.newBuilder(),
                    ProjectAggregateQueryResponse.ProjectQueryResponse.ProjectDto.Builder::addSample,
                    config.materializer()
                );
        } else {
            futureProjectDtoBuilder = CompletableFuture.completedFuture(ProjectAggregateQueryResponse.ProjectQueryResponse.ProjectDto.newBuilder());
        }
        return config.userNamesById()
            .thenCompose(users -> futureProjectDtoBuilder.thenApply(builder -> {
                try {
                    if (projectDto.getIncludeAnalysisList()) {
                        List<ProjectAnalysisState> sortedAnalysis = data.analysisIdToAnalysis().values()
                            .stream()
                            .sorted(Comparator.comparing(ProjectAnalysisState::createdTimeStamp).reversed())
                            .collect(Collectors.toList());
                        for (ProjectAnalysisState analysis : sortedAnalysis) {
                            builder.addAnalysis(analysisDto(data, analysis).toCompletableFuture().join());
                        }
                    }
                    final String ownerId = data.ownerId().toString();
                    builder.getProjectBuilder()
                        .setId(projectId.toString())
                        .setOwnerUsername(users.getOrDefault(ownerId, "Unknown"))
                        .setOwnerUserId(ownerId)
                        .setShared(data.shared())
                        .setName(data.name())
                        .setDescription(data.description())
                        .setState(projectProgress(data))
                        .setKeyspace(config.keyspace())
                        .setCreatedTimestamp(data.createdTimeStamp().getEpochSecond())
                        .setUpdatedTimestamp(data.lastUpdatedTimeStamp().getEpochSecond())
                        .setArchivedTimestamp(data.archivedTimeStamp().equals(Instant.MIN) ? 0 : data.archivedTimeStamp().getEpochSecond())
                        .setIsArchived(data.archiveState() == ProjectAggregateEvent.ProjectEvent.ArchiveState.ArchiveStateEnum.ARCHIVED)
                        .setMsRuns(data.sampleIdToSample().values().stream().map(s -> s.fractionIds().size()).reduce(0, Integer::sum));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return builder;
            }));
    }

    private AnalysisParameters.Builder analysisParametersDto(ProjectAnalysisState analysis, PMap<UUID, ProjectSampleState> sampleIdToSample) throws IOException {
        boolean isTimsTof = false;
        for(ProjectSampleState projectSampleState : sampleIdToSample.values()) {
            if (projectSampleState.instrument().getIonMobillityType() == IonMobilityType.TIMS) {
                isTimsTof = true;
                break;
            }
        }

        AnalysisParameters.Builder builder = AnalysisParameters.newBuilder().setIsTimsTof(isTimsTof);

        Set<WorkFlowType> workflows = new HashSet<>();
        float lfqK0Tolerance = 0f;

        for (WorkflowStep step : analysis.steps()) {
            workflows.add(ModelConversion.workflowType(step.getStepType()));
            builder.getDataRefineTableBuilder()
                .setChimeraAssociation(true);
            builder.setAnalysisAcquisitionMethod(analysis.analysisAcquisitionMethod());
            switch (step.getStepType()) {
                case DATA_REFINE: {
                    DataRefineParameters parameters = (DataRefineParameters) Helper.workflowStepParameters(step);
                    builder.getDataRefineTableBuilder()
                        .setMassCorrection(parameters.isDoMassCorrection());
                    break;
                }
                case DENOVO: {
                    DenovoParameters parameters = (DenovoParameters) Helper.workflowStepParameters(step);
                    MassUnit precursorMassUnit = parameters.isPPMForPrecursorTol() ? MassUnit.PPM : MassUnit.DA;
                    MassUnit fragmentMassUnit = parameters.isPPMForFragmentTol() ? MassUnit.PPM : MassUnit.DA;
                    builder.getDenovoParametersTableBuilder()
                        .setEnyzmeName(parameters.enzyme().name())
                        .setPrecursorTolerance(DtoConversion.massParameter(parameters.precursorTol(), precursorMassUnit))
                        .setFragmentTolerance(DtoConversion.massParameter(parameters.fragmentTol(), fragmentMassUnit))
                        .addAllFixedModifications(parameters.fixedModification().stream()
                            .map(m -> m.proto(Modification.newBuilder()))
                            ::iterator
                        )
                        .addAllVariableModifications(parameters.variableModification().stream()
                            .map(m -> m.proto(Modification.newBuilder()))
                            ::iterator);
                    break;
                }
                case SL_SEARCH:
                case DDA_SL_SEARCH: {
                    SlSearchParameters slSearchParameters = (SlSearchParameters) Helper.workflowStepParameters(step);
                    builder.getDataRefineTableBuilder()
                        .setRetentionTimeCorrection(true)
                        .setMassCorrection(true);
                    builder.getIdParametersTableBuilder()
                        .setPrecursorTolerance(slSearchParameters.getSlSearchSharedParameters().getPrecursorTolerance())
                        .setFragmentTolerance(slSearchParameters.getSlSearchSharedParameters().getFragmentTolerance())
                        .setSpectralLibraryId(ModelConversion.uuidFrom(slSearchParameters.getSpectraLibraryId()).toString())
                        .setCcsErrorTol(slSearchParameters.getSlSearchSharedParameters().getCcsTolerance())
                        .setEnableToleranceOptimization(slSearchParameters.getSlSearchSharedParameters().getEnableToleranceOptimization());
                    break;
                }
                case SL_PEPTIDE_SUMMARIZATION:
                    if (analysis.databases().size() > 0) {
                        builder.getIdParametersTableBuilder().setDatabaseId(analysis.databases().get(0).getId());
                    }
                    break;
                case DB_SUMMARIZE: {
                    DBSearchParameters dbSummarizeParameters = Helper.getDbSummarizeParameters(step);
                    DbSearchParameters dbSearchParameters = dbSearchParameters(dbSummarizeParameters, analysis.databases());
                    if (dbSearchParameters.getDatabasesList().size() > 0) {
                        builder.getIdParametersTableBuilder().setDatabaseId(dbSearchParameters.getDatabasesList().get(0).getId());
                    }
                    builder.getIdParametersTableBuilder()
                        .setPrecursorTolerance(dbSearchParameters.getPrecursorTol())
                        .setFragmentTolerance(dbSearchParameters.getFragmentTol())
                        .setEnyzmeName(dbSearchParameters.getEnzymeName())
                        .setEnzymeSpecificity(dbSearchParameters.getEnzymeSpecificityName());
                    dbSummarizeParameters.fixedModification().forEach(modi ->
                        builder.getIdParametersTableBuilder().addFixedModifications(modi.proto(Modification.newBuilder()))
                    );
                    dbSummarizeParameters.variableModification().forEach(modi ->
                        builder.getIdParametersTableBuilder().addVariableModifications(modi.proto(Modification.newBuilder()))
                    );
                    break;
                }
                case DIA_DB_SEARCH:  {
                    DiaDbSearchParameters diaDBSearchParameters = (DiaDbSearchParameters) Helper.workflowStepParameters(step);
                    DbSearchParameters dbSearchParameters = DtoAdaptors.dbSearchParametersFromDiaDb(diaDBSearchParameters, analysis.databases());
                    if (dbSearchParameters.getDatabasesList().size() > 0) {
                        builder.getDiaDbParametersTableBuilder().setDatabaseId(dbSearchParameters.getDatabasesList().get(0).getId());
                    }
                    builder.getDiaDbParametersTableBuilder()
                        .setPrecursorTolerance(dbSearchParameters.getPrecursorTol())
                        .setFragmentTolerance(dbSearchParameters.getFragmentTol())
                        .setEnyzmeName(dbSearchParameters.getEnzymeName())
                        .setEnzymeSpecificity(dbSearchParameters.getEnzymeSpecificityName())
                        .setEnableToleranceOptimization(dbSearchParameters.getEnableToleranceOptimization());
                    dbSearchParameters.getFixedModificationNamesList().forEach(name ->
                        builder.getDiaDbParametersTableBuilder().addFixedModifications(Modification.newBuilder().setName(name).build())
                    );
                    dbSearchParameters.getVariableModificationNamesList().forEach(name ->
                        builder.getDiaDbParametersTableBuilder().addVariableModifications(Modification.newBuilder().setName(name).build())
                    );
                    break;
                }
                case LFQ_SUMMARIZE: {
                    final LabelFreeQuantificationParameters parameters = (LabelFreeQuantificationParameters) Helper.workflowStepParameters(step);
                    lfqK0Tolerance = parameters.k0Tolerance();
                    break;
                }
                case DIA_LFQ_FILTER_SUMMARIZATION:
                case LFQ_FILTER_SUMMARIZATION: {
                    LfqFilter lfqFilter = (LfqFilter) Helper.workflowStepParameters(step);
                    builder.getQParametersTableBuilder()
                        .setQType(ModelConversion.workflowType(StepType.LFQ_FILTER_SUMMARIZATION))
                        .setOutlierRemovalEnabled(lfqFilter.getLabelFreeQueryParameters().getRemoveOutlier())
                        .setCvEnabled(lfqFilter.getFeatureVectorFilter().getUseCVFilter())
                        .setCcsErrorTol(lfqK0Tolerance);
                    break;
                }
                case DIA_LFQ_FEATURE_EXTRACTION: {
                    DiaFeatureExtractorParameters extractorParameters = (DiaFeatureExtractorParameters) Helper.workflowStepParameters(step);
                    lfqK0Tolerance = extractorParameters.getCcsTolerance();
                    break;
                }
                case REPORTER_ION_Q_BATCH_CALCULATION: {
                    ReporterIonQBatchCalculationParameters riqFilterSummarization = (ReporterIonQBatchCalculationParameters) Helper.workflowStepParameters(step);
                    builder.getQParametersTableBuilder()
                        .setQType(ModelConversion.workflowType(StepType.REPORTER_ION_Q_FILTER_SUMMARIZATION))
                        .setOutlierRemovalEnabled(false)
                        .setCvEnabled(false)
                        .setTmtQMethodName(riqFilterSummarization.getParameters().getMethod().getMethodName());
                    break;
                }
                case SILAC_FILTER_SUMMARIZE: {
                    final SilacFilterSummarizationParameters silacFilterSummarizationParameters = (SilacFilterSummarizationParameters) Helper.workflowStepParameters(step);
                    final SilacParameters silacParameters = silacFilterSummarizationParameters.getSilacParameters();
                    final SilacFilter silacFilter = silacFilterSummarizationParameters.getSilacFilter();
                    final SilacFilter.FeatureVectorFilter featureVectorFilter = silacFilter.getFeatureVectorFilter();
                    builder.getSilacParametersTableBuilder()
                        .setSilacQMethod(silacParameters.getSilacQMethod())
                        .setR2PCorrection(silacParameters.getR2PCorrection())
                        .setPrecursorTolerance(silacParameters.getPrecursorTolerance())
                        .setReferenceCondition(silacParameters.getConditions(silacParameters.getReferenceCondition()).getName())
                        .setMinCharge(featureVectorFilter.getMinCharge())
                        .setMaxCharge(featureVectorFilter.getMaxCharge())
                        .setAvergeArea((int) featureVectorFilter.getMinArea())
                        .setQuality((float) featureVectorFilter.getMinQuality())
                        .setLabelsWithIdPresent(featureVectorFilter.getMinLabelsWithId())
                        .setLabelsWithFeaturePresent(featureVectorFilter.getMinLabelsWithFeature())
                        .setReferenceLabelPresent(featureVectorFilter.getReferenceLabelPresent())
                        .setModifiedFormExclusion(silacParameters.getUseModifiedFormExclusion())
                        .setNormalizationMethod(silacParameters.getNormalization().getNormalizationMethod())
                        .setK0Range(silacParameters.getK0Range())
                        .setEnableMatchBetweenRun(silacParameters.getEnableMatchBetweenRuns());
                    break;
                }
            }
        }

        builder.addAllWorkFlowsEnabled(workflows);
        return builder;
    }

    private CompletionStage<Analysis.Builder> analysisDto(ProjectAggregateState data, ProjectAnalysisState analysis) throws IOException {
        final CompletionStage<Set<Float>> faimsCvFuture = sampleQuery.faimsCvsFoundInFractions(analysis.fractionIdToSampleId().keySet());
        Analysis.Builder analysisBuilder = Analysis.newBuilder()
            .setId(analysis.analysisId().toString())
            .setName(analysis.name())
            .setDescription(analysis.description())
            .setState(analysis.status().progressState())
            .setCreatedTimestamp(analysis.createdTimeStamp().getEpochSecond())
            .setPriority(analysis.priority())
            .addAllSampleIds(Iterables.transform(analysis.samples(), sampleFractions -> sampleFractions.sampleId().toString()));
        for (WorkflowStep step : analysis.steps()) {
            switch (step.getStepType()) {
                case FEATURE_DETECTION: {
                    analysisBuilder.setFeatureDetectionStep(featureDetectionStep(step));
                    continue;
                }
                case DATA_REFINE: {
                    analysisBuilder.addWorkFlowsEnabled(WorkFlowType.DATA_REFINEMENT)
                        .setAnalysisAcquisitionMethod(AnalysisAcquisitionMethod.DDA)
                        .setDataRefinementStep(dataRefinementStep(data, step));
                    continue;
                }
                case DIA_DB_DATA_REFINE: {
                    analysisBuilder.setAnalysisAcquisitionMethod(AnalysisAcquisitionMethod.DIA)
                        .getDbSearchStepBuilder()
                        .getDbSearchParametersBuilder()
                        .setCcsTolerance(step.getDiaDataRefineParameters().getCcsTolerance());
                    continue;
                }
                case DIA_DATA_REFINE: {
                    analysisBuilder.setAnalysisAcquisitionMethod(AnalysisAcquisitionMethod.DIA)
                        .getDenovoStepBuilder()
                        .getDeNovoParametersBuilder()
                        .setCcsTolerance(step.getDiaDataRefineParameters().getCcsTolerance());
                    continue;
                }
                case DENOVO: {
                    analysisBuilder.addWorkFlowsEnabled(WorkFlowType.DENOVO);
                    DeNovoParameters.Builder deNovoParametersBuilder = analysisBuilder
                        .getDenovoStepBuilder()
                        .setId(step.getId())
                        .getDeNovoParametersBuilder();
                    //dont use `set` so that we can grab CcsTolerance from DIA_DATA_REFINE
                    deNovoParametersBuilder.mergeFrom(denovoParameters(step));
                    continue;
                }
                case DB_SUMMARIZE: {
                    analysisBuilder.addWorkFlowsEnabled(WorkFlowType.DB_SEARCH);

                    DbSearchParameters.Builder dbSearchParametersBuilder = analysisBuilder.getDbSearchStepBuilder()
                        .setId(step.getId())
                        .getDbSearchParametersBuilder();
                    //dont use `set` so that we can grab CcsTolerance from DIA_DATA_REFINE
                    dbSearchParametersBuilder.mergeFrom(dbSearchParameters(Helper.getDbSummarizeParameters(step), analysis.databases()));
                    continue;
                }
                case DB_FILTER_SUMMARIZATION: {
                    analysisBuilder.getDbSearchStepBuilder()
                        .setFilterSummarization(DtoAdaptors.filterSummarizationWithDenovoTagParameters(step))
                        .setDefaultProteinFilter(step.getDbProteinHitFilter());
                    continue;
                }
                case DB_DENOVO_ONLY_TAG_SEARCH: {
                    analysisBuilder.getDbSearchStepBuilder()
                        .getFilterSummarizationBuilder()
                        .setDenovoOnlyTagSearch(step.getDenovoOnlyTagSearchParameters());
                    continue;
                }
                case PTM_FINDER_SUMMARIZE: {
                    analysisBuilder.addWorkFlowsEnabled(WorkFlowType.PTM_FINDER);

                    analysisBuilder.getPtmFinderStepBuilder()
                        .setPtmFinderParameters(DtoAdaptors.ptmFinderParameters(step))
                        .setId(step.getId());

                    continue;
                }
                case PTM_FINDER_FILTER_SUMMARIZATION: {
                    analysisBuilder.getPtmFinderStepBuilder()
                        .setFilterSummarization(DtoAdaptors.filterSummarizationWithDenovoTagParameters(step));
                    continue;
                }
                case PTM_FINDER_DENOVO_ONLY_TAG_SEARCH: {
                    analysisBuilder.getPtmFinderStepBuilder()
                        .getFilterSummarizationBuilder()
                        .setDenovoOnlyTagSearch(step.getDenovoOnlyTagSearchParameters());
                    continue;
                }
                case SPIDER_SUMMARIZE: {
                    analysisBuilder.addWorkFlowsEnabled(WorkFlowType.SPIDER);

                    analysisBuilder.getSpiderStepBuilder()
                        .setSpiderParameters(spiderParameters(step))
                        .setId(step.getId());

                    continue;
                }
                case SPIDER_FILTER_SUMMARIZATION: {
                    analysisBuilder.getSpiderStepBuilder()
                        .setFilterSummarization(DtoAdaptors.filterSummarizationWithDenovoTagParameters(step));
                    continue;
                }
                case SPIDER_DENOVO_ONLY_TAG_SEARCH: {
                    analysisBuilder.getSpiderStepBuilder()
                        .getFilterSummarizationBuilder()
                        .setDenovoOnlyTagSearch(step.getDenovoOnlyTagSearchParameters());
                    continue;
                }
                case LFQ_SUMMARIZE: {
                    final List<UUID> sampleIds = Lists.transform(analysis.samples(), OrderedSampleFractions::sampleId);
                    WorkflowStep rtAlignStep = analysis.findStep(StepType.LFQ_RETENTION_TIME_ALIGNMENT);
                    final RetentionTimeAlignmentParameters parameters = (RetentionTimeAlignmentParameters) Helper.workflowStepParameters(rtAlignStep);
                    analysisBuilder
                        .getLfqStepBuilder()
                        .setLfqParameters(DtoAdaptors.lfqParameters(step, parameters, sampleIds))
                        .setId(step.getId());
                    continue;
                }
                case DIA_LFQ_SUMMARIZE: {
                    List<Group> groups = analysis.findStep(StepType.DIA_LFQ_FILTER_SUMMARIZATION)
                        .getLfqFilter()
                        .getLabelFreeQueryParameters()
                        .getGroupsList();
                    DiaFeatureExtractorParameters extractorParameters = analysis.findStep(StepType.DIA_LFQ_FEATURE_EXTRACTION)
                        .getDiaFeatureExtractionParameters();

                    analysisBuilder
                        .getLfqStepBuilder()
                        .setLfqParameters(LfqParameters.newBuilder()
                            .setMassErrorTolerance(MassParameter.getDefaultInstance())
                            .setRtShift(extractorParameters.getRtShiftTolerance())
                            .setK0Tolerance(extractorParameters.getCcsTolerance())
                            .setFeatureIntensityThreshold(extractorParameters.getFeatureIntensity())
                            .setAutoDetectTolerance(extractorParameters.getAutoDetectRtTolerance())
                            .addAllGroups(groups)
                            .build())
                        .setId(step.getId());
                    continue;
                }
                case DIA_LFQ_FILTER_SUMMARIZATION: {
                    analysisBuilder
                    .addWorkFlowsEnabled(WorkFlowType.LFQ)
                        .getLfqStepBuilder()
                        .setLfqFilter(step.getLfqFilter())
                        .setDefaultProteinFeatureVectorFilter(step.getLfqProteinFeatureVectorFilter());
                    continue;
                }
                case LFQ_FILTER_SUMMARIZATION: {
                    WorkflowStep rtAlignStep = analysis.findStep(StepType.LFQ_RETENTION_TIME_ALIGNMENT);
                    final RetentionTimeAlignmentParameters parameters = (RetentionTimeAlignmentParameters) Helper.workflowStepParameters(rtAlignStep);
                    final WorkFlowType workFlowType;
                    if (parameters.sampleIdsForIdTransfer().size() > 0) {
                        workFlowType = WorkFlowType.LFQ_FRACTION_ID_TRANSFER;
                    } else {
                        workFlowType = WorkFlowType.LFQ;
                    }
                    analysisBuilder
                        .addWorkFlowsEnabled(workFlowType)
                        .getLfqStepBuilder()
                        .setLfqFilter(step.getLfqFilter())
                        .setDefaultProteinFeatureVectorFilter(step.getLfqProteinFeatureVectorFilter());
                    continue;
                }
                case REPORTER_ION_Q_BATCH_CALCULATION: {
                    analysisBuilder
                        .getReporterIonQStepBuilder()
                        .setReporterIonQParameters(DtoAdaptors.reporterIonQParameters(step));
                    continue;
                }
                case REPORTER_ION_Q_FILTER_SUMMARIZATION: {
                    // Sample names have been copied into parameters, and need to be maintained (ugh)
                    // Now we have to always update them before delivering them back to frontend.
                    ReporterIonQFilterSummarization filterParameters = step.getReporterIonQFilterSummarizationParameters();
                    ReporterIonQFilterSummarization.Builder updatedFilterParamaters = filterParameters.toBuilder()
                        .setExperimentSettings(updateSampleName(data, filterParameters.getExperimentSettings()));
                    analysisBuilder
                        .addWorkFlowsEnabled(WorkFlowType.REPORTER_ION_Q)
                        .getReporterIonQStepBuilder()
                        .setId(step.getId())
                        .setFilterSummarization(updatedFilterParamaters)
                        .setDefaultProteinFilter(step.getReporterIonQProteinFilter());
                    continue;
                }
                case DIA_DB_SEARCH:
                    DiaDbSearchParameters diaDBSearchParameters = (DiaDbSearchParameters) Helper.workflowStepParameters(step);
                    DbSearchParameters dbSearchParameters = DtoAdaptors.dbSearchParametersFromDiaDb(diaDBSearchParameters, analysis.databases());
                    analysisBuilder.getDbSearchStepBuilder()
                        .setDbSearchParameters(dbSearchParameters);
                    continue;
                case DIA_DB_FILTER_SUMMARIZE:
                    analysisBuilder.addWorkFlowsEnabled(WorkFlowType.DIA_DB_SEARCH)
                        .setAnalysisAcquisitionMethod(AnalysisAcquisitionMethod.DIA);
                    analysisBuilder.getDbSearchStepBuilder().setId(step.getId())
                        .setFilterSummarization(DtoAdaptors.filterSummarizationWithDenovoTagParameters(step))
                        .setDefaultProteinFilter(step.getDbProteinHitFilter());
                    continue;
                case SL_SEARCH:
                case DDA_SL_SEARCH: {
                    analysisBuilder.getSlStepBuilder()
                        .setAcquisitionMethod(step.getStepType() == StepType.SL_SEARCH ? SlStep.AcquisitionMethod.DIA : SlStep.AcquisitionMethod.DDA)
                        .setSlSearchStepParameters(DtoAdaptors.slSearchStepParameters(step.getSlSearchParameters()));
                    analysisBuilder.setAnalysisAcquisitionMethod(step.getStepType().equals(StepType.SL_SEARCH) ? AnalysisAcquisitionMethod.DIA : AnalysisAcquisitionMethod.DDA);
                    continue;
                }
                case SL_PEPTIDE_SUMMARIZATION: {
                    analysisBuilder.getSlStepBuilder()
                        .setSlProteinInferenceStepParameters(DtoAdaptors.slProteinInferenceStepParameters(
                            analysis.databases()
                        ));
                    continue;
                }
                case SL_FILTER_SUMMARIZATION: {
                    analysisBuilder
                        .addWorkFlowsEnabled(WorkFlowType.SPECTRAL_LIBRARY)
                        .getSlStepBuilder()
                        .setId(step.getId())
                        .setIdentificaitonFilterSummarization(DtoAdaptors.filterSummarizationWithDenovoTagParameters(step))
                        .setDefaultProteinFilter(step.getSlFilter());
                    continue;
                }
                case SILAC_FILTER_SUMMARIZE: {
                    final SilacFilterSummarizationParameters silacFilterSummarizationParamaters = step.getSilacFilterSummarizationParamaters();
                    final WorkFlowType workFlowType;
                    if (silacFilterSummarizationParamaters.getSilacParameters().getTransferOnlyGroup().getSamplesCount() > 0) {
                        workFlowType = WorkFlowType.SILAC_FRACTION_ID_TRANSFER;
                    } else {
                        workFlowType = WorkFlowType.SILAC;
                    }
                    analysisBuilder
                        .addWorkFlowsEnabled(workFlowType)
                        .getSilacStepBuilder()
                        .setId(step.getId())
                        .setSilacParameters(silacFilterSummarizationParamaters.getSilacParameters())
                        .setSilacFilter(silacFilterSummarizationParamaters.getSilacFilter());
                    continue;
                }
            }

            // Following block is for backward comparability before we added filter steps. (not sure if really required in production)
            {
                if (analysisBuilder.hasDbSearchStep()) {
                    final DbSearchStep.Builder dbSearchStepBuilder = analysisBuilder.getDbSearchStepBuilder();
                    if (!dbSearchStepBuilder.hasFilterSummarization()) {
                        dbSearchStepBuilder.setFilterSummarization(IdentificaitonFilterSummarization.getDefaultInstance());
                    }
                }
                if (analysisBuilder.hasPtmFinderStep()) {
                    final PtmFinderStep.Builder ptmFinderStepBuilder = analysisBuilder.getPtmFinderStepBuilder();
                    if (!ptmFinderStepBuilder.hasFilterSummarization()) {
                        ptmFinderStepBuilder.setFilterSummarization(IdentificaitonFilterSummarization.getDefaultInstance());
                    }
                }
                if (analysisBuilder.hasSpiderStep()) {
                    final SpiderStep.Builder spiderStepBuilder = analysisBuilder.getSpiderStepBuilder();
                    if (!spiderStepBuilder.hasFilterSummarization()) {
                        spiderStepBuilder.setFilterSummarization(IdentificaitonFilterSummarization.getDefaultInstance());
                    }
                }
                if (analysisBuilder.hasLfqStep()) {
                    final LfqStep.Builder lfqStepBuilder = analysisBuilder.getLfqStepBuilder();
                    if (!lfqStepBuilder.hasLfqFilter()) {
                        lfqStepBuilder.getLfqFilterBuilder()
                            .setLabelFreeQueryParameters(LfqFilter.LabelFreeQueryParameters.getDefaultInstance())
                            .setFeatureVectorFilter(LfqFilter.FeatureVectorFilter.getDefaultInstance());
                    }
                }
            }
        }
        return faimsCvFuture.thenCompose(set -> {
            analysisBuilder.addAllFaimsCvs(set);
            return completedFuture(analysisBuilder);
        });
    }

    private ReporterIonQExperimentSettings.Builder updateSampleName(ProjectAggregateState data, ReporterIonQExperimentSettings experimentSettings) {
        PMap<UUID, ProjectSampleState> samples = data.sampleIdToSample();
        ReporterIonQExperimentSettings.Builder updatedExperimentSettings = experimentSettings.toBuilder();

        switch (experimentSettings.getSelectedExperimentCase()) {
            case ALLEXPERIMENTS:
                // Do Nothing
                break;
            case SAMPLE:
                updateSampleName(data, updatedExperimentSettings.getSampleBuilder());
                break;
            default:
                throw new IllegalArgumentException("Unexpected selected experiment: " + experimentSettings.getSelectedExperimentCase());
        }

        if (experimentSettings.hasReferenceSample()) {
            updateSampleName(data, updatedExperimentSettings.getReferenceSampleBuilder());
        }

        if (experimentSettings.hasReferenceLabel()) {
            updateSampleName(data, updatedExperimentSettings.getReferenceLabelBuilder());
        }

        for (ReporterIonQExperimentSettings.ExperimentAlias.Builder experimentAlias : updatedExperimentSettings.getReferenceChannelsBuilderList()) {
            updateSampleName(data, experimentAlias);
        }

        for (ReporterIonQExperimentSettings.ExperimentAlias.Builder experimentAlias : updatedExperimentSettings.getAliasesBuilderList()) {
            updateSampleName(data, experimentAlias);
        }

        return updatedExperimentSettings;
    }

    private void updateSampleName(ProjectAggregateState data, ReporterIonQExperimentSettings.SampleSelection.Builder sampleBuilder) {
        sampleBuilder.setName(data.sampleName(UUID.fromString(sampleBuilder.getId())));
    }

    private void updateSampleName(ProjectAggregateState data, ReporterIonQExperimentSettings.ExperimentAlias.Builder sampleBuilder) {
        sampleBuilder.setSampleName(data.sampleName(UUID.fromString(sampleBuilder.getSampleId())));
    }

    public CompletionStage<ProjectAggregateQueryResponse.Builder> queryAnalysis(ProjectAggregateQueryRequest.AnalysisQueryRequest queryRequest, ProjectAggregateState data) throws IOException {
        final UUID analysisId = uuidFrom(queryRequest.getAnalysisId());
        final ProjectAnalysisState analysis = data.analysisIdToAnalysis().get(analysisId);
        if (analysis == null) {
            return completedFuture(
                ProjectAggregateQueryResponse.newBuilder()
                    .setFailure(ProjectAggregateQueryResponse.QueryFailure.newBuilder()
                        .setReason(ProjectAggregateQueryResponse.QueryFailure.Reason.NOT_FOUND)
                        .build()
                    )
            );
        }

        final CompletionStage<ProjectAggregateQueryResponse.AnalysisQueryResponse.Builder> futureBuilder;
        switch (queryRequest.getQueryCase()) {
            case GRAPHTASKIDS: {
                final ProjectAggregateQueryResponse.AnalysisQueryResponse.Builder analysisBuilder = ProjectAggregateQueryResponse.AnalysisQueryResponse.newBuilder();
                queryGraphTaskIds(data, analysis, queryRequest.getGraphTaskIds(), analysisBuilder.getGraphTaskIdsBuilder());
                futureBuilder = completedFuture(analysisBuilder);
                break;
            }
            case FILTERS: {
                futureBuilder = queryFilters(data, analysis, queryRequest.getFilters())
                    .thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse.newBuilder()::setFilterQueries);
                break;
            }
            case ANALYSISDTO:
                futureBuilder = queryAnalysisDto(data, analysis, queryRequest.getAnalysisDto())
                    .thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse.newBuilder()::setAnalysisDto);
                break;
            case TASKLISTING:
                final ProjectAggregateQueryResponse.AnalysisQueryResponse.Builder analysisBuilder = ProjectAggregateQueryResponse.AnalysisQueryResponse.newBuilder();
                final ProjectAggregateQueryResponse.TaskListing.Builder taskListingBuilder = analysisBuilder.getTaskListingBuilder();
                final Map<UUID, ProjectTaskPlanState> taskPlanIdToTaskPlan = data.taskPlanIdToTaskPlan();
                Streams.concat(
                    analysis.stepTaskPlanId().values().stream()
                        .flatMap(taskPlanIds -> taskPlanIds.stream()),
                    analysis.samples().stream()
                        .flatMap(sample -> sample.fractionIds().stream()
                            .flatMap(fractionId ->
                                data.sampleIdToSample().get(sample.sampleId()).fractionState().get(fractionId).stepTaskPlanId().values().stream()))
                ).map(taskPlanIdToTaskPlan::get)
                    .forEach(taskPlan -> taskListingBuilder.addTasks(projectTaskListing(data, taskPlan)));
                futureBuilder = completedFuture(analysisBuilder);
                break;
            case ANALYSISPARAMETERSDTO:
                futureBuilder = queryAnalysisParametersDto(data, analysis)
                    .thenApply(ProjectAggregateQueryResponse.AnalysisQueryResponse.newBuilder()::setAnalysisParametersDto);
                break;
            default:
                return failedCompletionStage(new IllegalArgumentException("Unexpected analysis query " + queryRequest.getQueryCase()));
        }
        return futureBuilder.thenApply(analysisBuilder -> ProjectAggregateQueryResponse.newBuilder()
            .setAnalysisQueryResponse(analysisBuilder.setAnalysisId(queryRequest.getAnalysisId()))
        );
    }

    private void queryGraphTaskIds(
        ProjectAggregateState data,
        ProjectAnalysisState analysis,
        ProjectAggregateQueryRequest.AnalysisQueryRequest.GraphTaskIds graphTaskIds,
        GraphTaskIds.Builder builder
    ) {
        final AnalysisTaskInformation taskInformation = AnalysisTaskInformation.create(config.keyspace(), data, projectId, analysis.analysisId());
        builder.setKeyspace(config.keyspace());
        builder.setProjectName(data.name());
        builder.setAnalysisName(taskInformation.analysisName());
        builder.setAnalysisAcquisitionMethod(analysis.analysisAcquisitionMethod());
        final List<WorkflowStep> steps = taskInformation.steps();
        final WorkFlowType workflowType;
        switch (graphTaskIds.getWorkflowSpecifierCase()) {
            case WORKFLOWTYPE:
                workflowType = graphTaskIds.getWorkflowType();
                break;
            case LASTWORKFLOW:
                //workflowType = ModelConversion.workflowType(steps.get(steps.size() - 1).getStepType());
                workflowType = Lists.reverse(steps).stream()
                    .map(step -> ModelConversion.workflowType(step.getStepType()))
                    .filter(x -> x == WorkFlowType.DENOVO || x == WorkFlowType.DB_SEARCH ||
                        x == WorkFlowType.PTM_FINDER || x == WorkFlowType.SPIDER || x == WorkFlowType.SPECTRAL_LIBRARY
                        || x == WorkFlowType.DIA_DB_SEARCH)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Unexpected workflow specifier " + graphTaskIds.getWorkflowSpecifierCase()));
                break;
            default:
                throw new IllegalStateException("Unexpected workflow specifier " + graphTaskIds.getWorkflowSpecifierCase());
        }

        final List<OrderedSampleFractions> samples;

        final String filterSampleId;
        if (workflowType == WorkFlowType.REPORTER_ION_Q) {
            final ReporterIonQExperimentSettings experimentSettings = steps.stream()
                .filter(step -> step.getStepType() == StepType.REPORTER_ION_Q_FILTER_SUMMARIZATION)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid workflow"))
                .getReporterIonQFilterSummarizationParameters()
                .getExperimentSettings();
            switch (experimentSettings.getSelectedExperimentCase()) {
                case ALLEXPERIMENTS:
                    filterSampleId = graphTaskIds.getSampleId();
                    break;
                case SAMPLE:
                    filterSampleId = experimentSettings.getSample().getId();
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected " + experimentSettings.getSelectedExperimentCase());
            }
        } else {
            filterSampleId = graphTaskIds.getSampleId();
        }

        if (Strings.isNullOrEmpty(graphTaskIds.getFractionId())) {
            if (Strings.isNullOrEmpty(filterSampleId)) {
                samples = taskInformation.samples();
            } else {
                UUID sampleId = UUID.fromString(filterSampleId);
                samples = taskInformation.samples().stream()
                    .filter(p -> p.sampleId().equals(sampleId))
                    .collect(Collectors.toList());
            }
        } else {
            UUID fractionId = UUID.fromString(graphTaskIds.getFractionId());
            if (Strings.isNullOrEmpty(filterSampleId)) {
                samples = taskInformation.samples().stream()
                    .filter(p -> p.fractionIds().contains(fractionId))
                    .map(p -> new OrderedSampleFractionsBuilder().sampleId(p.sampleId()).addFractionIds(fractionId).build())
                    .collect(Collectors.toList());

            } else {
                UUID sampleId = UUID.fromString(filterSampleId);
                samples = taskInformation.samples().stream()
                    .filter(p -> p.sampleId().equals(sampleId))
                    .filter(p -> p.fractionIds().contains(fractionId))
                    .map(p -> new OrderedSampleFractionsBuilder().sampleId(p.sampleId()).addFractionIds(fractionId).build())
                    .collect(Collectors.toList());
            }
        }

        builder
            .setProjectId(projectId.toString())
            .setAnalysisId(analysis.analysisId().toString())
            .setAnalysisCreatedTimeStamp(CommonFactory.convert(analysis.createdTimeStamp()))
            .setAllSampleIds(ModelConversion.uuidsToByteString(Lists.transform(taskInformation.samples(), OrderedSampleFractions::sampleId)))
            .addAllFastaHeaderParsers(taskInformation.fastaHeaderParsers())
            .addAllAnalysisDatabases(analysis.databases());

        for (OrderedSampleFractions sampleFractions : samples) {
            final UUID sampleId = sampleFractions.sampleId();
            List<UUID> fractionIds = sampleFractions.fractionIds();
            final ProjectSampleState sample = data.sampleIdToSample().get(sampleId);
            final SampleFractions.Builder sampleBuilder = builder.addSamplesBuilder()
                .setName(sample.name())
                .setSampleId(sampleId.toString())
                .setEnzyme(sample.enzyme())
                .setInstrument(sample.instrument())
                .setActivationMethod(sample.activationMethod());

            final Map<UUID, ProjectFractionState> fractions = sample.fractionState();
            for (UUID fractionId : fractionIds) {
                final String fractionIdString = fractionId.toString();
                builder.addFractionIds(fractionIdString);
                sampleBuilder.addFractionsIds(fractionIdString);
                sampleBuilder.addFractionSrcFile(fractions.get(fractionId).srcFile());
            }
        }

        final Map<UUID, UUID> dataLoadingStepFractionIdToTaskId = taskInformation.dataLoadingStepFractionIdToTaskId();
        final Map<UUID, UUID> featureDetectionStepFractionIdToTaskId = taskInformation.featureDetectionStepFractionIdToTaskId();
        final Map<UUID, UUID> dataRefineStepFractionIdToTaskId = taskInformation.dataRefineStepFractionIdToTaskId();
        final Map<UUID, UUID> diaDenovoStepFractionIdToTaskId = taskInformation.diaDenovoDataRefineStepFractionIdToTaskId();
        final Map<UUID, UUID> denovoStepFractionIdToTaskId = taskInformation.denovoStepFractionIdToTaskId();
        final Map<UUID, UUID> dbPreSearchStepFractionIdToTaskId = taskInformation.dbPreSearchStepFractionIdToTaskId();
        final Map<UUID, UUID> reporterIonQStepFractionIdToTaskId = taskInformation.reporterIonQStepFractionIdToTaskId();
        final Map<UUID, UUID> slSearchStepFractionIdToTaskId = taskInformation.slSearchStepFractionIdToTaskId();
        final Map<UUID, UUID> diaDbSearchNewStepFractionIdToTaskId = taskInformation.diaDbSearchStepFractionIdToTaskId();
        final Map<UUID, UUID> ddaSlSearchStepFractionIdToTaskId = taskInformation.ddaSlSearchStepFractionIdToTaskId();
        final Map<UUID, UUID> silacFeatureVectorSearchFractionIdToTaskId = taskInformation.silacFeatureVectorSearchTaskId();
        samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
            final String fractionIdString = fractionId.toString();
            Optional.ofNullable(dataLoadingStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToDataLoadingTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(featureDetectionStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToFeatureDetectionTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(dataRefineStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToRefinedTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(diaDenovoStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToRefinedTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(denovoStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToDenovoTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(dbPreSearchStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToPreDBSearchTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(reporterIonQStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToRiqTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(ddaSlSearchStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToSlPsmRepositoryTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(silacFeatureVectorSearchFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToSilacSearchTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(diaDbSearchNewStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToDiaDbSearchTaskId(fractionIdString, taskId.toString()));

            Optional.ofNullable(slSearchStepFractionIdToTaskId.get(fractionId))
                .ifPresent(taskId -> builder.putFractionIdToSlSearchTaskTaskId(fractionIdString, taskId.toString()));
        }));

        taskInformation.sampleIdToDenovoFilterTaskId()
            .entrySet()
            .forEach(entry -> builder.putSampleIdToDenovoFilterTaskId(entry.getKey().toString(), entry.getValue().toString()));

        taskInformation.lfqFilterSummarizationTaskId().ifPresent(taskId -> builder.setLfqFilterSummarzationTaskId(taskId.toString()));
        taskInformation.diaLfqFilterSummarizationTaskId().ifPresent(taskId -> builder.setLfqFilterSummarzationTaskId(taskId.toString()));

        taskInformation.reporterIonQSummarizationTaskId().ifPresent(taskId -> builder.setRiqSummarizeTaskId(taskId.toString()));

        taskInformation.lfqSummarizeFractionIndexToTaskId().values()
            .forEach(taskId -> builder.addLfqSummarizeTaskIds(taskId.toString()));
        taskInformation.diaLfqSummarizeFractionIndexToTaskId().values()
            .forEach(taskId -> builder.addLfqSummarizeTaskIds(taskId.toString()));

        taskInformation.lfqSummarizationFilterResult()
            .ifPresent(builder::setLfqSummarizationFilterResult);

        taskInformation.diaLfqSummarizationFilterResult()
            .ifPresent(builder::setLfqSummarizationFilterResult);

        taskInformation.silacFilterSummarizationTaskId()
            .ifPresent(taskId -> builder.setSilacFilterSummarizationTaskId(taskId.toString()));

        steps.stream()
            .filter(step -> step.getStepType() == StepType.LFQ_RETENTION_TIME_ALIGNMENT)
            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
            .findFirst()
            .ifPresent(p -> {
                try {
                    RetentionTimeAlignmentParameters parameters = OM.readValue(p, RetentionTimeAlignmentParameters.class);
                    parameters.groups().stream()
                        .flatMap(g -> g.sampleIds().stream())
                        .flatMap(sampleId -> taskInformation.fractionIdsForSample(sampleId).stream())
                        .forEach(fractionId -> {
                            UUID predictorId = taskInformation.fractionIdToRtAlignmentTaskId(fractionId);
                            if (predictorId != null) {
                                builder.putFractionIdToRtSegmentedPredictorId(fractionId.toString(), predictorId.toString());
                            }
                        });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        steps.stream()
            .filter(step -> step.getStepType() == StepType.DIA_LFQ_RETENTION_TIME_ALIGNMENT)
            .findFirst()
            .ifPresent(ignore -> {
                for (UUID fractionId : taskInformation.fractionIds()) {
                    UUID predictorId = taskInformation.fractionIdToDiaRtAlignmentTaskId(fractionId);
                    if (predictorId != null) {
                        builder.putFractionIdToRtSegmentedPredictorId(fractionId.toString(), predictorId.toString());
                    }
                }
            });

        steps.stream()
            .filter(step -> step.getStepType() == StepType.FEATURE_DETECTION)
            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
            .findFirst()
            .ifPresent(builder::setJsonFeatureDetectionParameters);

        steps.stream()
            .filter(step -> step.getStepType() == StepType.DENOVO)
            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
            .findFirst()
            .ifPresent(builder::setJsonDenovoParameters);

        steps.stream()
            .filter(step -> step.getStepType() == StepType.LFQ_SUMMARIZE)
            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
            .findFirst()
            .ifPresent(builder::setJsonLfqParameters);

        steps.stream()
            .filter(step -> step.getStepType() == StepType.LFQ_FILTER_SUMMARIZATION || step.getStepType() == StepType.DIA_LFQ_FILTER_SUMMARIZATION)
            .findFirst()
            .ifPresent(step -> builder.setLfqFilter(step.getLfqFilter()));

        steps.stream()
            .filter(step -> step.getStepType() == StepType.REPORTER_ION_Q_FILTER_SUMMARIZATION)
            .findFirst()
            .ifPresent(step -> builder.setReporterIonQSummaryFilterTaskParameters(step.getReporterIonQFilterSummarizationParameters()));

        steps.stream()
            .filter(step -> step.getStepType() == StepType.REPORTER_ION_Q_BATCH_CALCULATION)
            .findFirst()
            .ifPresent(step -> {
                final ReporterIonQBatchCalculationParameters reporterIonQBatchCalculationParameters = step.getReporterIonQBatchCalculationParameters();
                builder.setReporterIonQBatchCalculationParameters(reporterIonQBatchCalculationParameters);
                builder.setReporterIonQParameters(reporterIonQBatchCalculationParameters.getParameters());
            });

        switch (workflowType) {
            case DB_SEARCH: {
                final Map<UUID, UUID> searchStepFractionIdToTaskId = taskInformation.dbSearchStepFractionIdToTaskId();
                samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
                    final UUID taskId = searchStepFractionIdToTaskId.get(fractionId);
                    if (taskId == null) return;
                    builder.putFractionIdToPsmRepositoryTaskId(fractionId.toString(), taskId.toString());
                }));
                taskInformation.dbSearchSummarizeTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setIdentificationPeptideRepositoryTaskId(taskId);
                    builder.setPsmDecoyInfoTaskId(taskId);
                });
                taskInformation.dbFilterSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setProteinRepositoryTaskId(taskId)
                        .setPeptideRepositoryTaskId(taskId)
                        .setIdFilterSummarizationTaskId(taskId)
                        .setProteinHitRepositoryTaskId(taskId);
                });
                taskInformation.steps().stream()
                    .filter(step -> step.getStepType() == StepType.DB_SUMMARIZE)
                    .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                    .findFirst()
                    .ifPresent(builder::setJsonSearchParameters);

                Map<UUID, UUID> sampleIdToDenovoOnlyTaskId = taskInformation.dbDenovoOnlySearchStepSampleIdToTaskId();
                samples.forEach(pair -> Optional.ofNullable(sampleIdToDenovoOnlyTaskId.get(pair.sampleId()))
                    .ifPresent(taskId -> builder.putSampleIdToDenovoOnlySearchTaskId(pair.sampleId().toString(), taskId.toString())));
                break;
            }
            case PTM_FINDER: {
                taskInformation.ptmSearchSummarizeTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setIdentificationPeptideRepositoryTaskId(taskId);
                    builder.setPsmDecoyInfoTaskId(taskId);
                    samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
                        builder.putFractionIdToPsmRepositoryTaskId(fractionId.toString(), taskId);
                    }));
                });
                taskInformation.ptmFilterSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setProteinRepositoryTaskId(taskId)
                        .setPeptideRepositoryTaskId(taskId)
                        .setIdFilterSummarizationTaskId(taskId)
                        .setProteinHitRepositoryTaskId(taskId);
                });
                taskInformation.steps().stream()
                    .filter(step -> step.getStepType() == StepType.PTM_FINDER_SUMMARIZE)
                    .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                    .findFirst()
                    .ifPresent(builder::setJsonSearchParameters);
                Map<UUID, UUID> sampleIdToDenovoOnlyTaskId = taskInformation.ptmFinderDenovoOnlySearchStepSampleIdToTaskId();
                samples.forEach(pair -> Optional.ofNullable(sampleIdToDenovoOnlyTaskId.get(pair.sampleId()))
                    .ifPresent(taskId -> builder.putSampleIdToDenovoOnlySearchTaskId(pair.sampleId().toString(), taskId.toString())));
                break;
            }
            case SPIDER: {
                taskInformation.spiderSearchSummarizeTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setIdentificationPeptideRepositoryTaskId(taskId);
                    builder.setPsmDecoyInfoTaskId(taskId);
                    samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
                        builder.putFractionIdToPsmRepositoryTaskId(fractionId.toString(), taskId);
                    }));
                });
                taskInformation.spiderFilterSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setProteinRepositoryTaskId(taskId)
                        .setPeptideRepositoryTaskId(taskId)
                        .setIdFilterSummarizationTaskId(taskId)
                        .setProteinHitRepositoryTaskId(taskId);
                });
                taskInformation.steps().stream()
                    .filter(step -> step.getStepType() == StepType.SPIDER_SUMMARIZE)
                    .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                    .findFirst()
                    .ifPresent(builder::setJsonSearchParameters);
                Map<UUID, UUID> sampleIdToDenovoOnlyTaskId = taskInformation.spiderDenovoOnlySearchStepSampleIdToTaskId();
                samples.forEach(pair -> Optional.ofNullable(sampleIdToDenovoOnlyTaskId.get(pair.sampleId()))
                    .ifPresent(taskId -> builder.putSampleIdToDenovoOnlySearchTaskId(pair.sampleId().toString(), taskId.toString())));
                break;
            }
            case SPECTRAL_LIBRARY: {
                taskInformation.slFilterSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setIdFilterSummarizationTaskId(taskId);
                    if (!analysis.databases().isEmpty())  {
                        builder.setProteinRepositoryTaskId(taskId);
                    }
                });
                taskInformation.slPeptideSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setPeptideRepositoryTaskId(taskId);
                });
                final boolean isDia = taskInformation.stepParameters().containsKey(StepType.SL_SEARCH);
                builder.setSlSearchParameters(isDia ? (SlSearchParameters) taskInformation.stepParameters().get(StepType.SL_SEARCH):
                    (SlSearchParameters) taskInformation.stepParameters().get(StepType.DDA_SL_SEARCH));

                //so we can get mass calibration
                for(Map.Entry<UUID, UUID> entry : taskInformation.slSearchStepFractionIdToTaskId().entrySet()) {
                    builder.putFractionIdToPreDBSearchTaskId(entry.getKey().toString(), entry.getValue().toString())
                        .putFractionIdToSlPsmRepositoryTaskId(entry.getKey().toString(), entry.getValue().toString());
                }

                taskInformation.slPeptideSummarizationTaskId().map(UUID::toString).ifPresent(builder::setLibraryTaskId);

                break;
            }
            case DIA_DB_SEARCH: {
                taskInformation.diaDbFilterSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setIdFilterSummarizationTaskId(taskId);
                    builder.setProteinRepositoryTaskId(taskId);
                });
                taskInformation.diaDbPeptideSummarizationTaskId().map(UUID::toString).ifPresent(builder::setPeptideRepositoryTaskId);
                buildSlSearchForDiaDbWorkflow(builder.getSlSearchParametersBuilder(), taskInformation);

                //so we can get mass calibration
                for(Map.Entry<UUID, UUID> entry : taskInformation.diaDbSearchStepFractionIdToTaskId().entrySet()) {
                    builder.putFractionIdToPreDBSearchTaskId(entry.getKey().toString(), entry.getValue().toString())
                        .putFractionIdToSlPsmRepositoryTaskId(entry.getKey().toString(), entry.getValue().toString())
                        .putFractionIdToDiaDbSearchTaskId(entry.getKey().toString(), entry.getValue().toString());
                }
                taskInformation.slPeptideSummarizationTaskId().map(UUID::toString).ifPresent(builder::setLibraryTaskId);
                break;
            }
            case REPORTER_ION_Q:
                taskInformation.samples().forEach(sample -> {
                    UUID sampleId = sample.sampleId();
                    UUID taskId = taskInformation.reporterIonQNormalizationSampleIdToTaskId().get(sampleId);
                    if (taskId != null) {
                        builder.putAllReporterIonQNormalizationSampleIdToTaskId(sampleId.toString(), taskId.toString());
                    }
                });
                samples.forEach(sample -> {
                    final UUID sampleId = sample.sampleId();
                    final UUID taskId = taskInformation.reporterIonQNormalizationSampleIdToTaskId().get(sampleId);
                    if (taskId != null) {
                        final String taskIdString = taskId.toString();
                        builder.putSampleIdToRiqNormalizationTaskId(sampleId.toString(), taskIdString);
                        sample.fractionIds().forEach(fractionId -> {
                            builder.putFractionIdToPsmRepositoryTaskId(fractionId.toString(), taskIdString);
                        });
                    }
                });
                taskInformation.reporterIonQSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                    builder.setProteinRepositoryTaskId(taskId);
                    builder.setIdFilterSummarizationTaskId(taskId);
                    builder.setPeptideRepositoryTaskId(taskId);
                });

                switch(analysis.lastIdentificationType()) {
                    case DB:
                        taskInformation.dbFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinHitRepositoryTaskId);
                        taskInformation.dbSearchSummarizeTaskId().map(UUID::toString).ifPresent(builder::setIdentificationPeptideRepositoryTaskId);
                        builder.setJsonSearchParameters(taskInformation.steps().stream()
                            .filter(step -> step.getStepType() == StepType.DB_SUMMARIZE)
                            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                            .findFirst().orElseThrow(() -> new IllegalStateException("Invalid workflow"))
                        );
                        break;
                    case PTM_FINDER:
                        taskInformation.ptmFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinHitRepositoryTaskId);
                        taskInformation.ptmSearchSummarizeTaskId().map(UUID::toString).ifPresent(builder::setIdentificationPeptideRepositoryTaskId);
                        builder.setJsonSearchParameters(taskInformation.steps().stream()
                            .filter(step -> step.getStepType() == StepType.PTM_FINDER_SUMMARIZE)
                            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                            .findFirst().orElseThrow(() -> new IllegalStateException("Invalid workflow"))
                        );
                        break;
                    case SPIDER:
                        taskInformation.spiderFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinHitRepositoryTaskId);
                        taskInformation.spiderSearchSummarizeTaskId().map(UUID::toString).ifPresent(builder::setIdentificationPeptideRepositoryTaskId);
                        builder.setJsonSearchParameters(taskInformation.steps().stream()
                            .filter(step -> step.getStepType() == StepType.SPIDER_SUMMARIZE)
                            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                            .findFirst().orElseThrow(() -> new IllegalStateException("Invalid workflow"))
                        );
                        break;
                    case NONE:
                    default:
                        throw new IllegalArgumentException("Reporter IonQ Unsupported last IdentificationType " + analysis.lastIdentificationType());
                }
                break;
            case SILAC_FRACTION_ID_TRANSFER:
            case SILAC: {
                builder.setSilacParameters(
                    ((SilacFilterSummarizationParameters) taskInformation.stepParameters().get(StepType.SILAC_FILTER_SUMMARIZE)).getSilacParameters()
                );
                //dont break
            }
            case LFQ_FRACTION_ID_TRANSFER:
            case LFQ:
                steps.stream()
                    .filter(step -> step.getStepType() == StepType.DIA_LFQ_FEATURE_EXTRACTION)
                    .map(step -> {
                        DiaFeatureExtractorParameters extractorParameters = step.getDiaFeatureExtractionParameters();
                        LfqFilter lfqFilter = analysis.findStep(StepType.DIA_LFQ_FILTER_SUMMARIZATION).getLfqFilter();
                        List<UUID> sampleIds = samples.stream().map(OrderedSampleFractions::sampleId).collect(Collectors.toList());
                        return DtoAdaptors.fromDiaLfq(extractorParameters, lfqFilter, sampleIds);
                    })
                    .map(step -> {
                        try {
                            return OM.writeValueAsString(step);
                        } catch (JsonProcessingException e) {
                            throw new IllegalStateException("Unable to create Lfq Params");
                        }
                    })
                    .map(str -> new JsonObject(str).encode())
                    .findFirst()
                    .ifPresent(builder::setJsonLfqParameters);
                switch(analysis.lastIdentificationType()) {
                    case DB:
                        final Map<UUID, UUID> searchStepFractionIdToTaskId = taskInformation.dbSearchStepFractionIdToTaskId();
                        taskInformation.dbSearchSummarizeTaskId().map(UUID::toString).ifPresent(builder::setIdentificationPeptideRepositoryTaskId);
                        samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
                            final UUID taskId = searchStepFractionIdToTaskId.get(fractionId);
                            if (taskId == null) return;
                            builder.putFractionIdToPsmRepositoryTaskId(fractionId.toString(), taskId.toString());
                        }));
                        taskInformation.dbFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinHitRepositoryTaskId);
                        taskInformation.dbFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinRepositoryTaskId);
                        builder.setJsonSearchParameters(taskInformation.steps().stream()
                            .filter(step -> step.getStepType() == StepType.DB_SUMMARIZE)
                            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                            .findFirst().orElseThrow(() -> new IllegalStateException("Invalid workflow"))
                        );
                        break;
                    case PTM_FINDER:
                        taskInformation.ptmSearchSummarizeTaskId().map(UUID::toString).ifPresent(taskId -> {
                            builder.setIdentificationPeptideRepositoryTaskId(taskId);
                            samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
                                builder.putFractionIdToPsmRepositoryTaskId(fractionId.toString(), taskId);
                            }));
                        });
                        taskInformation.ptmFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinHitRepositoryTaskId);
                        taskInformation.ptmFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinRepositoryTaskId);
                        builder.setJsonSearchParameters(taskInformation.steps().stream()
                            .filter(step -> step.getStepType() == StepType.PTM_FINDER_SUMMARIZE)
                            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                            .findFirst().orElseThrow(() -> new IllegalStateException("Invalid workflow"))
                        );
                        break;
                    case SPIDER:
                        taskInformation.spiderSearchSummarizeTaskId().map(UUID::toString).ifPresent(taskId -> {
                            builder.setIdentificationPeptideRepositoryTaskId(taskId);
                            samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
                                builder.putFractionIdToPsmRepositoryTaskId(fractionId.toString(), taskId);
                            }));
                        });
                        taskInformation.spiderFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinHitRepositoryTaskId);
                        taskInformation.spiderFilterSummarizationTaskId().map(UUID::toString).ifPresent(builder::setProteinRepositoryTaskId);
                        builder.setJsonSearchParameters(taskInformation.steps().stream()
                            .filter(step -> step.getStepType() == StepType.SPIDER_SUMMARIZE)
                            .map(step -> new JsonObject(step.getStepParametersJson()).getJsonObject("parameters").encode())
                            .findFirst().orElseThrow(() -> new IllegalStateException("Invalid workflow"))
                        );
                        break;
                    case SPECTRAL_LIBRARY:
                        samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
                            final UUID taskId = slSearchStepFractionIdToTaskId.get(fractionId);
                            if (taskId == null) return;
                            builder.putFractionIdToSlPsmRepositoryTaskId(fractionId.toString(), taskId.toString());
                        }));
                        taskInformation.slPeptideSummarizationTaskId().map(UUID::toString).ifPresent(id ->
                            builder.setIdentificationPeptideRepositoryTaskId(id)
                                .setPeptideRepositoryTaskId(id)
                                .setLibraryTaskId(id)
                        );
                        taskInformation.slFilterSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                            builder.setIdFilterSummarizationTaskId(taskId);
                            if (!analysis.databases().isEmpty())  {
                                builder.setProteinRepositoryTaskId(taskId);
                            }
                        });
                        builder.setSlSearchParameters(analysis.analysisAcquisitionMethod().equals(AnalysisAcquisitionMethod.DIA)
                            ? (SlSearchParameters) taskInformation.stepParameters().get(StepType.SL_SEARCH)
                            : (SlSearchParameters) taskInformation.stepParameters().get(StepType.DDA_SL_SEARCH));
                        break;
                    case DIA_DB:
                        samples.forEach(sample -> sample.fractionIds().forEach(fractionId -> {
                            final UUID taskId = diaDbSearchNewStepFractionIdToTaskId.get(fractionId);
                            if (taskId == null) return;
                            builder.putFractionIdToSlPsmRepositoryTaskId(fractionId.toString(), taskId.toString());
                        }));
                        if (analysis.stepTypes().contains(StepType.SL_PEPTIDE_SUMMARIZATION)) {
                            //Set library taskId for psms create from SL
                            taskInformation.slPeptideSummarizationTaskId().map(UUID::toString)
                                .ifPresent(id -> builder.setLibraryTaskId(id));
                        }
                        taskInformation.diaDbPeptideSummarizationTaskId().map(UUID::toString).ifPresent(id ->
                            builder.setIdentificationPeptideRepositoryTaskId(id)
                                .setPeptideRepositoryTaskId(id)
                        );
                        taskInformation.diaDbFilterSummarizationTaskId().map(UUID::toString).ifPresent(taskId -> {
                            builder.setIdFilterSummarizationTaskId(taskId);
                            if (!analysis.databases().isEmpty())  {
                                builder.setProteinRepositoryTaskId(taskId);
                            }
                        });
                        if (analysis.analysisAcquisitionMethod().equals(AnalysisAcquisitionMethod.DIA)) {
                            buildSlSearchForDiaDbWorkflow(builder.getSlSearchParametersBuilder(), taskInformation);
                        } else {
                            builder.setSlSearchParameters((SlSearchParameters) taskInformation.stepParameters().get(StepType.DDA_SL_SEARCH));
                        }
                        break;
                    case NONE:
                    default:
                        throw new IllegalArgumentException("LFQ Unsupported last IdentificationType " + analysis.lastIdentificationType());
                }
                break;
            case DENOVO:
            case DATA_REFINEMENT:
                // They all just omit some values.
                break;
            default:
                throw new IllegalStateException("Unexpected workflow type " + workflowType);
        }
    }

    private SlSearchParameters.Builder buildSlSearchForDiaDbWorkflow(SlSearchParameters.Builder diaSlSearchBuilder, AnalysisTaskInformation taskInformation) {
        if(taskInformation.hasSlSearch()) {
            SlSearchParameters slStepSearchParams = (SlSearchParameters) taskInformation.stepParameters().get(StepType.SL_SEARCH);
            diaSlSearchBuilder
                .mergeFrom(slStepSearchParams);
        }
        DiaDbSearchParameters diaDBSearchParameters = (DiaDbSearchParameters) taskInformation.stepParameters().get(StepType.DIA_DB_SEARCH);
        diaSlSearchBuilder.getSlSearchSharedParametersBuilder()
            .setFragmentTolerance(diaDBSearchParameters.getFragmentTol())
            .setPrecursorTolerance(diaDBSearchParameters.getPrecursorTol())
            .setMinPeptideLength(diaDBSearchParameters.getDiaDbSearchSharedParameters().getPeptideLenMin())
            .setMaxPeptideLength(diaDBSearchParameters.getDiaDbSearchSharedParameters().getPeptideLenMax());
        return diaSlSearchBuilder;
    }

    public IDFilter.PsmFilter getPsmFilter(ProjectAnalysisState analysis, WorkFlowType workFlowType, DecoyInfo decoyInfo) {
        java.util.function.Function<Float, Float> fdrCalculator = fdr -> {
            return DecoyInfoWrapper.fdrToPValue(decoyInfo, fdr);
        };
        final StepType filterStepType = ModelConversion.filterSummarization(workFlowType, analysis.analysisAcquisitionMethod());
        switch (filterStepType) {
            case DB_FILTER_SUMMARIZATION:
                return PsmFilter.fromDto(analysis.findStep(filterStepType).getDbFilterSummarizationParameters(), fdrCalculator).toDto();
            case PTM_FINDER_FILTER_SUMMARIZATION:
                return PsmFilter.fromDto(analysis.findStep(filterStepType).getPtmFilterSummarizationParameters(), fdrCalculator).toDto();
            case SPIDER_FILTER_SUMMARIZATION:
                return PsmFilter.fromDto(analysis.findStep(filterStepType).getSpiderFilterSummarizationParameters(), fdrCalculator).toDto();
            default:
                throw new IllegalStateException("Unable to create psm filter for step type " + filterStepType);
        }
    }

    public IDFilter.PeptideFilter getPeptideFilter(ProjectAnalysisState analysis, WorkFlowType workFlowType, DecoyInfo decoyInfo) {
        java.util.function.Function<Float, Float> fdrCalculator = fdr -> {
            return DecoyInfoWrapper.fdrToPValue(decoyInfo, fdr);
        };
        final StepType filterStepType = ModelConversion.filterSummarization(workFlowType, analysis.analysisAcquisitionMethod());
        final IDFilter.PeptideFilter peptideFilter;
        switch (filterStepType) {
            case DB_FILTER_SUMMARIZATION:
                peptideFilter = analysis.findStep(filterStepType).getDbFilterSummarizationParameters().getIdPeptideFilter();
                break;
            case PTM_FINDER_FILTER_SUMMARIZATION:
                peptideFilter = analysis.findStep(filterStepType).getPtmFilterSummarizationParameters().getIdPeptideFilter();
                break;
            case SPIDER_FILTER_SUMMARIZATION:
                peptideFilter = analysis.findStep(filterStepType).getSpiderFilterSummarizationParameters().getIdPeptideFilter();
                break;
            default:
                throw new IllegalStateException("Unable to create psm filter for step type " + filterStepType);
        }
        return PeptideFilter.fromProto(peptideFilter, fdrCalculator).dto(IDFilter.PeptideFilter.newBuilder());
    }

    public SlFilter.PeptideFilter getSlPeptideFilter(SlFilterSummarizationResult slFilterSummarizationResult, ProjectAnalysisState analysis, StepType stepType) {
        IdentificaitonFilter filters = analysis.findStep(stepType).getSlFilterSummarizationParameters().getFilters();
        return filters.getSlPeptideFilter()
            .toBuilder()
            .setMinPValue(slFilterSummarizationResult.getMinPeptidePValue())
            .build();
    }

    public SlFilter.PsmFilter getSlPsmFilter(SlFilterSummarizationResult slFilterSummarizationResult, ProjectAnalysisState analysis, StepType stepType) {
        IdentificaitonFilter filters = analysis.findStep(stepType).getSlFilterSummarizationParameters().getFilters();
        return new SLPsmFilterBuilder()
            .includeDecoy(false)
            .minPValue((float) slFilterSummarizationResult.getMinPsmPValue())
            .build().proto();
    }

    public DenovoCandidateFilter getDenovoOnlyFilter(ProjectAnalysisState analysis, WorkFlowType workFlowType) {
        StepType filterStepType = ModelConversion.filterSummarization(workFlowType, analysis.analysisAcquisitionMethod());
        WorkflowStep workflowStep = analysis.findStep(filterStepType);
        final IdentificaitonFilter filterParameters;
        switch (filterStepType) {
            case DB_FILTER_SUMMARIZATION:
                filterParameters = workflowStep.getDbFilterSummarizationParameters();
                break;
            case PTM_FINDER_FILTER_SUMMARIZATION:
                filterParameters = workflowStep.getPtmFilterSummarizationParameters();
                break;
            case SPIDER_FILTER_SUMMARIZATION:
                filterParameters = workflowStep.getSpiderFilterSummarizationParameters();
                break;
            default:
                throw new IllegalStateException("Bad step cannot get denovo only filter");
        }

        return new DenovoCandidateFilterBuilder()
            .minAlc(filterParameters.getMinDenovoAlc())
            .build();
    }

    private CompletionStage<ProjectAggregateQueryResponse.AnalysisQueryResponse.FilterQueries.Builder> queryFilters(
        ProjectAggregateState data,
        ProjectAnalysisState analysis,
        ProjectAggregateQueryRequest.AnalysisQueryRequest.Filters query
    ) {
        final ProjectAggregateQueryResponse.AnalysisQueryResponse.FilterQueries.Builder builder = ProjectAggregateQueryResponse.AnalysisQueryResponse.FilterQueries.newBuilder();
        final PMap<WorkFlowType, FilterList> workflowTypeFilter = analysis.workflowTypeFilter();
        final AnalysisTaskInformation taskInformation = AnalysisTaskInformation.create(config.keyspace(), data, projectId, analysis.analysisId());
        final OrderedSampleFractionsList sampleFractions = new OrderedSampleFractionsList(analysis.samples());
        switch (query.getWorkflowSpecifierCase()) {
            case WORKFLOWTYPE:
                builder.addWorkflowType(query.getWorkflowType());
                break;
            case LASTWORKFLOW:
                builder.addWorkflowType(
                    Lists.reverse(analysis.steps()).stream()
                        .map(step -> ModelConversion.workflowType(step.getStepType()))
                        .filter(x -> x == WorkFlowType.DB_SEARCH || x == WorkFlowType.PTM_FINDER || x == WorkFlowType.SPIDER)
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Unexpected workflow specifier " + query.getWorkflowSpecifierCase()))
                );
                break;
            case ALLQUERIES:
                for (WorkFlowType workFlowType : workflowTypeFilter.keySet()) {
                    if (!taskInformation.isAnalysisWorkflowTypeStepDone(workFlowType)) continue;
                    builder.addWorkflowType(workFlowType);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected workflow specifier " + query.getWorkflowSpecifierCase());
        }
        CompletionStage<ProjectAggregateQueryResponse.AnalysisQueryResponse.FilterQueries.Builder> future = CompletableFuture.completedFuture(builder);
        for (WorkFlowType workFlowType : builder.getWorkflowTypeList()) {
            final FilterList filterList = workflowTypeFilter.get(workFlowType);

            //No Protein fdr conversion b/c we the conversion is already done when the filter is saved
            switch (workFlowType) {
                case DENOVO:
                    future = future.thenApply(currentBuilder -> {
                        Query immutableQuery = DenovoQuery.fromDto(filterList.getIdFiltersList().get(0).getDenovoCandidateFilter());
                        currentBuilder.addQueryJson(writeValueAsString(immutableQuery));
                        return currentBuilder;
                    });
                    break;
                case DB_SEARCH:
                    future = future
                        .thenApply(currentBuilder -> {
                            IDFilter idFilter = filterList.getIdFiltersList().get(0);
                            Query immutableQuery = DbSearchQuery.fromDtoNoProteinFdrConversion(
                                idFilter.getDenovoCandidateFilter(),
                                idFilter.getPsmFilter(),
                                idFilter.getPeptideFilter(),
                                idFilter.getProteinHitFilter()
                            );
                            currentBuilder.addQueryJson(writeValueAsString(immutableQuery));
                            return currentBuilder;
                        });
                    break;
                case PTM_FINDER:
                    future = future
                        .thenApply(currentBuilder -> {
                            IDFilter idFilter = filterList.getIdFiltersList().get(0);
                            Query immutableQuery = PtmFinderQuery.fromDtoNoProteinFdrConversion(
                                idFilter.getDenovoCandidateFilter(),
                                idFilter.getPsmFilter(),
                                idFilter.getPeptideFilter(),
                                idFilter.getProteinHitFilter()
                            );
                            currentBuilder.addQueryJson(writeValueAsString(immutableQuery));
                            return currentBuilder;
                        });
                    break;
                case SPIDER:
                    future = future
                        .thenApply(currentBuilder -> {
                            IDFilter idFilter = filterList.getIdFiltersList().get(0);
                            Query immutableQuery = SpiderQuery.fromDtoNoProteinFdrConversion(
                                idFilter.getDenovoCandidateFilter(),
                                idFilter.getPsmFilter(),
                                idFilter.getPeptideFilter(),
                                idFilter.getProteinHitFilter()
                            );
                            currentBuilder.addQueryJson(writeValueAsString(immutableQuery));
                            return currentBuilder;
                        });
                    break;
                case LFQ:
                    future = future.thenApply(currentBuilder -> {
                        LfqFilter lfqFilter;
                        LfqSummarizationFilterResult lfqSummarizationFilterResult;
                        if (analysis.containsStep(StepType.LFQ_FILTER_SUMMARIZATION)) {
                            lfqFilter = analysis.findStep(StepType.LFQ_FILTER_SUMMARIZATION).getLfqFilter();
                            lfqSummarizationFilterResult = taskInformation.lfqSummarizationFilterResult()
                                .orElseThrow(() -> new IllegalStateException("Unable to create lfq query, no lfq filter summarization result."));
                        } else {
                            lfqFilter = analysis.findStep(StepType.DIA_LFQ_FILTER_SUMMARIZATION).getLfqFilter();
                            lfqSummarizationFilterResult = taskInformation.diaLfqSummarizationFilterResult()
                                .orElseThrow(() -> new IllegalStateException("Unable to create lfq query, no lfq filter summarization result."));
                        }

                        List<UUID> sampleIds = analysis.samples().stream().map(OrderedSampleFractions::sampleId).collect(Collectors.toList());
                        Query immutableQuery = LabelFreeQuery.fromDto(
                            lfqFilter,
                            sampleIds,
                            lfqSummarizationFilterResult,
                            filterList.getLfqProteinFeatureVectorFilter(),
                            lfqSummarizationFilterResult.getNormValuesList()
                        );
                        currentBuilder.addQueryJson(writeValueAsString(immutableQuery));
                        return currentBuilder;
                    });
                    break;
                case REPORTER_ION_Q:
                    FilterList dbFilterList = workflowTypeFilter.get(WorkFlowType.DB_SEARCH);
                    future = future
                        // TODO - Getting DecoyInfo is slow .. we need to do this only once, not for every query.
                        .thenCombine(psmPeptideDecoyInfoFuture(taskInformation, DtoAdaptors.identificationTypeToWorkFlowType(analysis.lastIdentificationType())), Pair::create)
                        .thenApply(pair -> {
                            ProjectAggregateQueryResponse.AnalysisQueryResponse.FilterQueries.Builder currentBuilder = pair.first();
                            Pair<DecoyInfo, DecoyInfo> psmPeptideDecoys = pair.second();
                            final DecoyInfo psmDecoyInfo = psmPeptideDecoys.first();
                            final DecoyInfo peptideDecoyInfo = psmPeptideDecoys.second();
                            WorkflowStep riqFilterSummarizationStep = analysis.findStep(StepType.REPORTER_ION_Q_FILTER_SUMMARIZATION);
                            ReporterIonQFilterSummarization reporterIonQFilterSummarization = riqFilterSummarizationStep.getReporterIonQFilterSummarizationParameters();
                            ReporterIonQSpectrumFilter spectrumFilter = ReporterIonQSpectrumFilter.newBuilder()
                                .mergeFrom(reporterIonQFilterSummarization.getSpectrumFilter())
                                .setMinPValue(reporterIonQFilterSummarization.getSpectrumFilter().getFilterTypeCase() == ReporterIonQSpectrumFilter.FilterTypeCase.MINPVALUE
                                    ? reporterIonQFilterSummarization.getSpectrumFilter().getMinPValue()
                                    : DecoyInfoWrapper.fdrToPValue(psmDecoyInfo, reporterIonQFilterSummarization.getSpectrumFilter().getMaxFDR())
                                )
                                .build();

                            ReporterIonQBatchCalculationParameters batchParameters = analysis.findStep(StepType.REPORTER_ION_Q_BATCH_CALCULATION).getReporterIonQBatchCalculationParameters();
                            List<ReporterIonQExperimentSettings.ExperimentAlias> referenceChannels = reporterIonQFilterSummarization.getExperimentSettings().getReferenceChannelsList();
                            RiqPeptideFilter riqPeptideFilter = RiqPeptideFilter.fromDto(dbFilterList.getIdFilters(0).getPeptideFilter());
                            List<Integer> selectedChannels = ReporterIonQMethodQuery.selectedChannelIndices(reporterIonQFilterSummarization.getExperimentSettings());
                            Query immutableQuery = ReporterIonQMethodQuery.fromDto(
                                spectrumFilter,
                                referenceChannels,
                                batchParameters.getParameters().getMethod(),
                                sampleFractions,
                                filterList.getReporterIonIonQProteinFilter(),
                                riqPeptideFilter,
                                selectedChannels,
                                reporterIonQFilterSummarization.getExperimentSettings().getAliasesList()
                            );
                            currentBuilder.addQueryJson(writeValueAsString(immutableQuery));
                            return currentBuilder;
                        });
                    break;
                case DIA_DB_SEARCH:{
                    SlFilter diaDbFilter = filterList.getDiaDbFilter();
                    future = future.thenApply(currentbuilder -> currentbuilder
                        .addQueryJson(
                            writeValueAsString(SlQuery.fromProtoNoFdrConversion(diaDbFilter))
                        ));
                    break;
                }
                case SPECTRAL_LIBRARY: {
                    final SlFilter slFilter = filterList.getSlFilter();
                    future = future.thenApply(currentbuilder -> currentbuilder
                        .addQueryJson(
                            writeValueAsString(SlQuery.fromProtoNoFdrConversion(slFilter))
                        ));
                    break;
                }
                case SILAC: {
                    SilacFilterSummarizationParameters filterSummarizationParamaters = analysis.findStep(StepType.SILAC_FILTER_SUMMARIZE).getSilacFilterSummarizationParamaters();
                    SilacParameters silacParameters = filterSummarizationParamaters.getSilacParameters();
                    int referenceCondition = silacParameters.getReferenceCondition();
                    ImmutableMap.Builder<UUID, Integer> fractionReferenceLabelIndexBuilder = ImmutableMap.builder();
                    for (SilacParameters.Group group : silacParameters.getGroupsList()) {
                        for (SilacParameters.Sample sample : group.getSamplesList()) {
                            Integer label = sample.getLabelToConditionList().indexOf(referenceCondition);
// TODO SampleId not populated by frontend/parser.
//                            for (UUID fractionId : sampleFractions.sampleById(UUID.fromString(sample.getSampleId())).fractionIds()) {
//                                fractionReferenceLabelIndexBuilder.put(fractionId, label);
//                            }
                        }
                    }
                    final SilacQuery silacQuery = SilacQuery.fromDto(
                        filterSummarizationParamaters.getSilacFilter().getFeatureVectorFilter(),
                        filterList.getSilacProteinFilter(),
                        fractionReferenceLabelIndexBuilder.build()
                    );
                    future = future.thenApply(currentbuilder -> currentbuilder.addQueryJson(writeValueAsString(silacQuery)));
                    break;
                }
                default:
                    throw new IllegalStateException("Invalid request for filter of workflow type " + workFlowType.name());
            }
        }
        return future;
    }

    private static String writeValueAsString(Object t) throws IllegalStateException {
        try {
            return OM.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public  CompletionStage<Pair<DecoyInfo, DecoyInfo>> psmPeptideDecoyInfoFuture(AnalysisTaskInformation taskInformation, WorkFlowType workFlowType) {
        final UUID searchSummariationTaskId;
        switch (workFlowType) {
            case DB_SEARCH:
                searchSummariationTaskId = taskInformation.dbSearchSummarizeTaskId().orElseThrow(IllegalStateException::new);
                break;
            case PTM_FINDER:
                searchSummariationTaskId = taskInformation.ptmSearchSummarizeTaskId().orElseThrow(IllegalStateException::new);
                break;
            case SPIDER:
                searchSummariationTaskId = taskInformation.spiderSearchSummarizeTaskId().orElseThrow(IllegalStateException::new);
                break;
            default:
                throw new IllegalStateException("Invalid workflow type cannot load decoy info wrapper " + workFlowType.name());
        }

        CompletionStage<DecoyInfo> psmFuture = config.applicationStorageAsync()
            .thenCompose(applicationStorage -> applicationStorage.getTaskOutputRepository()
                .load(searchSummariationTaskId, com.bsi.peaks.model.proto.DecoyInfo::parseFrom)
            )
            .thenApply(optional -> optional.orElseThrow(NoSuchElementException::new))
            .thenApply(DecoyInfoWrapper::decoyInfoFromProto);

        CompletionStage<DecoyInfo> peptideFuture = config.applicationStorageAsync()
            .thenCompose(applicationStorage -> applicationStorage.getPeptideDecoyInfoRepository()
                .load(searchSummariationTaskId)
            )
            .thenApply(optional -> optional.orElseThrow(NoSuchElementException::new));

        return psmFuture.thenCombine(peptideFuture, Pair::create);
    }

    public CompletionStage<ProteinDecoyInfo> proteinDecoyInfoFuture(AnalysisTaskInformation taskInformation, WorkFlowType workFlowType) {
        final UUID filterSummarizationTaskId;
        switch (workFlowType) {
            case DB_SEARCH:
                filterSummarizationTaskId = taskInformation.dbFilterSummarizationTaskId().orElseThrow(IllegalStateException::new);
                break;
            case PTM_FINDER:
                filterSummarizationTaskId = taskInformation.ptmFilterSummarizationTaskId().orElseThrow(IllegalStateException::new);
                break;
            case SPIDER:
                filterSummarizationTaskId = taskInformation.spiderFilterSummarizationTaskId().orElseThrow(IllegalStateException::new);
                break;
            case SPECTRAL_LIBRARY:
                filterSummarizationTaskId = taskInformation.slFilterSummarizationTaskId().orElseThrow(IllegalStateException::new);
                break;
            default:
                throw new IllegalStateException("Invalid workflow type cannot load decoy info wrapper " + workFlowType.name());
        }

        return config.applicationStorageAsync()
            .thenCompose(applicationStorage -> applicationStorage.getProteinDecoyInfoRepository().loadArchive(filterSummarizationTaskId))
            .thenApply(optional -> optional.orElseThrow(NoSuchElementException::new))
            .thenApply(DecoyInfoWrapper::proteinDecoyInfoFromProto);
    }

    public CompletionStage<ProteinDecoyInfo> proteinDecoyInfoFuture(AnalysisTaskInformation taskInformation, StepType stepType) {
        final UUID filterSummarizationTaskId;

        switch (stepType) {
            case DIA_DB_FILTER_SUMMARIZE:
                filterSummarizationTaskId = taskInformation.diaDbFilterSummarizationTaskId().orElseThrow(IllegalStateException::new);
                break;
            case SL_FILTER_SUMMARIZATION:
                filterSummarizationTaskId = taskInformation.slFilterSummarizationTaskId().orElseThrow(IllegalStateException::new);
                break;
            default:
                throw new IllegalStateException("Cannot set SL Filter unexpected stepType: " + stepType);
        }

        return config.applicationStorageAsync()
            .thenCompose(applicationStorage -> applicationStorage.getProteinDecoyInfoRepository().loadArchive(filterSummarizationTaskId))
            .thenApply(optional -> optional.orElseThrow(NoSuchElementException::new))
            .thenApply(DecoyInfoWrapper::proteinDecoyInfoFromProto);
    }


    private CompletionStage<ProjectAggregateQueryResponse.AnalysisQueryResponse.AnalysisParametersDto.Builder> queryAnalysisParametersDto(
        ProjectAggregateState data,
        ProjectAnalysisState analysis
    ) throws IOException {
        final PMap<UUID, ProjectSampleState> sampleIdToSample = data.sampleIdToSample();
        AnalysisParameters.Builder analysisParameterBuilder = analysisParametersDto(analysis, sampleIdToSample);

        analysis.samples().forEach(sampleFractionIds -> {
            ProjectSampleState sample = sampleIdToSample.get(sampleFractionIds.sampleId());
            analysisParameterBuilder.addSampleFractionNames(AnalysisParameters.SampleFrationName.newBuilder()
                .setSampleName(sample.name())
                .addAllFractionSourceFile(sample.fractionState().values().stream().map(f -> f.srcFile())::iterator)
            );
        });

        return CompletableFuture.completedFuture(ProjectAggregateQueryResponse.AnalysisQueryResponse.AnalysisParametersDto.newBuilder()
            .setAnalysisParameters(analysisParameterBuilder));
    }

    private CompletionStage<ProjectAggregateQueryResponse.AnalysisQueryResponse.AnalysisDto.Builder> queryAnalysisDto(ProjectAggregateState data, ProjectAnalysisState analysis, ProjectAggregateQueryRequest.AnalysisQueryRequest.AnalysisDto analysisDto) throws IOException {
        final PMap<UUID, ProjectSampleState> sampleIdToSample = data.sampleIdToSample();
        final CompletionStage<Analysis.Builder> analysisDtoFuture = analysisDto(data, analysis);
        return Source.from(analysis.samples())
            .mapAsync(config.parallelism(), sampleFractions -> sampleQuery.sampleDto(sampleIdToSample.get(sampleFractions.sampleId())))
            .runFold(
                ProjectAggregateQueryResponse.AnalysisQueryResponse.AnalysisDto.newBuilder(),
                ProjectAggregateQueryResponse.AnalysisQueryResponse.AnalysisDto.Builder::addSample,
                config.materializer()
            )
            .thenCombine(analysisDtoFuture, (analysisBuilder, anaylsisDtoBuilder) -> analysisBuilder.setAnalysis(anaylsisDtoBuilder));
    }

    public boolean isAnalysisWorkflowTypeStepDone(ProjectAggregateState data, UUID analysisId, WorkFlowType workflowType) {
        final AnalysisTaskInformation analysisTaskInformation = AnalysisTaskInformation.create(config.keyspace(), data, projectId, analysisId);
        final ProjectAnalysisState analysis = analysisTaskInformation.analysis();
        final int numberOfSamples = analysis.samples().size();

        final PMap<StepType, PSet<UUID>> stepTaskPlanId = analysis.stepTaskPlanId();

        if (!Iterables.any(stepTaskPlanId.keySet(), x -> ModelConversion.workflowType(x) == workflowType)) {
            return false;
        }

        // Reverse order is more efficient since last task is likely last to complete.
        return Lists.reverse(analysis.steps()).stream()
            .map(WorkflowStep::getStepType)
            .filter(step -> ModelConversion.workflowType(step) == workflowType)
            .allMatch(step -> {
                final PSet<UUID> taskPlanIds = stepTaskPlanId.get(step);
                if (taskPlanIds == null) return false;
                switch (ModelConversion.taskLevel(step)) {
                    case FRACTIONLEVELTASK:
                        if (taskPlanIds.size() < analysis.calculateFractionLevelTaskCount(analysisTaskInformation, step)) return false;
                        break;
                    case SAMPLELEVELTASK:
                        if (taskPlanIds.size() < numberOfSamples) return false;
                        break;
                    case INDEXEDFRACTIONLEVELTASK:
                        if (taskPlanIds.size() < analysis.calculateFractionIndexLevelTaskCount(analysisTaskInformation, step)) return false;
                        break;
                    case ANALYSISLEVELTASK:
                    case ANALYSISDATAINDEPENDENTLEVELTASK:
                        if (taskPlanIds.isEmpty()) return false;
                        break;
                }
                return Iterables.all(taskPlanIds, taskPlanId -> data.taskState(taskPlanId)
                    .map(ProjectTaskState::progress)
                    .orElse(com.bsi.peaks.model.system.Progress.PENDING) == com.bsi.peaks.model.system.Progress.DONE
                );
            });
    }

}
