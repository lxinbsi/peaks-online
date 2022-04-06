package com.bsi.peaks.server.es.state;

import akka.japi.Function;
import com.bsi.peaks.analysis.parameterModels.dto.AnalysisAcquisitionMethod;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.projectAggregate.AnalysisInformation;
import com.bsi.peaks.event.projectAggregate.Fraction;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.AnalysisEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.ProjectEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.ProjectEvent.ArchiveState.ArchiveStateEnum;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.SampleEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.TaskDone.TaskOutputCase;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState.AnalysisState;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState.ProjectStatus;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState.SampleState;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState.TaskPlanState;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState.TaskState;
import com.bsi.peaks.event.projectAggregate.ProjectInformation;
import com.bsi.peaks.event.projectAggregate.ProjectPermission;
import com.bsi.peaks.event.projectAggregate.SampleActivationMethod;
import com.bsi.peaks.event.projectAggregate.SampleEnzyme;
import com.bsi.peaks.event.projectAggregate.SampleInformation;
import com.bsi.peaks.event.projectAggregate.SampleParameters;
import com.bsi.peaks.event.projectAggregate.WorkflowStepFilter;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.ActivationMethod;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.proto.Enzyme;
import com.bsi.peaks.model.proto.SimplifiedFastaSequenceDatabaseInfo;
import com.bsi.peaks.model.system.OrderedSampleFractionsBuilder;
import com.bsi.peaks.model.system.Progress;
import com.bsi.peaks.model.system.levels.LevelTask;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFactory;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFractionLevelTask;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.es.state.ProjectTaskState.DataState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.immutables.value.Value;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;
import static com.bsi.peaks.model.ModelConversion.uuidToByteString;
import static com.bsi.peaks.model.ModelConversion.uuidsFrom;

@PeaksImmutable
@JsonDeserialize(builder = ProjectAggregateStateBuilder.class)
public interface ProjectAggregateState {

    //project information
    default UUID ownerId() {
        return UUID.randomUUID();
    }

    default boolean shared() {
        return false;
    }

    default String name() {
        return "";
    }

    default String description() {
        return "";
    }

    default Instant createdTimeStamp() {
        return Instant.now();
    }

    default Instant lastUpdatedTimeStamp() {
        return Instant.now();
    }

    default Instant archivedTimeStamp() {
        return Instant.MIN;
    }

    default Status status() {
        return Status.PENDING;
    }

    default PMap<UUID, ProjectAnalysisState> analysisIdToAnalysis() {
        return HashTreePMap.empty();
    }

    default PMap<UUID, ProjectSampleState> sampleIdToSample() {
        return HashTreePMap.empty();
    }

    default PMap<UUID, ProjectTaskPlanState> taskPlanIdToTaskPlan() {
        return HashTreePMap.empty();
    }

    default PMap<UUID, ProjectTaskState> taskIdToTask() {
        return HashTreePMap.empty();
    }

    default int numberOfRetriesPerTask() {
        return 1;
    }

    default ArchiveStateEnum archiveState() {
        return ArchiveStateEnum.NOT_ARCHIVED;
    }

    Optional<String> storageKeyspace(); // For backward comparability, absense of keyspace will indicate we run on system keyspace.

    default PMap<UUID, Integer> daemonFractionIdToTimeout() { return HashTreePMap.empty(); }

    default void proto(com.bsi.peaks.event.projectAggregate.ProjectAggregateState.Builder builder) {
        builder.setVersion(SystemConfig.version);
        builder.setOwnerId(uuidToByteString(ownerId()));
        builder.setShared(shared());
        builder.setName(name());
        builder.setDescription(description());
        builder.setCreatedTimestamp(CommonFactory.convert(createdTimeStamp()));
        builder.setLastUpdatedTimeStamp(CommonFactory.convert(lastUpdatedTimeStamp()));
        builder.setArchivedTimeStamp(CommonFactory.convert(archivedTimeStamp()));
        switch (status()) {
            case PENDING:
                builder.setStatus(ProjectStatus.PENDING);
                break;
            case CREATED:
                builder.setStatus(ProjectStatus.CREATED);
                break;
            case DELETING:
                builder.setStatus(ProjectStatus.DELETING);
                break;
            case DELETED:
                builder.setStatus(ProjectStatus.DELETED);
                break;
            default:
                throw new IllegalArgumentException("Unexpected project status " + status());
        }
        builder.setNumberOfRetriesPerTask(numberOfRetriesPerTask());
        builder.setArchiveState(archiveState());
        analysisIdToAnalysis().forEach((analysisId, analysisState) -> analysisState.proto(builder.addAnalysisStatesBuilder()));
        sampleIdToSample().forEach((sampleId, sampleState) -> sampleState.proto(builder.addSampleStatesBuilder()));
        taskPlanIdToTaskPlan().forEach((taskPlanId, taskPlanState) -> taskPlanState.proto(builder.addTaskPlanStatesBuilder()));
        taskIdToTask().forEach((taskId, taskState) -> taskState.proto(builder.addTaskStatesBuilder()));
        builder.setInstrumentDaemonFractionIds(ModelConversion.uuidsToByteString(daemonFractionIdToTimeout().keySet()));
        builder.addAllDaemonSampleTimeouts(daemonFractionIdToTimeout().values());
        storageKeyspace().ifPresent(builder::setStorageKeyspace);
    }

    static ProjectAggregateState from(com.bsi.peaks.event.projectAggregate.ProjectAggregateState proto) {
        final MemoryOptimizedFactory memoryOptimizedFactory = new MemoryOptimizedFactory();
        final ProjectAggregateStateBuilder builder = new ProjectAggregateStateBuilder()
            .memoryOptimizedFactory(memoryOptimizedFactory)
            .createdTimeStamp(CommonFactory.convert(proto.getCreatedTimestamp()))
            .lastUpdatedTimeStamp(CommonFactory.convert(proto.getLastUpdatedTimeStamp()))
            .archivedTimeStamp(CommonFactory.convert(proto.getArchivedTimeStamp()))
            .name(proto.getName())
            .description(proto.getDescription())
            .ownerId(ModelConversion.uuidFrom(proto.getOwnerId()))
            .shared(proto.getShared())
            .numberOfRetriesPerTask(proto.getNumberOfRetriesPerTask())
            .archiveState(proto.getArchiveState());

        final String storageKeyspace = proto.getStorageKeyspace();
        if (!Strings.isNullOrEmpty(storageKeyspace)) {
            builder.storageKeyspace(storageKeyspace);
        }

        switch (proto.getStatus()) {
            case PENDING:
                builder.status(Status.PENDING);
                break;
            case CREATED:
                builder.status(Status.CREATED);
                break;
            case DELETING:
                builder.status(Status.DELETING);
                break;
            case DELETED:
                builder.status(Status.DELETED);
                break;
            default:
                throw new IllegalArgumentException("Unexpected project status " + proto.getStatus());
        }

        PMap<UUID, ProjectAnalysisState> analysisIdToAnalysis = HashTreePMap.empty();
        for (AnalysisState analysisState : proto.getAnalysisStatesList()) {
            final ProjectAnalysisState projectAnalysisState = ProjectAnalysisState.from(analysisState, memoryOptimizedFactory);
            analysisIdToAnalysis = analysisIdToAnalysis.plus(projectAnalysisState.analysisId(), projectAnalysisState);
        }

        PMap<UUID, ProjectSampleState> sampleIdToSample = HashTreePMap.empty();
        for (SampleState sampleState : proto.getSampleStatesList()) {
            final ProjectSampleState projectSampleState = ProjectSampleState.from(sampleState, memoryOptimizedFactory);
            sampleIdToSample = sampleIdToSample.plus(projectSampleState.sampleId(), projectSampleState);
        }

        PMap<UUID, ProjectTaskPlanState> taskPlanIdToTaskPlan = HashTreePMap.empty();
        for (TaskPlanState taskPlanState : proto.getTaskPlanStatesList()) {
            final ProjectTaskPlanState projectTaskPlanState = ProjectTaskPlanState.from(taskPlanState, memoryOptimizedFactory);
            taskPlanIdToTaskPlan = taskPlanIdToTaskPlan.plus(projectTaskPlanState.taskPlanId(), projectTaskPlanState);
        }

        PMap<UUID, ProjectTaskState> taskIdToTask = HashTreePMap.empty();
        for (TaskState taskState : proto.getTaskStatesList()) {
            final ProjectTaskState projectTaskState = ProjectTaskState.from(taskState, memoryOptimizedFactory);
            taskIdToTask = taskIdToTask.plus(projectTaskState.taskId(), projectTaskState);
        }

        PMap<UUID, Integer> daemonFractionIdToTimeout = HashTreePMap.empty();
        List<UUID> fractionIds = Lists.newArrayList(uuidsFrom(proto.getInstrumentDaemonFractionIds()));
        List<Integer> daemonSampleTimeoutsList = proto.getDaemonSampleTimeoutsList();
        for (int i = 0; i < fractionIds.size(); i++) {
            daemonFractionIdToTimeout = daemonFractionIdToTimeout.plus(fractionIds.get(i), daemonSampleTimeoutsList.get(i));
        }

        return builder
            .analysisIdToAnalysis(analysisIdToAnalysis)
            .sampleIdToSample(sampleIdToSample)
            .taskPlanIdToTaskPlan(taskPlanIdToTaskPlan)
            .taskIdToTask(taskIdToTask)
            .daemonFractionIdToTimeout(daemonFractionIdToTimeout)
            .build();
    }

    @JsonIgnore
    @Value.Lazy
    default List<ProjectSampleState> orderSamples() {
        return this.sampleIdToSample().values().stream()
            .sorted(Comparator.comparing(ProjectSampleState::createdTimeStamp))
            .collect(Collectors.toList());
    }

    @JsonIgnore
    @Value.Auxiliary
    default MemoryOptimizedFactory memoryOptimizedFactory() {
        return new MemoryOptimizedFactory();
    }

    default Optional<UUID> taskPlanIdFromTaskId(UUID taskId) {
        return taskPlanIdToTaskPlan().entrySet().stream()
            .filter(entry -> entry.getValue().taskIds().contains(taskId))
            .map(Map.Entry::getKey)
            .findFirst();
    }

    default ProjectAggregateState plusSampleEvent(UUID sampleId, SampleEvent.SampleCreated created) {
        if (sampleIdToSample().get(sampleId) != null) {
            throw new IllegalStateException("Sample with id " + sampleId + " already exists");
        }

        ProjectSampleState sample = new ProjectSampleStateBuilder()
            .sampleId(sampleId)
            .build();

        return withSampleIdToSample(sampleIdToSample().plus(sampleId, sample));
    }

    ProjectAggregateState withSampleIdToSample(PMap<UUID, ProjectSampleState> value);

    ProjectAggregateState withAnalysisIdToAnalysis(PMap<UUID, ProjectAnalysisState> value);

    ProjectAggregateState withTaskPlanIdToTaskPlan(PMap<UUID, ProjectTaskPlanState> value);

    ProjectAggregateState withTaskIdToTask(PMap<UUID, ProjectTaskState> value);

    ProjectAggregateState withDaemonFractionIdToTimeout(PMap<UUID, Integer> value);

    ProjectAggregateState withName(String name);

    ProjectAggregateState withDescription(String description);

    ProjectAggregateState withLastUpdatedTimeStamp(Instant value);

    ProjectAggregateState withArchivedTimeStamp(Instant value);

    ProjectAggregateState withOwnerId(UUID ownerId);

    ProjectAggregateState withShared(boolean shared);

    ProjectAggregateState withStatus(Status status);

    ProjectAggregateState withArchiveState(ArchiveStateEnum archiveState);

    default ProjectAggregateState plusAnalysis(UUID analysisId, Instant timestamp, Function<ProjectAnalysisState, ProjectAnalysisState> transform) throws Exception {
        final PMap<UUID, ProjectAnalysisState> analysisIdToAnalysis = analysisIdToAnalysis();
        final ProjectAnalysisState oldAnalysisState = analysisIdToAnalysis.get(analysisId);
        final ProjectAnalysisState newAnalysisState = transform.apply(oldAnalysisState);
        if (newAnalysisState == oldAnalysisState) return this;
        return withAnalysisIdToAnalysis(analysisIdToAnalysis.plus(analysisId, newAnalysisState.withLastUpdatedTimeStamp(timestamp)));
    }

    default ProjectAggregateState minusAnalysis(UUID analysisId) {
        return withAnalysisIdToAnalysis(analysisIdToAnalysis().minus(analysisId));
    }

    default ProjectAggregateState plusSample(UUID sampleId, Instant timestamp, Function<ProjectSampleState, ProjectSampleState> transform) throws Exception {
        final PMap<UUID, ProjectSampleState> sampleIdToSample = sampleIdToSample();
        final ProjectSampleState oldSample = sampleIdToSample.get(sampleId);
        final ProjectSampleState newSample = transform.apply(oldSample);
        if (oldSample == newSample) return this;
        return withSampleIdToSample(sampleIdToSample.plus(sampleId, newSample.withLastUpdatedTimeStamp(timestamp)));
    }

    default ProjectAggregateState minusSample(UUID sampleId) {
        return withSampleIdToSample(sampleIdToSample().minus(sampleId));
    }

    default ProjectAggregateState plusTaskPlan(UUID taskPlanId, Function<ProjectTaskPlanState, ProjectTaskPlanState> transform) throws Exception {
        final PMap<UUID, ProjectTaskPlanState> taskPlanStateMap = taskPlanIdToTaskPlan();
        final ProjectTaskPlanState taskPlanState = taskPlanStateMap.get(taskPlanId);
        return withTaskPlanIdToTaskPlan(taskPlanStateMap.plus(taskPlanId, transform.apply(taskPlanState)));
    }

    default ProjectAggregateState plusTask(UUID taskId, Function<ProjectTaskState, ProjectTaskState> transform) throws Exception {
        final PMap<UUID, ProjectTaskState> taskStateMap = taskIdToTask();
        final ProjectTaskState taskState = taskStateMap.get(taskId);
        return withTaskIdToTask(taskStateMap.plus(taskId, transform.apply(taskState)));
    }

    default ProjectAggregateState plusSampleFraction(UUID sampleId, UUID fractionId, Instant timestamp, Function<ProjectFractionState, ProjectFractionState> transform) throws Exception {
        return plusSample(sampleId, timestamp, sampleState -> {
            if (sampleState == null) {
                throw new IllegalArgumentException("SampleId " + sampleId + " does not exist");
            }
            return sampleState.plusFraction(fractionId, transform);
        });
    }

    default ProjectAggregateState nextState(ProjectAggregateEvent event) throws Exception {
        final Instant timestamp = CommonFactory.convert(event.getTimeStamp());
        switch (event.getEventCase()) {
            case PROJECT:
                return nextState(timestamp, event.getProject())
                    .withLastUpdatedTimeStamp(timestamp);
            case ANALYSIS:
                return nextState(timestamp, event.getAnalysis())
                    .withLastUpdatedTimeStamp(timestamp)
                    .withArchiveState(ArchiveStateEnum.NOT_ARCHIVED);
            case SAMPLE:
                return nextState(timestamp, event.getSample())
                    .withLastUpdatedTimeStamp(timestamp)
                    .withArchiveState(ArchiveStateEnum.NOT_ARCHIVED);
            case TASK:
                return nextState(timestamp, event.getTask())
                    .withLastUpdatedTimeStamp(timestamp);
            default:
                throw new IllegalArgumentException("Unexpected case " + event.getEventCase());
        }
    }

    default ProjectAggregateState nextState(Instant timestamp, ProjectEvent projectEvent) throws Exception {
        switch (projectEvent.getEventCase()) {
            case PROJECTCREATED:
                ProjectAggregateStateBuilder builder = new ProjectAggregateStateBuilder().from(this);
                ProjectEvent.ProjectCreated projectCreated = projectEvent.getProjectCreated();
                String storageKeyspace = projectCreated.getStorageKeyspace();
                if (!Strings.isNullOrEmpty(storageKeyspace)) {
                    builder.storageKeyspace(storageKeyspace);
                }
                return builder
                    .status(Status.CREATED)
                    .createdTimeStamp(timestamp)
                    .build();
            case PROJECTDELETING:
                return withStatus(Status.DELETING);
            case PROJECTDELETED:
                return withStatus(Status.DELETED);
            case PROJECTINFORMATION:
                final ProjectInformation projectInformation = projectEvent.getProjectInformation();
                return withName(projectInformation.getName())
                    .withDescription(projectInformation.getDescription())
                    .withArchiveState(ArchiveStateEnum.NOT_ARCHIVED);
            case PROJECTPERMISSION:
                final ProjectPermission projectPermission = projectEvent.getProjectPermission();
                final UUID ownerId = uuidFrom(projectPermission.getOwnerId());
                final boolean shared = projectPermission.getShared();
                return withOwnerId(ownerId).withShared(shared);
            case ARCHIVESTATE:
                ArchiveStateEnum archiveState = projectEvent.getArchiveState().getArchiveState();
                ProjectAggregateState data = this;
                switch (archiveState) {
                    case ARCHIVED:
                        data = data.withArchivedTimeStamp(timestamp);
                }
                return data.withArchiveState(archiveState);
            case DAEMONFRACTIONSBATCHCREATED:
                Map<UUID, Integer> newFractionIdToTimeout = new HashMap<>();
                ProjectEvent.DaemonFractionsBatchCreated daemonFractionsBatchCreated = projectEvent.getDaemonFractionsBatchCreated();
                int timeout = daemonFractionsBatchCreated.getTimeout();
                ModelConversion.uuidsFrom(daemonFractionsBatchCreated.getFractionIds())
                    .forEach(fractionId -> newFractionIdToTimeout.put(fractionId, timeout));
                return withDaemonFractionIdToTimeout(daemonFractionIdToTimeout().plusAll(newFractionIdToTimeout));
            default:
                throw new IllegalArgumentException("Unexpected case " + projectEvent.getEventCase());
        }
    }

    default ProjectAggregateState nextState(Instant timestamp, AnalysisEvent analysisEvent) throws Exception {
        final UUID analysisId = uuidFrom(analysisEvent.getAnalysisId());
        switch (analysisEvent.getEventCase()) {
            case ANALYSISCREATED: {
                return plusAnalysis(analysisId, timestamp, oldAnalaysis -> {
                    final AnalysisEvent.AnalysisCreated analysisCreated = analysisEvent.getAnalysisCreated();

                    List<SimplifiedFastaSequenceDatabaseInfo> databasesList = analysisCreated.getDatabasesList();
                    if (databasesList.isEmpty()) {
                        // For backward compatibility before databaseList was added to analysis.
                        databasesList = BackwardCompatibility.getSimplifiedFastaSequenceDatabaseInfos(analysisCreated.getStepsList());
                    }

                    final ProjectAnalysisStateBuilder builder = ProjectAnalysisState.builder(analysisId, oldAnalaysis)
                        .createdTimeStamp(timestamp)
                        .status(ProjectAnalysisState.Status.CREATED)
                        .addAllSteps(analysisCreated.getStepsList())
                        .addAllDatabases(databasesList);

                    final Map<UUID, ProjectSampleState> sampleIdToSample = sampleIdToSample();
                    for (UUID sampleId : uuidsFrom(analysisCreated.getSampleIds())) {
                        ProjectSampleState sample = sampleIdToSample.get(sampleId);
                        builder.addSamples(new OrderedSampleFractionsBuilder().sampleId(sampleId).fractionIds(sample.fractionIds()).build());
                    }
                    return builder.build();
                });
            }
            case ANALYSISDONE: {
                return plusAnalysis(analysisId, timestamp, analysis -> analysis.withStatus(ProjectAnalysisState.Status.DONE));
            }
            case ANALYSISINFORMATIONSET: {
                return plusAnalysis(analysisId, timestamp, oldAnalaysis -> {
                    final AnalysisInformation analysisInformationSet = analysisEvent.getAnalysisInformationSet();
                    return ProjectAnalysisState.builder(analysisId, oldAnalaysis)
                        .name(analysisInformationSet.getName())
                        .description(analysisInformationSet.getDescription())
                        .build();
                });
            }
            case TASKADDED: {
                final ProjectAggregateEvent.TaskAdded taskAdded = analysisEvent.getTaskAdded();
                final UUID taskPlanId = memoryOptimizedFactory().uuid(taskAdded.getTaskPlanId());
                final ProjectTaskPlanState taskPlanState = taskPlanIdToTaskPlan().get(taskPlanId);
                if (taskPlanState == null) {
                    throw new IllegalArgumentException("Attempt to add nonexistent taskPlanId " + taskPlanId);
                }
                final StepType stepType = taskPlanState.stepType();
                final ProjectAggregateState data = plusAnalysis(analysisId, timestamp, analysis -> analysis
                    .plusTaskPlan(stepType, taskPlanId)
                    .withStatus(ProjectAnalysisState.Status.PROCESSING)
                    .plusTargetContaminantDatabaseIds(taskPlanState.referencedTargetDatabaseIds(), taskPlanState.referencedContaminantDatabaseIds())
                );
                final Optional<ProjectTaskState> task = taskState(taskPlanState);
                if (task.isPresent() && task.get().progress() == Progress.DONE) {
                    return data;
                } else {
                    // Increase number of retries.
                    return data.plusTaskPlan(taskPlanId, taskPlan -> taskPlan.withProcessAttemptsRemaining(numberOfRetriesPerTask()));
                }
            }
            case PRIORITYSET: {
                final int priority = analysisEvent.getPrioritySet().getPriority();
                return plusAnalysis(analysisId, timestamp, oldAnalysis -> ProjectAnalysisState.builder(analysisId, oldAnalysis)
                    .priority(priority)
                    .build()
                );
            }
            case FILTERSET: {
                final WorkflowStepFilter filter = analysisEvent.getFilterSet();
                return plusAnalysis(analysisId, timestamp, analysis -> analysis
                    .plusWorkflowTypeFilter(filter.getWorkFlowType(), filter.getFilter())
                );
            }
            case ANALYSISFAILED: {
                final AnalysisEvent.AnalysisFailed analysisFailed = analysisEvent.getAnalysisFailed();
                switch (analysisFailed.getCause()) {
                    case CANCELLED:
                        return plusAnalysis(analysisId, timestamp, analysis -> analysis
                            .withStatus(ProjectAnalysisState.Status.CANCELLED)
                        );
                    default:
                        return plusAnalysis(analysisId, timestamp, analysis -> analysis
                            .withStatus(ProjectAnalysisState.Status.FAILED)
                        );
                }
            }
            case ANALYSISDELETED:
                return minusAnalysis(analysisId);
            default:
                throw new IllegalArgumentException("Unexpected case " + analysisEvent.getEventCase());
        }
    }

    default ProjectAggregateState nextState(Instant timestamp, SampleEvent sampleEvent) throws Exception {
        final UUID sampleId = memoryOptimizedFactory().uuid(sampleEvent.getSampleId());
        switch (sampleEvent.getEventCase()) {
            case UPLOADQUEUED: {
                final SampleEvent.UploadQueued uploadQueued = sampleEvent.getUploadQueued();
                final UUID fractionId = memoryOptimizedFactory().uuid(uploadQueued.getFractionId());

                // Match 13, 2020.
                // Old data does not have this. We can ignore, since we are not processing.
                if (uploadQueued.getUploadId().isEmpty()) {
                    return this;
                }

                final UUID uploadId = memoryOptimizedFactory().uuid(uploadQueued.getUploadId());
                return plusSample(sampleId, timestamp, sample -> {
                    sample = sample
                        .plusFraction(fractionId, fraction -> ProjectFractionState.builder(fraction)
                            .uploadState(UploadState.QUEUED)
                            .uploadId(uploadId)
                            .remotePathName(uploadQueued.getRemotePathName())
                            .subPath(uploadQueued.getSubPath())
                            .stepTaskPlanId(HashTreePMap.empty()) // Clear out previous taskPlans if they exist. They will no longer be valid.
                            .build()
                        );
                    if (sample.isCancelled()) {
                        // Queuing a new upload, will effectively undo cancel.
                        sample = sample.withIsCancelled(false);
                    }
                    if (sample.hasFailedTask() && !sampleHasFailedTask(sample)) {
                        // If queuing has cleared past failed Data Loading taskPlans, then we can clear the failed flag as well.
                        sample = sample.withHasFailedTask(false);
                    }
                    return sample;
                });
            }
            case UPLOADTIMEOUT: {
                final SampleEvent.UploadTimeout uploadTimeout = sampleEvent.getUploadTimeout();
                final UUID fractionId = memoryOptimizedFactory().uuid(uploadTimeout.getFractionId());
                return plusSampleFraction(sampleId, fractionId, timestamp, fraction -> fraction.withUploadState(UploadState.TIMEOUT));
            }
            case UPDATEDFRACTIONNAME: {
                SampleEvent.UpdatedFractionName updatedFractionName = sampleEvent.getUpdatedFractionName();
                final UUID fractionId = memoryOptimizedFactory().uuid(updatedFractionName.getFractionId());
                return plusSampleFraction(sampleId, fractionId, timestamp, fraction -> fraction.withSrcFile(updatedFractionName.getNewName()));
            }
            case SAMPLEENZYMEUPDATED: {
                final SampleEnzyme sampleEnzymeUpdated = sampleEvent.getSampleEnzymeUpdated();
                final Enzyme enzyme = sampleEnzymeUpdated.getEnzyme();
                if (sampleIdToSample().get(sampleId).status() == ProjectSampleState.Status.CREATED) {
                    return plusSample(sampleId, timestamp, oldSample -> ProjectSampleState.builder(sampleId, oldSample)
                        .enzyme(enzyme)
                        .build()
                    );
                } else {
                    return this;
                }
            }
            case SAMPLEACTIVATIONMETHODUPDATED: {
                final SampleActivationMethod sampleActivationMethodUpdated = sampleEvent.getSampleActivationMethodUpdated();
                final ActivationMethod activationMethod = sampleActivationMethodUpdated.getActivationMethod();
                if (sampleIdToSample().get(sampleId).status() == ProjectSampleState.Status.CREATED && activationMethod != ActivationMethod.UNDEFINED &&
                    activationMethod != ActivationMethod.UNRECOGNIZED) {
                    return plusSample(sampleId, timestamp, oldSample -> ProjectSampleState.builder(sampleId, oldSample)
                        .activationMethod(activationMethod)
                        .build()
                    );
                } else {
                    return this;
                }
            }
            case CREATED:
                return nextState(sampleId, timestamp, sampleEvent.getCreated());
            case SAMPLEINFORMATIONSET: {
                final SampleInformation sampleInformationSet = sampleEvent.getSampleInformationSet();
                return plusSample(sampleId, timestamp, oldSample -> ProjectSampleState.builder(sampleId, oldSample)
                    .name(sampleInformationSet.getName())
                    .build()
                );
            }
            case UPLOADSTARTED: {
                final SampleEvent.UploadStarted uploadStarted = sampleEvent.getUploadStarted();
                final UUID fractionId = memoryOptimizedFactory().uuid(uploadStarted.getFractionId());
                final UUID uploadId = uuidFrom(uploadStarted.getUploadId());
                return plusSampleFraction(sampleId, fractionId, timestamp, fraction -> ProjectFractionState.builder(fraction)
                    .uploadId(uploadId)
                    .uploadState(UploadState.STARTED)
                    .build()
                );
            }
            case UPLOADDONE: {
                final SampleEvent.UploadCompleted uploadDone = sampleEvent.getUploadDone();
                final UUID fractionId = memoryOptimizedFactory().uuid(uploadDone.getFractionId());
                return plusSampleFraction(sampleId, fractionId, timestamp, fraction -> fraction.withUploadState(UploadState.COMPLETED));
            }
            case UPLOADFAILED: {
                final SampleEvent.UploadFailed uploadFailed = sampleEvent.getUploadFailed();
                final UUID fractionId = memoryOptimizedFactory().uuid(uploadFailed.getFractionId());
                return plusSampleFraction(sampleId, fractionId, timestamp, fraction -> fraction.withUploadState(UploadState.FAILED));
            }
            case TASKADDED: {
                final ProjectAggregateEvent.TaskAdded taskAdded = sampleEvent.getTaskAdded();
                final UUID taskPlanId = memoryOptimizedFactory().uuid(taskAdded.getTaskPlanId());
                final ProjectTaskPlanState taskPlanState = taskPlanIdToTaskPlan().get(taskPlanId);
                if (taskPlanState == null) {
                    throw new IllegalArgumentException("Attempt to add nonexistent taskPlanId " + taskPlanId);
                }
                final StepType stepType = taskPlanState.stepType();
                final MemoryOptimizedFractionLevelTask fractionLevelTask = (MemoryOptimizedFractionLevelTask) taskPlanState.level();
                final UUID fractionId = fractionLevelTask.getFractionId();
                return plusSampleFraction(sampleId, fractionId, timestamp, fraction ->
                    fraction.plusStepTaskPlanId(stepType, taskPlanId)
                );

            }
            case PRIORITYSET: {
                final int priority = sampleEvent.getPrioritySet().getPriority();
                return plusSample(sampleId, timestamp, sample -> sample.withPriority(priority));
            }
            case CANCELLED:
                return plusSample(sampleId, timestamp, sample -> sample.withIsCancelled(true));
            case DELETED:
                return minusSample(sampleId);
            default:
                throw new IllegalArgumentException("Unexpected case " + sampleEvent.getEventCase());
        }
    }

    default ProjectAggregateState nextState(Instant timestamp, ProjectAggregateEvent.TaskEvent taskEvent) throws Exception {
        final UUID taskPlanId = memoryOptimizedFactory().uuid(taskEvent.getTaskPlanId());
        switch (taskEvent.getEventCase()) {
            case TASKCREATED: {
                final ProjectAggregateEvent.TaskCreated taskCreated = taskEvent.getTaskCreated();
                final TaskDescription taskDescription = taskCreated.getTaskDescription();
                final LevelTask level = memoryOptimizedFactory().internLevelTask(taskDescription);
                return plusTaskPlan(taskPlanId, taskPlan -> {
                    if (taskPlan != null) {
                        // Never reuse taskPlanIds for different TaskDescriptions. Simply create a new taskPlanId.
                        throw new IllegalStateException("Taskplan " + taskPlanId + " already exists while handling created event");
                    }
                    final ProjectTaskPlanStateBuilder builder = new ProjectTaskPlanStateBuilder()
                        .taskPlanId(taskPlanId)
                        .stepType(taskDescription.getStepType())
                        .level(level)
                        .processAttemptsRemaining(numberOfRetriesPerTask());
                    switch (taskDescription.getTaskCase()) {
                        case JSONPARAMATERS:
                            return builder
                                .jsonTaskParamaters(taskDescription.getJsonParamaters())
                                .build();
                        case PARAMETERS:
                            return builder
                                .taskParameters(taskDescription.getParameters())
                                .build();
                        default:
                            throw new IllegalArgumentException("TaskDescription must have task parameters");
                    }
                }).withArchiveState(ArchiveStateEnum.NOT_ARCHIVED);
            }
            case TASKQUEUED: {
                final ProjectAggregateEvent.TaskQueued taskQueued = taskEvent.getTaskQueued();
                final UUID taskId = uuidFrom(taskQueued.getTaskId());
                ProjectAggregateState data = this;
                data = data.plusTaskPlan(taskPlanId, taskPlan -> {
                    if (taskPlan == null) {
                        throw new IllegalStateException("Taskplan " + taskPlanId + " does not exist");
                    }
                    return taskPlan
                        .plusTaskIds(taskId)
                        .withProcessAttemptsRemaining(taskPlan.processAttemptsRemaining() - 1);
                });
                data = data.plusTask(taskId, task -> {
                    if (task != null) {
                        throw new IllegalStateException("Task " + taskId + " already exists while handling task queued event");
                    }
                    return new ProjectTaskStateBuilder()
                        .taskPlanId(taskPlanId)
                        .taskId(taskId)
                        .queuedTimestamp(timestamp)
                        .progress(Progress.QUEUED)
                        .priority(taskQueued.getPriority())
                        .build();
                });
                return data.withArchiveState(ArchiveStateEnum.NOT_ARCHIVED);
            }
            case TASKPROCESSING: {
                final ProjectAggregateEvent.TaskProcessing taskProcessing = taskEvent.getTaskProcessing();
                final UUID taskId = uuidFrom(taskProcessing.getTaskId());
                final UUID workerId = memoryOptimizedFactory().uuid(taskProcessing.getWorkerId());
                final String workerIPAddress = taskProcessing.getWorkerIPAddress();
                return plusTask(taskId, task -> {
                    if (task == null) {
                        throw new IllegalStateException("Task " + taskId + " does not exist");
                    }
                    if (task.progress() != Progress.QUEUED) {
                        throw new IllegalArgumentException("Task " + taskId + " is already " + task.progress());
                    }
                    return new ProjectTaskStateBuilder()
                        .from(task)
                        .progress(Progress.IN_PROGRESS)
                        .dataState(DataState.BUILDING)
                        .startTimestamp(timestamp)
                        .workerId(workerId)
                        .workerIpAddress(workerIPAddress)
                        .build();
                }).withArchiveState(ArchiveStateEnum.NOT_ARCHIVED);

            }
            case TASKDONE: {
                final ProjectAggregateEvent.TaskDone taskDone = taskEvent.getTaskDone();
                final UUID taskId = uuidFrom(taskDone.getTaskId());
                final PMap<UUID, ProjectTaskState> taskStateMap = taskIdToTask();
                final ProjectTaskState task = taskStateMap.get(taskId);
                if (task == null) {
                    throw new IllegalStateException("Task " + taskId + " does not exist");
                }
                switch (task.progress()) {
                    case QUEUED:
                    case IN_PROGRESS:
                    case CANCELLED:
                        // Done can only follow the above states.
                        break;
                    default:
                        throw new IllegalArgumentException("Task " + taskId + " is already " + task.progress());
                }
                final ProjectTaskStateBuilder builder = new ProjectTaskStateBuilder()
                    .from(task)
                    .progress(Progress.DONE)
                    .dataState(DataState.READY)
                    .completedTimestamp(timestamp);
                if (taskDone.getTaskOutputCase() != TaskOutputCase.TASKOUTPUT_NOT_SET) {
                    builder.taskDone(taskDone);
                }
                return withTaskIdToTask(taskStateMap.plus(taskId, builder.build()))
                    .updateAnalysisSampleWithTaskDone(taskId)
                    .withArchiveState(ArchiveStateEnum.NOT_ARCHIVED);
            }
            case TASKDELETING: {
                final ProjectAggregateEvent.TaskDeleting taskDeleting = taskEvent.getTaskDeleting();
                final UUID taskId = uuidFrom(taskDeleting.getTaskId());
                return plusTask(taskId, task -> {
                    if (task.dataState() == DataState.DELETED) {
                        throw new IllegalStateException("Task already deleted. Cannot revert to DELETING state.");
                    } else {
                        return task.withDataState(DataState.DELETING);
                    }
                });
            }
            case TASKCANCELLED: {
                final ProjectAggregateEvent.TaskCancelled taskCancelled = taskEvent.getTaskCancelled();
                final UUID taskId = uuidFrom(taskCancelled.getTaskId());
                return plusTask(taskId, task -> {
                    switch (task.progress()) {
                        case QUEUED:
                        case IN_PROGRESS:
                            return task.withProgress(Progress.CANCELLED);
                        default:
                            throw new IllegalArgumentException("Task " + taskId + " is already " + task.progress());
                    }
                });
            }
            case TASKFAILED: {
                final ProjectAggregateEvent.TaskFailed taskFailed = taskEvent.getTaskFailed();
                final UUID taskId = uuidFrom(taskFailed.getTaskId());
                return plusTask(taskId, task -> {
                    switch (task.progress()) {
                        case QUEUED:
                        case IN_PROGRESS:
                        case CANCELLED:
                            return task.withProgress(Progress.FAILED);
                        default:
                            throw new IllegalArgumentException("Task " + taskId + " is already " + task.progress());
                    }
                }).updateAnalysisSampleWithTaskFailed(taskId).withArchiveState(ArchiveStateEnum.NOT_ARCHIVED);
            }
            case TASKDELETED: {
                final ProjectAggregateEvent.TaskDeleted taskDeleted = taskEvent.getTaskDeleted();
                final UUID taskId = uuidFrom(taskDeleted.getTaskId());
                return plusTask(taskId, task -> task.withDataState(DataState.DELETED));
            }
            default:
                throw new IllegalArgumentException("Unexpected case " + taskEvent.getEventCase());
        }
    }

    default ProjectAggregateState nextState(UUID sampleId, Instant timestamp, SampleEvent.SampleCreated created) throws Exception {
        return plusSample(sampleId, timestamp, oldSample -> {
            final MemoryOptimizedFactory memoryOptimizedFactory = memoryOptimizedFactory();
            final SampleParameters sampleParameters = created.getSampleParameters();
            final ProjectSampleStateBuilder builder = ProjectSampleState.builder(sampleId, oldSample)
                .addAllSteps(created.getStepsList());

            PMap<UUID, ProjectFractionState> fractionStateMap = HashTreePMap.empty();
            for (Fraction fraction : created.getFractionsList()) {
                final UUID fractionId = memoryOptimizedFactory.uuid(fraction.getFractionId());
                builder.addFractionIds(fractionId);
                fractionStateMap = fractionStateMap.plus(
                    fractionId,
                    new ProjectFractionStateBuilder()
                        .fractionID(fractionId)
                        .srcFile(fraction.getSrcFile())
                        .lastUpdateTimeStamp(Instant.now())
                        .build()
                );
            }

            return builder
                .createdTimeStamp(timestamp)
                .activationMethod(sampleParameters.getActivationMethod())
                .acquisitionMethod(sampleParameters.getAcquisitionMethod())
                .enzyme(memoryOptimizedFactory.internOther(sampleParameters.getEnzyme()))
                .instrument(memoryOptimizedFactory.internOther(sampleParameters.getInstrument()))
                .fractionState(fractionStateMap)
                .build();
        });
    }

    default ProjectAggregateState updateAnalysisSampleWithTaskDone(UUID taskId) {
        final UUID taskPlanId = taskPlanIdFromTaskId(taskId).orElseThrow(IllegalArgumentException::new);
        final PMap<UUID, ProjectTaskPlanState> taskPlans = taskPlanIdToTaskPlan();
        final ProjectTaskPlanState taskPlan = taskPlans.get(taskPlanId);

        // Only update state if this is the latest taskId for taskPlan.
        if (!taskPlan.latestTaskId().orElseThrow(IllegalStateException::new).equals(taskId)) {
            return this;
        }

        ProjectAggregateState data = this;
        final LevelTask level = taskPlan.level();
        if (level instanceof MemoryOptimizedFractionLevelTask) {
            final MemoryOptimizedFractionLevelTask fractionLevelTask = (MemoryOptimizedFractionLevelTask) level;
            final UUID sampleId = fractionLevelTask.getSampleId();

            // Conditionally update sample state to DONE.
            final PMap<UUID, ProjectSampleState> sampleIdToSample = sampleIdToSample();
            final ProjectSampleState sample = sampleIdToSample.get(sampleId);
            if (sample.status() == ProjectSampleState.Status.PROCESSING) {
                if (isSampleDone(sample)) {
                    data = data.withSampleIdToSample(sampleIdToSample.plus(sampleId, sample.withAllTasksDone(true)));
                }
            }
        }
        // Save memory by removing parameters after task has completed.
        data = data.withTaskPlanIdToTaskPlan(taskPlans.plus(taskPlanId, taskPlan
            .withJsonTaskParamaters(null)
            .withTaskParameters(null)
        ));
        return data;
    }

    default ProjectAggregateState updateAnalysisSampleWithTaskFailed(UUID taskId) {
        final UUID taskPlanId = taskPlanIdFromTaskId(taskId).orElseThrow(IllegalArgumentException::new);
        final ProjectTaskPlanState taskPlan = taskPlanIdToTaskPlan().get(taskPlanId);

        // Only update state if this is the latest taskId for taskPlan.
        if (!taskPlan.latestTaskId().orElseThrow(IllegalStateException::new).equals(taskId)) {
            return this;
        }

        ProjectAggregateState data = this;
        final LevelTask level = taskPlan.level();
        if (level instanceof MemoryOptimizedFractionLevelTask) {
            final MemoryOptimizedFractionLevelTask fractionLevelTask = (MemoryOptimizedFractionLevelTask) level;
            final UUID sampleId = fractionLevelTask.getSampleId();
            final PMap<UUID, ProjectSampleState> sampleIdToSample = sampleIdToSample();
            final ProjectSampleState sample = sampleIdToSample.get(sampleId);
            // Sample may be deleted, so do a null check.
            if (sample != null && !sample.hasFailedTask()) {
                data = data.withSampleIdToSample(sampleIdToSample.plus(sampleId, sample.withHasFailedTask(true)));
            }
        }

        // Conditionally update analysis state to FAILED.
        final PMap<UUID, ProjectAnalysisState> analysisIdToAnalysis = data.analysisIdToAnalysis();
        for (ProjectAnalysisState analysis : analysisIdToAnalysis.values()) {
            // Skip analysis if not processing or does not contain task plan.
            if (analysis.status() != ProjectAnalysisState.Status.PROCESSING) continue;
            if (!analysis.stepTaskPlanId().getOrDefault(taskPlan.stepType(), HashTreePSet.empty()).contains(taskPlanId))
                continue;
            final UUID analysisId = analysis.analysisId();
            data = data.withAnalysisIdToAnalysis(analysisIdToAnalysis.plus(analysisId, analysis.withStatus(ProjectAnalysisState.Status.FAILED)));
        }
        return data;
    }

    default boolean isSampleDone(ProjectSampleState sample) {
        return Iterables.all(sample.fractionState().values(), this::isFractionDone);
    }

    default boolean isFractionDone(ProjectFractionState fraction) {
        if (fraction.uploadState() != UploadState.COMPLETED) {
            return false;
        }
        if (fraction.stepTaskPlanId().isEmpty()) {
            return false;
        }
        return Iterables.all(
            fraction.stepTaskPlanId().values(),
            taskPlanId -> taskState(taskPlanId)
                .map(ProjectTaskState::progress)
                .orElse(Progress.PENDING) == Progress.DONE
            );
    }

    default boolean sampleHasFailedTask(ProjectSampleState sample) {
        return Iterables.any(sample.fractionState().values(), this::fractionHasFailedTask);
    }

    default boolean fractionHasFailedTask(ProjectFractionState fraction) {
        return Iterables.any(
            fraction.stepTaskPlanId().values(),
            taskPlanId -> taskState(taskPlanId)
                .map(ProjectTaskState::progress)
                .orElse(Progress.PENDING) == Progress.FAILED
            );
    }

    default boolean isAnalysisDone(AnalysisTaskInformation taskInfo, ProjectAnalysisState analysis) {
        final int numberOfSamples = analysis.samples().size();

        final PMap<StepType, PSet<UUID>> stepTaskPlanId = analysis.stepTaskPlanId();

        // Reverse order is more efficient since last task is likely last to complete.
        return Lists.reverse(analysis.steps()).stream()
            .map(WorkflowStep::getStepType)
            .allMatch(step -> {
                final PSet<UUID> taskPlanIds = stepTaskPlanId.get(step);
                if (taskPlanIds == null) return false;
                switch (ModelConversion.taskLevel(step)) {
                    case FRACTIONLEVELTASK:
                        if (taskPlanIds.size() < analysis.calculateFractionLevelTaskCount(taskInfo, step)) return false;
                        break;
                    case SAMPLELEVELTASK:
                        if (taskPlanIds.size() < numberOfSamples) return false;
                        break;
                    case INDEXEDFRACTIONLEVELTASK:
                        if (taskPlanIds.size() < analysis.calculateFractionIndexLevelTaskCount(taskInfo, step)) return false;
                        break;
                    case ANALYSISLEVELTASK:
                    case ANALYSISDATAINDEPENDENTLEVELTASK:
                        if (taskPlanIds.isEmpty()) return false;
                        break;
                }
                return Iterables.all(taskPlanIds, taskPlanId -> taskState(taskPlanId)
                    .map(ProjectTaskState::progress)
                    .orElse(Progress.PENDING) == Progress.DONE
                );
            });
    }

    default int taskPlanPriority(UUID taskplanId) {
        return taskPlanPriority(taskPlanIdToTaskPlan().get(taskplanId));
    }

    default int taskPlanPriority(ProjectTaskPlanState taskPlan) {
        final UUID taskPlanId = taskPlan.taskPlanId();
        final StepType stepType = taskPlan.stepType();
        final OptionalInt maxSamplePriority = sampleIdToSample().values().stream()
            .filter(sample -> {
                final ProjectSampleState.Status status = sample.status();
                return status == ProjectSampleState.Status.CREATED
                    || status == ProjectSampleState.Status.PROCESSING;
            })
            .filter(sample -> Iterables.any(
                sample.fractionState().values(),
                fraction -> fraction.stepTaskPlanId().containsValue(taskPlanId)
            ))
            .mapToInt(ProjectSampleState::priority)
            .max();

        final OptionalInt maxAnalysisPriority = analysisIdToAnalysis().values().stream()
            .filter(analysis -> {
                final ProjectAnalysisState.Status status = analysis.status();
                return status == ProjectAnalysisState.Status.CREATED
                    || status == ProjectAnalysisState.Status.PROCESSING
                    || status == ProjectAnalysisState.Status.PENDING;
            })
            .filter(analysis -> {
                final Set<UUID> analysisTaskPlanIds = analysis.stepTaskPlanId().get(stepType);
                return analysisTaskPlanIds != null && analysisTaskPlanIds.contains(taskPlanId);
            })
            .mapToInt(ProjectAnalysisState::priority)
            .max();

        if (maxSamplePriority.isPresent()) {
            if (maxAnalysisPriority.isPresent()) {
                return Math.max(maxSamplePriority.getAsInt(), maxAnalysisPriority.getAsInt());
            } else {
                return maxSamplePriority.getAsInt();
            }
        } else {
            return maxAnalysisPriority.orElse(0);
        }
    }

    default boolean isTaskPlanReferencedBySample(ProjectTaskPlanState taskPlan) {
        final UUID taskPlanId = taskPlan.taskPlanId();
        return sampleIdToSample().values().stream()
            .anyMatch(sample -> Iterables.any(
                sample.fractionState().values(),
                fraction -> fraction.stepTaskPlanId().containsValue(taskPlanId)
            ));
    }

    default boolean isTaskPlanReferencedByAnalysis(ProjectTaskPlanState taskPlan) {
        final UUID taskPlanId = taskPlan.taskPlanId();
        final StepType stepType = taskPlan.stepType();
        return analysisIdToAnalysis().values().stream()
            .anyMatch(analysis -> {
                final Set<UUID> analysisTaskPlanIds = analysis.stepTaskPlanId().get(stepType);
                return analysisTaskPlanIds != null && analysisTaskPlanIds.contains(taskPlanId);
            });
    }

    @Value.Lazy
    default boolean allTasksAreDeleted() {
        return Iterables.all(taskIdToTask().values(), task -> task.dataState() == DataState.DELETED);
    }

    default Optional<ProjectTaskState> taskState(UUID taskPlanId) {
        return taskState(taskPlanIdToTaskPlan().get(taskPlanId));
    }

    default Optional<ProjectTaskState> taskState(ProjectTaskPlanState taskPlan) {
        return Optional.ofNullable(taskPlan)
            .flatMap(ProjectTaskPlanState::latestTaskId)
            .map(taskIdToTask()::get);
    }

    default String sampleName(UUID id) {
        return Optional.ofNullable(sampleIdToSample().get(id))
            .map(ProjectSampleState::name)
            .orElse("");
    }

    enum Status {
        PENDING,
        CREATED,
        DELETING,
        DELETED,
    }
}
