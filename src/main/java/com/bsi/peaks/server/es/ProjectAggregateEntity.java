package com.bsi.peaks.server.es;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ClusterShardingSettings;
import akka.cluster.sharding.ShardRegion;
import akka.dispatch.Futures;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.japi.function.Function2;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import akka.japi.tuple.Tuple3;
import akka.persistence.SnapshotOffer;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.bsi.peaks.algorithm.module.peaksdb.decoy.DecoyInfo;
import com.bsi.peaks.algorithm.module.peaksdb.decoy.ProteinDecoyInfo;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.TaskPlanLookupRepository;
import com.bsi.peaks.data.storage.application.cache.FractionCache;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.CommonTasksFactory;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.ProjectAggregateEventFactory;
import com.bsi.peaks.event.ProjectAggregateEventFactory.AnalysisEventFactory;
import com.bsi.peaks.event.ProjectAggregateEventFactory.SampleEventFactory;
import com.bsi.peaks.event.ProjectAggregateEventFactory.TaskEventFactory;
import com.bsi.peaks.event.WorkCommandFactory;
import com.bsi.peaks.event.project.ProjectEventToRestore;
import com.bsi.peaks.event.projectAggregate.AnalysisInformation;
import com.bsi.peaks.event.projectAggregate.Fraction;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateCommand;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateCommand.ArchiveNotification;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateCommand.TaskNotification;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateCommand.UploadNotification;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.AnalysisEvent.AnalysisFailedCause;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.ProjectEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.ProjectEvent.ArchiveState.ArchiveStateEnum;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse.QueryFailure;
import com.bsi.peaks.event.projectAggregate.RemoteFraction;
import com.bsi.peaks.event.projectAggregate.RemoteSample;
import com.bsi.peaks.event.projectAggregate.SampleInformation;
import com.bsi.peaks.event.projectAggregate.SampleParameters;
import com.bsi.peaks.event.projectAggregate.WorkflowStepFilter;
import com.bsi.peaks.event.tasks.JsonTaskParameters;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.TaskParameters;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.event.uploader.FileToUpload;
import com.bsi.peaks.event.uploader.UploadFiles;
import com.bsi.peaks.event.work.BatchSubmitTasks;
import com.bsi.peaks.event.work.CancelTask;
import com.bsi.peaks.event.work.SubmitTask;
import com.bsi.peaks.event.work.TaskPriority;
import com.bsi.peaks.event.work.TaskResourceOverride;
import com.bsi.peaks.event.work.WorkCommand;
import com.bsi.peaks.messages.service.SlFilterSummarizationResult;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.core.AcquisitionMethod;
import com.bsi.peaks.model.core.ActivationMethod;
import com.bsi.peaks.model.dto.FilePath;
import com.bsi.peaks.model.dto.FilterList;
import com.bsi.peaks.model.dto.IDFilter;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.dto.SlFilter;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.filter.DenovoCandidateFilter;
import com.bsi.peaks.model.filter.DenovoCandidateFilterBuilder;
import com.bsi.peaks.model.filter.ProteinHitFilter;
import com.bsi.peaks.model.filter.SlProteinHitFilter;
import com.bsi.peaks.model.proteomics.Enzyme;
import com.bsi.peaks.model.proteomics.Instrument;
import com.bsi.peaks.model.system.OrderedSampleFractions;
import com.bsi.peaks.model.system.Task;
import com.bsi.peaks.model.system.levels.LevelTask;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFactory;
import com.bsi.peaks.server.es.communication.UserManagerCommunication;
import com.bsi.peaks.server.es.project.ProjectAggregateQuery;
import com.bsi.peaks.server.es.project.ProjectAggregateQueryConfig;
import com.bsi.peaks.server.es.project.ProjectAggregateSampleQuery;
import com.bsi.peaks.server.es.state.AnalysisTaskInformation;
import com.bsi.peaks.server.es.state.ProjectAggregateState;
import com.bsi.peaks.server.es.state.ProjectAggregateStateBuilder;
import com.bsi.peaks.server.es.state.ProjectAnalysisState;
import com.bsi.peaks.server.es.state.ProjectFractionState;
import com.bsi.peaks.server.es.state.ProjectSampleState;
import com.bsi.peaks.server.es.state.ProjectTaskPlanState;
import com.bsi.peaks.server.es.state.ProjectTaskState;
import com.bsi.peaks.server.es.state.ProjectTaskState.DataState;
import com.bsi.peaks.server.es.state.UploadState;
import com.bsi.peaks.server.es.tasks.DataLoadingTask;
import com.bsi.peaks.server.es.tasks.DataLoadingTaskBuilder;
import com.bsi.peaks.server.es.tasks.Helper;
import com.bsi.peaks.server.parsers.QueryParser;
import com.bsi.peaks.server.service.CassandraMonitorService;
import com.bsi.peaks.server.service.UploaderService;
import com.bsi.peaks.service.common.DecoyInfoWrapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.pcollections.PCollectionsModule;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import com.typesafe.config.Config;
import org.jetbrains.annotations.NotNull;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;
import static com.bsi.peaks.model.ModelConversion.uuidsFrom;
import static com.bsi.peaks.server.es.WorkManager.RESOURCE_OVERRIDE_PATTERN;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ProjectAggregateEntity extends PeaksEntityCommandPersistentActor<ProjectAggregateState, ProjectAggregateEvent, ProjectAggregateCommand> {
    public static final ProjectMessageExtractor MESSAGE_EXTRACTOR = new ProjectMessageExtractor();
    public static final String CLUSTER_NAMESPACE = "ProjectAggregate";
    protected static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new Jdk8Module())
        .registerModule(new ProtobufModule())
        .registerModule(new JavaTimeModule())
        .registerModule(new PCollectionsModule());
    private static final UUID ADMIN_USERID = UUID.fromString(UserManager.ADMIN_USERID);
    private static final String RUN_WORK_CREATOR = "RUN_WORK_CREATOR";
    private final AnalysisWorkCreator workCreator;
    private final TaskDeleter taskDeleter;
    private final Function<ActorSystem, ActorRef> workManagerActor;
    private final UUID projectId;
    private final ApplicationStorageFactory storageFactory;
    private final ProjectAggregateCommandFactory commandFactory;
    private final SourceQueueWithComplete<Tuple3<StepType, UUID, LevelTask>> deleteQueue;
    private final UniqueKillSwitch killSwitch;
    private ProjectAggregateState lastCreateWorkData;
    private final Map<UUID, CompletableFuture<List<Pair<TaskDescription, UUID>>>> analysisFutureTaskPlanIds = new HashMap<>();
    private final UploaderService uploaderService;
    private final CassandraMonitorService cassandraMonitorService;

    private final ProjectAggregateQuery query;
    private final ProjectAggregateSampleQuery sampleQuery;

    public static final String PENDING_UPLOAD_TIMEOUT = "peaks.server.data-upload.pending-timeout";
    public static final String TASK_DELETE_PARALLELISM = "peaks.server.task-delete-parallelism";
    private final Duration pendingUploadTimeout;

    private static final String TASK_SUBMISSION_TICK = "TASK_SUBMISSION_TICK";
    private static final String TASK_SUBMISSION_INTERVAL = "peaks.server.task-submission-interval";
    private final Duration taskSubmissionInterval;
    private final Cancellable taskSubmissionRoutine;
    private final List<SubmitTask> batchedTasks = new ArrayList<>();

    protected ProjectAggregateEntity(
        int numberOfRetriesPerTask,
        AnalysisWorkCreator workCreator,
        TaskDeleter taskDeleter,
        Function<ActorSystem, ActorRef> workManagerActor,
        UserManagerCommunication userManagerCommunication,
        ApplicationStorageFactory storageFactory,
        FractionCache fractionCache,
        UploaderService uploaderService,
        CassandraMonitorService cassandraMonitorService
    ) {
        super(ProjectAggregateEvent.class, 15);
        this.workCreator = workCreator;
        this.taskDeleter = taskDeleter;
        this.workManagerActor = workManagerActor;
        this.storageFactory = storageFactory;
        this.uploaderService = uploaderService;
        this.cassandraMonitorService = cassandraMonitorService;
        this.data = new ProjectAggregateStateBuilder()
            .from(this.data)
            .numberOfRetriesPerTask(numberOfRetriesPerTask)
            .build();
        final int prefixLength = CLUSTER_NAMESPACE.length() + 1;
        projectId = UUID.fromString(getSelf().path().name().substring(prefixLength, prefixLength + UUID_LENGTH));
        ProjectAggregateQueryConfig queryConfig = new ProjectAggregateQueryConfig() {
            @Override
            public UUID projectId() {
                return ProjectAggregateEntity.this.projectId;
            }

            @Override
            public String keyspace() {
                return ProjectAggregateEntity.this.keyspace();
            }

            @Override
            public int parallelism() {
                return ProjectAggregateEntity.this.storageFactory.getParallelism();
            }

            @Override
            public Materializer materializer() {
                return ActorMaterializer.create(ProjectAggregateEntity.this.context());
            }

            @Override
            public ApplicationStorage applicationStorage() {
                return ProjectAggregateEntity.this.applicationStorage();
            }

            @Override
            public CompletionStage<? extends ApplicationStorage> applicationStorageAsync() {
                return ProjectAggregateEntity.this.applicationStorageAsync();
            }

            @Override
            public UserManagerCommunication userManagerCommunication() {
                return userManagerCommunication;
            }

            @Override
            public FractionCache fractionCache() {
                return fractionCache;
            }
        };

        this.query = new ProjectAggregateQuery(queryConfig, cassandraMonitorService);
        this.sampleQuery = new ProjectAggregateSampleQuery(queryConfig);

        commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);

        final Config config = context().system().settings().config();
        this.pendingUploadTimeout = config.hasPath(PENDING_UPLOAD_TIMEOUT)? config.getDuration(PENDING_UPLOAD_TIMEOUT) : Duration.ofSeconds(20);

        final int taskDeleteParallelism = config.hasPath(TASK_DELETE_PARALLELISM)? config.getInt(TASK_DELETE_PARALLELISM) : 3;

        final Set<UUID> inProgressDeleteTaskIds = Sets.newConcurrentHashSet();
        final Pair<SourceQueueWithComplete<Tuple3<StepType, UUID, LevelTask>>, UniqueKillSwitch> pair = Source.<Tuple3<StepType, UUID, LevelTask>>queue(Integer.MAX_VALUE, OverflowStrategy.backpressure())
            .filter(t -> {
                final UUID taskId = t.t2();
                if (inProgressDeleteTaskIds.contains(taskId)) {
                    log.warning("{}: Task {} delete already in progress. Skipping delete.", persistenceId(), taskId);
                    return false;
                }
                final ProjectTaskState projectTaskState = data.taskIdToTask().get(taskId);
                if (projectTaskState.dataState() == DataState.DELETING) {
                    return true;
                } else {
                    log.warning("{}: Task {} queued for deletion has unexpected data state {}. Skipping delete.", persistenceId(), taskId, projectTaskState.dataState());
                    return false;
                }
            })
            .mapAsync(taskDeleteParallelism, t -> {
                final UUID taskId = t.t2();
                inProgressDeleteTaskIds.add(taskId);
                log.info("{}: Task {} deletion started.", persistenceId(), taskId);
                return taskDeleter.deleteTask(keyspace(), t.t1(), taskId, t.t3()).handle((done, throwable) -> Pair.create(taskId, throwable));
            })
            .viaMat(KillSwitches.single(), Keep.both())
            .toMat(Sink.foreach(p -> {
                UUID taskId = p.first();
                Throwable throwable = p.second();
                inProgressDeleteTaskIds.remove(taskId);
                if (throwable == null) {
                    sendSelf(commandFactory.task(taskId).deleted());
                    log.info("{}: Task {} deletion done", persistenceId(), taskId);
                } else {
                    log.error(throwable, "{}: Exception during deletion of task {}", persistenceId(), taskId);
                }
            }), Keep.left())
            .run(ActorMaterializer.create(this.context()));
        deleteQueue = pair.first();
        killSwitch = pair.second();

        taskSubmissionInterval = config.hasPath(TASK_SUBMISSION_INTERVAL) ?
            config.getDuration(TASK_SUBMISSION_INTERVAL) : Duration.ofSeconds(3);
        taskSubmissionRoutine = getContext().system().scheduler().schedule(
            taskSubmissionInterval,
            taskSubmissionInterval,
            getSelf(),
            TASK_SUBMISSION_TICK,
            getContext().dispatcher(),
            getSelf()
        );
    }

    @Override
    public void postStop() throws Exception {
        try {
            taskSubmissionRoutine.cancel();
            deleteQueue.complete();
            killSwitch.shutdown();
        } finally {
            super.postStop();
        }
    }

    public static void start(
        ActorSystem system,
        int numberOfRetriesPerTask,
        AnalysisWorkCreator workCreator,
        TaskDeleter taskDeleter,
        Function<ActorSystem, ActorRef> workManagerActor,
        UserManagerCommunication userManagerCommunication,
        ApplicationStorageFactory storageFactory,
        FractionCache fractionCache,
        UploaderService uploaderService,
        CassandraMonitorService cassandraMonitorService
    ) {
        final ClusterShardingSettings settings = ClusterShardingSettings.create(system);
        final ClusterSharding clusterSharding = ClusterSharding.get(system);
        final Props props = Props.create(ProjectAggregateEntity.class, numberOfRetriesPerTask, workCreator, taskDeleter, workManagerActor, userManagerCommunication, storageFactory, fractionCache, uploaderService, cassandraMonitorService);
        clusterSharding.start(CLUSTER_NAMESPACE, props, settings, MESSAGE_EXTRACTOR);
    }

    @NotNull
    public static String persistenceId(String projectId) {
        return MessageFormat.format("{0}-{1}", CLUSTER_NAMESPACE, projectId);
    }

    @NotNull
    public static ActorRef instance(ActorSystem system) {
        return ClusterSharding.get(system).shardRegion(CLUSTER_NAMESPACE);
    }

    @NotNull
    public static String persistenceId(UUID projectId) {
        return persistenceId(projectId.toString());
    }

    @NotNull
    public static String persistenceId(ByteString projectId) {
        return persistenceId(uuidFrom(projectId));
    }

    protected ProjectAggregateEventFactory eventFactory(UUID userId) {
        return ProjectAggregateEventFactory.project(projectId, userId);
    }

    protected ProjectAggregateCommandFactory commandFactory(UUID userId) {
        return ProjectAggregateCommandFactory.project(projectId, userId);
    }

    @Override
    protected String eventTag(ProjectAggregateEvent event) {
        switch (event.getEventCase()) {
            case PROJECT:
                return eventTag(event.getProject().getEventCase());
            case ANALYSIS:
                return eventTag(event.getAnalysis().getEventCase());
            case SAMPLE:
                return eventTag(event.getSample().getEventCase());
            case TASK:
                return eventTag(event.getTask().getEventCase());
            default:
                return "ProjectAggregateEvent.Unknown";
        }
    }

    @NotNull
    public static String eventTag(ProjectAggregateEvent.TaskEvent.EventCase eventCase) {
        return "ProjectAggregateEvent.TaskEvent." + eventCase.name();
    }

    @NotNull
    public static String eventTag(ProjectAggregateEvent.SampleEvent.EventCase eventCase) {
        return "ProjectAggregateEvent.SampleEvent." + eventCase.name();
    }

    @NotNull
    public static String eventTag(ProjectAggregateEvent.AnalysisEvent.EventCase eventCase) {
        return "ProjectAggregateEvent.AnalysisEvent." + eventCase.name();
    }

    @NotNull
    public static String eventTag(ProjectEvent.EventCase eventCase) {
        return "ProjectAggregateEvent.ProjectEvent." + eventCase.name();
    }

    @Override
    protected ProjectAggregateState defaultState() {
        return new ProjectAggregateStateBuilder().build();
    }

    @Override
    protected String commandTag(ProjectAggregateCommand command) {
        return command.getCommandCase().name();
    }

    private <T> FI.UnitApply<ProjectAggregateQueryRequest> handleQuery(Function<ProjectAggregateQueryRequest, T> select, Function2<T, ProjectAggregateState, CompletionStage<ProjectAggregateQueryResponse.Builder>> responder) {
        return query -> {
            UUID userId = uuidFrom(query.getUserId());
            CompletionStage<ProjectAggregateQueryResponse.Builder> futureResponse;
            if (data.status() == ProjectAggregateState.Status.PENDING) {
                futureResponse = completedFuture(
                    ProjectAggregateQueryResponse.newBuilder()
                        .setFailure(QueryFailure.newBuilder()
                            .setReason(QueryFailure.Reason.NOT_EXIST)
                        )
                );
            } else if (data.status() == ProjectAggregateState.Status.DELETED) {
                futureResponse = completedFuture(
                    ProjectAggregateQueryResponse.newBuilder()
                        .setFailure(QueryFailure.newBuilder()
                            .setReason(QueryFailure.Reason.NOT_FOUND)
                        )
                );
            } else if (!readPermission(userId)) {
                futureResponse = completedFuture(
                    ProjectAggregateQueryResponse.newBuilder()
                        .setFailure(QueryFailure.newBuilder()
                            .setReason(QueryFailure.Reason.ACCESS_DENIED)
                        )
                );
            } else {
                try {
                    futureResponse = responder.apply(select.apply(query), data);
                } catch (Exception e) {
                    futureResponse = Futures.failedCompletionStage(e);
                }
            }
            final long startTime = System.currentTimeMillis();
            replyFuture(futureResponse
                .exceptionally(throwable -> {
                    log.error(throwable, "{}: Error handling query", persistenceId());
                    return ProjectAggregateQueryResponse.newBuilder()
                        .setFailure(QueryFailure.newBuilder()
                            .setReason(QueryFailure.Reason.ERROR)
                            .setDescription(throwable.getMessage() != null ? throwable.getMessage() : "")
                        );
                })
                .thenApply(builder -> {
                    long processTime = System.currentTimeMillis() - startTime;
                    if (processTime > 1000) {
                        try {
                            log.warning("{}: Slow query {} process time {} seconds: {}", persistenceId(), query.getRequestCase(), processTime / 1000, JsonFormat.printer().includingDefaultValueFields().print(query));
                        } catch (InvalidProtocolBufferException e) {
                            log.error(e, "{}: Unable to serialize query", persistenceId());
                        }
                    }
                    return builder
                        .setProjectId(query.getProjectId())
                        .setDeliveryId(query.getDeliveryId())
                        .setSequenceNr(lastSequenceNr())
                        .build();
                }));
        };
    }

    private boolean readPermission(UUID userId) {
        return ADMIN_USERID.equals(userId) || data.shared() || data.ownerId().equals(userId);
    }

    private boolean writePermission(UUID userId) {
        return ADMIN_USERID.equals(userId) || data.ownerId().equals(userId);
    }

    @Override
    protected void onRecoveryComplete(List<ProjectAggregateEvent> recoveryEvents) throws Exception {
        super.onRecoveryComplete(recoveryEvents);
        switch (data.status()) {
            case CREATED:
                data.taskIdToTask().forEach((taskId, task) -> {
                    switch (task.progress()) {
                        case QUEUED:
                        case IN_PROGRESS:
                            final int priority = data.taskPlanPriority(task.taskPlanId());
                            if (priority > 0) {
                                sendWorkManagerTaskPriority(taskId, priority);
                            }
                    }
                });
                cleanupUnfinishedUploads();
                timeoutUploads();
                sendSelf(RUN_WORK_CREATOR);
                break;
            case DELETING:
                // If error occurs during delete, then we can have a deleted project that still contains tasks with data.
                // Upon recovery, retry delete.
                for (ProjectTaskPlanState taskPlan : data.taskPlanIdToTaskPlan().values()) {
                    final Optional<ProjectTaskState> taskState = data.taskState(taskPlan);
                    if (taskState.isPresent()) {
                        final ProjectTaskState task = taskState.get();
                        switch (task.dataState()) {
                            case READY:
                            case DELETING:
                                deleteTask(taskPlan, task.taskId());
                                break;
                        }
                    }
                }
                break;
        }
    }

    @Override
    protected GeneratedMessageV3 snapShotFromState(ProjectAggregateState state) {
        final com.bsi.peaks.event.projectAggregate.ProjectAggregateState.Builder buider = com.bsi.peaks.event.projectAggregate.ProjectAggregateState.newBuilder();
        state.proto(buider);
        return buider.build();
    }

    @Override
    protected void onSnapShotOffer(SnapshotOffer snapshotOffer) {
        data = ProjectAggregateState.from((com.bsi.peaks.event.projectAggregate.ProjectAggregateState) snapshotOffer.snapshot());
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .matchEquals(RUN_WORK_CREATOR, event -> createWorkAndSendWorkManager())
            .matchEquals(TASK_SUBMISSION_TICK, ignore -> submitTasksInBatch())
            //Archive restore
            .match(ProjectEventToRestore.class, this::restore)
            //COMMANDS
            //projects
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasCreateProject, commandThenAck(this::handleCreateProject))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasDeleteProject, commandThenAck(this::handleDeleteProject))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasSetProjectInformation, commandThenAck(this::handleSetProjectInformation))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasSetProjectPermission, commandThenAck(this::handleSetProjectPermission))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasArchiveNotification, commandThenAck(this::handleArchiveNotification))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasRestartFailedUploads, commandThenAck(this::handleRestartFailedUploads))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasCreateDaemonFractionsBatch, commandThenAck(this::handleCreateDaemonFractionsBatch))
            //analysis
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasCreateAnalysis, commandThenAck(this::handleCreateAnalysis))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasCancelAnalysis, commandThenAck(this::handleCancelAnalysis))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasDeleteAnalysis, commandThenAck(this::handleDeleteAnalysis))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasSetAnalysisInformation, commandThenAck(this::handleSetAnalysisInformation))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasSetAnalysisPriority, commandThenAck(this::setAnalysisPriority))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasSetFilter, commandThenAck(this::handleSetFilter))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasUserSetFilter, commandThenAck(this::handleUserSetFilter))
            //samples
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasDeleteSamplesBatch, commandThenAck(this::handleDeleteSamplesBatch))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasCreateSamplesBatch, commandThenAck(this::handleCreateSamples))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasCancelSample, commandThenAck(this::handleCancelSample))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasDeleteSample, commandThenAck(this::handleDeleteSample))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasSetSampleInformation, commandThenAck(this::handleSetSampleInformation))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasUploadNotification, commandThenAck(this::handleUploadNotification))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasRemoteUploadQueueNotification, commandThenAck(this::handleRemoteUploadQueueNotification))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasUpdateFractionName, commandThenAck(this::handleUpdateFractionName))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasUpdateSampleEnzyme, commandThenAck(this::handleUpdateEnzyme))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasUpdateSampleActivationMethod, commandThenAck(this::handleUpdateActivationMethod))
            //task
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasVerifyTask, commandThenAck(this::handleVerifyTask))
            .match(ProjectAggregateCommand.class, ProjectAggregateCommand::hasTaskNotification, commandThenAck(this::handleTaskNotification))
            //upload
            //Query
            .match(ProjectAggregateQueryRequest.class, ProjectAggregateQueryRequest::hasProjectQueryRequest, handleQuery(ProjectAggregateQueryRequest::getProjectQueryRequest, (queryRequest, data) -> {
                CompletionStage<ProjectAggregateQueryResponse.Builder> result = query.queryProject(queryRequest, data);
                timeoutUploads();
                return result;
            }))
            .match(ProjectAggregateQueryRequest.class, ProjectAggregateQueryRequest::hasSampleQueryRequest, handleQuery(ProjectAggregateQueryRequest::getSampleQueryRequest, sampleQuery::querySample))
            .match(ProjectAggregateQueryRequest.class, ProjectAggregateQueryRequest::hasAnalysisQueryRequest, handleQuery(ProjectAggregateQueryRequest::getAnalysisQueryRequest, query::queryAnalysis));
    }

    private void restore(ProjectEventToRestore events) {
        try {
            if (data.status() != ProjectAggregateState.Status.PENDING) {
                throw new IllegalArgumentException("Restore only allowed on pending project");
            }

            for (ProjectAggregateEvent event : events.getEventsList()) {
                try {
                    persist(event, () -> mediator.tell(new DistributedPubSubMediator.Publish(persistenceId(), event), getSelf()));
                } catch (Throwable e) {
                    log.error(e, "{}: Unarchived event failed to transition state. Skipping, but keeping event in history.", persistenceId());
                }
            }

            //gather all task creations
            final TaskPlanLookupRepository taskPlanLookupRepository = applicationStorage().getTaskPlanLookupRepository();
            final MemoryOptimizedFactory memoryOptimizedFactory = data.memoryOptimizedFactory();
            CompletionStage<Done> taskSaveFuture = CompletableFuture.completedFuture(Done.getInstance());
            for (ProjectAggregateEvent event : events.getEventsList()) {
                if (event.hasTask() && event.getTask().hasTaskCreated()) {
                    ProjectAggregateEvent.TaskCreated taskCreated = event.getTask().getTaskCreated();
                    TaskDescription taskDescription = taskCreated.getTaskDescription();
                    LevelTask levelTask = memoryOptimizedFactory.internLevelTask(taskDescription);
                    UUID taskPlanId = uuidFrom(event.getTask().getTaskPlanId());
                    switch (taskDescription.getTaskCase()) {
                        case JSONPARAMATERS:
                            taskSaveFuture = taskSaveFuture
                                .thenCompose(ignore -> taskPlanLookupRepository.saveJson(projectId, taskPlanId, taskDescription.getStepType(), levelTask, taskDescription.getJsonParamaters()));
                            break;
                        case PARAMETERS:
                            taskSaveFuture = taskSaveFuture
                                .thenCompose(ignore -> taskPlanLookupRepository.saveProto(projectId, taskPlanId, taskDescription.getStepType(), levelTask, taskDescription.getParameters()));
                            break;
                        default:
                            throw new IllegalStateException("Task description has invalid task case " + taskDescription.getTaskCase());
                    }
                }
            }

            taskSaveFuture.whenComplete((done, throwable) -> {
                if (throwable != null) {
                    log.error(throwable, "{}: Error updating TaskPlanLookup for reuse of tasks.", persistenceId());
                } else {
                    log.info("{}: Done updating TaskPlanLookup for reuse of tasks.", persistenceId());
                }
            });

            defer(Done.getInstance(), msg -> {
                reply(msg);
                log.info("{}: Done unarchiving project. TaskPlanLookup will update in background.", persistenceId());
            });
        } catch (Throwable e) {
            reply(e);
        }
    }

    private void submitTasksInBatch() {
        if (batchedTasks.size() > 0) {
            if (batchedTasks.size() > 1) {
                sendWorkManagerCommand(b -> b.setSubmitTasks(BatchSubmitTasks.newBuilder().addAllTasks(batchedTasks).build()));
            } else {
                sendWorkManagerCommand(b -> b.setSubmitTask(batchedTasks.get(0)));
            }
            log.info("{} tasks sent to work manager in batch", batchedTasks.size());
            batchedTasks.clear();
        }
    }

    private ApplicationStorage applicationStorage() {
        switch (data.status()) {
            case CREATED:
            case DELETING:
                return storageFactory.getStorage(keyspace());
            default:
                // Before Created, keyspace is unknown.
                // After Deleting, the keyspace may no longer exist.
                throw new IllegalStateException("Cannot access application storage for project, while project is " + data.status());
        }
    }

    private CompletionStage<? extends ApplicationStorage> applicationStorageAsync() {
        switch (data.status()) {
            case CREATED:
            case DELETING:
                return storageFactory.getStorageAsync(keyspace());
            default:
                // Before Created, keyspace is unknown.
                // After Deleting, the keyspace may no longer exist.
                throw new IllegalStateException("Cannot access application storage for project, while project is " + data.status());
        }
    }

    private Project getSimplefiedProjectWithCreationTime(com.bsi.peaks.common.Instant creationTime) {
        Instant converted = CommonFactory.convert(creationTime);
        return Project.newBuilder()
            .setId(projectId.toString())
            .setName(data.name())
            .setCreatedTimestamp(converted.getEpochSecond())
            .build();
    }

    @Override
    protected long deliveryId(ProjectAggregateCommand command) {
        return command.getDeliveryId();
    }

    private ActorRef workManagerActorRef() {
        try {
            return workManagerActor.apply(context().system());
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private void createWorkAndSendWorkManager() throws Exception {
        if (data.status() != ProjectAggregateState.Status.CREATED) {
            return;
        }
        final ProjectAggregateEventFactory eventFactory = eventFactory(ADMIN_USERID);

        final List<UUID> failedAnalysisIds = analysisFutureTaskPlanIds.entrySet().stream()
            .filter(entry -> entry.getValue().isCompletedExceptionally())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        for (UUID analysisId : failedAnalysisIds) {
            analysisFutureTaskPlanIds.remove(analysisId);
            final AnalysisEventFactory analysisFactory = eventFactory.analysis(analysisId);
            persist(analysisFactory.fail(AnalysisFailedCause.WORKCREATOR));
        }

        final List<UUID> doneAnalysisIds = analysisFutureTaskPlanIds.entrySet().stream()
            .filter(entry -> entry.getValue().isDone())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        // Failed analysis no longer affect priority of tasks.
        boolean recalucateTaskPriority = !failedAnalysisIds.isEmpty();

        final PMap<UUID, ProjectAnalysisState> analysisIdToAnalysis = data.analysisIdToAnalysis();
        for (UUID analysisId : doneAnalysisIds) {
            final List<Pair<TaskDescription, UUID>> taskPairs = analysisFutureTaskPlanIds.remove(analysisId).getNow(null);
            final ProjectAnalysisState analysis = analysisIdToAnalysis.get(analysisId);
            final AnalysisEventFactory analysisFactory = eventFactory.analysis(analysisId);
            switch (analysis.status()) {
                case CREATED:
                case PROCESSING:
                    final PMap<StepType, PSet<UUID>> stepTaskPlanId = analysis.stepTaskPlanId();
                    for (Pair<TaskDescription, UUID> taskPair : taskPairs) {
                        final TaskDescription taskDescription = taskPair.first();
                        final StepType stepType = taskDescription.getStepType();
                        final UUID taskPlanId = taskPair.second();
                        if (stepTaskPlanId.containsKey(stepType) && stepTaskPlanId.get(stepType).contains(taskPlanId)) {
                            // If work creator is creating duplicate tasks, this will happen.
                            log.warning("{}: TaskPlan {} already part of analysis.", persistenceId(), taskPlanId);
                            continue;
                        }
                        if (!data.taskPlanIdToTaskPlan().containsKey(taskPlanId)) {
                            // If taskPlanLookup returns a new taskPlanId.
                            persist(eventFactory.taskplan(taskPlanId).created(taskDescription, data.numberOfRetriesPerTask()));
                        }
                        persist(analysisFactory.taskplanAdded(taskPlanId));
                        final Optional<ProjectTaskState> taskState = data.taskState(taskPlanId);
                        if (taskState.isPresent()) {
                            final ProjectTaskState task = taskState.get();
                            switch (task.progress()) {
                                case QUEUED:
                                case IN_PROGRESS:
                                    // Reuse queued task
                                    recalucateTaskPriority = true;
                                    break;
                                case DONE:
                                    switch (task.dataState()) {
                                        case READY:
                                            onStepCompleteSetFilter(analysisId, stepType);
                                            break;
                                        case DELETING:
                                        case DELETED:
                                            submitTask(taskPlanId, taskDescription);
                                            break;
                                        default:
                                            throw new IllegalStateException("Task is DONE with unexpected data state  " + task.dataState());
                                    }
                                    break;
                                case STOPPING:
                                case CANCELLED:
                                case FAILED:
                                    submitTask(taskPlanId, taskDescription);
                                    break;
                                default:
                                    throw new IllegalStateException("Unexpected task progress " + task.progress());
                            }
                        } else {
                            submitTask(taskPlanId, taskDescription);
                        }
                    }
            }
        }
        if (recalucateTaskPriority) {
            recalculateTaskPriority();
        }

        if (lastCreateWorkData == data) {
            return;
        }
        lastCreateWorkData = data;

        for (ProjectAnalysisState analysis : data.analysisIdToAnalysis().values()) {
            switch (analysis.status()) {
                case CREATED:
                case PROCESSING:
                    final UUID analysisId = analysis.analysisId();
                    if (analysisFutureTaskPlanIds.containsKey(analysisId)) {
                        // Already waiting for lookup of taskPlanIds
                        continue;
                    }
                    final AnalysisEventFactory analysisFactory = eventFactory.analysis(analysisId);
                    final AnalysisTaskInformation analysisTaskInformation = AnalysisTaskInformation.create(keyspace(), data, projectId, analysisId);
                    if (data.isAnalysisDone(analysisTaskInformation, analysis)) {
                        persist(analysisFactory.done());
                    } else {
                        final List<TaskDescription> taskDescriptions;
                        try {
                            long timeBefore = System.currentTimeMillis();
                            taskDescriptions = workCreator.workFrom(AnalysisTaskInformation.create(keyspace(), data, projectId, analysisId));
                            long timeAfter = System.currentTimeMillis();
                            log.info("{}: Running work creator for analysis {} took {} ms.", persistenceId(), analysisId, timeAfter - timeBefore);
                            log.info("Created " + taskDescriptions.size() + " task descriptions: " + taskDescriptions.stream().map(td -> td.getStepType().name()).collect(Collectors.joining(", ")));
                        } catch (Throwable e) {
                            log.error(e, "{}: Failure running work creator for analysis {}", persistenceId(), analysisId);
                            persist(analysisFactory.fail(AnalysisFailedCause.WORKCREATOR));
                            return;
                        }
                        if (taskDescriptions.isEmpty()) {
                            continue;
                        }
                        analysisFutureTaskPlanIds.put(analysisId, lookupOrCreateTaskPlan(taskDescriptions)
                            .whenComplete((pairs, throwable) -> {
                                sendSelf(RUN_WORK_CREATOR);
                                if (throwable != null) {
                                    log.error(throwable, "{}: Error running taskPlanId lookup for analysis {}", persistenceId(), analysisId);
                                }
                            })
                        );
                    }
            }
        }
    }

    private CompletableFuture<List<Pair<TaskDescription, UUID>>> lookupOrCreateTaskPlan(List<TaskDescription> taskDescriptions) {
        final MemoryOptimizedFactory memoryOptimizedFactory = data.memoryOptimizedFactory();
        final int parallelism = storageFactory.getParallelism();
        return Source.fromCompletionStage(applicationStorageAsync())
            .map(ApplicationStorage::getTaskPlanLookupRepository)
            .flatMapConcat(taskPlanLookupRepository -> Source.from(taskDescriptions)
                .mapAsync(parallelism, taskDescription -> {
                    final StepType stepType = taskDescription.getStepType();
                    final LevelTask level = memoryOptimizedFactory.internLevelTask(taskDescription);
                    switch (taskDescription.getTaskCase()) {
                        case JSONPARAMATERS:
                            return taskPlanLookupRepository.lookupJson(projectId, stepType, level, taskDescription.getJsonParamaters())
                                .thenApply(taskPlanId -> Pair.create(taskDescription, taskPlanId))
                                .thenCompose(pair -> {
                                    ProjectTaskPlanState taskPlan = data.taskPlanIdToTaskPlan().get(pair.second());
                                    if (taskPlan != null &&
                                        taskPlan.latestTaskId().isPresent() &&
                                        data.taskIdToTask().get(taskPlan.latestTaskId().get()).progress().equals(com.bsi.peaks.model.system.Progress.FAILED)
                                    ) {
                                        //don't reuse failed task
                                        LevelTask levelTask = memoryOptimizedFactory.internLevelTask(taskDescription);
                                        UUID taskPlanId2 = UUID.randomUUID();
                                        return taskPlanLookupRepository.saveJson(projectId, taskPlanId2, taskDescription.getStepType(), levelTask, taskDescription.getJsonParamaters())
                                            .thenApply(planId -> Pair.create(pair.first(), taskPlanId2));
                                    } else {
                                        return completedFuture(pair);
                                    }
                                })
                                .whenComplete((pair, error) -> {
                                    log.debug("Looked up json task plan id: " + pair.second() + " for " + pair.first().getStepType().name());
                                });
                        case PARAMETERS:
                            return taskPlanLookupRepository.lookupProto(projectId, stepType, level, taskDescription.getParameters())
                                .thenApply(taskPlanId -> Pair.create(taskDescription, taskPlanId))
                                .thenCompose(pair -> {
                                    ProjectTaskPlanState taskPlan = data.taskPlanIdToTaskPlan().get(pair.second());
                                    if (taskPlan != null &&
                                        taskPlan.latestTaskId().isPresent() &&
                                        data.taskIdToTask().get(taskPlan.latestTaskId().get()).progress().equals(com.bsi.peaks.model.system.Progress.FAILED)
                                    ) {
                                        //don't reuse failed task
                                        LevelTask levelTask = memoryOptimizedFactory.internLevelTask(taskDescription);
                                        UUID taskPlanId2 = UUID.randomUUID();
                                        return taskPlanLookupRepository.saveProto(projectId, taskPlanId2, taskDescription.getStepType(), levelTask, taskDescription.getParameters())
                                            .thenApply(planId -> Pair.create(pair.first(), taskPlanId2));
                                    } else {
                                        return completedFuture(pair);
                                    }
                                })
                                .whenComplete((pair, error) -> {
                                    log.debug("Looked up proto task plan id: " + pair.second() + " for " + pair.first().getStepType().name());
                                });
                        default:
                            throw new IllegalArgumentException("Unexpected task parameters " + taskDescription.getTaskCase());
                    }
                })
            )
            .runWith(Sink.seq(), ActorMaterializer.create(context()))
            .toCompletableFuture();
    }

    protected void sendWorkManagerCommand(java.util.function.Function<WorkCommand.Builder, WorkCommand.Builder> build) {
        this.deliver(workManagerActorRef().path(), (Long deliveryId) -> build.apply(WorkCommandFactory.builder())
            .setDeliveryId(deliveryId)
            .build());
    }


    private ProjectAggregateCommand handleCreateProject(ProjectAggregateCommand command) throws Exception {
        log.info("{}: Created project!", persistenceId());
        UUID userId = uuidFrom(command.getUserId());
        ProjectAggregateCommand.CreateProject createProject = command.getCreateProject();
        ProjectAggregateEventFactory eventFactory = eventFactory(userId);

        // create the events first to maintain timestamps
        ProjectAggregateEvent informationEvent = eventFactory.projectInformation(createProject.getProjectInformation());
        ProjectAggregateEvent permissionEvent = eventFactory.projectPermission(createProject.getProjectPermission());

        com.bsi.peaks.common.Instant creationTime = CommonFactory.nowTimeStamp();
        // this might fail, in such case don't persist any event
        String keyspace = storageFactory.getKeyspace(getSimplefiedProjectWithCreationTime(creationTime));
        ProjectAggregateEvent creationEvent = eventFactory.projectCreated(keyspace).toBuilder().setTimeStamp(creationTime).build();

        persist(informationEvent);
        persist(permissionEvent);
        persist(creationEvent);

        // no need to block now, it's should be already created, otherwise the blocking will happen
        // in loader
        storageFactory.initStorage(keyspace);

        ProjectAggregateCommand.Builder commandBuilder = ProjectAggregateCommand.newBuilder()
            .setProjectId(command.getProjectId())
            .setUserId(command.getUserId())
            .setTimeStamp(command.getTimeStamp());
        if (createProject.getCreateSamples().getCreateSamplesCount() > 0) {
            ProjectAggregateCommand batchSamplesCreateCommand = commandBuilder.setCreateSamplesBatch(createProject.getCreateSamples())
                .build();
            handleCreateSamples(batchSamplesCreateCommand);
        }

        if (createProject.hasCreateAnalyses()) {
            ProjectAggregateCommand analysisCreateCommand = commandBuilder
                .setCreateAnalysis(createProject.getCreateAnalyses())
                .build();
            handleCreateAnalysis(analysisCreateCommand);
        }

        return command;
    }

    private ProjectAggregateCommand handleCreateSamples(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();

        final ProjectAggregateCommand.CreateSamplesBatch createSamplesBatch = command.getCreateSamplesBatch();

        for (ProjectAggregateCommand.CreateSample createSample : createSamplesBatch.getCreateSamplesList()) {
            handleCreateSample(userId, createSample);
        }

        return command;
    }

    private void handleCreateSample(UUID userId, ProjectAggregateCommand.CreateSample createSample) throws Exception {
        final SampleInformation sampleInformation = createSample.getSampleInformation();
        final SampleParameters sampleParameters = createSample.getSampleParameters();

        final int priority = createSample.getPriority();
        final String name = sampleInformation.getName();
        final UUID sampleId = uuidFrom(createSample.getSampleId());

        if (data.sampleIdToSample().containsKey(sampleId)) {
            throw new IllegalArgumentException("Sample ID already exists");
        }

        log.info("{}: creating sample {}", persistenceId(), sampleId);

        SampleEventFactory sampleEventFactory = eventFactory(userId).sample(sampleId);

        List<WorkflowStep> steps = createSample.getStepsList();

        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Name cannot be empty");
        }
        if (data.sampleIdToSample().values().stream().anyMatch(sample -> name.equals(sample.name()))) {
            throw new IllegalArgumentException("Sample name already exists");
        }

        if (steps.isEmpty()) {
            throw new IllegalArgumentException("Workflow steps cannot be empty");
        }

        for (WorkflowStep step : steps) {
            if (step.getStepType() != StepType.DATA_LOADING) {
                throw new IllegalArgumentException("Invalid sample step " + step.getStepType());
            }
            if (Strings.isNullOrEmpty(step.getStepParametersJson())) {
                throw new IllegalArgumentException("Step rejected, step type parameters cannot be empty or null.");
            }
        }

        final int fractionCount = createSample.getFractionsCount();
        if (fractionCount == 0) {
            throw new IllegalArgumentException("Fractions are required.");
        }
        final List<Fraction> fractionList = new ArrayList<>(fractionCount);
        final Set<UUID> existingFractionIds = data.sampleIdToSample().values().stream()
            .flatMap(sample -> sample.fractionIds().stream())
            .collect(Collectors.toSet());
        final Set<UUID> newFractionId = new HashSet<>();
        for (Fraction fraction : createSample.getFractionsList()) {
            final UUID fractionId = uuidFrom(fraction.getFractionId());
            if (existingFractionIds.contains(fractionId)) {
                throw new IllegalStateException("Fraction rejected, since already exists as part of project.");
            }
            if (!newFractionId.add(fractionId)) {
                throw new IllegalStateException("Fraction rejected, duplicate fractionId in list.");
            }
            fractionList.add(fraction);
        }

        persist(sampleEventFactory.setInformation(sampleInformation));
        persist(sampleEventFactory.setPriority(priority));
        persist(sampleEventFactory.create(
            sampleParameters,
            fractionList,
            Iterables.transform(steps, step -> step.toBuilder().setId(UUID.randomUUID().toString()).build())
        ));
    }

    private ProjectAggregateCommand handleCreateAnalysis(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();

        final ProjectAggregateCommand.CreateAnalysis createAnalysis = command.getCreateAnalysis();
        final AnalysisInformation analysisInformation = createAnalysis.getAnalysisInformation();

        final UUID analysisId = uuidFrom(createAnalysis.getAnalysisId());
        if (data.analysisIdToAnalysis().containsKey(analysisId)) {

            // Verify this IS duplicate. Doesn't need to be complete check, as this is more of a courtesy to the sender.
            {
                Iterator<UUID> existingSampleIdIterator = Iterables.transform(data.analysisIdToAnalysis().get(analysisId).samples(), OrderedSampleFractions::sampleId).iterator();
                for (UUID sampleId : uuidsFrom(createAnalysis.getSampleIds().toByteArray())) {
                    if (!existingSampleIdIterator.hasNext() || !existingSampleIdIterator.next().equals(sampleId)) {
                        throw new IllegalArgumentException(persistenceId() + ": Attempt to create analysis " + analysisId + "  that already exists.");
                    }
                }
                if (existingSampleIdIterator.hasNext()) {
                    throw new IllegalArgumentException(persistenceId() + ": Attempt to create analysis " + analysisId + "  that already exists.");
                }
            }

            log.info("{}: Second command to create analysis {} received. Ignoring, and acking since it is likely just a retry message.", persistenceId(), analysisId);
            return command;
        }

        log.info("{}: creating analysis {}", persistenceId(), analysisId);

        final AnalysisEventFactory analysisEventFactory = eventFactory(userId).analysis(analysisId);

        final String name = analysisInformation.getName();
        final Integer priority = createAnalysis.getPriority();

        final List<WorkflowStep> steps = createAnalysis.getStepsList();
        final Iterable<UUID> sampleIdsIterator = uuidsFrom(createAnalysis.getSampleIds().toByteArray());
        final List<UUID> sampleIds = new ArrayList<>();
        for (UUID sampleId : sampleIdsIterator) {
            sampleIds.add(sampleId);
        }

        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Name cannot be empty");
        }

        if (steps.isEmpty()) {
            throw new IllegalArgumentException("Workflow steps cannot be empty");
        }
        for (WorkflowStep step : steps) {
            switch (step.getStepType()) {
                case DATA_LOADING:
                case UNKNOWN:
                    throw new IllegalArgumentException("Step rejected, step type cannot be " + step.getStepType());
            }
            if (step.getParametersCase() == WorkflowStep.ParametersCase.PARAMETERS_NOT_SET) {
                throw new IllegalArgumentException("Step " + step.getStepType() + " rejected, step type parameters cannot be empty or null.");
            }
        }

        if (sampleIds.size() == 0) {
            throw new IllegalArgumentException("Samples required");
        }
        for (UUID sampleId : sampleIds) {
            if (!data.sampleIdToSample().containsKey(sampleId)) {
                throw new IllegalArgumentException("Sample " + sampleId + " is not part of project");
            }
        }

        persist(analysisEventFactory.setInformation(analysisInformation));
        persist(analysisEventFactory.setPriority(priority));
        persist(analysisEventFactory.create(
            sampleIds,
            Iterables.transform(steps, step -> step.toBuilder().setId(UUID.randomUUID().toString()).build()),
            createAnalysis.getDatabasesList()
        ));
        defer(RUN_WORK_CREATOR, this::sendSelf);

        return command;
    }

    private void handleCreateDaemonFractionsBatch(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        ProjectAggregateCommand.CreateDaemonFractionsBatch createDaemonFractionsBatch = command.getCreateDaemonFractionsBatch();
        List<UUID> fractionIds = Lists.newArrayList(uuidsFrom(createDaemonFractionsBatch.getFractionIds()));
        int timeout = createDaemonFractionsBatch.getTimeout();
        ProjectAggregateEvent projectAggregateEvent = eventFactory(userId).daemonFractionsBatchCreated(fractionIds, timeout);
        persist(projectAggregateEvent);
    }

    private void handleCancelAnalysis(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();
        final ProjectAggregateCommand.CancelAnalysis cancelAnalysis = command.getCancelAnalysis();
        final UUID analysisId = uuidFrom(cancelAnalysis.getAnalysisId());
        final ProjectAnalysisState analysis = data.analysisIdToAnalysis().get(analysisId);
        if (analysis == null) {
            throw new IllegalArgumentException("Analysis " + analysisId + " does not exist");
        }
        switch (analysis.status()) {
            case CREATED:
            case PROCESSING:
                persist(eventFactory(userId).analysis(analysisId).cancel());
                recalculateTaskPriority();
                break;
        }
    }

    private void handleCancelSample(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();
        final ProjectAggregateCommand.CancelSample cancelAnalysis = command.getCancelSample();
        final UUID sampleId = uuidFrom(cancelAnalysis.getSampleId());
        final ProjectSampleState sample = data.sampleIdToSample().get(sampleId);
        if (sample == null) {
            throw new IllegalArgumentException("Sample " + sampleId + " does not exist");
        }
        if (sample.isCancelled()) {
            // Already cancelled.
            return;
        }
        persist(eventFactory(userId).sample(sampleId).cancel());
        recalculateTaskPriority();
    }

    private void handleDeleteSamplesBatch(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();

        ProjectAggregateCommand.DeleteSamplesBatch batchDeletion = command.getDeleteSamplesBatch();
        ProjectSampleState.Status selectedStatus;
        switch (batchDeletion.getByStatus()) {
            case PENDING:
            case CREATED:
                selectedStatus = ProjectSampleState.Status.CREATED;
                break;
            case CANCELLED:
                selectedStatus = ProjectSampleState.Status.CANCELLED;
                break;
            case FAILED:
                selectedStatus = ProjectSampleState.Status.FAILED;
                break;
            case DONE:
                selectedStatus = ProjectSampleState.Status.DONE;
                break;
            case PROCESSING:
                selectedStatus = ProjectSampleState.Status.PROCESSING;
                break;
            default:
                throw new IllegalArgumentException("Unexpected status " + batchDeletion.getByStatus());
        }

        Set<UUID> sampleIds = data.sampleIdToSample().values().stream()
            .filter(sample -> sample.status() == selectedStatus)
            .map(ProjectSampleState::sampleId).collect(Collectors.toSet());

        // do validation for all samples first
        data.analysisIdToAnalysis().forEach((key, analysisState) -> {
            analysisState.samples().forEach(sample -> {
                if (sampleIds.contains(sample.sampleId())) {
                    throw new IllegalStateException("Samples trying to delete is used by an analysis "
                        + analysisState.name() + " that's not deleted");
                }
            });
        });

        // now try to delete all of these samples
        boolean needTaskPriorityRecalculation = false;
        for (UUID sampleId : sampleIds) {
            final ProjectSampleState sample = data.sampleIdToSample().get(sampleId);
            if (sample != null) {
                log.info("{}: Deleting Sample {}", persistenceId(), sampleId);
                persist(eventFactory(userId).sample(sampleId).delete());
                if (!sample.allTasksDone()) { // The only time we can be sure there is no processing is when all tasks as done.
                    needTaskPriorityRecalculation = true;
                }
            }
        }
        if (needTaskPriorityRecalculation) {
            recalculateTaskPriority();
        }
        deleteUnreferencedDoneTasks();
    }

    private ProjectAggregateCommand handleDeleteProject(ProjectAggregateCommand command) throws Exception {
        switch (data.status()) {
            case PENDING:
                throw new IllegalStateException("Cannot delete project that has not been created");
            case DELETED:
                return command;
            //everything else let it pass
        }

        if (command.getDeleteProject().getForceDeleteWithoutKeyspace()) {
            // Short circuit for forced deletion
            log.info("{}: Force deleting project after keyspace drop", persistenceId());
            // This is the ultimate deletion, as the keyspace is gone, we have to delete the project no matter what
            // Try our best to cancel tasks from work manager if they are in process
            // Ongoing archiving will just fail, don't care about them as the data is gone anyways
            for (ProjectTaskState task : data.taskIdToTask().values()) {
                switch (task.dataState()) {
                    case DELETING:
                    case DELETED:
                        continue; // do nothing
                    default:
                        switch (task.progress()) {
                            case QUEUED:
                            case IN_PROGRESS:
                                sendWorkManagerCancel(task.taskId());
                                break;
                            default: // do nothing
                        }
                }
            }

            persist(eventFactory(ADMIN_USERID).projectDeleted());
            defer(Done.getInstance(), ignore -> deleteSelf());

            return command;
        }

        rejectCommandDuringArchiving();
        log.info("{}: Deleting project", persistenceId());

        final UUID userId = uuidFrom(command.getUserId());
        ProjectAggregateCommandFactory commandFactory = commandFactory(userId);

        // Retry
        // These should never happen unless failure occured during last delete/cancel.
        // If user is impatient, and clicks delete multiple times, this can also trigger below logic.
        for (ProjectTaskState task : data.taskIdToTask().values()) {
            switch (task.dataState()) {
                case DELETING:
                    log.info("{}: Incomplete deleting of task {}. Will start deleting again now.", persistenceId(), task.taskId());
                    deleteTask(task);
                    break;
                case DELETED:
                    // Do Nothing.
                    continue;
                default:
                    switch (task.progress()) {
                        case CANCELLED:
                            log.info("{}: Previously cancelled task {} still not deleted. Sending another cancel to WorkManager before deletion.", persistenceId(), task.taskId());
                            sendWorkManagerCancel(task.taskId());
                            break;
                        // Other cases will handled after Samples and Analyses have been deleted.
                    }
            }
        }

        // Delete Analyses - this will trigger delete of no longer required tasks.
        for (ProjectAnalysisState analysisState : data.analysisIdToAnalysis().values()) {
            handleDeleteAnalysis(commandFactory.analysis(analysisState.analysisId()).delete());
        }

        // Delete Sample - this will trigger delete of no longer required tasks.
        for (ProjectSampleState sample : data.sampleIdToSample().values()) {
            handleDeleteSample(commandFactory.sample(sample.sampleId()).delete());
        }

        persist(eventFactory(userId).projectDeleting());

        // If anything is not deleting or cancelling now, is very unexpected.
        // As a precaution, we check and cleanup any tasks that were not deleting.
        for (ProjectTaskState task : data.taskIdToTask().values()) {
            switch (task.dataState()) {
                case DELETING:
                case DELETED:
                    // Do Nothing.
                    continue;
                default:
                    switch (task.progress()) {
                        case CANCELLED:
                            // Wait for cancel from WorkManager to respond.
                            continue;
                        case QUEUED:
                        case IN_PROGRESS:
                            log.info("{}: Task {} is unexpectedly {}. Sending cancel to WorkManager.", persistenceId(), task.taskId(), task.progress());
                            sendWorkManagerCancel(task.taskId());
                            break;
                        default:
                            log.info("{}: Task {} is unexpectedly {}. Deleting now.", persistenceId(), task.taskId(), task.progress());
                            deleteTask(task);
                            break;
                    }
            }
        }

        if (data.allTasksAreDeleted()) {
            deleteProject();
        }

        return command;
    }

    private void sendWorkManagerCancel(UUID taskId) {
        sendWorkManagerCommand(b -> b.setCancelTask(
            CancelTask.newBuilder()
                .setProjectId(projectId.toString())
                .setTaskId(taskId.toString())
                .build()
        ));
    }

    private void rejectCommandDuringArchiving() {
        if (data.archiveState() == ArchiveStateEnum.ARCHIVING) {
            throw new IllegalStateException("Command rejected during archive process");
        }
    }

    private void handleDeleteAnalysis(ProjectAggregateCommand command) throws Exception {
        final UUID analysisId = uuidFrom(command.getDeleteAnalysis().getAnalysisId());
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();
        final ProjectAnalysisState analysis = data.analysisIdToAnalysis().get(analysisId);
        if (analysis != null) {
            log.info("{}: Deleting Analysis {}", persistenceId(), analysisId);
            persist(eventFactory(userId).analysis(analysisId).delete());
            if (analysis.status() == ProjectAnalysisState.Status.PROCESSING) {
                recalculateTaskPriority();
            }
            deleteUnreferencedDoneTasks();
        }
    }

    private void handleDeleteSample(ProjectAggregateCommand command) throws Exception {
        final UUID sampleId = uuidFrom(command.getDeleteSample().getSampleId());
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();

        data.analysisIdToAnalysis().forEach((key, analysisState) -> {
            final boolean foundSample = analysisState.samples()
                    .stream()
                    .anyMatch(pair -> pair.sampleId().equals(sampleId));
            if (foundSample) {
                throw new IllegalStateException("Trying to delete a sample being used by an analysis that's not deleted");
            }
        });

        final ProjectSampleState sample = data.sampleIdToSample().get(sampleId);
        if (sample != null) {
            log.info("{}: Deleting Sample {}", persistenceId(), sampleId);
            persist(eventFactory(userId).sample(sampleId).delete());
            if (sample.status() == ProjectSampleState.Status.PROCESSING) {
                recalculateTaskPriority();
            }
            deleteUnreferencedDoneTasks();
        }
    }

    private void handleVerifyTask(ProjectAggregateCommand command) throws Exception {
        final ProjectAggregateCommand.VerifyTask verifyTask = command.getVerifyTask();

        final HashSet<UUID> verifyTaskIds = Sets.newHashSet(uuidsFrom(verifyTask.getTaskIds()));

        for (ProjectTaskState projectTaskState : data.taskIdToTask().values()) {
            final UUID taskId = projectTaskState.taskId();
            if (verifyTaskIds.remove(taskId)) {
                final int priority;
                switch (projectTaskState.progress()) {
                    case QUEUED:
                    case IN_PROGRESS:
                        priority = data.taskPlanPriority(projectTaskState.taskPlanId());
                        break;
                    default:
                        priority = 0;
                        break;
                }
                if (priority == 0) {
                    log.info("{}: cancelling taskId {} with 0 priority during task verification.", persistenceId(), taskId);
                    sendWorkManagerCancel(taskId);
                } else {
                    sendWorkManagerTaskPriority(taskId, priority);
                }
            } else {
                switch (projectTaskState.progress()) {
                    case DONE:
                    case FAILED:
                        // We are in expected state;
                        continue;
                }
                submitTask(projectTaskState.taskPlanId());
                persist(eventFactory(ADMIN_USERID).taskplan(projectTaskState.taskPlanId()).failed(taskId, "Task verification revealed WorkManager not tracking task"));
            }
        }
        for (UUID taskId : verifyTaskIds) {
            log.warning("{}: unknown taskId {} encountered during task verification. Telling WorkManaget to cancel unknown task.", persistenceId(), taskId);
            sendWorkManagerCancel(taskId);
        }
    }

    private void handleSetProjectInformation(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();
        persist(eventFactory(userId).projectInformation(command.getSetProjectInformation()));
    }

    private void handleSetProjectPermission(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();
        persist(eventFactory(userId).projectPermission(command.getSetProjectPermission()));
    }

    private void validateCommandPermission(UUID userId) {
        if (data.status() != ProjectAggregateState.Status.CREATED) {
            throw new IllegalArgumentException("Project not created.");
        }
        if (!readPermission(userId)) {
            throw new IllegalArgumentException("Permission denied");
        }
    }

    private void handleSetSampleInformation(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();
        final ProjectAggregateCommand.SetSampleInformation setSampleInformation = command.getSetSampleInformation();
        final UUID sampleId = uuidFrom(setSampleInformation.getSampleId());
        if (!data.sampleIdToSample().containsKey(sampleId)) {
            throw new IllegalArgumentException("Sample " + sampleId + " does not exist");
        }
        persist(eventFactory(userId).sample(sampleId).setInformation(setSampleInformation.getSampleInformation()));
    }

    private void handleSetAnalysisInformation(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();
        final ProjectAggregateCommand.SetAnalysisInformation setAnalysisInformation = command.getSetAnalysisInformation();
        final UUID analysisId = uuidFrom(setAnalysisInformation.getAnalysisId());
        ProjectAnalysisState analysis = data.analysisIdToAnalysis().get(analysisId);
        if (analysis == null) {
            throw new IllegalArgumentException("Analysis " + analysisId + " does not exist");
        }
        AnalysisInformation analysisInformation = setAnalysisInformation.getAnalysisInformation();
        if (analysisInformation.getName().isEmpty()) {
            analysisInformation = analysisInformation.toBuilder()
                .setName(analysis.name())
                .build();
        }
        persist(eventFactory(userId).analysis(analysisId).setInformation(analysisInformation));

        final String newDescription = analysisInformation.getDescription();
        if (!newDescription.equals(analysis.description()) && newDescription.matches(RESOURCE_OVERRIDE_PATTERN)) {
            // override resource in work manager
            analysis.stepTaskPlanId().values().stream().flatMap(PSet::stream).forEach(taskPlanId -> {
                ProjectTaskPlanState plan = data.taskPlanIdToTaskPlan().get(taskPlanId);
                final Optional<ProjectTaskState> taskState = data.taskState(plan);
                if (taskState.isPresent()) {
                    final UUID taskId = taskState.get().taskId();
                    sendWorkManagerResourceOverride(taskId, newDescription);
                }
            });
        }
    }

    private void setAnalysisPriority(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        rejectCommandDuringArchiving();
        final ProjectAggregateCommand.SetAnalysisPriority setAnalysisPriority = command.getSetAnalysisPriority();
        final UUID analysisId = uuidFrom(setAnalysisPriority.getAnalysisId());
        final ProjectAnalysisState analysis = data.analysisIdToAnalysis().get(analysisId);
        if (analysis == null) {
            throw new IllegalArgumentException("Analysis " + analysisId + " does not exist");
        }
        final int priority = setAnalysisPriority.getPriority();
        if (priority != analysis.priority()) {
            log.info("{}: Setting priority {} to analysis {}", persistenceId(), priority, analysisId);
            persist(eventFactory(userId).analysis(analysisId).setPriority(priority));
            switch (analysis.status()) {
                case CREATED:
                case PROCESSING:
                    recalculateTaskPriority();
            }
        }
    }

    private void cleanupUnfinishedUploads() throws Exception {
        Instant systemStart = Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean().getStartTime());
        if (data.lastUpdatedTimeStamp().isAfter(systemStart)) {
            // Note this only works when there is one master node.
            // If Uploader is ever separated from master, or Uploader implements a persistence queue, then we should NOT timeout QUEUED.
            return;
        }
        for (ProjectSampleState sample : data.sampleIdToSample().values()) {
            UUID sampleId = sample.sampleId();
            for (ProjectFractionState fraction : sample.fractionState().values()) {
                switch (fraction.uploadState()) {
                    case PENDING:
                        log.info("{}: Time out pending upload for sample {} fraction {} that never started from before system restart.", persistenceId(), sampleId, fraction.fractionID());
                        break;
                    case QUEUED: // If ever Uploader persists the queue, then this should never happen.
                        log.info("{}: Time out pending upload {} for sample {} fraction {} that never started from before system restart.", persistenceId(), fraction.uploadId(), sampleId, fraction.fractionID());
                        break;
                    case STARTED:
                        log.info("{}: Time out pending upload {} for sample {} fraction {} that never started from before system restart.", persistenceId(), fraction.uploadId(), sampleId, fraction.fractionID());
                        break;
                    default:
                        continue;
                }
                persist(eventFactory(ADMIN_USERID)
                    .sample(sampleId)
                    .uploadTimeout(fraction.fractionID(), "Upload was never completed before system restart")
                );
            }
        }
    }

    private void handleRemoteUploadQueueNotification(ProjectAggregateCommand projectAggregateCommand) throws Exception {
        final ProjectAggregateCommand.RemoteUploadQueueNotification notification = projectAggregateCommand.getRemoteUploadQueueNotification();
        final UUID userId = uuidFrom(projectAggregateCommand.getUserId());
        ProjectAggregateCommandFactory projectAggregateCommandFactory = commandFactory(userId);
        ProjectAggregateEventFactory eventFactory = eventFactory(userId);

        List<ProjectAggregateEvent> events = new ArrayList<>();
        // First gather all events, through normal notification code path. This will force validation before persistence begins.
        for (RemoteSample sample : notification.getSamplesList()) {
            UUID sampleId = uuidFrom(sample.getSampleId());
            ProjectAggregateCommandFactory.SampleCommandFactory sampleCommandFactory = commandFactory.sample(sampleId);
            for (RemoteFraction fraction : sample.getFractionsList()) {
                UUID fractionId = uuidFrom(fraction.getFractionId());
                UUID uploadId = uuidFrom(fraction.getUploadId());
                UploadNotification uploadNotification = sampleCommandFactory.uploadFraction(fractionId, uploadId)
                    .queue(fraction.getRemotePathName(), fraction.getSubPath())
                    .getUploadNotification();
                ProjectAggregateEvent event = calculateUploadNotificationEvent(eventFactory, uploadNotification);
                if (event != null) {
                    events.add(event);
                }
            }
        }

        // Persist after, in order to have all or nothing approach to bulk command.
        for (ProjectAggregateEvent event : events) {
            persist(event);
        }
    }

    private void handleUpdateFractionName(ProjectAggregateCommand projectAggregateCommand) throws Exception {
        ProjectAggregateCommand.UpdateFractionName updateFractionName = projectAggregateCommand.getUpdateFractionName();
        final UUID userId = uuidFrom(projectAggregateCommand.getUserId());
        ProjectAggregateEventFactory eventFactory = eventFactory(userId);
        ProjectAggregateEvent updatedFractionNameEvent =
            eventFactory.sample(uuidFrom(updateFractionName.getSampleId()))
            .updatedFractionName(uuidFrom(updateFractionName.getFractionId()), updateFractionName.getNewName());
        persist(updatedFractionNameEvent);
    }

    private void handleUpdateEnzyme(ProjectAggregateCommand projectAggregateCommand) throws Exception {
        final ProjectAggregateCommand.UpdateSampleEnzyme updateSampleEnzyme = projectAggregateCommand.getUpdateSampleEnzyme();
        final UUID userId = uuidFrom(projectAggregateCommand.getUserId());
        ProjectAggregateEventFactory eventFactory = eventFactory(userId);
        ProjectAggregateEvent updatedSampleEnzymeEvent =
            eventFactory.sample(uuidFrom(updateSampleEnzyme.getSampleId()))
                .updatedSampleEnzyme(updateSampleEnzyme.getSampleEnzyme());
        persist(updatedSampleEnzymeEvent);
    }

    private void handleUpdateActivationMethod(ProjectAggregateCommand projectAggregateCommand) throws Exception {
        final ProjectAggregateCommand.UpdateSampleActivationMethod updateSampleActivationMethod =
            projectAggregateCommand.getUpdateSampleActivationMethod();
        final UUID userId = uuidFrom(projectAggregateCommand.getUserId());
        ProjectAggregateEventFactory eventFactory = eventFactory(userId);
        ProjectAggregateEvent updatedSampleActivationMethodEvent =
            eventFactory.sample(uuidFrom(updateSampleActivationMethod.getSampleId()))
                .updatedSampleActivationMethod(updateSampleActivationMethod.getSampleActivationMethod());
        persist(updatedSampleActivationMethodEvent);
    }

    private void handleUploadNotification(ProjectAggregateCommand projectAggregateCommand) throws Exception {
        UUID userId = uuidFrom(projectAggregateCommand.getUserId());
        ProjectAggregateEventFactory eventFactory = eventFactory(userId);
        ProjectAggregateEvent event = calculateUploadNotificationEvent(eventFactory, projectAggregateCommand.getUploadNotification());
        if (event == null) return;
        persist(event);

        ProjectAggregateEvent.SampleEvent sampleEvent = event.getSample();

        // If Upload Completed, then start DataLoading.
        if (sampleEvent.hasUploadDone()) {
            ProjectAggregateEvent.SampleEvent.UploadCompleted uploadDone = sampleEvent.getUploadDone();

            UUID sampleId = uuidFrom(sampleEvent.getSampleId());
            UUID fractionId = uuidFrom(uploadDone.getFractionId());

            ProjectSampleState sample = data.sampleIdToSample().get(sampleId);
            ProjectFractionState fraction = sample.fractionState().get(fractionId);

            UUID taskplanId = UUID.randomUUID();
            final String sourceFile = sample.fractionState().get(fractionId).srcFile();
            final DataLoadingTask dataLoadingTask = new DataLoadingTaskBuilder()
                .fractionId(fractionId)
                .uploadedFile(uploadDone.getSourceFile())
                .sourceFile(sourceFile)
                .activationMethod(ActivationMethod.from(sample.activationMethod()))
                .acquisitionMethod(AcquisitionMethod.from(sample.acquisitionMethod()))
                .enzyme(Enzyme.from(sample.enzyme()))
                .instrument(Instrument.from(sample.instrument()))
                .build();
            TaskDescription taskDescription = CommonTasksFactory.taskDescriptionFractionLevel(
                dataLoadingTask,
                StepType.DATA_LOADING,
                sampleId,
                fractionId
            ).toBuilder().setKeyspace(keyspace()).build();
            persist(eventFactory.taskplan(taskplanId).created(taskDescription, data.numberOfRetriesPerTask()));
            persist(eventFactory.sample(sampleId).taskplanAdded(taskplanId));

            submitTask(taskplanId, taskDescription);
        }
    }

    private ProjectAggregateEvent calculateUploadNotificationEvent(ProjectAggregateEventFactory eventFactory, UploadNotification uploadNotification) throws Exception {
        final UUID sampleId = uuidFrom(uploadNotification.getSampleId());
        final ProjectSampleState sampleState = data.sampleIdToSample().get(sampleId);
        if (sampleState == null) {
            throw new IllegalStateException("Sample " + sampleId + " does not exist");
        }

        final UUID fractionId = uuidFrom(uploadNotification.getFractionId());
        final ProjectFractionState fractionState = sampleState.fractionState().get(fractionId);
        if (fractionState == null) {
            throw new IllegalStateException("Fraction " + fractionId + " does not exist");
        }

        final UUID uploadId = uuidFrom(uploadNotification.getUploadId());
        final UploadState currentUploadState = fractionState.uploadState();

        switch (uploadNotification.getNotificationCase()) {
            case QUEUED: {
                /*
                if (sampleState.hasFailedTask()) {
                    throw new IllegalStateException("Sample " + sampleId + " has failed task. All further uploads refused.");
                }
                 */
                switch (currentUploadState) {
                    case PENDING:
                        break;
                    case QUEUED:
                        if (uploadId.equals(fractionState.uploadId())) {
                            // duplicate message. Already queued.
                            return null;
                        } else {
                            log.info( persistenceId() + ": Queued upload of sample {} fraction {}, replacing upload {} with upload {}.", sampleId, fractionId, fractionState.uploadId(), uploadId);
                        }
                    case TIMEOUT:
                        log.info("{}: Queued upload of sample {} fraction {} that previously timed out.", persistenceId(), sampleId, fractionId);
                        break;
                    case FAILED:
                        log.info("{}: Queued upload of sample {} fraction {} that previously failed during upload.", persistenceId(), sampleId, fractionId);
                        break;
                    case COMPLETED:
                        log.info("{}: Queued upload of sample {} fraction {} that previously completed during upload, this should come from retry", persistenceId(), sampleId, fractionId);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected uploadState " + currentUploadState);
                }
                UploadNotification.UploadQueued queued = uploadNotification.getQueued();
                return eventFactory.sample(sampleId).uploadQueued(fractionId, uploadId, queued.getRemotePathName(), queued.getSubPath());
            }
            case STARTED: {
                if (sampleState.hasFailedTask()) {
                    throw new IllegalStateException("Sample " + sampleId + " has failed task. All further uploads refused.");
                }
                // Do not remove this check! To overcome a cancelled sample, QUEUE an upload. This will update the state to no longer be cancelled.
                // Resuming a cancelled sample could be formalized into a separate event. But let's not commit ourselves to that behaviour just yet. Too many event types will pollute our event history.
                if (sampleState.isCancelled()) {
                    throw new IllegalStateException("Sample " + sampleId + " is cancelled. All further uploads refused.");
                }
                switch (currentUploadState) {
                    case PENDING:
                        return eventFactory.sample(sampleId).uploadStarted(fractionId, uploadId);
                    case QUEUED:
                        // When the upload is queued, an uploadId was assigned.
                        if (uploadId.equals(fractionState.uploadId())) {
                            return eventFactory.sample(sampleId).uploadStarted(fractionId, uploadId);
                        } else {
                            throw new IllegalStateException("Unexpected uploadId " + uploadId);
                        }
                    default:
                        if (uploadId.equals(fractionState.uploadId())) {
                            // duplicate message. Already started.
                            return null;
                        } else {
                            throw new IllegalStateException("Upload already started");
                        }
                }
            }
            case FAILED: {
                if (uploadId.equals(fractionState.uploadId())) {
                    switch (currentUploadState) {
                        case PENDING:
                            throw new IllegalStateException("Cannot complete upload that never started");
                        case STARTED:
                            final UploadNotification.UploadFailed failed = uploadNotification.getFailed();
                            return eventFactory.sample(sampleId).uploadFailed(fractionId, uploadId, failed.getError());
                        case FAILED:
                            // duplicate message. Already completed.
                            return null;
                        default:
                            throw new IllegalStateException("Unexpected uploadState " + currentUploadState);
                    }
                } else {
                    throw new IllegalStateException("Unexpected uploadId " + uploadId);
                }
            }
            case COMPLETED: {
                if (uploadId.equals(fractionState.uploadId())) {
                    switch (currentUploadState) {
                        case PENDING:
                            throw new IllegalStateException("Cannot complete upload that never started");
                        case STARTED:
                            final UploadNotification.UploadCompleted completed = uploadNotification.getCompleted();
                            return eventFactory.sample(sampleId).uploadCompleted(fractionId, uploadId, completed.getSourceFile());
                        case COMPLETED:
                            // duplicate message. Already completed.
                            return null;
                        default:
                            throw new IllegalStateException("Unexpected uploadState " + currentUploadState);
                    }
                } else {
                    throw new IllegalStateException("Unexpected uploadId " + uploadId);
                }
            }
            default: {
                throw new IllegalStateException("Unexpected UploadNotification " + uploadNotification.getNotificationCase());
            }
        }
    }

    private void handleTaskNotification(ProjectAggregateCommand command) throws Exception {
        final TaskNotification taskNotification = command.getTaskNotification();
        final UUID taskId = uuidFrom(taskNotification.getTaskId());
        switch (taskNotification.getNotificationCase()) {
            case PROCESSING: {
                final TaskNotification.Processing processing = taskNotification.getProcessing();
                final ProjectTaskState taskState = data.taskIdToTask().get(taskId);
                if (taskState == null) {
                    throw new IllegalStateException("Task " + taskId + " does not exist");
                }
                switch (taskState.progress()) {
                    case DONE:
                    case FAILED:
                    case STOPPING:
                        throw new IllegalStateException("Task " + taskId + " is " + taskState.progress());
                    case IN_PROGRESS:
                        // Duplicate, already processing.
                        return;
                }
                final UUID workerId = uuidFrom(processing.getWorkerId());
                final ProjectAggregateEvent event = eventFactory(ADMIN_USERID)
                    .taskplan(taskState.taskPlanId())
                    .processing(taskId, workerId, processing.getWorkerIPAddress());
                persist(event);
                return;
            }
            case UPDATEPROGRESS: {
                final TaskNotification.UpdateProgress updateProgress = taskNotification.getUpdateProgress();
                final ProjectTaskState taskState = data.taskIdToTask().get(taskId);
                data = data.plusTask(taskId, task -> {
                    if (taskState == null) {
                        throw new IllegalStateException("Task " + taskId + " does not exist");
                    }
                    switch (taskState.progress()) {
                        case DONE:
                        case FAILED:
                        case CANCELLED:
                            throw new IllegalStateException("Task " + taskId + " is " + taskState.progress());
                    }
                    return task.withProcessingProgress(updateProgress.getProgress());
                });
                return;
            }
            case COMPLETED: {
                final TaskNotification.Completed completed = taskNotification.getCompleted();
                final ProjectTaskState taskState = data.taskIdToTask().get(taskId);
                if (taskState == null) {
                    throw new IllegalStateException("Task " + taskId + " does not exist");
                }
                switch (taskState.progress()) {
                    case DONE:
                        log.info("{}: Ignoring duplicate task {} COMPLETED notification.", persistenceId(), taskId);
                        return;
                    case FAILED:
                    case CANCELLED:
                        throw new IllegalStateException("Task " + taskId + " is " + taskState.progress());
                }

                final TaskEventFactory taskEventFactory = eventFactory(ADMIN_USERID).taskplan(taskState.taskPlanId());
                switch (completed.getTaskOuputCase()) {
                    case TASKOUPUT_NOT_SET:
                        persist(taskEventFactory.done(taskId));
                        break;
                    case LFQSUMMARIZATIONFILTERRESULT:
                        persist(taskEventFactory.done(taskId, completed.getLfqSummarizationFilterResult()));
                        break;
                    case SLFILTERSUMMARIZATIONRESULT:
                        persist(taskEventFactory.done(taskId, completed.getSlFilterSummarizationResult()));
                        break;
                    case TASKOUTPUTJSON:
                        persist(taskEventFactory.doneJson(taskId, completed.getTaskOutputJson()));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected TaskOutput " + completed.getTaskOuputCase() + " for taskId " + taskId);
                }

                UUID taskPlanId = taskState.taskPlanId();
                final StepType stepType = data.taskPlanIdToTaskPlan().get(taskPlanId).stepType();
                for(ProjectAnalysisState analysis : data.analysisIdToAnalysis().values()) {
                    boolean hasTask = Optional.ofNullable(analysis.stepTaskPlanId().get(stepType))
                        .filter(planIds -> planIds.contains(taskPlanId))
                        .isPresent();
                    if (hasTask) {
                        final UUID analysisId = analysis.analysisId();
                        onStepCompleteSetFilter(analysisId, stepType);
                        final AnalysisTaskInformation analysisTaskInformation = AnalysisTaskInformation.create(keyspace(), data, projectId, analysisId);
                        if (data.isAnalysisDone(analysisTaskInformation, analysis)) {
                            persist(eventFactory(ADMIN_USERID).analysis(analysisId).done());
                        }
                    }
                }

                defer(RUN_WORK_CREATOR, this::sendSelf);
                return;
            }
            case FAILED: {
                final TaskNotification.Failed failed = taskNotification.getFailed();
                final ProjectTaskState taskState = data.taskIdToTask().get(taskId);
                if (taskState == null) {
                    throw new IllegalStateException("Task " + taskId + " does not exist");
                }
                switch (taskState.progress()) {
                    case DONE:
                    case FAILED:
                        throw new IllegalStateException("Task " + taskId + " is " + taskState.progress());
                    case QUEUED:
                    case IN_PROGRESS:
                        // Retry
                        submitTask(taskState.taskPlanId(), failed.getCause().endsWith("OOM"));
                }
                persist(eventFactory(uuidFrom(command.getUserId())).taskplan(taskState.taskPlanId()).failed(taskId, failed.getCause()), this::recalculateTaskPriority);
                deleteTask(taskState);
                return;
            }
            case NOTFOUND: {
                final ProjectTaskState taskState = data.taskIdToTask().get(taskId);
                if (taskState == null) {
                    throw new IllegalStateException("Task " + taskId + " does not exist");
                }
                switch (taskState.progress()) {
                    case DONE:
                    case FAILED:
                        // Doesn't matter that WorkManager could not find task... it is already terminated.
                        return;
                    case PENDING:
                    case QUEUED:
                    case IN_PROGRESS:
                        // Retry
                        submitTask(taskState.taskPlanId());
                }
                persist(eventFactory(uuidFrom(command.getUserId())).taskplan(taskState.taskPlanId()).failed(taskId, "WorkManager cannot find task"));
                deleteTask(taskState);
                return;
            }
            case DELETED: {
                final ProjectTaskState taskState = data.taskIdToTask().get(taskId);
                if (taskState == null) {
                    throw new IllegalStateException("Task " + taskId + " does not exist");
                }
                if (taskState.dataState() == DataState.DELETED) {
                    return; // Ignore duplicate delete notification.
                }
                persist(eventFactory(uuidFrom(command.getUserId())).taskplan(taskState.taskPlanId()).deleted(taskId));
                switch (data.status()) {
                    case DELETING:
                    case DELETED:
                        if (data.allTasksAreDeleted()) {
                            deleteProject();
                        }
                }
                return;
            }
            default:
                throw new IllegalStateException("Unexpected TaskNotification " + taskNotification.getNotificationCase() + " for taskId " + taskId);
        }
    }

    private void deleteProject() throws Exception {
        taskDeleter.deleteProject(projectId).whenComplete((done, throwable) -> {
            if (throwable != null) {
                log.error(throwable, "{}: Exception while deleting project.", persistenceId());
            }
        });
        persist(eventFactory(ADMIN_USERID).projectDeleted());
        defer(Done.getInstance(), ignore -> deleteSelf());
    }

    private void handleRestartFailedUploads(ProjectAggregateCommand command) throws Exception {
        UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        ProjectAggregateEventFactory eventFactory = eventFactory(userId);
        List<FileToUpload> files = new ArrayList<>();
        Map<UUID, RemoteSample.Builder> samples = new HashMap<>();
        for (ProjectSampleState sample : data.sampleIdToSample().values()) {

            // Short circuit - nothing to retry.
            if (sample.allTasksDone()) continue;

            if (sample.hasFailedTask()) {

                // We must wait until failed tasks are finished deleting.
                // Any attempt to circumvent this check can lead to Data Loading occurring at the same time as delete.
                // Do no introduce this race condition. Do not risk losing data.
                boolean allFailedTasksAreDeleted = true;
                for (ProjectFractionState fractionState : sample.fractionState().values()) {
                    for (UUID taskPlanId : fractionState.stepTaskPlanId().values()) {
                        if (data.taskState(taskPlanId).isPresent()) {
                            ProjectTaskState taskState = data.taskState(taskPlanId).get();
                            if (taskState.progress() == com.bsi.peaks.model.system.Progress.FAILED && taskState.dataState() != DataState.DELETED) {
                                allFailedTasksAreDeleted = false;
                                ProjectTaskPlanState taskPlanState = data.taskPlanIdToTaskPlan().get(taskPlanId);
                                // try to delete it again
                                deleteQueue.offer(Tuple3.create(taskPlanState.stepType(), taskState.taskId(), taskPlanState.level()));
                            }
                        }
                    }
                }

                if (!allFailedTasksAreDeleted) {
                    log.info("{}: Skipping retry of uploading sample {} fraction {}, since failed tasks are still deleting.");
                    continue;
                }
            }
            final UUID sampleId = sample.sampleId();
            for (ProjectFractionState fraction : sample.fractionState().values()) {
                final UUID fractionId = fraction.fractionID();
                if (Strings.isNullOrEmpty(fraction.remotePathName())) continue; // We can only restart remote uploads.
                switch (fraction.uploadState()) {
                    case STARTED:
                        log.info("{}: Skipping retry of remote upload for sample {} fraction {}, since it is already in progress", persistenceId(), sampleId, fractionId);
                        continue;
                    case PENDING:
                        log.info("{}: Remote upload for sample {} fraction {} was never queued. Requesting uploader to retry.", persistenceId(), sampleId, fractionId);
                        break;
                    case QUEUED:
                        log.info("{}: Remote upload for sample {} fraction {} was already queued. Requesting uploader to retry.", persistenceId(), sampleId, fractionId);
                        break;
                    case FAILED:
                        log.info("{}: Remote upload for sample {} fraction {} previously failed. Requesting uploader to retry.", persistenceId(), sampleId, fractionId);
                        break;
                    case TIMEOUT:
                        log.info("{}: Remote upload for sample {} fraction {} previously timed out. Requesting uploader to retry.", persistenceId(), sampleId, fractionId);
                        break;
                    case COMPLETED:
                        if (data.fractionHasFailedTask(fraction)) {
                            // We already checked that data is finished deleting above, so we can safely proceed to upload again.
                            log.info("{}: Remote upload for sample {} fraction {} previously failed during task processing. Requesting uploader to retry, and process again.", persistenceId(), sampleId, fractionId);
                            break;
                        } else {
                            // No problem fix.
                            continue;
                        }
                }
                files.add(FileToUpload.newBuilder()
                    .setFile(FilePath.newBuilder()
                        .setRemotePathName(fraction.remotePathName())
                        .setSubPath(fraction.subPath())
                        .build()
                    )
                    .setProjectId(projectId.toString())
                    .setSampleId(sampleId.toString())
                    .setFractionId(fractionId.toString())
                    .setUploadTaskId(UUID.randomUUID().toString()) // Generate new uploadId, to avoid conflating with previous upload attempts.
                    .build()
                );
            }
        }
        if (files.isEmpty()) {
            log.warning("{}: Retry of failed samples requested, but none available for retry.", persistenceId());
            return;
        }
        uploaderService.upload(UploadFiles.newBuilder().addAllFiles(files).build());
    }

    private void handleArchiveNotification(ProjectAggregateCommand command) throws Exception {
        UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);
        ArchiveNotification archiveNotification = command.getArchiveNotification();
        switch (archiveNotification.getNotificationCase()) {
            case START:
                if (query.projectProgress(data) != Progress.State.DONE) {
                    throw new IllegalStateException("Cannot archive unless project is done processing");
                }
                if (data.archiveState() != ArchiveStateEnum.ARCHIVING) {
                    persist(eventFactory(userId).archiveState(ArchiveStateEnum.ARCHIVING));
                }
                return;
            case DONE:
                // If for any reason, state is NOT_ARCHIVED, do not change it to ARCHIVED.
                if (data.archiveState() == ArchiveStateEnum.ARCHIVING) {
                    persist(eventFactory(userId).archiveState(ArchiveStateEnum.ARCHIVED));
                }
                return;
            case CANCEL:
                if (data.archiveState() == ArchiveStateEnum.ARCHIVING) {
                    persist(eventFactory(userId).archiveState(ArchiveStateEnum.NOT_ARCHIVED));
                }
                return;
            default:
                throw new IllegalArgumentException("Unexpected Archive Notification " + archiveNotification.getNotificationCase());
        }
    }

    private void submitTask(UUID taskPlanId, TaskDescription taskDescription) throws Exception {
        final PMap<UUID, ProjectTaskPlanState> taskPlans = data.taskPlanIdToTaskPlan();
        ProjectTaskPlanState taskPlan = taskPlans.get(taskPlanId);
        if (taskPlan == null) {
            throw new IllegalStateException("TaskPlan " + taskPlanId + " does not exist");
        }
        if (taskPlan.processAttemptsRemaining() > 0) {
            switch (taskDescription.getTaskCase()) {
                case JSONPARAMATERS:
                    if (taskPlan.jsonTaskParamaters() == null) {
                        log.info("{}: Adding back parameters for previously completed taskPlan {}", persistenceId(), taskPlanId);
                        taskPlan = taskPlan.withJsonTaskParamaters(taskDescription.getJsonParamaters());
                        data = data.withTaskPlanIdToTaskPlan(taskPlans.plus(taskPlan.taskPlanId(), taskPlan));
                    }
                    break;
                case PARAMETERS:
                    if (taskPlan.taskParameters() == null) {
                        log.info("{}: Adding back parameters for previously completed taskPlan {}", persistenceId(), taskPlanId);
                        taskPlan = taskPlan.withTaskParameters(taskDescription.getParameters());
                        data = data.withTaskPlanIdToTaskPlan(taskPlans.plus(taskPlan.taskPlanId(), taskPlan));
                    }
                    break;
            }
            submitTask(taskPlan, false);
        } else {
            log.warning("Task plan {} have no attempts remaining", taskPlan.taskPlanId());
        }
    }

    private void submitTask(UUID taskPlanId) throws Exception {
        submitTask(taskPlanId, false);
    }

    private void submitTask(UUID taskPlanId, boolean oom) throws Exception {
        final ProjectTaskPlanState taskPlan = data.taskPlanIdToTaskPlan().get(taskPlanId);
        if (taskPlan == null) {
            throw new IllegalStateException("Taskplan " + taskPlanId + " does not exist");
        }
        if (taskPlan.processAttemptsRemaining() > 0 && taskPlan.stepType() != StepType.DATA_LOADING) {
            // we don't retry data loading task here as they will almost always fail
            // handle the retry logic in the loader itself
            submitTask(taskPlan, oom);
        }
    }

    private void submitTask(ProjectTaskPlanState taskPlan, boolean oom) throws Exception {
        submitTask(taskPlan, data.taskPlanPriority(taskPlan), oom);
    }

    private String findResourceOverride(UUID taskPlanId) {
        Optional<ProjectAnalysisState> analysis = data.analysisIdToAnalysis().values().stream()
            .filter(a -> a.description().matches(RESOURCE_OVERRIDE_PATTERN) &&
                a.stepTaskPlanId().values().stream().flatMap(Collection::stream).anyMatch(id -> id.equals(taskPlanId)))
            .findFirst();
        return analysis.isPresent() ? analysis.get().description() : "";
    }

    private void submitTask(ProjectTaskPlanState taskPlan, int priority, boolean oom) throws Exception {
        if (priority == 0) return;
        final TaskEventFactory taskEventFactory = eventFactory(ADMIN_USERID).taskplan(taskPlan.taskPlanId());
        final UUID taskId = UUID.randomUUID();
        final ProjectAggregateEvent event = taskEventFactory.queued(taskId, priority);
        final String keyspace = keyspace();
        //log.info("{}: Submit taskPlan {} task {} to work manager", persistenceId(), taskPlan.taskPlanId(), taskId);

        final TaskDescription description = taskDescriptionWithTaskId(taskPlan, taskId, keyspace).toBuilder()
            .setResourceOverride(findResourceOverride(taskPlan.taskPlanId()))
            .build();

        final SubmitTask submitTask = SubmitTask.newBuilder()
            .setTaskId(taskId.toString())
            .setProjectId(projectId.toString())
            .setPriority(priority)
            .setTaskDescription(description)
            .setDoubleResource(oom)
            .build();

        //persist(event, () -> sendWorkManagerCommand(b -> b.setSubmitTask(submitTask)));
        // queue task submission, send to work manager later
        persist(event, () -> batchedTasks.add(submitTask));
    }

    private String keyspace() {
        return data.storageKeyspace().orElseGet(storageFactory::getSystemKeyspace);
    }

    private void recalculateTaskPriority() throws Exception {
        for (ProjectTaskPlanState taskPlan : data.taskPlanIdToTaskPlan().values()) {
            final int planPriority = data.taskPlanPriority(taskPlan);
            final Optional<ProjectTaskState> taskState = data.taskState(taskPlan);
            if (taskState.isPresent()) {
                final ProjectTaskState task = taskState.get();
                switch (task.progress()) {
                    case QUEUED:
                    case IN_PROGRESS:
                        final UUID taskId = task.taskId();
                        if (planPriority == 0) {
                            sendWorkManagerCancel(taskId);
                            persist(eventFactory(ADMIN_USERID).taskplan(taskPlan.taskPlanId()).cancelled(taskId));
                        } else if (task.priority() != planPriority) {
                            sendWorkManagerTaskPriority(taskId, planPriority);
                        }
                        break;
                    case CANCELLED: {
                        submitTask(taskPlan, planPriority, false);
                        break;
                    }
                }
            } else {
                submitTask(taskPlan, planPriority, false);
            }
        }
    }

    private void sendWorkManagerTaskPriority(UUID taskId, int planPriority) {
        sendWorkManagerCommand(b -> b.setTaskPriority(
            TaskPriority.newBuilder()
                .setProjectId(projectId.toString())
                .setTaskId(taskId.toString())
                .setPriority(planPriority)
                .build()
        ));
    }

    private void sendWorkManagerResourceOverride(UUID taskId, String resourceOverride) {
        sendWorkManagerCommand(b -> b.setTaskResourceOverride(
            TaskResourceOverride.newBuilder()
                .setProjectId(projectId.toString())
                .setTaskId(taskId.toString())
                .setResourceOverride(resourceOverride)
                .build()
        ));
    }

    private void deleteUnreferencedDoneTasks() throws Exception {
        for (ProjectTaskPlanState taskPlan : data.taskPlanIdToTaskPlan().values()) {
            if (data.isTaskPlanReferencedBySample(taskPlan)) continue;
            if (data.isTaskPlanReferencedByAnalysis(taskPlan)) continue;
            final Optional<ProjectTaskState> taskState = data.taskState(taskPlan);
            if (taskState.isPresent()) {
                final ProjectTaskState task = taskState.get();
                if (task.dataState() == DataState.READY) {
                    deleteTask(taskPlan, task.taskId());
                }
            }
        }
    }

    private void deleteTask(ProjectTaskState task) throws Exception {
        final ProjectTaskPlanState taskPlan = data.taskPlanIdToTaskPlan().get(task.taskPlanId());
        deleteTask(taskPlan, task.taskId());
    }

    private void deleteTask(ProjectTaskPlanState taskPlan, UUID taskId) throws Exception {
        persist(
            eventFactory(ADMIN_USERID).taskplan(taskPlan.taskPlanId()).deleting(taskId),
            () -> deleteQueue.offer(Tuple3.create(taskPlan.stepType(), taskId, taskPlan.level()))
        );
    }

    private void handleSetFilter(ProjectAggregateCommand command) throws Exception {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);

        final ProjectAggregateCommand.SetFilter setFilter = command.getSetFilter();
        final UUID analysisId = uuidFrom(setFilter.getAnalysisId());

        final ProjectAnalysisState analysis = data.analysisIdToAnalysis().get(analysisId);
        if (analysis == null) {
            throw new IllegalStateException("Analysis " + analysisId + " does not exist");
        }

        final WorkflowStepFilter filter = setFilter.getFilter();
        final WorkFlowType workFlowType = filter.getWorkFlowType();
        if (!query.isAnalysisWorkflowTypeStepDone(data, analysisId, workFlowType)) {
            throw new IllegalStateException("Analysis " + analysisId + " not done workflow " + workFlowType);
        }
        if (filter.getFilter().equals(analysis.workflowTypeFilter().get(workFlowType))) {
            // Duplicate filter set.
            return;
        }
        persist(eventFactory(userId).analysis(analysisId).setFilter(filter));
    }

    private void handleUserSetFilter(ProjectAggregateCommand command) {
        final UUID userId = uuidFrom(command.getUserId());
        validateCommandPermission(userId);

        ProjectAggregateCommand.UserSetFilter setFilter = command.getUserSetFilter();
        final UUID analysisId = uuidFrom(setFilter.getAnalysisId());
        final WorkflowStepFilter filter = setFilter.getFilter();
        final WorkFlowType workFlowType = filter.getWorkFlowType();
        final FilterList filterFilter = filter.getFilter();
        if (!query.isAnalysisWorkflowTypeStepDone(data, analysisId, workFlowType)) {
            throw new IllegalStateException("Analysis " + analysisId + " not done workflow " + workFlowType);
        }

        final CompletionStage<Optional<ProteinDecoyInfo>> futureDecoy;
        final AnalysisTaskInformation taskInformation = AnalysisTaskInformation.create(keyspace(), data, projectId, analysisId);
        switch (workFlowType) {
            case DB_SEARCH:
            case PTM_FINDER:
            case SPIDER:
                futureDecoy = query.proteinDecoyInfoFuture(taskInformation, workFlowType)
                    .thenApply(Optional::of);
                break;
            case SPECTRAL_LIBRARY:
                if (taskInformation.hasSlProteinInference()) {
                    futureDecoy = query.proteinDecoyInfoFuture(taskInformation, StepType.SL_FILTER_SUMMARIZATION)
                        .thenApply(Optional::of);
                } else {
                    futureDecoy = completedFuture(Optional.empty());
                }
                break;
                //if no protein inference return empty decoy info
            case DIA_DB_SEARCH:
                futureDecoy = query.proteinDecoyInfoFuture(taskInformation, StepType.DIA_DB_FILTER_SUMMARIZE)
                        .thenApply(Optional::of);
                break;
            //if no protein inference return empty decoy info
            default:
                futureDecoy = completedFuture(Optional.empty());
        }

        futureDecoy
            .thenAccept(proteinDecoyInfo -> {
                FilterList currentFilterList = data.analysisIdToAnalysis().get(analysisId).workflowTypeFilter().get(workFlowType);
                FilterList mergedFilterList = QueryParser.mergeFilters(
                    filterFilter,
                    currentFilterList,
                    fdr -> {
                        if (workFlowType.equals(WorkFlowType.SPECTRAL_LIBRARY)) {
                            SlFilter.ProteinHitFilter proteinHitFilter = filterFilter.getSlFilter().getProteinHitFilter();
                            return DecoyInfoWrapper.fdrToProScore(proteinDecoyInfo.get(), fdr, proteinHitFilter.getMinUniquePeptides());
                        } else if (workFlowType.equals(WorkFlowType.DIA_DB_SEARCH)) {
                            SlFilter.ProteinHitFilter proteinHitFilter = filterFilter.getDiaDbFilter().getProteinHitFilter();
                            return DecoyInfoWrapper.fdrToProScore(proteinDecoyInfo.get(), fdr, proteinHitFilter.getMinUniquePeptides());
                        } else {
                            IDFilter.ProteinHitFilter proteinHitFilter = filterFilter.getIdFilters(0).getProteinHitFilter();
                            return DecoyInfoWrapper.fdrToProScoreCountBased(proteinDecoyInfo.get(), fdr, proteinHitFilter.getMinUniquePeptides());
                        }
                    },
                    pValue -> {
                        if (workFlowType.equals(WorkFlowType.SPECTRAL_LIBRARY)) {
                            SlFilter.ProteinHitFilter proteinHitFilter = filterFilter.getSlFilter().getProteinHitFilter();
                            return DecoyInfoWrapper.proScoreToFdr(proteinDecoyInfo.get(), pValue, proteinHitFilter.getMinUniquePeptides());
                        } else if (workFlowType.equals(WorkFlowType.DIA_DB_SEARCH)) {
                            SlFilter.ProteinHitFilter proteinHitFilter = filterFilter.getDiaDbFilter().getProteinHitFilter();
                            return DecoyInfoWrapper.proScoreToFdr(proteinDecoyInfo.get(), pValue, proteinHitFilter.getMinUniquePeptides());
                        } else {
                            IDFilter.ProteinHitFilter proteinHitFilter = filterFilter.getIdFilters(0).getProteinHitFilter();
                            return DecoyInfoWrapper.proScoreToFdrCountBased(proteinDecoyInfo.get(), pValue, proteinHitFilter.getMinUniquePeptides());
                        }
                    }
                );

                if (currentFilterList.equals(mergedFilterList)) {
                    // Duplicate filter set.
                    return;
                }

                try {
                    ProjectAggregateEvent event = eventFactory(userId).analysis(analysisId)
                        .setFilter(WorkflowStepFilter.newBuilder()
                            .setWorkFlowType(workFlowType)
                            .setFilter(mergedFilterList)
                            .build());
                    persist(event);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).toCompletableFuture().join(); //TODO blocking logic find better alternative is to sent a filter set msg
    }

    private void onStepCompleteSetFilter(UUID analysisId, StepType stepType) {
        final ProjectAnalysisState analysis = data.analysisIdToAnalysis().get(analysisId);
        final AnalysisTaskInformation analysisTaskInformation = AnalysisTaskInformation.create(keyspace(), data, projectId, analysisId);
        try {
            ProjectAggregateCommandFactory.AnalysisCommandFactory analysisCommandFactory = commandFactory(ADMIN_USERID)
                .analysis(analysisId);
            final WorkFlowType workFlowType;
            final CompletionStage<FilterList> futureFilterList;
            AnalysisTaskInformation taskInformation = AnalysisTaskInformation.create(keyspace(), data, projectId, analysisId);
            WorkflowStep workflowStep = taskInformation.workflowStep().get(stepType);
            switch (stepType) {
                case DENOVO_FILTER:
                    workFlowType = ModelConversion.workflowType(stepType);
                    futureFilterList = CompletableFuture.completedFuture(FilterList.newBuilder()
                        .addIdFilters(IDFilter.newBuilder()
                            .setDenovoCandidateFilter(new DenovoCandidateFilterBuilder()
                                .from(DenovoCandidateFilter.DEFAULT)
                                .minAlc(workflowStep.getDenovoFilterParameters().getMinDenovoAlc())
                                .build().toDto()
                            )
                            .build()).build()
                    );
                    break;
                case DB_DENOVO_ONLY_TAG_SEARCH:
                case DB_FILTER_SUMMARIZATION:
                case PTM_FINDER_DENOVO_ONLY_TAG_SEARCH:
                case PTM_FINDER_FILTER_SUMMARIZATION:
                case SPIDER_DENOVO_ONLY_TAG_SEARCH:
                case SPIDER_FILTER_SUMMARIZATION:
                    workFlowType = ModelConversion.workflowType(stepType);
                    //for full db DB_FILTER_SUMMARIZATION is not the last step and there are many last DB_DENOVO_ONLY_TAG_SEARCH (per sample)
                    if (!taskInformation.isAnalysisWorkflowTypeStepDone(workFlowType)) {
                        return;
                    }
                    futureFilterList = query.psmPeptideDecoyInfoFuture(taskInformation, workFlowType)
                        .thenCombine(query.proteinDecoyInfoFuture(taskInformation, workFlowType),
                            (psmPeptide, protein) -> Tuple3.create(psmPeptide.first(), psmPeptide.second(), protein))
                        .thenApply(decoyInfos -> {
                                DecoyInfo psm = decoyInfos.t1();
                                DecoyInfo peptide = decoyInfos.t2();
                                ProteinDecoyInfo protein = decoyInfos.t3();
                                return FilterList.newBuilder()
                                    .addIdFilters(IDFilter.newBuilder()
                                        .setDenovoCandidateFilter(query.getDenovoOnlyFilter(analysis, workFlowType).toDto())
                                        .setPsmFilter(query.getPsmFilter(analysis, workFlowType, psm))
                                        .setPeptideFilter(query.getPeptideFilter(analysis, workFlowType, peptide))
                                        .setProteinHitFilter(ProteinHitFilter.fromDto(workflowStep.getDbProteinHitFilter(),
                                            fdr -> DecoyInfoWrapper.fdrToProScoreCountBased(protein, fdr, workflowStep.getDbProteinHitFilter().getMinUniquePeptides()),
                                            pvalue -> DecoyInfoWrapper.proScoreToFdrCountBased(protein, pvalue, workflowStep.getDbProteinHitFilter().getMinUniquePeptides())).toDto())
                                        .build())
                                    .build();
                            }
                        );
                    break;
                case DIA_DB_FILTER_SUMMARIZE:
                case SL_FILTER_SUMMARIZATION:
                    final SlFilterSummarizationResult slFilterSummarizationResult;
                    switch (stepType) {
                        case DIA_DB_FILTER_SUMMARIZE:
                            slFilterSummarizationResult = taskInformation.diaFilterSummarizationResult().orElseThrow(() -> new IllegalStateException("Cannot set parameters, filter step result not set"));
                            break;
                        case SL_FILTER_SUMMARIZATION:
                            slFilterSummarizationResult = taskInformation.slFilterSummarizationResult().orElseThrow(() -> new IllegalStateException("Cannot set parameters, filter step result not set"));
                            break;
                        default:
                            throw new IllegalStateException("Cannot set SL Filter unexpected stepType: " + stepType);
                    }
                    workFlowType = ModelConversion.workflowType(stepType);
                    futureFilterList = query.proteinDecoyInfoFuture(taskInformation, stepType)
                        .thenApply(proteinDecoyInfo -> {
                            SlFilter.ProteinHitFilter proto = SlProteinHitFilter.fromProto(workflowStep.getSlFilter(),
                                fdr -> DecoyInfoWrapper.fdrToProScore(proteinDecoyInfo, fdr, workflowStep.getSlFilter().getMinUniquePeptides()),
                                pvalue -> DecoyInfoWrapper.proScoreToFdr(proteinDecoyInfo, pvalue, workflowStep.getSlFilter().getMinUniquePeptides())
                            )
                                .proto(SlFilter.ProteinHitFilter.newBuilder());
                            SlFilter slFilter = SlFilter.newBuilder()
                                .setProteinHitFilter(proto)
                                .setPeptideFilter(query.getSlPeptideFilter(slFilterSummarizationResult, analysis, stepType))
                                .setPsmFilter(query.getSlPsmFilter(slFilterSummarizationResult, analysis, stepType))
                                .build();
                            FilterList.Builder builder = FilterList.newBuilder();
                            if (stepType == StepType.DIA_DB_FILTER_SUMMARIZE){
                                builder.setDiaDbFilter(slFilter);
                            } else {
                                builder.setSlFilter(slFilter);
                            }
                            return builder.build();
                        });
                    break;
                case LFQ_FILTER_SUMMARIZATION:
                case DIA_LFQ_FILTER_SUMMARIZATION:
                    workFlowType = ModelConversion.workflowType(stepType);
                    futureFilterList = CompletableFuture.completedFuture(
                        FilterList.newBuilder()
                            .setLfqProteinFeatureVectorFilter(workflowStep.getLfqProteinFeatureVectorFilter())
                            .build()
                    );
                    break;
                case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                    workFlowType = ModelConversion.workflowType(stepType);
                    futureFilterList = CompletableFuture.completedFuture(
                        FilterList.newBuilder().setReporterIonIonQProteinFilter(workflowStep.getReporterIonQProteinFilter()).build()
                    );
                    break;
                case SILAC_FILTER_SUMMARIZE:
                    workFlowType = ModelConversion.workflowType(stepType);
                    futureFilterList = CompletableFuture.completedFuture(
                        FilterList.newBuilder().setSilacProteinFilter(workflowStep.getSilacProteinFilter()).build()
                    );
                    break;
                default:
                    return;
            }
            futureFilterList.whenComplete((filter, throwable) -> {
                if (throwable == null) {
                    sendSelf(analysisCommandFactory
                        .setFilter(WorkflowStepFilter.newBuilder()
                            .setWorkFlowType(workFlowType)
                            .setFilter(filter)
                            .build()
                        ));
                } else{
                    log.error(throwable, "{}: workflow step {} failed to set filter", persistenceId(), stepType);
                }
            });
        } catch (Throwable throwable) {
            log.error(throwable, "{}: workflow step {} failed to set filter", persistenceId(), stepType);
        }
    }

    /*
     Theoretically, this should only happen if user closes their browser.

     If a fraction state is PENDING, then:

     a) It is a Remote Upload, and will receive a UploadQueued momentarily.
        Action:
            - Wait long enough that Uploader has chance to send UploadQueued.
            - Anything pending beyond this wait, is a browser upload.
            - Wait until: fraction creation + pending upload timeout < now

     b) It is a Browser Upload that hasn't started:
        Note: The STARTED can be trusted completely. Do NOT think STARTED is wrong. Why? Keep reading.
            - When server restarts, all STARTED are timed out.
            - If there is a STARTED, it is because an UploadStarted notification arrived after server restart. Making the STARTED true.
            - The UploadHandler will always send a FAILED or DONE after a STARTED.
            - The UploadHandler is completely reliable, if not, fix it. It already has plenty of error handling to gaurentee sending FAILED or DONE after STARTED.
            - The only reason FAILED or DONE is not received, is server restart. But this is handled in cleanupUnfinishedUploads() method. NOT HERE!
        Action:
            - If there are any started uploads, do NOT time out anything. The browser cannot upload all fractions
            at the same time. The pending fraction is just waiting for one of the started uploads to finish before
            starting this upload. This is the safe decision, but may hide the truth (browser was closed) for a while.
            If there is a combination of multiple browsers and uploader, then we cannot identify closed browsers
            until AFTER all other uploads to project are complete. This will eventually happen, so be patient.
            - A browser has many concurrent uploads. There is NO gap in time between uploads for this reason. There
            will always be at least one upload STARTED. The only gap in time exists prior to first upload starting.
            - If NO uploads active AND duration (pendingUploadTimeout) has passed since fraction was created,
            We can timeout. Specifically: fraction creation + pending upload timeout < now

      Combined ACTION:
        1. If any upload STARTED, do not TIMEOUT anything.
        2. Otherwise, if fraction creation + pending upload timeout < now, then TIMEOUT fraction upload.

      Note:
        - pending upload timeout is a very small number.
        - We expect that either an UploadQueued or any UploadStarted to happen within seconds of sample creation.

      WE DON NOT TIMEOUT:
        - QUEUED: They are only TIMEOUT by cleanupUnfinishedUploads() after system restart.
        - STARTED: They are only TIMEOUT by cleanupUnfinishedUploads() after system restart.

      WE ONLY TIMEOUT:
        - PENDING

    */
    private void timeoutUploads() {
        if (query.projectProgress(data) != Progress.State.PROGRESS) return;

        boolean startedExist = data.sampleIdToSample().values().stream()
            .flatMap(sample -> sample.fractionState().values().stream())
            .anyMatch(fraction -> fraction.uploadState() == UploadState.STARTED);

        if (startedExist) return;

        final Instant now = Instant.now();
        try {
            for (ProjectSampleState sample : data.sampleIdToSample().values()) {
                for (ProjectFractionState fraction : sample.fractionState().values()) {
                    if (fraction.uploadState() != UploadState.PENDING) continue;
                    UUID fractionId = fraction.fractionID();
                    final Duration timeout;
                    if (data.daemonFractionIdToTimeout().containsKey(fractionId)) {
                        timeout = Duration.ofHours(data.daemonFractionIdToTimeout().get(fractionId));
                    } else {
                        timeout = pendingUploadTimeout;
                    }
                    if (fraction.lastUpdateTimeStamp().plus(timeout).isBefore(now)) {
                        UUID sampleId = sample.sampleId();
                        log.info("{}: Will now time out pending upload sample {} fraction {}", persistenceId(), sampleId, fractionId);
                        ProjectAggregateEvent timeoutEvent = eventFactory(ADMIN_USERID)
                            .sample(sampleId)
                            .uploadTimeout(fractionId, "Fraction pending for too long. Browser must have been closed.");
                        persist(timeoutEvent);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, "{}: Exception encountered while trying to timeout upending uploads", persistenceId());
        }
    }

    @Override
    protected ProjectAggregateState nextState(ProjectAggregateState data, ProjectAggregateEvent event) throws Exception {
        return data.nextState(event);
    }

    private static TaskDescription taskDescriptionWithTaskId(ProjectTaskPlanState taskPlan, UUID taskId, String keyspace) throws ClassNotFoundException, IOException {
        final JsonTaskParameters jsonTaskParameters = taskPlan.jsonTaskParamaters();
        TaskDescription.Builder builder;
        if (jsonTaskParameters != null) {
            final Task taskWithTaskId = Helper.convertParameters(jsonTaskParameters).withTaskId(taskId).withKeyspace(keyspace);
            builder = CommonTasksFactory.taskDescriptionBuilderByJson(taskWithTaskId, taskPlan.stepType());
            taskPlan.level().fill(builder);
        } else {
            final TaskParameters taskParameters = taskPlan.taskParameters();
            if (taskParameters != null) {
                builder = TaskDescription.newBuilder()
                    .setStepType(taskPlan.stepType())
                    .setParameters(taskParameters.toBuilder()
                        .setTaskId(taskId.toString())
                        .setKeyspace(keyspace)
                        .build()
                    );
                taskPlan.level().fill(builder);
            } else {
                throw new IllegalArgumentException("TaskPlan must has parameters");
            }
        }
        return builder.setKeyspace(keyspace).build();
    }

    static class ProjectMessageExtractor implements ShardRegion.MessageExtractor {

        @Override
        public String entityId(Object message) {
            if (message instanceof ProjectAggregateCommand) {
                final ProjectAggregateCommand command = (ProjectAggregateCommand) message;
                return persistenceId(command.getProjectId());
            }
            if (message instanceof ProjectAggregateEvent) {
                final ProjectAggregateEvent event = (ProjectAggregateEvent) message;
                return persistenceId(event.getProjectId());
            }
            if (message instanceof ProjectAggregateQueryRequest) {
                final ProjectAggregateQueryRequest query = (ProjectAggregateQueryRequest) message;
                return persistenceId(query.getProjectId());
            }
            if (message instanceof ProjectEventToRestore) {
                final ProjectEventToRestore eventToRestore = (ProjectEventToRestore) message;
                if (eventToRestore.getEventsCount() > 0) {
                    return persistenceId(eventToRestore.getEvents(0).getProjectId());
                }
            }
            return null;
        }

        @Override
        public Object entityMessage(Object message) {
            return message;
        }

        @Override
        public String shardId(Object message) {
            // We only have 1 shard.
            return "1";
        }
    }
}
