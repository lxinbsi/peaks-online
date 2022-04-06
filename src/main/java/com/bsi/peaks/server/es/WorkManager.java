package com.bsi.peaks.server.es;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import akka.dispatch.Futures;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.japi.function.Function2;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import akka.japi.tuple.Tuple3;
import akka.japi.tuple.Tuple4;
import akka.pattern.BackoffSupervisor;
import akka.pattern.PatternsCS;
import akka.persistence.SnapshotOffer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.WorkCommandFactory;
import com.bsi.peaks.event.WorkEventFactory;
import com.bsi.peaks.event.common.AckStatus;
import com.bsi.peaks.event.common.CommandAck;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateCommand;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.work.BatchSubmitTasks;
import com.bsi.peaks.event.work.CancelProject;
import com.bsi.peaks.event.work.CancelTask;
import com.bsi.peaks.event.work.KillWorker;
import com.bsi.peaks.event.work.RegisterWorker;
import com.bsi.peaks.event.work.SoftwareVersion;
import com.bsi.peaks.event.work.SubmitTask;
import com.bsi.peaks.event.work.TaskCancelled;
import com.bsi.peaks.event.work.TaskCompleted;
import com.bsi.peaks.event.work.TaskFailed;
import com.bsi.peaks.event.work.TaskPriority;
import com.bsi.peaks.event.work.TaskProcessing;
import com.bsi.peaks.event.work.TaskRegistration;
import com.bsi.peaks.event.work.TaskResourceOverride;
import com.bsi.peaks.event.work.TaskStartedBatch;
import com.bsi.peaks.event.work.TaskWorkedCancelled;
import com.bsi.peaks.event.work.ThreadLimit;
import com.bsi.peaks.event.work.WorkCommand;
import com.bsi.peaks.event.work.WorkCommandAck;
import com.bsi.peaks.event.work.WorkEvent;
import com.bsi.peaks.event.work.WorkEventAck;
import com.bsi.peaks.event.work.WorkManagerSnapshot;
import com.bsi.peaks.event.work.WorkQueryRequest;
import com.bsi.peaks.event.work.WorkQueryResponse;
import com.bsi.peaks.event.work.WorkQueryResponse.QueryFailure;
import com.bsi.peaks.event.work.WorkQueryResponse.QueryTasksResponse;
import com.bsi.peaks.event.work.WorkRequest;
import com.bsi.peaks.event.work.WorkerDisconnected;
import com.bsi.peaks.event.work.WorkerRegistration;
import com.bsi.peaks.message.WorkerMessageFactory;
import com.bsi.peaks.messages.worker.PongWorker;
import com.bsi.peaks.messages.worker.StartTask;
import com.bsi.peaks.messages.worker.StartTaskBatch;
import com.bsi.peaks.messages.worker.WorkerCommand;
import com.bsi.peaks.messages.worker.WorkerCommandAck;
import com.bsi.peaks.messages.worker.WorkerProcessState;
import com.bsi.peaks.messages.worker.WorkerResponse;
import com.bsi.peaks.messages.worker.WorkerState;
import com.bsi.peaks.model.dto.MonitorWorker;
import com.bsi.peaks.model.dto.MonitorWorkerProcess;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.model.dto.ProjectTaskListing;
import com.bsi.peaks.model.dto.Status;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.dto.WorkTaskListing;
import com.bsi.peaks.model.dto.WorkerType;
import com.bsi.peaks.server.cluster.PeaksTaskResourceCalculator;
import com.bsi.peaks.server.cluster.Worker;
import com.bsi.peaks.server.es.work.WorkManagerConfig;
import com.bsi.peaks.server.es.work.WorkManagerState;
import com.bsi.peaks.server.es.work.WorkManagerStateBuilder;
import com.bsi.peaks.server.es.work.WorkerRef;
import com.bsi.peaks.server.service.CassandraMonitorService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import scala.Option;

import java.net.URLDecoder;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.bsi.peaks.event.work.WorkQueryResponse.QueryWorkerResponse;
import static com.bsi.peaks.event.work.WorkQueryResponse.newBuilder;
import static com.bsi.peaks.model.ModelConversion.uuidFrom;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class WorkManager
    extends PeaksPersistentActor<WorkManagerState, WorkEvent, WorkEventAck, WorkCommand> {
    public final static String RESOURCE_OVERRIDE_PATTERN = "<<<\\d+,\\d+,\\d+>>>";
    public static final String NAME = "taskmanager";
    private static final String DISCONNECT_WORKER_CHECK_TICK = "DISCONNECT_WORKER_CHECK_TICK";
    private static final int CHECK_CASSANDRA_MONITOR_CHECK = 5;
    public static final String PERSISTENCE_ID = "WorkManager";
    public static final String DISCONNECTED_WORKER = "Disconnected worker";
    public static final UUID ADMIN_USERID = UUID.fromString(UserManager.ADMIN_USERID);
    private final WorkManagerConfig config;
    private final PeaksTaskResourceCalculator resourceCalculator;
    private final CassandraMonitorService cassandraMonitorService;
    private final static String SCHEDULED_TICK = "SCHEDULED_TICK";
    private final Cancellable scheduledTask;
    private final static int FIXED_CPU_THREADS_WITH_GPU = 8;

    // Highest priority first.
    private static final Comparator<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>> PRIORITY_COMPARATOR =Comparator.comparingLong(entry -> entry.getValue().t2());

    // First in, first out.
    private static final Comparator<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>> TIME_STAMP_COMPARATOR = Comparator.<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>>comparingLong(entry -> entry.getValue().t1().getEpochSecond()).reversed();

    private static final Comparator<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>> TASK_COMPARATOR = PRIORITY_COMPARATOR.thenComparing(TIME_STAMP_COMPARATOR);

    protected WorkManager(WorkManagerConfig config, PeaksTaskResourceCalculator resourceCalculator, CassandraMonitorService cassandraMonitorService) {
        super(WorkEvent.class, true);
        this.config = config;
        this.resourceCalculator = resourceCalculator;
        this.cassandraMonitorService = cassandraMonitorService;
        this.scheduledTask = scheduleTask();

        final Duration unreachableDisconnectHalfLife = config.unreachableDisconnect().dividedBy(8);
        getContext().system().scheduler().schedule(
            unreachableDisconnectHalfLife,
            unreachableDisconnectHalfLife,
            getSelf(),
            DISCONNECT_WORKER_CHECK_TICK,
            getContext().dispatcher(),
            getSelf()
        );
    }

    private Cancellable scheduleTask() {
        return getContext().system().scheduler().schedule(
            Duration.ofSeconds(0),
            Duration.ofSeconds(CHECK_CASSANDRA_MONITOR_CHECK),
            self(),
            SCHEDULED_TICK,
            context().dispatcher(),
            self()
        );
    }

    public static void start(
        ActorSystem system,
        WorkManagerConfig config,
        PeaksTaskResourceCalculator resourceCalculator,
        CassandraMonitorService cassandraMonitorService
    ) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(WorkManager.class, config, resourceCalculator, cassandraMonitorService),
                    NAME + "Instance",
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(10),
                    0.2
                ),
                PoisonPill.getInstance(),
                ClusterSingletonManagerSettings.create(system)
            ),
            NAME
        );
        system.actorOf(
            ClusterSingletonProxy.props("/user/" + NAME, ClusterSingletonProxySettings.create(system)),
            NAME + "Proxy"
        );
    }

    public static ActorRef instance(ActorSystem system) {
        return system.actorFor("/user/" + NAME + "Proxy");
    }

    @Override
    public String persistenceId() {
        return PERSISTENCE_ID;
    }

    @Override
    protected WorkManagerState defaultState() {
        return new WorkManagerStateBuilder()
            .build();
    }

    @Override
    protected void onRecoveryComplete(List<WorkEvent> recoveryEvents) throws Exception {
        super.onRecoveryComplete(recoveryEvents);
        final WorkManagerState data = this.data;
        for (UUID workerId : data.idleWorkers(null)) {
            log.info("Disconnecting idle worker {} on recovery of WorkManager", workerId);
            final ActorPath workerActorPath = data.workers().get(workerId).workerActorPath();
            removeUndeliveredMessages(workerActorPath);
            persist(WorkEventFactory.workerDisconnected(workerId));
        }
        Map<UUID, List<UUID>> projectIdTaskIds = new HashMap<>();
        PMap<UUID, Tuple3<Instant, Integer, TaskRegistration>> newTasks = HashTreePMap.empty();
        for (Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>> entry : data.tasks().entrySet()) {
            final UUID taskId = entry.getKey();
            final Tuple3<Instant, Integer, TaskRegistration> value = entry.getValue();
            final TaskRegistration taskRegistration = value.t3();

            newTasks = newTasks.plus(taskId, Tuple3.create(value.t1(), 0, taskRegistration));

            projectIdTaskIds
                .computeIfAbsent(UUID.fromString(taskRegistration.getProjectId()), ignore -> new ArrayList<>())
                .add(taskId);
        }
        projectIdTaskIds.forEach((projectId, taskIds) -> {
            final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
            sendProjectCommand(commandFactory.verifyTasks(taskIds));
        });
        this.data = data.withTasks(newTasks);
    }

    @Override
    protected GeneratedMessageV3 snapShotFromState(WorkManagerState state) {
        final WorkManagerSnapshot.Builder builder = WorkManagerSnapshot.newBuilder();
        state.snapshot(builder);
        return builder.build();
    }

    @Override
    protected void onSnapShotOffer(SnapshotOffer snapshotOffer) {
        data = WorkManagerState.from((WorkManagerSnapshot) snapshotOffer.snapshot());
    }

    @Override
    public void onRecoveryFailure(Throwable cause, Option<Object> event) {
        log.info("Recovery failed for workmanager will delete all related events. \nPlease wait a couple of minutes before accessing the work manager page.");
        deleteMessages(Long.MAX_VALUE);
        return;
    }

    @Override
    protected WorkManagerState nextState(WorkManagerState data, WorkEvent event) throws Exception {
        switch (event.getEventCase()) {
            case WORKERREGISTRATION: {
                final WorkerRegistration workerRegistration = event.getWorkerRegistration();
                final UUID workerId = UUID.fromString(workerRegistration.getWorkerId());
                final Set<StepType> supportsStepTypes = ImmutableSet.copyOf(workerRegistration.getSupportsStepTypesList());
                return data
                    .plusWorker(workerId, WorkerRef.from(workerRegistration), supportsStepTypes)
                    .plusWorkerContactNow(workerId);
            }
            case WORKERDISCONNECTED: {
                final WorkerDisconnected workerDisconnected = event.getWorkerDisconnected();
                final UUID workerId = UUID.fromString(workerDisconnected.getWorkerId());
                return data.minusWorker(workerId);
            }
            case TASKREGISTRATION: {
                final TaskRegistration taskRegistration = event.getTaskRegistration();
                final UUID taskId = UUID.fromString(taskRegistration.getTaskId());
                final UUID projectId = UUID.fromString(taskRegistration.getProjectId());
                final int weight = taskRegistration.getWeight();
                return data.plusTask(taskId, taskRegistration, weight, CommonFactory.convert(event.getTimeStamp()));
            }
            case TASKSTARTEDBATCH: {
                TaskStartedBatch batch = event.getTaskStartedBatch();
                final UUID workerId = UUID.fromString(batch.getWorkerId());
                WorkManagerState updated = data.updateWorker(workerId,
                    data.workers().get(workerId).update(batch.getCpuLeft(), batch.getGpuLeft(), batch.getMemoryLeft()));
                for (String taskIdString : batch.getTaskIdsList()) {
                    final UUID taskId = UUID.fromString(taskIdString);
                    updated = updated
                        .minusReadyTask(taskId)
                        .assignWorkerToTask(workerId, taskId, CommonFactory.convert(event.getTimeStamp()));
                }
                return updated;
            }
            case TASKCOMPLETED: {
                final TaskCompleted taskCompleted = event.getTaskCompleted();
                final UUID taskId = UUID.fromString(taskCompleted.getTaskId());
                final UUID workerId = UUID.fromString(taskCompleted.getWorkerId());
                return data.minusTask(taskId).removeTaskFromWorker(workerId, taskId);
            }
            case TASKCANCELLED: {
                final TaskCancelled taskCancelled = event.getTaskCancelled();
                final UUID taskId = UUID.fromString(taskCancelled.getTaskId());
                final UUID workerId = findWorkerByTask(taskId).orElse(null);
                WorkManagerState updated = data.minusReadyTask(taskId).minusTask(taskId);
                if (workerId != null) {
                    return updated.removeTaskFromWorker(workerId, taskId);
                } else {
                    return updated;
                }
            }
            case TASKWORKEDCANCELLED: {
                final TaskWorkedCancelled cancelled = event.getTaskWorkedCancelled();
                final UUID taskId = UUID.fromString(cancelled.getTaskId());
                final UUID workerId = UUID.fromString(cancelled.getWorkerId());
                return data.removeTaskFromWorker(workerId, taskId);
            }
            case TASKFAILED: {
                final TaskFailed taskFailed = event.getTaskFailed();
                final UUID taskId = UUID.fromString(taskFailed.getTaskId());
                final UUID workerId = UUID.fromString(taskFailed.getWorkerId());
                return data.minusTask(taskId).removeTaskFromWorker(workerId, taskId);
            }
            case THREADLIMIT: {
                final ThreadLimit threadLimit = event.getThreadLimit();
                return data.withThreadLimit(threadLimit.getThreadLimit());
            }
            case SOFTWAREVERSION:
                final SoftwareVersion softwareVersion = event.getSoftwareVersion();
                return data.withSoftwareVersion(softwareVersion.getVersion());
            default:
                return data;
        }
    }

    @Override
    protected String eventTag(WorkEvent event) {
        return "WorkEvent." + event.getEventCase().name();
    }

    @Override
    protected String commandTag(WorkCommand command) {
        return "WorkCommand." + command.getCommandCase().name();
    }

    @Override
    protected WorkEventAck ackAcceptEvent(WorkEvent event) {
        return WorkEventFactory.ackAccept(event);
    }

    @Override
    protected WorkEventAck ackRejectEvent(WorkEvent event, String rejectDescription) {
        return WorkEventFactory.ackReject(event, rejectDescription);
    }

    @Override
    protected ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .matchEquals(SCHEDULED_TICK, this::checkStashedTasks)
            .match(WorkCommand.class, WorkCommand::hasWorkRequest, commandAckOnFailure(this::workRequest))
            .match(WorkCommand.class, WorkCommand::hasCancelProject, commandAckOnFailure(this::cancelProject))
            .match(WorkCommand.class, WorkCommand::hasCancelTask, commandAckOnFailure(this::cancelTask))
            .match(WorkCommand.class, WorkCommand::hasRegisterWorker, commandAckOnFailure(this::registerWorker))
            .match(WorkCommand.class, WorkCommand::hasSubmitTasks, commandAckOnFailure(this::handleSubmitTasksInBatch))
            .match(WorkCommand.class, WorkCommand::hasSubmitTask, ignoreThenAckOr(this::ignoreSubmitTask, this::handleSubmitTaskCommand))
            .match(WorkCommand.class, WorkCommand::hasKillWorker, commandAckOnFailure(this::killWorker))
            .match(WorkCommand.class, WorkCommand::hasTaskPriority, commandAckOnFailure(this::taskPriority))
            .match(WorkCommand.class, WorkCommand::hasTaskResourceOverride, commandAckOnFailure(this::taskResourceOverride))
            .match(WorkEvent.class, WorkEvent::hasWorkerRegistration, workEvent -> {
                log.warning("Ignoring WorkerRegistration event. Use RegisterWorker command instead.");
            })
            .match(WorkEvent.class, WorkEvent::hasWorkerDisconnected, workEvent -> {
                log.warning("Ignoring WorkerDisconnected event. This is an internal event.");
            })
            .match(WorkEvent.class, WorkEvent::hasTaskCompleted, this::handleTaskCompleted)
            .match(WorkEvent.class, WorkEvent::hasTaskFailed, this::handleTaskFailed)
            .match(WorkEvent.class, WorkEvent::hasTaskProcessing, this::handleTaskWorkProcessing)
            .match(WorkEvent.class, WorkEvent::hasSoftwareVersion, ignoreThenAckOr(this::ignoreSoftwareVersionEvent, this::persistAck))
            .match(WorkEvent.class, WorkEvent::hasThreadLimit, ignoreThenAckOrDebug(this::ignoreThreadLimitEvent, this::persistAck))
            .match(WorkQueryRequest.class, WorkQueryRequest::hasListTasks, handleQuery(WorkQueryRequest::getListTasks, this::listTasks))
            .match(WorkQueryRequest.class, WorkQueryRequest::hasListPendingTasks, handleQuery(WorkQueryRequest::getListPendingTasks, this::listPendingTasks))
            .match(WorkQueryRequest.class, WorkQueryRequest::hasState, handleQuery(WorkQueryRequest::getState, this::getState))
            .match(WorkQueryRequest.class, WorkQueryRequest::hasListWorkers, handleQuery(WorkQueryRequest::getListWorkers, this::queryWorker))
            .match(WorkerResponse.class, WorkerResponse::hasPongWorker, this::pongHandler)
            .match(WorkerCommandAck.class, this::workerCommandAck)
            .matchEquals(DISCONNECT_WORKER_CHECK_TICK, ignore -> this.disconnectWorkerCheck());
    }

    private void checkStashedTasks(String tick) throws Exception {
        boolean hasQueuedTasks = !data.queuedTasks().isEmpty();
        if (hasQueuedTasks) {
            Boolean isFull = cassandraMonitorService.checkFull(ADMIN_USERID).toCompletableFuture().join();
            while (!isFull && !data.queuedTasks().isEmpty()) {
                SubmitTask submitTask = data.queuedTasks().get(0);
                data = data.dequeueTask(submitTask);
                handleSubmitTask(submitTask, 1 + queuedTasks(ImmutableSet.of(UUID.fromString(submitTask.getTaskId()))));
            }
        }
    }

    @Override
    public void postStop() throws Exception {
        scheduledTask.cancel();
        super.postStop();
    }

    private CompletionStage<WorkQueryResponse.Builder> getState(WorkQueryRequest.QueryState queryState, WorkManagerState workManagerState) throws JsonProcessingException {
        return completedFuture(WorkQueryResponse.newBuilder()
            .setStateJson(OM.writeValueAsString(data))
        );
    }

    private <T> FI.UnitApply<WorkQueryRequest> handleQuery(Function<WorkQueryRequest, T> select, Function2<T, WorkManagerState, CompletionStage<WorkQueryResponse.Builder>> responder) {
        return query -> {
            CompletionStage<WorkQueryResponse.Builder> futureResponse;
            UUID userId = uuidFrom(query.getUserId());
            if (!readPermission(userId)) {
                futureResponse = completedFuture(
                    newBuilder()
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
            replyFuture(futureResponse
                .exceptionally(throwable -> {
                    log.error(throwable, "{}: Error handling query", persistenceId());
                    return newBuilder()
                        .setFailure(QueryFailure.newBuilder()
                            .setReason(QueryFailure.Reason.ERROR)
                            .setDescription(throwable.getMessage())
                        );
                })
                .thenApply(builder -> builder
                    .setDeliveryId(query.getDeliveryId())
                    .build()
                ));
        };
    }

    private boolean readPermission(UUID userId) {
        return true;
    }


    protected void sendProjectCommand(ProjectAggregateCommand command) {
        deliver(projectActorRef().path(), (Long deliveryId) -> command.toBuilder()
            .setDeliveryId(deliveryId)
            .build());
    }

    private ActorRef projectActorRef() {
        try {
            return config.projectActorRef().apply(context().system());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected long deliveryId(WorkCommand command) {
        return command.getDeliveryId();
    }

    private boolean ignoreThreadLimitEvent(WorkEvent event) {
        return data.threadLimit() == event.getThreadLimit().getThreadLimit();
    }

    private boolean ignoreSoftwareVersionEvent(WorkEvent event) {
        return data.softwareVersion().equals(event.getSoftwareVersion().getVersion());
    }

    private void taskResourceOverride(WorkCommand command) {
        final TaskResourceOverride resourceOverride = command.getTaskResourceOverride();
        final UUID taskId = UUID.fromString(resourceOverride.getTaskId());
        if (data.readyTasks().contains(taskId) && data.tasks().containsKey(taskId) && !resourceOverride.getResourceOverride().isEmpty()) {
            final Tuple3<Instant, Integer, TaskRegistration> taskValue = data.tasks().get(taskId);
            TaskRegistration updated = taskValue.t3().toBuilder()
                .setTaskDescription(taskWithResource(1 + queuedTasks(ImmutableSet.of(taskId)), taskValue.t3().getTaskDescription().toBuilder()
                    .setResourceOverride(resourceOverride.getResourceOverride()).build(), false
                ))
                .build();
            log.info("Task {}: {} resource override to {}",
                taskValue.t3().getTaskDescription().getStepType(), taskId, resourceOverride.getResourceOverride());
            data = data.withTasks(data.tasks().plus(taskId, Tuple3.create(taskValue.t1(), taskValue.t2(), updated)));
        }
        reply(WorkCommandFactory.ackAccept(command));
    }

    private void taskPriority(WorkCommand command) {
        final TaskPriority taskPriority = command.getTaskPriority();
        final UUID taskId = UUID.fromString(taskPriority.getTaskId());
        final PMap<UUID, Tuple3<Instant, Integer, TaskRegistration>> tasks = data.tasks();
        final Tuple3<Instant, Integer, TaskRegistration> taskValue = tasks.get(taskId);
        if (taskValue == null) {
            reply(WorkCommandFactory.ackAccept(command));
            final UUID projectId = UUID.fromString(taskPriority.getProjectId());
            final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
            sendProjectCommand(commandFactory.task(taskId).notFound());
            return;
        }
        if (data.readyTasks().contains(taskId)) {
            // only reset priority for ready tasks. It doesn't make sense to reset priority to cancelled tasks
            // or tasks already in progress
            final long currentPriority = taskValue.t2();
            final int newPriority = taskWeight(taskValue.t3().getTaskDescription(), taskPriority.getPriority());
            if (currentPriority != newPriority) {
                log.info("Task {} has new priority {}", taskId, newPriority);
                TaskRegistration override = taskValue.t3().toBuilder()
                    .setTaskDescription(taskWithResource(1 + queuedTasks(ImmutableSet.of(taskId)), taskValue.t3().getTaskDescription(), false))
                    .build();
                data = data.withTasks(tasks.plus(taskId, Tuple3.create(taskValue.t1(), newPriority, override)));
            }
        }
        reply(WorkCommandFactory.ackAccept(command));
    }

    private CompletionStage<WorkQueryResponse.Builder> listPendingTasks(WorkQueryRequest.QueryTasks query, WorkManagerState data) {
        final List<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>> readyTasks = data.tasks().entrySet().stream()
            .filter(task -> data.readyTasks().contains(task.getKey()) && task.getValue().t2() > 0) // task.getValue().t2() is priority
            .sorted(TASK_COMPARATOR.reversed())
            .collect(Collectors.toList());

        return Source.from(readyTasks)
            .map(entry -> entry.getValue().t3())
            .runWith(Sink.seq(), ActorMaterializer.create(getContext().getSystem()))
            .thenApply(list -> WorkQueryResponse.newBuilder()
                .setListPendingTasks(WorkQueryResponse.QueryPendingTasksResponse.newBuilder()
                    .addAllTasks(list)
                    .build()
                )
            );
    }

    private CompletionStage<WorkQueryResponse.Builder> listTasks(WorkQueryRequest.QueryTasks query, WorkManagerState data) {
        final QueryTasksResponse.Builder response = QueryTasksResponse.newBuilder();
        final ActorSystem system = getContext().getSystem();
        final PMap<UUID, Tuple3<Instant, Integer, TaskRegistration>> tasks = data.tasks();
        final PMap<UUID, WorkerRef> workers = data.workers();

        return Source.from(data.tasks().values())
            .map(t -> UUID.fromString(t.t3().getProjectId()))
            .mapAsync(config.parallelism(), projectId -> {
                final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
                return PatternsCS.ask(projectActorRef(), commandFactory.queryTaskListing(), config.timeout())
                    .thenApply(projectQueryResponse -> (ProjectAggregateQueryResponse) projectQueryResponse)
                    .thenApply(ProjectAggregateQueryResponse::getProjectQueryResponse)
                    .thenApply(ProjectAggregateQueryResponse.ProjectQueryResponse::getTaskListing);
            })
            .mapConcat(ProjectAggregateQueryResponse.TaskListing::getTasksList)
            .runFold(
                new HashMap<UUID, ProjectTaskListing>(),
                (map, task) -> {
                    map.put(UUID.fromString(task.getTaskId()), task);
                    return map;
                },
                ActorMaterializer.create(system)
            )
            .thenApply(projectTasks -> {
                final WorkQueryResponse.Builder responseBuilder = newBuilder();
                final QueryTasksResponse.Builder builder = responseBuilder.getListTasksBuilder();
                for (Tuple3<Instant, Integer, TaskRegistration> t : data.tasks().values()) {
                    final Instant queued = t.t1();
                    final int priority = t.t2();
                    final TaskRegistration taskRegistration = t.t3();
                    final String taskIdString = taskRegistration.getTaskId();
                    final UUID taskId = UUID.fromString(taskIdString);
                    final WorkTaskListing.Builder taskBuilder = builder.addTasksBuilder();
                    taskBuilder
                        .setTaskId(taskIdString)
                        .setType(taskRegistration.getTaskDescription().getStepType())
                        .setQueuedTimeStamp(queued.getEpochSecond())
                        .setProjectId(taskRegistration.getProjectId())
                        .setPriority(priority)
                        .setState(Progress.State.QUEUED); // Will be overwritten later in progress.

                    for (Map.Entry<UUID, PMap<UUID, Instant>> entry : data.workerTasks().entrySet()) {
                        for (Map.Entry<UUID, Instant> task : entry.getValue().entrySet()) {
                            if (task.getKey().equals(taskId)) {
                                final UUID workerId = entry.getKey();
                                taskBuilder
                                    .setWorkerId(workerId.toString())
                                    .setWorkerIpAddress(data.workers().get(workerId).ipAddress())
                                    .setStartedTimeStamp(task.getValue().getEpochSecond())
                                    .setState(Progress.State.PROGRESS);
                                break;
                            }
                        }
                    }

                    final ProjectTaskListing projectTaskListing = projectTasks.get(UUID.fromString(taskRegistration.getProjectId()));
                    if (projectTaskListing != null) {
                        taskBuilder
                            .setProjectName(projectTaskListing.getProjectName())
                            .addAllAnalysisId(projectTaskListing.getAnalysisIdList())
                            .addAllAnalysisName(projectTaskListing.getAnalysisNameList())
                            .addAllSampleId(projectTaskListing.getSampleIdList())
                            .addAllSampleName(projectTaskListing.getSampleNameList())
                            .addAllFractionId(projectTaskListing.getFractionIdList())
                            .addAllFractionName(projectTaskListing.getFractionNameList());
                    }

                }
                return responseBuilder;
            });
    }

    private void killWorker(WorkCommand command) throws Exception {
        final KillWorker killWorker = command.getKillWorker();
        final UUID workerId = UUID.fromString(killWorker.getWorkerId());
        final WorkerRef workerRef = data.workers().get(workerId);
        if (workerRef == null) {
            reply(WorkCommandFactory.ackAccept(command));
        } else {
            sendWorkerCommand(WorkerMessageFactory.worker(workerId).stopWorker(), workerRef.workerActorPath());
            persist(WorkEventFactory.workerDisconnected(workerId), () -> {
                reply(WorkCommandFactory.ackAccept(command));
            });
        }
    }

    private void pongHandler(WorkerResponse response) {
        try {
            final UUID workerId = UUID.fromString(response.getWorkerId());
            final PongWorker pongWorker = response.getPongWorker();
            data = data.plusWorkerContactNow(workerId);
        } catch (Exception exception) {
            log.error(exception, "Exception while handling pong");
        }
    }

    private void handleTaskWorkProcessing(WorkEvent workEvent) {
        TaskProcessing taskWorkProcessing = workEvent.getTaskProcessing();
        final UUID taskId = UUID.fromString(taskWorkProcessing.getTaskId());
        final Tuple3<Instant, Integer, TaskRegistration> taskValue = data.tasks().get(taskId);
        if (taskValue == null) {
            return;
        }
        final UUID projectId = UUID.fromString(taskValue.t3().getProjectId());
        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
        sendProjectCommand(commandFactory.task(taskId).updateProgress(taskWorkProcessing.getProgress()));
        //Do not persist
    }

    private void handleTaskFailed(WorkEvent workEvent) {
        final TaskFailed taskFailed = workEvent.getTaskFailed();
        final UUID taskId = UUID.fromString(taskFailed.getTaskId());
        log.info("Handling TaskFailed from worker {} for taskId {}", taskFailed.getWorkerId(), taskId);
        final Tuple3<Instant, Integer, TaskRegistration> taskValue = data.tasks().get(taskId);
        if (taskValue == null) {
            log.warning("Unable to find task with task id {}", taskId);
            reply(ackRejectEvent(workEvent, "Unable to find work registration " + taskId.toString()));
            return;
        }
        final UUID projectId = UUID.fromString(taskValue.t3().getProjectId());
        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
        sendProjectCommand(commandFactory.task(taskId).failed(taskFailed.getError()));
        persistAck(workEvent);
        final UUID workerId = UUID.fromString(taskFailed.getWorkerId());
        data = data.plusWorkerContactNow(workerId);
    }

    private void handleTaskCompleted(WorkEvent workEvent) {
        final TaskCompleted taskCompleted = workEvent.getTaskCompleted();
        final UUID taskId = UUID.fromString(taskCompleted.getTaskId());
        final UUID workerId = UUID.fromString(taskCompleted.getWorkerId());
        log.info("Handling TaskCompleted from worker {} for taskId {}", taskCompleted.getWorkerId(), taskId);
        final Tuple3<Instant, Integer, TaskRegistration> taskValue = data.tasks().get(taskId);
        if (taskValue == null) {
            log.warning("Unable to find task with task id {}", taskId);
            reply(ackRejectEvent(workEvent, "Unable to find worker registration " + taskId.toString()));
            return;
        }
        final UUID projectId = UUID.fromString(taskValue.t3().getProjectId());
        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
        sendProjectCommand(commandFactory.task(taskId).completed(taskCompleted));
        persistAck(workEvent);
        data = data.plusWorkerContactNow(workerId);
    }

    private boolean ignoreSubmitTask(WorkCommand workCommand) {
        final SubmitTask submitTask = workCommand.getSubmitTask();
        final UUID taskId = UUID.fromString(submitTask.getTaskId());
        return data.tasks().containsKey(taskId);
    }

    private void handleSubmitTasks(List<SubmitTask> tasks) throws Exception {
        int pendingTasks = tasks.size() + queuedTasks(
            tasks.stream().map(st -> UUID.fromString(st.getTaskId())).collect(Collectors.toSet())
        );
        List<WorkEvent> registrations = new ArrayList<>();
        for (SubmitTask submitTask : tasks) {
            if (!checkCassandra(submitTask)) {
                final boolean doubleResource = submitTask.getDoubleResource();
                final int adjustedWeight = taskWeight(submitTask.getTaskDescription(), submitTask.getPriority());
                final TaskDescription descriptionWithResource = taskWithResource(
                    pendingTasks, submitTask.getTaskDescription(), doubleResource
                );

                final WorkEvent taskRegistration = WorkEventFactory.builder()
                    .setTaskRegistration(TaskRegistration.newBuilder()
                        .setProjectId(submitTask.getProjectId())
                        .setTaskId(submitTask.getTaskId())
                        .setTaskDescription(descriptionWithResource)
                        .setWeight(adjustedWeight)
                        .build())
                    .build();
                registrations.add(taskRegistration);
            }
        }

        for (WorkEvent taskRegistration : registrations) {
            persist(taskRegistration, () -> {
                Tuple4<UUID, Integer, Integer, Integer> maxResource = maxResource();
                TaskDescription descriptionWithResource = taskRegistration.getTaskRegistration().getTaskDescription();
                int cpuRequired = descriptionWithResource.getCpu();
                int gpuRequired = descriptionWithResource.getGpu();
                int memoryRequired = descriptionWithResource.getMemory();
                if (gpuRequired > 0 ) {
                    // if gpu is required, send it to all gpu workers with available gpu
                    for (UUID workerId : data.workersWithIdleGPU(descriptionWithResource)) {
                        sendWorkerCommand(WorkerMessageFactory.worker(workerId).offerTask());
                    }
                } else if (maxResource.t1() != null && (cpuRequired > maxResource.t2() || gpuRequired > maxResource.t3() || memoryRequired > maxResource.t4())) {
                    // if the task's required resource is beyond all workers, send it to the one with most resource
                    sendWorkerCommand(WorkerMessageFactory.worker(maxResource.t1()).offerTask());
                } else {
                    for (UUID workerId : data.idleWorkers(descriptionWithResource)) {
                        sendWorkerCommand(WorkerMessageFactory.worker(workerId).offerTask());
                    }
                }
            });
        }
    }

    private void handleSubmitTask(SubmitTask submitTask, int pendingTasks) throws Exception {
        final boolean doubleResource = submitTask.getDoubleResource();
        final int adjustedWeight = taskWeight(submitTask.getTaskDescription(), submitTask.getPriority());
        final TaskDescription descriptionWithResource = taskWithResource(pendingTasks, submitTask.getTaskDescription(), doubleResource);

        final WorkEvent taskRegistration = WorkEventFactory.builder()
            .setTaskRegistration(TaskRegistration.newBuilder()
                .setProjectId(submitTask.getProjectId())
                .setTaskId(submitTask.getTaskId())
                .setTaskDescription(descriptionWithResource)
                .setWeight(adjustedWeight)
                .build())
            .build();

        persist(taskRegistration, () -> {
            Tuple4<UUID, Integer, Integer, Integer> maxResource = maxResource();
            int cpuRequired = descriptionWithResource.getCpu();
            int gpuRequired = descriptionWithResource.getGpu();
            int memoryRequired = descriptionWithResource.getMemory();
            if (gpuRequired > 0 ) {
                // if gpu is required, send it to all gpu workers with available gpu
                for (UUID workerId : data.workersWithIdleGPU(descriptionWithResource)) {
                    sendWorkerCommand(WorkerMessageFactory.worker(workerId).offerTask());
                }
            } else if (maxResource.t1() != null && (cpuRequired > maxResource.t2() || gpuRequired > maxResource.t3() || memoryRequired > maxResource.t4())) {
                // if the task's required resource is beyond all workers, send it to the one with most resource
                sendWorkerCommand(WorkerMessageFactory.worker(maxResource.t1()).offerTask());
            } else {
                for (UUID workerId : data.idleWorkers(descriptionWithResource)) {
                    sendWorkerCommand(WorkerMessageFactory.worker(workerId).offerTask());
                }
            }
        });
    }

    private void handleSubmitTasksInBatch(WorkCommand workCommand) throws Exception {
        BatchSubmitTasks submitTasks = workCommand.getSubmitTasks();
        handleSubmitTasks(submitTasks.getTasksList());
        defer(ackAccept(workCommand), this::reply);
    }

    private CommandAck handleSubmitTaskCommand(WorkCommand workCommand) throws Exception {
        final SubmitTask submitTask = workCommand.getSubmitTask();
        if (!checkCassandra(submitTask)) {
            handleSubmitTask(submitTask, 1 + queuedTasks(ImmutableSet.of(UUID.fromString(submitTask.getTaskId()))));
        }
        return ackAccept(workCommand);
    }

    private boolean checkCassandra(SubmitTask submitTask) {
        boolean isFull = cassandraMonitorService.checkFull(ADMIN_USERID)
            .toCompletableFuture().join();
        if (isFull) {
            data = data.queueTask(submitTask);
            log.error("Cassandra node(s) is full, please check with admin to free space");
        }
        return isFull;
    }

    private int taskWeight(TaskDescription taskDescription, int priority) {
        switch (taskDescription.getStepType()) {
            case DENOVO_FILTER:
            case DB_SUMMARIZE:
            case DB_FILTER_SUMMARIZATION:
            case PTM_FINDER_SUMMARIZE:
            case PTM_FINDER_FILTER_SUMMARIZATION:
            case SPIDER_SUMMARIZE:
            case SPIDER_FILTER_SUMMARIZATION:
            case LFQ_SUMMARIZE:
                return priority + 1;
            default:
                return priority;
        }
    }

    private void registerWorker(WorkCommand workCommand) throws Exception {
        final RegisterWorker registerWorker = workCommand.getRegisterWorker();
        final UUID workerId = UUID.fromString(registerWorker.getWorkerId());
        final int threadCount = registerWorker.getThreadCount();
        final int gpuCount = registerWorker.getGpuCount();
        final int memoryAmount = registerWorker.getMemoryAmount();
        final WorkerRef workerRef = data.workers().get(workerId);
        final ActorPath senderPath = getSendorPath();
        final boolean isDataLoader = registerWorker.getSupportsStepTypesCount() == 1 && registerWorker.getSupportsStepTypes(0) == StepType.DATA_LOADING;
        if (!Strings.isNullOrEmpty(config.checkVersion()) && !registerWorker.getSoftwareVersion().equals(config.checkVersion())) {
            log.error("Worker {}:{} has wrong version {}", workerId, senderPath, registerWorker.getSoftwareVersion());
            reply(WorkCommandFactory.ackReject(workCommand, "Wrong version"));
        } else if (registerWorker.getSupportsStepTypesCount() == 0) {
            log.error("Worker {}:{} does not support any StepTypes", workerId, senderPath);
            reply(WorkCommandFactory.ackReject(workCommand, "Not supported StepTypes provided"));
        } else if (workerRef == null) {
            final int threadLimit = data.threadLimit();
            if (!isDataLoader && data.currentThreadCount() + threadCount > threadLimit) {
                log.warning("Worker {}:{}({} threads) trying to join but ignored, over thread count limit {}", workerId, senderPath, threadCount, threadLimit);
                reply(WorkCommandFactory.ackReject(workCommand, "Over thread limit"));
            } else {
                log.info("Worker {}:{} registered", workerId, senderPath);
                final WorkEvent workerRegistration = WorkEventFactory.workerRegistration(
                    workerId,
                    senderPath.toSerializationFormat(),
                    threadCount,
                    gpuCount,
                    memoryAmount,
                    registerWorker.getSupportsStepTypesList(),
                    registerWorker.getProcessId(),
                    registerWorker.getIpAddress(),
                    registerWorker.getSoftwareVersion(),
                    registerWorker.getAttached()
                );
                persist(workerRegistration, () -> reply(WorkCommandFactory.ackAccept(workCommand)));
            }
        } else {
            final ActorPath workerPath = workerRef.workerActorPath();
            if (senderPath.equals(workerPath)) {
                data = data.plusWorkerContactNow(workerId);
                log.debug("Worker {} already registered. Ignoring registration request.", workerId);
                reply(WorkCommandFactory.ackAccept(workCommand));
            } else {
                log.warning("Worker {} already registered with path {}. Actor with different path {} trying to register. Ignoring registration request.", workerId, workerPath, senderPath);
                reply(WorkCommandFactory.ackReject(workCommand, "Already registered"));
            }
        }
    }

    private ActorPath getSendorPath() {
        final ActorPath path = getSender().path();
        if (path.toStringWithoutAddress().startsWith("/system/receptionist")) {
            return ActorPaths.fromString(URLDecoder.decode(path.name()));
        } else {
            return path;
        }
    }

    private void workRequest(WorkCommand workCommand) throws Exception {
        final WorkRequest workRequest = workCommand.getWorkRequest();
        final UUID workerId = UUID.fromString(workRequest.getWorkerId());
        final Map<UUID, WorkerRef> workers = data.workers();
        if (!workers.containsKey(workerId)) {
            log.info("Worker {} not registered. Ignoring work request.", workerId);
            return;
        }
        final ActorPath senderPath = getSendorPath();
        final ActorPath workerPath = workers.get(workerId).workerActorPath();
        if (!workerPath.equals(senderPath)) {
            log.info("Work request ignored, worker {} sender {} not of expected path {}", workerId, senderPath, workerPath);
            return;
        }

        data = data.plusWorkerContactNow(workerId);
        scheduleWork(workRequest);
    }

    private void cancelProject(WorkCommand workCommand) throws Exception {
        final CancelProject cancelProject = workCommand.getCancelProject();
        final String projectId = cancelProject.getProjectId();
        log.info("Received cancel project {} command", projectId);

        PMap<UUID, Tuple3<Instant, Integer, TaskRegistration>> tasks = data.tasks();
        final PMap<UUID, WorkerRef> workers = data.workers();
        final Map<UUID, UUID> taskIdToWorkerId = new HashMap<>();

        for (Map.Entry<UUID, PMap<UUID, Instant>> entry : data.workerTasks().entrySet()) {
            for (Map.Entry<UUID, Instant> task : entry.getValue().entrySet()) {
                final UUID taskId = task.getKey();
                final UUID workerId = entry.getKey();
                final Tuple3<Instant, Integer, TaskRegistration> taskValue = tasks.get(taskId);
                if (taskValue == null) {
                    log.warning("Task {} not found, but worker {} is working on it. Sending cancel to worker.", taskId, workerId);
                    sendWorkerCommand(WorkerMessageFactory.worker(workerId).cancelTask(taskId));
                    continue;
                }
                if (projectId.equals(taskValue.t3().getProjectId())) {
                    taskIdToWorkerId.put(taskId, workerId);
                    log.info("Cancelling project {} task {} on worker {}", projectId, taskId, workerId);
                    sendWorkerCommand(WorkerMessageFactory.worker(workerId).cancelTask(taskId));
                }
            }
        }

        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(UUID.fromString(projectId), ADMIN_USERID);
        for (Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>> entry : tasks.entrySet()) {
            final UUID taskId = entry.getKey();
            final UUID workerId = taskIdToWorkerId.get(taskId);
            if (projectId.equals(entry.getValue().t3().getProjectId())) {
                if (workerId == null) {
                    sendProjectCommand(commandFactory.task(taskId).failed("Task cancelled"));
                    persist(WorkEventFactory.taskCancelled(taskId));
                } else {
                    persist(WorkEventFactory.taskWorkedCancelled(taskId, workerId));
                }
            }
        }

        defer(WorkCommandFactory.ackAccept(workCommand), this::reply);
    }

    private void cancelTask(WorkCommand workCommand) throws Exception {
        final CancelTask cancelTask = workCommand.getCancelTask();
        final UUID taskId = UUID.fromString(cancelTask.getTaskId());
        final Tuple3<Instant, Integer, TaskRegistration> task = data.tasks().get(taskId);
        if (task == null) {
            final UUID projectId = UUID.fromString(cancelTask.getProjectId());
            final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
            sendProjectCommand(commandFactory.task(taskId).notFound());
            return;
        }
        Optional<UUID> workerId = findWorkerByTask(taskId);
        WorkCommandAck ack = WorkCommandFactory.ackAccept(workCommand);
        if (workerId.isPresent()) {
            log.info("Work Manager cancelling worker {} working on task {}.", workerId, taskId);
            final WorkerCommand cancelTaskCommand = WorkerMessageFactory.worker(workerId.get()).cancelTask(taskId);
            sendWorkerCommand(cancelTaskCommand);
            persist(WorkEventFactory.taskWorkedCancelled(taskId, workerId.get()));
        } else {
            final UUID projectId = UUID.fromString(task.t3().getProjectId());
            log.info("Work Manager found no worker working on task {}. Cancelling task.", taskId);
            final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
            sendProjectCommand(commandFactory.task(taskId).failed("Task cancelled"));
            persist(WorkEventFactory.taskCancelled(taskId));
        }
        defer(ack, this::reply);
    }

    private Optional<UUID> findWorkerByTask(UUID taskId) {
        for (Map.Entry<UUID, PMap<UUID, Instant>> entry : data.workerTasks().entrySet()) {
            if (entry.getValue().containsKey(taskId)) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }

    private TaskDescription overrideTaskResource(UUID taskId, int cpu, int gpu, int memory) {
        Tuple3<Instant, Integer, TaskRegistration> oldTaskValue = data.tasks().getOrDefault(taskId, null);
        if (oldTaskValue != null) {
            TaskDescription newDescription = oldTaskValue.t3().getTaskDescription().toBuilder()
                .setCpu(cpu)
                .setGpu(gpu)
                .setMemory(memory)
                .build();
            data = data.withTasks(data.tasks().plus(taskId,
                Tuple3.create(oldTaskValue.t1(), oldTaskValue.t2(), oldTaskValue.t3().toBuilder().setTaskDescription(newDescription).build())));
            log.info("Task " + taskId + " resource override to CPU/MEMORY/GPU: " + cpu + "/" + memory + "/" + gpu);
            return newDescription;
        } else {
            return null;
        }
    }

    // tuple is cpuAvailable, gpuAvailable, memoryAvailable, assigned tasks
    private Tuple4<Integer, Integer, Integer, List<StartTask>> assignTasks(
        UUID workerId, boolean isDataLoader, int cpuAvailable, int gpuAvailable, int memoryAvailable,
        List<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>> doableTasks) {
        final Tuple4<UUID, Integer, Integer, Integer> maxResource = maxResource();
        final WorkerRef target = data.workers().get(workerId);
        final int maxCPU = target.threadCount();
        final int maxMemory = target.memoryAmount();
        final int maxGPU = target.gpuCount();
        List<StartTask> assignedTasks = new ArrayList<>();
        for (Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>> task : doableTasks) {
            final UUID taskId = task.getKey();
            final TaskDescription taskDescription = task.getValue().t3().getTaskDescription();
            int cpuRequired = taskDescription.getCpu();
            int gpuRequired = taskDescription.getGpu();
            int memoryRequired = taskDescription.getMemory();
            if (cpuAvailable == 0 || (!isDataLoader && memoryAvailable == 0) || (gpuRequired > 0 && gpuAvailable == 0)) {
                // this worker no longer has resource
                break;
            } else if (gpuRequired > 0) {
                // if we require gpu, just queue it to the GPU worker and try to schedule it with as much resource as possible
                cpuRequired = Math.min(cpuRequired, maxCPU);
                gpuRequired = Math.min(gpuRequired, maxGPU);
                memoryRequired = Math.min(memoryRequired, maxMemory);
                if (cpuRequired != taskDescription.getCpu() || gpuRequired != taskDescription.getGpu() || memoryRequired != taskDescription.getMemory()) {
                    TaskDescription taskOverride = overrideTaskResource(taskId, cpuRequired, gpuRequired, memoryRequired);
                    assignedTasks.add(StartTask.newBuilder().setTaskId(taskId.toString()).setTaskDescription(taskOverride).build());
                } else {
                    assignedTasks.add(StartTask.newBuilder().setTaskId(taskId.toString()).setTaskDescription(taskDescription).build());
                }
                cpuAvailable = Math.max(0, cpuAvailable - cpuRequired);
                gpuAvailable = Math.max(0, gpuAvailable - gpuRequired);
                memoryAvailable = Math.max(0, memoryAvailable - memoryRequired);
            } else if (!isDataLoader && (cpuRequired > maxResource.t2() || gpuRequired > maxResource.t3() || memoryRequired > maxResource.t4())) {
                // this task cannot fit into any of the workers, we try to run it with most available resource
                cpuRequired = Math.min(cpuRequired, maxResource.t2());
                gpuRequired = Math.min(gpuRequired, maxResource.t3());
                memoryRequired = Math.min(memoryRequired, maxResource.t4());
                TaskDescription taskOverride = overrideTaskResource(taskId, cpuRequired, gpuRequired, memoryRequired);
                assignedTasks.add(StartTask.newBuilder().setTaskId(taskId.toString()).setTaskDescription(taskOverride).build());
                cpuAvailable = Math.max(0, cpuAvailable - cpuRequired);
                gpuAvailable = Math.max(0, gpuAvailable - gpuRequired);
                memoryAvailable = Math.max(0, memoryAvailable - memoryRequired);
            } else if (cpuAvailable >= cpuRequired && gpuAvailable >= gpuRequired && memoryAvailable >= memoryRequired) {
                // has enough resource, assign it
                assignedTasks.add(StartTask.newBuilder().setTaskId(taskId.toString()).setTaskDescription(taskDescription).build());
                cpuAvailable -= cpuRequired;
                gpuAvailable -= gpuRequired;
                memoryAvailable -= memoryRequired;
            } else if (memoryRequired <= maxResource.t4() && memoryRequired > maxMemory) {
                // this task can run somewhere else but not on this worker, safe to jump it
                log.info("Task {} cannot run on worker {} because of memory constraint, skip it for now",
                    taskId, workerId);
            } else {
                break;
            }
        }

        return Tuple4.create(cpuAvailable, gpuAvailable, memoryAvailable, assignedTasks);
    }

    private void scheduleWork(WorkRequest request) throws Exception {
        // try to send works to this worker
        final UUID workerId = UUID.fromString(request.getWorkerId());
        final boolean isDataLoader = data.workerSupportsStepTypes().get(workerId).contains(StepType.DATA_LOADING);
        final Set<StepType> supportsStepTypes = data.workerSupportsStepTypes().get(workerId);

        final List<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>> readyTasks = data.tasks().entrySet().stream()
            .filter(task -> data.readyTasks().contains(task.getKey()) && task.getValue().t2() > 0) // task.getValue().t2() is priority
            .sorted(TASK_COMPARATOR.reversed())
            .filter(task -> supportsStepTypes.contains(task.getValue().t3().getTaskDescription().getStepType()))
            .collect(Collectors.toList());

        final List<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>> gpuTasks = readyTasks.stream()
            .filter(task -> task.getValue().t3().getTaskDescription().getGpu() > 0)
            .collect(Collectors.toList());

        final List<Map.Entry<UUID, Tuple3<Instant, Integer, TaskRegistration>>> cpuTasks = readyTasks.stream()
            .filter(task -> task.getValue().t3().getTaskDescription().getGpu() == 0)
            .collect(Collectors.toList());

        // try schedule gpu tasks first then do it for cpu tasks
        Tuple4<Integer, Integer, Integer, List<StartTask>> gpuAssignment = assignTasks(
            workerId, isDataLoader, request.getAvailableCPU(), request.getAvailableGPU(), request.getAvailableMemory(), gpuTasks
        );

        List<StartTask> assignedTasks = new ArrayList<>(gpuAssignment.t4());

        Tuple4<Integer, Integer, Integer, List<StartTask>> cpuAssignment = assignTasks(
            workerId, isDataLoader, gpuAssignment.t1(), gpuAssignment.t2(), gpuAssignment.t3(), cpuTasks
        );
        assignedTasks.addAll(cpuAssignment.t4());
        List<UUID> taskIds = assignedTasks.stream().map(startTask -> UUID.fromString(startTask.getTaskId())).collect(Collectors.toList());

        if (!taskIds.isEmpty() || workerResourceChanged(workerId, cpuAssignment.t1(), cpuAssignment.t2(), cpuAssignment.t3())) {
            // in case the worker's resource is changed, even if we are not doing any concrete task, we persist this to update it's resource
            // but we won't persist anything if there is no resource change
            WorkEvent taskStartedBatch = WorkEventFactory.taskStartedBatch(workerId, taskIds, cpuAssignment.t1(), cpuAssignment.t2(), cpuAssignment.t3());
            persist(taskStartedBatch, () -> {
                if (!taskIds.isEmpty()) {
                    log.info("Sending worker {} start tasks in batch: {}", workerId, taskIds.stream().map(UUID::toString).collect(Collectors.joining(",")));
                    final WorkerCommand workerCommand = WorkerMessageFactory.worker(workerId).startTaskBatch(assignedTasks);
                    sendWorkerCommand(workerCommand, getSendorPath());

                    // update project
                    final WorkerRef workerRef = data.workers().get(workerId);
                    for (UUID taskId : taskIds) {
                        final Tuple3<Instant, Integer, TaskRegistration> taskValue = data.tasks().get(taskId);
                        if (taskValue == null) {
                            log.warning("Unable to find task with task id {}", taskId);
                            continue;
                        }
                        final UUID projectId = UUID.fromString(taskValue.t3().getProjectId());
                        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
                        sendProjectCommand(commandFactory.task(taskId).processing(workerId, workerRef.ipAddress()));
                    }
                }
            });
        }

        if (taskIds.isEmpty()) {
            // even there is no task we still send it to worker as an acknowledgement
            final WorkerCommand workerCommand = WorkerMessageFactory.worker(workerId).startTaskBatch(assignedTasks);
            sendWorkerCommand(workerCommand, getSendorPath());
        }
    }

    private boolean hasGPUWorker() {
        for (Map.Entry<UUID, WorkerRef> worker : data.workers().entrySet()) {
            if (worker.getValue().gpuCount() > 0) {
                return true;
            }
        }
        return false;
    }

    private int totalAvailableThreads() {
        int total = 0;
        for (Map.Entry<UUID, WorkerRef> worker : data.workers().entrySet()) {
            total += worker.getValue().availableThread();
        }
        return total;
    }

    private List<Integer> workerAvailableThreads() {
        return data.workers().values().stream().map(WorkerRef::availableThread).collect(Collectors.toList());
    }

    private int maxAvailableThreadsByWorker() {
        int maxThreads = 0;
        for (Map.Entry<UUID, WorkerRef> worker : data.workers().entrySet()) {
            maxThreads = Math.max(maxThreads, worker.getValue().availableThread());
        }
        return maxThreads;
    }

    private Tuple4<UUID, Integer, Integer, Integer> maxResource() {
        // Get the max cpu, gpu, memory from the worker with most memory, with workerId
        UUID workerId = null;
        int maxCPU = 0, maxGPU = 0, maxMemory = 0;
        for (Map.Entry<UUID, WorkerRef> worker : data.workers().entrySet()) {
            if (worker.getValue().memoryAmount() > maxMemory) {
                maxCPU = worker.getValue().threadCount();
                maxGPU = worker.getValue().gpuCount();
                maxMemory = worker.getValue().memoryAmount();
                workerId = worker.getKey();
            }
        }

        return Tuple4.create(workerId, maxCPU, maxGPU, maxMemory);
    }

    private boolean workerResourceChanged(UUID workerId, int cpuAvailable, int gpuAvailable, int memoryAvailable) {
        WorkerRef worker = data.workers().get(workerId);
        return worker.availableThread() != cpuAvailable || worker.availableGPU() != gpuAvailable || worker.availableMemory() != memoryAvailable;
    }

    private int queuedTasks(Set<UUID> taskIds) {
        return (int)data.tasks().entrySet().stream()
            .filter(entry -> !taskIds.contains(entry.getKey())
                && entry.getValue().t3().getTaskDescription().getStepType() != StepType.DATA_LOADING
                && data.readyTasks().contains(entry.getKey())
            )
            .count();
    }

    // attach cpu, gpu, memory resource needed to task description
    private TaskDescription taskWithResource(int queuedTasks, TaskDescription description, boolean doubleResource) {
        if (description.getStepType() == StepType.DATA_LOADING) {
            // this is fixed for data loading
            return description.toBuilder().setCpu(1).setGpu(0).setMemory(0).build();
        } else {
            int memoryMultiplier = doubleResource ? 2 : 1;
            if (!description.getResourceOverride().isEmpty()) {
                String[] tokens = description.getResourceOverride()
                    .trim()
                    .replace("<<<", "")
                    .replace(">>>", "")
                    .split(",");
                try {
                    int cpu = Integer.parseInt(tokens[0]);
                    int gpu = Integer.parseInt(tokens[1]);
                    int memory = Integer.parseInt(tokens[2]) * memoryMultiplier;
                    return description.toBuilder().setCpu(cpu).setGpu(gpu).setMemory(memory).build();
                } catch (NumberFormatException e) {
                    // do nothing
                }
            }

            int cpu = config.minTaskThreads();
            int gpu = 0;
            int memory = cpu * 2 + 2;
            try {
                // override cpu and memory from resource calculator
                Worker.TaskWrapper wrapper = new Worker.TaskWrapper(description);

                // default cpu assignment attempt
                double bestTime = Double.MAX_VALUE;
                int bound = Math.min(config.maxTaskThreads(), maxAvailableThreadsByWorker());
                for (int t = config.minTaskThreads(); t <= bound; t += config.taskThreadsIncrement()) {
                    int tasksPerRound = 0;
                    for (int workerThreads : workerAvailableThreads()) {
                        tasksPerRound += Math.floorDiv(workerThreads, t);
                    }
                    double time = Math.ceil((double)queuedTasks / tasksPerRound) / (Math.log(t) / Math.log(2));
                    if (time < bestTime) {
                        cpu = t;
                        bestTime = time;
                    }
                }

                // actual assigned cpu
                cpu = resourceCalculator.calculateCPUThreads(
                    wrapper.service(), description.getKeyspace(), wrapper.get(), cpu
                );
                // give at least 1gb memory
                memory = Math.max(1, resourceCalculator.calculateMemoryAmount(
                    description.getStepType(), description.getKeyspace(), wrapper.get(), cpu
                )) * memoryMultiplier;
                gpu = resourceCalculator.calculateGPUs(
                    wrapper.service(), description.getKeyspace(), wrapper.get()
                );
                if (gpu < 0) {
                    if (hasGPUWorker()) {
                        gpu = -gpu;
                    } else {
                        gpu = 0;
                    }
                }

                if (gpu > 0) {
                    cpu = FIXED_CPU_THREADS_WITH_GPU;
                }

                log.info("Task " + wrapper.taskId() + " of " + description.getStepType() + " cpu/memory/gpu calculated to be "
                    + cpu + "/" + memory + "/" + gpu);
            } catch (Exception e) {
                log.error(e, "Unable to calculate cpu/memory/gpu with resource calculator, use default values");
            }

            return description.toBuilder().setCpu(cpu).setGpu(gpu).setMemory(memory).build();
        }
    }

    private void sendWorkerCommand(WorkerCommand command) {
        final UUID workerId = UUID.fromString(command.getWorkerId());
        final WorkerRef workerRef = data.workers().get(workerId);
        if (workerRef == null) {
            log.warning("Worker {} not found. Command {} will not be sent to worker.", workerId, command.getCommandCase());
        } else {
            sendWorkerCommand(command, workerRef.workerActorPath());
        }
    }

    private void sendWorkerCommand(WorkerCommand workerCommand, ActorPath workerPath) {
        Objects.requireNonNull(workerPath);
        log.debug("Sending worker {} command {}", workerPath, workerCommand.getCommandCase());
        this.deliver(workerPath, deliveryId -> workerCommand.toBuilder().setDeliveryId(deliveryId).build());
    }


    private CompletionStage<List<Pair<UUID, WorkerState>>> queryWorkerState() {
        final ActorSystem system = getContext().getSystem();
        final WorkManagerState data = this.data;
        return Source.from(data.workers().entrySet())
                .mapAsync(config.parallelism(), entry -> {
                    final ActorRef workerRef = system.actorFor(entry.getValue().workerActorPath());
                    return PatternsCS.ask(workerRef, WorkerMessageFactory.worker(entry.getKey()).queryWorker(), config.timeout())
                            .thenApply(response -> {
                                final WorkerResponse workerResponse = (WorkerResponse) response;
                                return Pair.create(entry.getKey(), workerResponse.getWorkerState());
                            })
                            .exceptionally(e -> Pair.create(entry.getKey(), null));
                })
                .runWith(Sink.seq(), ActorMaterializer.create(system));
    }

    private void handleStartTaskRejection(UUID workerId, StartTask startTask, String error) throws Exception {
        final UUID taskId = UUID.fromString(startTask.getTaskId());
        final UUID projectId = UUID.fromString(data.tasks().get(taskId).t3().getProjectId());
        final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
        persist(WorkEventFactory.taskFailed(taskId, workerId, error));
        sendProjectCommand(commandFactory.task(taskId).failed(error));
    }

    private void workerCommandAck(WorkerCommandAck ack) {
        final long deliveryId = ack.getDeliveryId();
        final UUID workerId = UUID.fromString(ack.getWorkerId());
        data = data.plusWorkerContactNow(workerId);
        try {
            if (ack.getAckStatus() == AckStatus.REJECT) {
                final WorkerCommand workerCommand = getUnconfirmedDelivery(deliveryId)
                    .map(WorkerCommand.class::cast)
                    .orElseThrow(() -> new NoSuchElementException("DeliveryId does not exist on WorkerCommandAck from {}" + getSender()));

                switch (workerCommand.getCommandCase()) {
                    case STARTTASK: {
                        final StartTask startTask = workerCommand.getStartTask();
                        final String error = ack.getAckStatusDescription();
                        handleStartTaskRejection(workerId, startTask, error);
                    }
                    case STARTTASKBATCH: {
                        final StartTaskBatch batch = workerCommand.getStartTaskBatch();
                        final String error = ack.getAckStatusDescription();
                        for (StartTask startTask : batch.getStartTasksList()) {
                            handleStartTaskRejection(workerId, startTask, error);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, "workerCommandAck handling threw exception");
        }
        confirmDelivery(deliveryId);
    }

    private void disconnectWorkerCheck() {
        log.debug("WorkManager is checking worker heartbeats");
        final Instant now = Instant.now();
        data.workersLastContact().forEach((workerId, lastContact) -> {
            final WorkerRef workerRef = data.workers().get(workerId);
            if (workerRef == null) {
                return;
            }
            final ActorPath workerPath = workerRef.workerActorPath();
            final Duration durationSinceLastContact = Duration.between(lastContact, now);
            if (durationSinceLastContact.compareTo(config.unreachableDisconnect()) > 0) {
                final String timeoutCause = MessageFormat.format("Worker {0} has been silent for {1} seconds.", workerId, durationSinceLastContact.getSeconds());
                log.info(timeoutCause);
                try {
                    final PMap<UUID, Instant> tasks = data.workerTasks().get(workerId);
                    if (tasks != null && !tasks.isEmpty()) {
                        for (Map.Entry<UUID, Instant> task : tasks.entrySet()) {
                            final UUID taskId = task.getKey();
                            final Tuple3<Instant, Integer, TaskRegistration> taskValue = data.tasks().get(taskId);
                            if (taskValue == null) {
                                log.info("Disconnect worker {} at ip {}. Task not found.", workerId, workerRef.ipAddress());
                            } else {
                                log.info("Disconnect worker {} at ip {} and fail task {}", workerId, workerRef.ipAddress(), taskId);
                                final UUID projectId = UUID.fromString(taskValue.t3().getProjectId());
                                final ProjectAggregateCommandFactory commandFactory = ProjectAggregateCommandFactory.project(projectId, ADMIN_USERID);
                                persist(WorkEventFactory.taskFailed(taskId, workerId, timeoutCause));
                                sendProjectCommand(commandFactory.task(taskId).failed(timeoutCause));
                            }
                        }
                    } else {
                        log.info("Disconnect idle worker {} at ip {}.", workerId, workerRef.ipAddress());
                    }
                    removeUndeliveredMessages(workerPath);
                    persist(WorkEventFactory.workerDisconnected(workerId));
                } catch (Exception e) {
                    log.error(e, "Exception during disconnect of worker " + workerId);
                }
            } else {
                log.debug("Worker {} has not been updated for {} seconds, ping it", workerId, durationSinceLastContact.getSeconds());
                final WorkerCommand pingWorkerCommand = WorkerMessageFactory.worker(workerId).pingWorker();
                getContext().getSystem().actorFor(workerPath).tell(pingWorkerCommand, getSelf());
            }
        });
    }

    private CompletionStage<WorkQueryResponse.Builder> queryWorker(WorkQueryRequest.QueryWorker query, WorkManagerState data) {
        return queryWorkerState().thenApply(list -> {
            QueryWorkerResponse.Builder responseBuilder = QueryWorkerResponse.newBuilder();
            Map<UUID, WorkerState> workerStateMap = list.stream()
                    .filter(pair -> pair.second() != null)
                    .collect(Collectors.toMap(Pair::first, Pair::second));
            data.workers().forEach((workerId, workerRef) -> {
                if (workerRef == null) {
                    return;
                }
                MonitorWorker.Builder workerBuilder = MonitorWorker.newBuilder();
                WorkerState workerState = workerStateMap.get(workerId);
                Instant instant = data.workersLastContact().get(workerId);
                if (instant == null && workerState != null) { // a query from worker just now
                    instant = Instant.now();
                }
                boolean statsNotAvailable = false;
                if (workerState == null) {
                    workerState = WorkerState.getDefaultInstance();
                    statsNotAvailable = true;
                }
                final PMap<UUID, Instant> tasks = data.workerTasks().get(workerId);
                final Set<StepType> supportStepType =
                    data.workerSupportsStepTypes().getOrDefault(workerId, Collections.emptySet());
                WorkerType workerType = supportStepType.size() == 1 && supportStepType.contains(StepType.DATA_LOADING) ?
                    WorkerType.LOADER : WorkerType.WORKER;
                workerBuilder.setWorkerId(workerId.toString())
                    .setWorkerType(workerType)
                    .setProcessId(workerRef.processId())
                    .setIPAddress(workerRef.ipAddress())
                    .setPort(workerRef.workerActorPath().address().port().isEmpty() ? "" : workerRef.workerActorPath().address().port().get().toString())
                    //.setAvailableMemoryBytes(workerState.getAvailableMemoryBytes())
                    .setFreeMemoryBytes(workerState.getFreeMemoryBytes())
                    .setUsedMemoryBytes(workerState.getUsedMemoryBytes())
                    .setCpuUsagePercent(workerState.getCpuUsagePercent())
                    .setStatus(tasks.isEmpty() ? Status.IDLE : Status.OCCUPIED)
                    .setTotalGPU(workerRef.gpuCount())
                    .setTotalThreads(workerRef.threadCount())
                    .setAvailableMemoryBytes((long)workerRef.memoryAmount() * 1073741824L)
                    .setTaskId(workerType == WorkerType.LOADER && tasks.size() > 0 ? tasks.keySet().toArray()[0].toString() : "")
                    .setStatsNotAvailable(statsNotAvailable);
                    //.setThread(workerRef.threadCount());
                if (instant != null) {
                    workerBuilder.setLastTimeCommunicated(instant.getEpochSecond());
                }

                for (WorkerProcessState process : workerState.getProcessesList()) {
                    final UUID taskId = UUID.fromString(process.getTaskId());
                    Tuple3<Instant, Integer, TaskRegistration> taskValue = data.tasks().get(taskId);
                    if (taskValue != null) {
                        TaskDescription description = taskValue.t3().getTaskDescription();
                        workerBuilder.addProcesses(MonitorWorkerProcess.newBuilder()
                            .setTaskId(taskId.toString())
                            .setType(taskValue.t3().getTaskDescription().getStepType())
                            .setPriority(taskValue.t2())
                            .setThreads(description.getCpu())
                            .setGpu(description.getGpu())
                            .setUsedMemoryBytes(process.getUsedMemoryBytes())
                            .setAvailableMemoryBytes((long)description.getMemory() * 1073741824L)
                            .setFreeMemoryBytes(process.getFreeMemoryBytes())
                            .setCpuUsagePercent(process.getCpuUsagePercent())
                            .setEstimatedThreads(description.getCpu())
                            .build()
                        );
                    }
                }

                responseBuilder.addWorkers(workerBuilder.build());
            });

            return WorkQueryResponse.newBuilder()
                .setListWorkers(responseBuilder);
        });
    }
}
