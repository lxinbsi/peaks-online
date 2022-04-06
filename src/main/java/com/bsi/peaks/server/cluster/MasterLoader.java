package com.bsi.peaks.server.cluster;

import akka.Done;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.japi.pf.FI;
import akka.japi.tuple.Tuple3;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.WorkCommandFactory;
import com.bsi.peaks.event.WorkEventFactory;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.work.LoaderPing;
import com.bsi.peaks.event.work.ResourceRelease;
import com.bsi.peaks.event.work.ServiceRequest;
import com.bsi.peaks.event.work.TaskCompleted;
import com.bsi.peaks.event.work.WorkCommand;
import com.bsi.peaks.event.work.WorkCommandAck;
import com.bsi.peaks.event.work.WorkEvent;
import com.bsi.peaks.event.work.WorkEventAck;
import com.bsi.peaks.message.WorkerMessageFactory;
import com.bsi.peaks.messages.service.CancelService;
import com.bsi.peaks.messages.worker.PingWorker;
import com.bsi.peaks.messages.worker.StartTask;
import com.bsi.peaks.messages.worker.StartTaskBatch;
import com.bsi.peaks.messages.worker.WorkerCommand;
import com.bsi.peaks.messages.worker.WorkerProcessState;
import com.bsi.peaks.messages.worker.WorkerResponse;
import com.bsi.peaks.messages.worker.WorkerState;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.server.Launcher;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.es.WorkManager;
import com.bsi.peaks.server.es.tasks.DataLoadingTask;
import com.bsi.peaks.server.es.tasks.Helper;
import com.bsi.peaks.server.service.UploaderService;
import com.bsi.peaks.service.common.ServiceCancellationException;
import com.bsi.peaks.service.messages.results.TaskFailure;
import com.google.common.collect.ImmutableList;
import com.sun.management.OperatingSystemMXBean;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author xhe
 *         created on 9/17/20.
 */
public class MasterLoader extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private static final String WORKER_CLEANUP_TICK = "WORKER_CLEANUP";
    public static final String NAME = "masterLoader";
    public static final String PING = "PING";

    private final UUID masterLoaderId = UUID.randomUUID();
    private final SystemConfig systemConfig;
    private final WorkerMessageFactory messageFactory;
    private final OperatingSystemMXBean operatingSystemMXBean;
    private final UploaderService uploaderService;
    private final String version;
    private final ActorRef masterRef;
    private final Cancellable cleanupTask;
    private final ApplicationStorageFactory storageFactory;

    private final Queue<Worker.TaskWrapper> pendingTasks = new LinkedList<>();
    private final Map<UUID, Pair<Worker.TaskWrapper, Process>> tasks = new ConcurrentHashMap<>();
    private final Map<UUID, Pair<WorkEvent, Instant>> results = new HashMap<>();
    private final Map<UUID, Tuple3<ActorRef, String, Instant>> actors = new HashMap<>();
    private Instant lastWorkRequestSentAt = null;

    private final int totalCPU;
    private int curCPU = 0;
    private final int loaderMemoryInGB;

    public static Props props(
        SystemConfig sysConfig,
        ActorRef masterRef,
        UploaderService uploaderService,
        ApplicationStorageFactory storageFactory

    ) {
        return Props.create(MasterLoader.class, () -> new MasterLoader(sysConfig,  masterRef, uploaderService, storageFactory));
    }

    private MasterLoader(
        SystemConfig config,
        ActorRef masterRef,
        UploaderService uploaderService,
        ApplicationStorageFactory storageFactory
    ) {
        this.systemConfig = config;
        this.messageFactory = WorkerMessageFactory.worker(masterLoaderId);
        this.version = config.getVersion();
        this.masterRef = masterRef;
        this.cleanupTask = scheduledCleanup(config.getClusterWorkerCleanupInterval());
        this.uploaderService = uploaderService;
        this.loaderMemoryInGB = context().system().settings().config().getInt("peaks.server.data-loader.memory");

        // define worker states
        this.operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        this.totalCPU = systemConfig.getDataLoaderParallelism();
        this.storageFactory = storageFactory;

        registerMasterLoader();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        cleanupTask.cancel();
        log.info("MasterLoader {} is stopped", masterLoaderId);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(WorkerCommand.class, WorkerCommand::hasPing, this::handlePing)
            .match(ServiceRequest.class, this::handleServiceRequest)
            .match(LoaderPing.class, this::handleLoaderPing)
            .match(WorkerCommand.class, WorkerCommand::hasOfferTask, commandAckOnFailure(command -> {
                tellWorkManager(messageFactory.ackAccept(command));
                requestWork();
            }))
            .match(WorkerCommand.class, WorkerCommand::hasStartTaskBatch, commandAckOnFailure(command -> {
                tellWorkManager(messageFactory.ackAccept(command));
                lastWorkRequestSentAt = null;
                final StartTaskBatch batch = command.getStartTaskBatch();
                if (batch.getStartTasksCount() > 0) {
                    log.info("Received tasks from master: {}", batch.getStartTasksList().stream().map(StartTask::getTaskId).collect(Collectors.joining(",")));
                    for (StartTask startTask : batch.getStartTasksList()) {
                        final TaskDescription taskDescription = startTask.getTaskDescription();
                        Worker.TaskWrapper task = new Worker.TaskWrapper(taskDescription);
                        if (canDoWork(task)) {
                            doWork(task);
                        } else {
                            pendingTasks.add(task);
                        }
                    }
                } else {
                    log.debug("Received no tasks from master");
                }
            }))
            .match(WorkEvent.class, WorkEvent::hasTaskProcessing, this::tellWorkManager)
            .match(WorkerCommand.class, WorkerCommand::hasCancelTask, commandAckOnFailure(command -> {
                tellWorkManager(messageFactory.ackAccept(command));
                final UUID taskId = UUID.fromString(command.getCancelTask().getTaskId());
                pendingTasks.removeIf(task -> {
                    try {
                        return task.taskId().equals(taskId);
                    } catch (Exception e) {
                        log.error("Could not convert task description into data loading task");
                    }
                    return true;
                });
                if (actors.containsKey(taskId)) {
                    Tuple3<ActorRef, String, Instant> actorService = actors.get(taskId);
                    actorService.t1().tell(CancelService.newBuilder().setService(actorService.t2()).build(), self());
                }
                if (tasks.containsKey(taskId)) {
                    Process process = tasks.get(taskId).second();
                    if (process != null) {
                        process.destroyForcibly();
                    }
                }
            }))
            .match(WorkerCommand.class, WorkerCommand::hasQueryState, this::handleWorkerStateQuery)
            .match(WorkCommandAck.class, ignore -> {})
            .match(WorkEventAck.class, workEventAck -> {
                final WorkEvent ackEvent = workEventAck.getEvent();
                if (ackEvent.hasTaskCompleted()) {
                    UUID taskId = UUID.fromString(ackEvent.getTaskCompleted().getTaskId());
                    results.remove(taskId);
                } else if (ackEvent.hasTaskFailed()) {
                    UUID taskId = UUID.fromString(ackEvent.getTaskFailed().getTaskId());
                    results.remove(taskId);
                }
            })
            .match(TaskFailure.class, failure -> {
                sender().tell(PoisonPill.getInstance(), self());
                if (tasks.containsKey(failure.taskId())) {
                    if (failure.error() instanceof ServiceCancellationException) {
                        log.info("Received failure caused by cancellation for task {}, ignore it", failure.taskId());
                    } else {
                        Exception error = new RuntimeException("Received failure from service", failure.error());
                        fail(failure.taskId(), error);
                    }
                } else {
                    log.warning("Received TaskFailure for task {} but not working on it", failure.taskId());
                }
            })
            .match(TaskCompleted.class, ignore -> {
                final Worker.TaskWrapper taskWrapper = this.tasks.get(ignore.getTaskId()).first();
                final DataLoadingTask dataLoadingTask = (DataLoadingTask) Helper.convertParameters(taskWrapper.getTaskDescription().getJsonParamaters());
                uploaderService.releaseQuota(dataLoadingTask.sourceFile());
                sendWorkManagerEvent(WorkEvent.newBuilder().setTaskCompleted(ignore).build());
            })
            .matchEquals(WORKER_CLEANUP_TICK, ignore -> cleanup())
            .match(ResourceRelease.class, this::releaseResource)
            .matchAny(o -> log.debug("Unexpected message received {}", o))
            .build();
    }

    private void doWork(Worker.TaskWrapper task) {
        useResource(task);
        offLoadDataLoading(task);
    }

    private void useResource(Worker.TaskWrapper task) {
        curCPU += task.getTaskDescription().getCpu();
    }

    private void handlePing(WorkerCommand event) {
        final PingWorker ping = event.getPing();
        getSender().tell(messageFactory.pongWorker(ping), getSelf());
    }

    private void handleWorkerStateQuery(WorkerCommand command) {
        try {
            OperatingSystemMXBean bean = operatingSystemMXBean;
            final long totalMem = Runtime.getRuntime().totalMemory();
            final long freeMem = Runtime.getRuntime().freeMemory();
            final long usedMem = totalMem - freeMem;
            WorkerState.Builder stateBuilder = WorkerState.newBuilder()
                .setIPAddress(InetAddress.getLocalHost().getHostAddress())
                .setProcessId(Integer.valueOf(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]))
                .setPort("0")
                .setAvailableMemoryBytes(totalMem)
                .setFreeMemoryBytes(freeMem)
                .setUsedMemoryBytes(usedMem)
                .setCpuUsagePercent((float)bean.getProcessCpuLoad());

            for (UUID taskId : tasks.keySet()) {
                // always add a default process state, with all default values of 0 in case of time out
                stateBuilder.addProcesses(WorkerProcessState.newBuilder().setTaskId(taskId.toString()).build());
            }
            final WorkerState workerState = stateBuilder.build();
            final WorkerResponse workerResponse = WorkerMessageFactory.worker(masterLoaderId).workerStateResponse(workerState);
            sender().tell(workerResponse, self());
        } catch (Exception e) {
            getSender().tell(e, getSelf());
        }
    }

    private boolean canDoWork(Worker.TaskWrapper task) {
        int requiredCPU = task.getTaskDescription().getCpu() > 0 ? task.getTaskDescription().getCpu() : systemConfig.getWorkerDefaultCPU();
        return availableCPU() >= requiredCPU;
    }


    private void releaseResource(ResourceRelease release) {
        UUID taskId = UUID.fromString(release.getTaskId());
        actors.remove(taskId);
        final Pair<Worker.TaskWrapper, Process> pair = tasks.remove(taskId);
        if (pair != null) {
            curCPU -= pair.first().getTaskDescription().getCpu();
        }
        while (!pendingTasks.isEmpty() && canDoWork(pendingTasks.peek())) {
            final Worker.TaskWrapper task = pendingTasks.poll();
            doWork(task);
        }
        if (availableCPU() >= 1) {
            requestWork();
        }
    }

    private void requestWork() {
        if (lastWorkRequestSentAt == null) {
            sendWorkManagerCommand(WorkCommandFactory.workRequest(masterLoaderId, this.availableCPU(), 0, 0));
            lastWorkRequestSentAt = Instant.now();
        } else {
            log.error("MasterLoader is already requesting work, don't request it again");
        }
    }


    private void sendWorkManagerEvent(WorkEvent event) {
        WorkManager.instance(getContext().getSystem()).tell(event, getSelf());
    }

    private void sendWorkManagerCommand(WorkCommand command) {
        WorkManager.instance(getContext().getSystem()).tell(command, getSelf());
    }

    private void offLoadDataLoading(Worker.TaskWrapper task) {
        // to block on storage if migration is still happening
        storageFactory.getStorage(task.getTaskDescription().getKeyspace());
        String service = "dataLoadingService";
        int memory = loaderMemoryInGB;
        int cpu = 1;
        CompletableFuture.supplyAsync(() -> {
            List<String> cmd = ImmutableList.of(
                "java", "-Djava.library.path=.",
                "-cp", System.getProperty("java.class.path"),
                "-XX:+UseG1GC", "-XX:SoftRefLRUPolicyMSPerMB=100", "-XX:MaxGCPauseMillis=500",
                "-XX:G1HeapRegionSize=32m", "-XX:+CrashOnOutOfMemoryError",
                "-Xms" + memory + "G", "-Xmx" + memory + "G",
                Launcher.class.getName(),
                "-c", Launcher.confFile,
                "-p", task.taskId().toString() + "|" + service + "|" + cpu + "|" + masterLoaderId.toString()
            );
            ProcessBuilder builder = new ProcessBuilder(cmd);
            try {
                log.info("Starting sub process for task " + task.taskId() + ": " + String.join(" ", cmd));
                Process process = builder.inheritIO().start();
                tasks.put(task.taskId(), Pair.create(task, process));
                int code = process.waitFor();
                log.info("Process for task {} exited with code {}", task.taskId(), code);
                uploaderService.releaseQuota(((DataLoadingTask)task.get()).uploadedFile());
                ResourceRelease release = ResourceRelease.newBuilder().setTaskId(task.taskId().toString()).build();
                self().tell(release, self());
                if (code != 0) {
                    fail(task.taskId(), new IllegalStateException("Loader sub process exited with code " + code));
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            return Done.getInstance();
        });
    }

    private void handleServiceRequest(ServiceRequest request) {
        UUID taskId = UUID.fromString(request.getTaskId());
        if (tasks.containsKey(taskId)) {
            if (actors.containsKey(taskId)) {
                final Tuple3<ActorRef, String, Instant> replacement =
                    Tuple3.create(actors.get(taskId).t1(), request.getService(),  Instant.now());
                actors.put(taskId, replacement);
            } else {
                actors.put(taskId, Tuple3.create(sender(), request.getService(), Instant.now()));
            }
            final TaskDescription taskDescription = tasks.get(taskId).first().getTaskDescription();
            sender().tell(taskDescription, self());
        } else {
            log.error("Received a service request with unmatched task id: " + taskId + ", this should never happen!");
        }
    }

    private void handleLoaderPing(LoaderPing loaderPing) {
        UUID taskId = UUID.fromString(loaderPing.getTaskId());
        if (actors.containsKey(taskId)) {
            final Tuple3<ActorRef, String, Instant> original = actors.get(taskId);
            final Tuple3<ActorRef, String, Instant> replacement = Tuple3.create(sender(), original.t2(), Instant.now());
            actors.put(taskId, replacement);
        } else {
            final Tuple3<ActorRef, String, Instant> replacement = Tuple3.create(sender(), "", Instant.now());
            actors.put(taskId, replacement);
        }
    }

    private void fail(UUID taskId, Exception error) {
        final WorkEvent workEvent = WorkEventFactory.taskFailed(taskId, masterLoaderId, error.toString());
        tellWorkManager(workEvent);
        results.put(taskId, Pair.create(workEvent, Instant.now()));
    }

    protected FI.UnitApply<WorkerCommand> commandAckOnFailure(FI.UnitApply<WorkerCommand> next) {
        return command -> {
            try {
                final UUID workerId = UUID.fromString(command.getWorkerId());
                if (!workerId.equals(workerId)) {
                    throw new IllegalArgumentException("WorkerCommand does has different masterLoaderId {}" + command.getWorkerId());
                }
                next.apply(command);
            } catch (Throwable throwable) {
                log.error(throwable, "Failed validating command {} from sender {}", command.getCommandCase(), sender().getClass().getSimpleName());
                getSender().tell(messageFactory.ackReject(command, throwable.getMessage()), getSelf());
            }
        };
    }

    private void tellWorkManager(Object message) {
        if (message instanceof WorkCommand) {
            final WorkCommand command = (WorkCommand) message;
            if (command.hasWorkRequest()) {
                log.info("Worker {} send a work request to master", masterLoaderId);
            }
        }
        WorkManager.instance(getContext().getSystem()).tell(message, getSelf());
    }

    private Cancellable scheduledCleanup(FiniteDuration interval) {
        return getContext().system().scheduler().schedule(
            interval,
            interval,
            getSelf(),
            WORKER_CLEANUP_TICK,
            getContext().dispatcher(),
            getSelf()
        );
    }

    private int availableCPU() {
        return totalCPU - curCPU;
    }

    private void cleanup() {
        for (UUID taskId : actors.keySet()) {
            Tuple3<ActorRef, String, Instant> actor = actors.get(taskId);
            if (Instant.now().isAfter(actor.t3().plusNanos(systemConfig.getWorkerHeartbeatTimeout().toNanos()))) {
                actor.t1().tell(PoisonPill.getInstance(), self());
                if (tasks.containsKey(taskId)) {
                    Process process = tasks.get(taskId).second();
                    if (process != null) {
                        process.destroyForcibly();
                    }
                }
            } else {
                actor.t1().tell(PING, self());
            }
        }

        // check lost messages
        if (lastWorkRequestSentAt != null) {
            Instant deadline = lastWorkRequestSentAt.plusNanos(3 * systemConfig.getClusterWorkerCleanupInterval().toNanos());
            if (Instant.now().isAfter(deadline)) {
                lastWorkRequestSentAt = null;
                if (availableCPU() >= 1) {
                    requestWork();
                }
            }
        }

        // retry sending results
        for (Pair<WorkEvent, Instant> result : results.values()) {
            if (result.second().isBefore(Instant.now().minusNanos(systemConfig.getClusterMasterCleanupInterval().toNanos()))) {
                tellWorkManager(result.first());
            }
        }
        registerMasterLoader();
    }
    protected void ackAccept(WorkerCommand command) {
        getSender().tell(messageFactory.ackAccept(command), getSelf());
    }

    protected void ackReject(WorkerCommand command, String error) {
        getSender().tell(messageFactory.ackReject(command, error), getSelf());
    }

    private void registerMasterLoader() {
        List<StepType> supportsStepTypes = ImmutableList.of(StepType.DATA_LOADING);
        sendWorkManagerCommand(WorkCommandFactory.registerWorker(masterLoaderId, this.availableCPU(), 0, 0,
            version, supportsStepTypes, MasterLoader.getPID(), MasterLoader.getIPAddress(), true));
    }

    private static long getPID() {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        if (processName != null && processName.length() > 0) {
            try {
                return Long.parseLong(processName.split("@")[0]);
            }
            catch (Exception e) {
                return 0;
            }
        }

        return 0;
    }

    private static String getIPAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "";
        }
    }
}
