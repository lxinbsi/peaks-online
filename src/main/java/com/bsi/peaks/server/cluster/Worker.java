package com.bsi.peaks.server.cluster;

import akka.Done;
import akka.actor.AbstractActor;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.client.ClusterClient;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.japi.Predicate;
import akka.japi.pf.FI;
import akka.japi.tuple.Tuple3;
import akka.util.Timeout;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.TaskLogRepository;
import com.bsi.peaks.event.WorkCommandFactory;
import com.bsi.peaks.event.WorkEventFactory;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.TaskParameters;
import com.bsi.peaks.event.work.*;
import com.bsi.peaks.message.WorkerMessageFactory;
import com.bsi.peaks.messages.service.CancelService;
import com.bsi.peaks.messages.service.DBSearchSummarizeResult;
import com.bsi.peaks.messages.service.IdSummarizeTaskResult;
import com.bsi.peaks.messages.service.LfqSummarizationFilterResult;
import com.bsi.peaks.messages.service.PtmFinderSummarizeResult;
import com.bsi.peaks.messages.service.ServiceTaskResult;
import com.bsi.peaks.messages.service.SlFilterSummarizationResult;
import com.bsi.peaks.messages.service.SpiderSummarizeResult;
import com.bsi.peaks.messages.service.TagSearchResult;
import com.bsi.peaks.messages.worker.PingWorker;
import com.bsi.peaks.messages.worker.StartTask;
import com.bsi.peaks.messages.worker.StartTaskBatch;
import com.bsi.peaks.messages.worker.WorkerCommand;
import com.bsi.peaks.messages.worker.WorkerProcessState;
import com.bsi.peaks.messages.worker.WorkerState;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.system.Task;
import com.bsi.peaks.model.system.steps.DbSummarizeStepOutput;
import com.bsi.peaks.model.system.steps.DbSummarizeStepOutputBuilder;
import com.bsi.peaks.model.system.steps.PtmFinderSummarizeStepOutput;
import com.bsi.peaks.model.system.steps.PtmFinderSummarizeStepOutputBuilder;
import com.bsi.peaks.model.system.steps.SpiderSummarizeStepOutput;
import com.bsi.peaks.model.system.steps.SpiderSummarizeStepOutputBuilder;
import com.bsi.peaks.server.Launcher;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.dependency.WorkerDependency;
import com.bsi.peaks.server.es.tasks.Helper;
import com.bsi.peaks.service.common.ServiceCancellationException;
import com.bsi.peaks.service.common.messages.FdrMessageAdaptor;
import com.bsi.peaks.service.messages.results.TaskFailure;
import com.bsi.peaks.service.services.ServiceActor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.management.OperatingSystemMXBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import scala.compat.java8.FutureConverters;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * @author Shengying Pan
 * Created by span on 1/23/17.
 */
public class Worker extends AbstractActor {
    public static final String NAME = "worker";
    public static final String PING = "PING";
    private static final String WORKER_CLEANUP_TICK = "WORKER_CLEANUP";
    private static File NULL_FILE = new File(
        (System.getProperty("os.name")
            .startsWith("Windows") ? "NUL" : "/dev/null")
    );
    private final UUID workerId = UUID.randomUUID();
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final boolean detached;
    private final SystemConfig systemConfig;
    private final ApplicationStorageFactory storageFactory;
    private final ActorRef clusterClient;
    private final Cancellable cleanupTask;
    private final WorkerMessageFactory messageFactory;
    private final OperatingSystemMXBean operatingSystemMXBean;
    private final boolean debugMode;
    private final Map<String, Integer> debugCounters = new HashMap<>();
    private final Queue<TaskWrapper> pendingTasks = new LinkedList<>();
    // only tasks need to handle concurrency as tasks are created asynchronously in detached mode
    private final Map<UUID, Tuple3<TaskWrapper, Process, List<String>>> tasks = new ConcurrentHashMap<>();
    private final Map<UUID, Pair<WorkEvent, Instant>> results = new HashMap<>();
    private final Map<UUID, Tuple3<ActorRef, String, Instant>> actors = new HashMap<>();
    private Instant lastWorkRequestSentAt = null;

    private final int totalCPU;
    private final List<String> gpus;
    private final int totalMemory;
    private int curCPU = 0;
    private final Set<String> usedGPUs = new HashSet<>();
    private int curMemory = 0;

    public Worker(
        SystemConfig systemConfig,
        ApplicationStorageFactory storageFactory,
        ActorRef clusterClient,
        Map<String, Class<? extends ServiceActor<?>>> serviceActors
    ) {
        this.systemConfig = systemConfig;
        this.storageFactory = storageFactory;
        this.clusterClient = clusterClient;
        this.messageFactory = WorkerMessageFactory.worker(workerId);
        Config debugConfig = context().system().settings().config().getConfig("peaks.worker.debug");
        debugMode = debugConfig.getBoolean("enabled");


        // define worker states
        this.cleanupTask = scheduledCleanup(systemConfig.getClusterWorkerCleanupInterval());
        this.totalCPU = systemConfig.getWorkerMaxCPU();
        this.gpus = systemConfig.getWorkerGPUs();
        this.totalMemory = systemConfig.getWorkerMaxMemory();
        this.detached = systemConfig.isWorkerDetached();
        log.info("Worker " + workerId + " is starting in " + (detached ? "detached" : "attached")
            + " mode with the resource CPU/MEMORY/GPU: " + totalCPU + "/" + totalMemory + "/[" + String.join(",", gpus) + "]");

        if (debugMode) {
            for (String serviceName : serviceActors.keySet()) {
                // check debug mode
                debugCounters.put(serviceName, 0);
            }
        }

        workerRegistration();
        requestWork();
        operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    }

    public static Props props(
        SystemConfig systemConfig,
        ApplicationStorageFactory storageFactory,
        ActorRef clusterClient,
        Map<String, Class<? extends ServiceActor<?>>> serviceActors
    ) {
        return Props.create(Worker.class, () -> new Worker(systemConfig, storageFactory, clusterClient, serviceActors));
    }

    private void workerRegistration() {
        log.debug("Worker {} updating its presence by registering to master", workerId);
        List<StepType> supportsStepTypes = ImmutableList.of(
            //Sample steps
            StepType.DATA_REFINE,
            StepType.FEATURE_DETECTION,
            //Denovo Steps
            StepType.DENOVO,
            StepType.DENOVO_FILTER,
            //Peptide Library Search Steps
            StepType.PEPTIDE_LIBRARY_PREPARE,
            //Db Steps
            StepType.FDB_PROTEIN_CANDIDATE_FROM_FASTA,
            StepType.DB_TAG_SEARCH,
            StepType.DB_TAG_SUMMARIZE,
            StepType.DB_PRE_SEARCH,
            StepType.DB_BATCH_SEARCH,
            StepType.DB_SUMMARIZE,
            StepType.DB_FILTER_SUMMARIZATION,
            StepType.DB_DENOVO_ONLY_TAG_SEARCH,
            //Ptm Steps
            StepType.PTM_FINDER_BATCH_SEARCH,
            StepType.PTM_FINDER_SUMMARIZE,
            StepType.PTM_FINDER_FILTER_SUMMARIZATION,
            StepType.PTM_FINDER_DENOVO_ONLY_TAG_SEARCH,
            //Spider Steps
            StepType.SPIDER_BATCH_SEARCH,
            StepType.SPIDER_SUMMARIZE,
            StepType.SPIDER_FILTER_SUMMARIZATION,
            StepType.SPIDER_DENOVO_ONLY_TAG_SEARCH,
            //Spectral Library
            StepType.SL_SEARCH,
            StepType.SL_PEPTIDE_SUMMARIZATION,
            StepType.SL_FILTER_SUMMARIZATION,
            //DDA Library Search
            StepType.DDA_SL_SEARCH,
            //DIA DB Search
            StepType.DIA_DB_PRE_SEARCH,
            StepType.DIA_DB_SEARCH,
            StepType.DIA_DB_PEPTIDE_SUMMARIZE,
            StepType.DIA_DB_FILTER_SUMMARIZE,
            //DIA Denovo
            StepType.DIA_DATA_REFINE,
            StepType.DIA_DB_DATA_REFINE
        );
        tellMaster(WorkCommandFactory.registerWorker(
            workerId, totalCPU, gpus.size(), totalMemory, systemConfig.getVersion(), supportsStepTypes, getPID(), getIPAddress(), !detached
        ));
    }

    public static long getPID() {
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

    public static String getIPAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "";
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(WorkerCommand.class, WorkerCommand::hasPing, this::handlePing)
            .match(WorkerCommand.class, WorkerCommand::hasStopWorker, commandAckOnFailure(this::handleStopWorkerCommand))
            .match(WorkerCommand.class, WorkerCommand::hasCancelTask, commandAckOnFailure(command -> {
                tellMaster(messageFactory.ackAccept(command));
                final UUID taskId = UUID.fromString(command.getCancelTask().getTaskId());
                pendingTasks.removeIf(task -> task.taskId().equals(taskId));
                if (actors.containsKey(taskId)) {
                    Tuple3<ActorRef, String, Instant> actorService = actors.get(taskId);
                    actorService.t1().tell(CancelService.newBuilder().setService(actorService.t2()).build(), self());
                }
                if (tasks.containsKey(taskId)) {
                    Process process = tasks.get(taskId).t2();
                    if (process != null) {
                        process.destroyForcibly();
                    }
                }
            }))
            .match(WorkerCommand.class, WorkerCommand::hasStartTaskBatch, commandAckOnFailure(command -> {
                tellMaster(messageFactory.ackAccept(command));
                lastWorkRequestSentAt = null;
                final StartTaskBatch batch = command.getStartTaskBatch();
                if (batch.getStartTasksCount() > 0) {
                    log.info("Received tasks from master: {}", batch.getStartTasksList().stream().map(StartTask::getTaskId).collect(Collectors.joining(",")));
                    for (StartTask startTask : batch.getStartTasksList()) {
                        processStartTask(startTask);
                    }
                } else {
                    log.debug("Received no tasks from master");
                }
            }))
            .match(WorkerCommand.class, WorkerCommand::hasOfferTask, commandAckOnFailure(command -> {
                tellMaster(messageFactory.ackAccept(command));
                requestWork();
            }))
            .match(ServiceRequest.class, this::handleServiceRequest)
            .match(ServiceContainerUpdate.class, update -> {
                if (actors.containsKey(update.taskId())) {
                    Tuple3<ActorRef, String, Instant> actor = actors.get(update.taskId());
                    actors.put(update.taskId(), Tuple3.create(actor.t1(), actor.t2(), Instant.now()));
                }
            })
            .match(WorkerCommand.class, WorkerCommand::hasQueryState, this::handleWorkerStateQuery)
            .matchEquals(WORKER_CLEANUP_TICK, ignore -> cleanup())
            .match(ResourceRelease.class, this::releaseResource)
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
                if (!detached) {
                    // only release resource for attached service, for attached ones, release when sub process is killed
                    ResourceRelease release = ResourceRelease.newBuilder().setTaskId(failure.taskId().toString()).build();
                    self().tell(release, self());
                }
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
            .match(ServiceTaskResult.class, result -> {
                sender().tell(PoisonPill.getInstance(), self());
                if (!detached) {
                    // kill service actor in attached mode
                    ResourceRelease release = ResourceRelease.newBuilder().setTaskId(result.getTaskId()).build();
                    self().tell(release, self());
                }
                handleTaskResult(result, (int) result.getRealTimeMs(), (int) result.getCpuTimeMs());
            })
            .matchAny(o -> log.debug("Unexpected message received {}", o))
            .build();
    }

    private void cleanup() {
        //check service timeout in detached mode
        if (detached) {
            for (UUID taskId : actors.keySet()) {
                Tuple3<ActorRef, String, Instant> actor = actors.get(taskId);
                if (Instant.now().isAfter(actor.t3().plusNanos(systemConfig.getWorkerHeartbeatTimeout().toNanos()))) {
                    actor.t1().tell(PoisonPill.getInstance(), self());
                    if (tasks.containsKey(taskId)) {
                        Process process = tasks.get(taskId).t2();
                        if (process != null) {
                            process.destroyForcibly();
                        }
                    }
                } else {
                    actor.t1().tell(PING, self());
                }
            }
        }

        // check lost messages
        if (lastWorkRequestSentAt != null) {
            Instant deadline = lastWorkRequestSentAt.plusNanos(3 * systemConfig.getClusterWorkerCleanupInterval().toNanos());
            if (Instant.now().isAfter(deadline)) {
                if (availableCPU() >= 2) {
                    lastWorkRequestSentAt = null;
                    requestWork();
                }
            }
        }

        // retry sending results
        for (Pair<WorkEvent, Instant> result : results.values()) {
            if (result.second().isBefore(Instant.now().minusNanos(systemConfig.getClusterMasterCleanupInterval().toNanos()))) {
                tellMaster(result.first());
            }
        }
        workerRegistration();
    }

    private void processStartTask(StartTask startTask) throws IOException, ClassNotFoundException {
        TaskWrapper task = new TaskWrapper(startTask.getTaskDescription());
        if (task.getTaskDescription().getGpu() > 0 && gpus.size() == 0) {
            fail(task.taskId(), new IllegalStateException("Worker " + workerId + " received gpu task " + task.taskId() + " but has no gpu"));
        } else if (canDoWork(task)) {
            doWork(task);
        } else {
            // put it in the queue for now
            pendingTasks.add(task);
        }
    }

    private int availableCPU() {
        return totalCPU - curCPU;
    }

    private List<String> availableGPUs() {
        List<String> availableGPUs = new ArrayList<>(gpus);
        availableGPUs.removeAll(usedGPUs);
        return availableGPUs;
    }

    private int availableMemory() {
        return totalMemory - curMemory;
    }

    private boolean canDoWork(TaskWrapper task) {
        int requiredCPU = task.taskDescription.getCpu() > 0 ? task.taskDescription.getCpu() : systemConfig.getWorkerDefaultCPU();
        int requiredGPU = task.taskDescription.getGpu() > 0 ? task.taskDescription.getGpu() : systemConfig.getWorkerDefaultGPU();
        int requiredMemory = task.taskDescription.getMemory() > 0 ? task.taskDescription.getMemory() : systemConfig.getWorkerDefaultMemory();
        return availableCPU() >= requiredCPU && availableGPUs().size() >= requiredGPU && availableMemory() >= requiredMemory;
    }

    private void releaseResource(ResourceRelease release) {
        if (release.getTaskId() == null || release.getTaskId().isEmpty()) {
            log.error("invalid task id");
            return;
        }
        UUID taskId = UUID.fromString(release.getTaskId());
        actors.remove(taskId);
        Tuple3<TaskWrapper, Process, List<String>> tuple = tasks.remove(taskId);
        if (tuple != null) {
            curMemory -= tuple.t1().taskDescription.getMemory();
            curCPU -= tuple.t1().taskDescription.getCpu();
            usedGPUs.removeAll(tuple.t3());
        }

        while (!pendingTasks.isEmpty() && canDoWork(pendingTasks.peek())) {
            TaskWrapper task = pendingTasks.poll();
            doWork(task);
        }
        if (availableCPU() >= 2) {
            requestWork();
        }
    }

    private void requestWork() {
        if (lastWorkRequestSentAt == null) {
            tellMaster(WorkCommandFactory.workRequest(workerId, availableCPU(), availableGPUs().size(), availableMemory()));
            lastWorkRequestSentAt = Instant.now();
        } else {
            log.debug("Duplicate work request, last work request sent at " + lastWorkRequestSentAt.toEpochMilli());
        }
    }

    private void handleServiceRequest(ServiceRequest request) {
        // this is only possible in detached mode
        UUID taskId = UUID.fromString(request.getTaskId());
        if (tasks.containsKey(taskId)) {
            String fixedPathString = ActorPath.fromString(request.getContainerPath()).toStringWithAddress(sender().path().address());
            ActorSelection selection = context().actorSelection(fixedPathString);
            ActorRef serviceActor = FutureConverters
                .toJava(selection.resolveOne(Timeout.create(systemConfig.getWorkerMessageTimeout())))
                .toCompletableFuture().join();
            actors.put(taskId, Tuple3.create(serviceActor, request.getService(), Instant.now()));
            sender().tell(tasks.get(taskId).t1().taskDescription, self());
        } else {
            log.error("Received a service request with unmatched task id: " + taskId + ", this should never happen!");
        }
    }

    private void handlePing(WorkerCommand event) {
        final PingWorker ping = event.getPing();
        tellMaster(messageFactory.pongWorker(ping));
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
                stateBuilder.addProcesses(WorkerProcessState.newBuilder().setTaskId(taskId.toString()));
            }

            if (detached) {
                // in detached mode, collect information from services
                ActorRef aggregator = getContext().watch(getContext().actorOf(
                    WorkerStateAggregator.props(sender(), workerId, stateBuilder, actors.size())
                ));
                actors.values().forEach(tuple -> {
                    ActorRef serviceContainer = tuple.t1();
                    serviceContainer.tell(command, aggregator);
                });
            } else {
                // in attached mode, everything is inside the same JVM
                for (UUID taskId : actors.keySet()) {
                    stateBuilder.addProcesses(WorkerProcessState.newBuilder().setTaskId(taskId.toString()).build());
                }
                sender().tell(WorkerMessageFactory.worker(workerId).workerStateResponse(stateBuilder.build()), self());
            }
        } catch (Exception e) {
            getSender().tell(e, getSelf());
        }
    }

    private void fail(UUID taskId, Exception error) {
        log.info("Updating master about the failure of task {}: {}", taskId, error);
        final WorkEvent workEvent = WorkEventFactory.taskFailed(taskId, workerId, error.toString());
        tellMaster(workEvent);
        results.put(taskId, Pair.create(workEvent, Instant.now()));
    }

    private void handleStopWorkerCommand(WorkerCommand command) {
        log.info("Worker {} received WorkerPoisonPill. Now stopping.", workerId);
        tellMaster(messageFactory.ackAccept(command));
        getContext().stop(self());
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

    private void handleTaskResult(ServiceTaskResult result, int realTimeMs, int cpuTimeMs) {
        final WorkEvent workEvent;
        if (result.getTaskId() == null || result.getTaskId().isEmpty()) {
            log.error("No task id found!");
            return;
        }
        final UUID taskId = UUID.fromString(result.getTaskId());
        Tuple3<TaskWrapper, Process, List<String>> taskTuple = tasks.getOrDefault(taskId, null);
        if (taskTuple == null) {
            log.warning("Received task result for task {} but its process is not tracked, ignore it", taskId);
            return;
        }

        try {
            final StepType stepType = taskTuple.t1().getTaskDescription().getStepType();
            log.info("Starting work on result type " + stepType);

            ImmutableMap<StepType, Predicate<ServiceTaskResult>> stepTypeVerificationMap = ImmutableMap.<StepType, Predicate<ServiceTaskResult>>builder()
                .put(StepType.SL_SEARCH, ServiceTaskResult::hasSlSearchResult)
                .put(StepType.SL_PEPTIDE_SUMMARIZATION, ServiceTaskResult::hasSlPeptideSummarizationResult)
                .put(StepType.SL_FILTER_SUMMARIZATION, ServiceTaskResult::hasSlFilterSummarizationResult)
                .put(StepType.SILAC_RT_ALIGNMENT, ServiceTaskResult::hasSilacRtAlignmentResult)
                .put(StepType.SILAC_FEATURE_ALIGNMENT, ServiceTaskResult::hasSilacFeatureAlignmentResult)
                .put(StepType.SILAC_FEATURE_VECTOR_SEARCH, ServiceTaskResult::hasSilacFeatureVectorSearchResult)
                .put(StepType.SILAC_PEPTIDE_SUMMARIZE, ServiceTaskResult::hasSilacPeptideSummarizeResult)
                .put(StepType.SILAC_FILTER_SUMMARIZE, ServiceTaskResult::hasSilacFilterSummarizeResult)
                .put(StepType.DDA_SL_SEARCH, ServiceTaskResult::hasSlSearchResult)
                .put(StepType.DIA_DB_SEARCH, ServiceTaskResult::hasDiaDbSearchResult)
                .put(StepType.DIA_DB_PEPTIDE_SUMMARIZE, ServiceTaskResult::hasSlPeptideSummarizationResult)
                .put(StepType.DIA_DB_FILTER_SUMMARIZE, ServiceTaskResult::hasSlFilterSummarizationResult)
                .put(StepType.DIA_DATA_REFINE, ServiceTaskResult::hasDiaDenovoSearchResult)
                .put(StepType.DIA_DB_DATA_REFINE, ServiceTaskResult::hasDiaDenovoSearchResult)
                .put(StepType.DIA_DB_PRE_SEARCH, ServiceTaskResult::hasDiaDbPreSearchResult)
                .put(StepType.DIA_LFQ_RETENTION_TIME_ALIGNMENT, ServiceTaskResult::hasLfqDiaRetentionTimeAlignmentResult)
                .put(StepType.DIA_LFQ_FEATURE_EXTRACTION, ServiceTaskResult::hasLfqDiaFeatureExtractorResult)
                .put(StepType.DIA_LFQ_SUMMARIZE, ServiceTaskResult::hasLfqDiaSummarizerResult)
                .put(StepType.DIA_LFQ_FILTER_SUMMARIZATION, ServiceTaskResult::hasLfqSummarizationFilterResult)
                .build();

            switch (stepType) {
                case DATA_REFINE: {
                    if (!result.hasDataRefineResult()) {
                        throw new IllegalStateException("Expected DataRefineResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case FEATURE_DETECTION: {
                    if (!result.hasFeatureDetectionResult()) {
                        throw new IllegalStateException("Expected FeatureDetectionResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DENOVO: {
                    if (!result.hasDenovoResult()) {
                        throw new IllegalStateException("Expected DenovoResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case FDB_PROTEIN_CANDIDATE_FROM_FASTA: {
                    if (!result.hasFastDBProteinCandidateFromFastaResult()) {
                        throw new IllegalStateException("Expected Fast Db protein candidate fasta instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case PEPTIDE_LIBRARY_PREPARE: {
                    if (!result.hasPeptideLibraryPrepareResult()) {
                        throw new IllegalStateException("Expected PeptideLibraryPrepare instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DB_TAG_SEARCH: {
                    final TagSearchResult tagSearchResult = result.getTagSearchResult();
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DB_TAG_SUMMARIZE: {
                    if (!result.hasTagSearchSummarizeResult()) {
                        throw new IllegalStateException("Expected TagSearchSummarizeResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DB_PRE_SEARCH: {
                    if (!result.hasDBPreSearchResult()) {
                        throw new IllegalStateException("Expected DataRefineResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DB_BATCH_SEARCH: {
                    if (!result.hasDBSearchFractionResult()) {
                        throw new IllegalStateException("Expected DBSearchFractionResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DB_SUMMARIZE: {
                    final DBSearchSummarizeResult dbSearchSummarizeResult = result.getDBSearchSummarizeResult();
                    final IdSummarizeTaskResult idSummarizeTaskResult = dbSearchSummarizeResult.getIdSummarizationResult();
                    final DbSummarizeStepOutput output = new DbSummarizeStepOutputBuilder()
                        .fdrs(FdrMessageAdaptor.fromFdr(idSummarizeTaskResult.getFdrsList()))
                        .fdrMapping(idSummarizeTaskResult.getFdrMappingMap())
                        .build();
                    workEvent = WorkEventFactory.taskCompletedJson(taskId, workerId, realTimeMs, cpuTimeMs, output);
                    break;
                }
                case DB_DENOVO_ONLY_TAG_SEARCH:
                case PTM_FINDER_DENOVO_ONLY_TAG_SEARCH:
                case SPIDER_DENOVO_ONLY_TAG_SEARCH: {
                    if (!result.hasDeNovoOnlyTagSearchServiceResult()) {
                        throw new IllegalStateException("Expected DeNovoOnlyTagSearchResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case PTM_FINDER_BATCH_SEARCH: {
                    if (!result.hasPtmFinderFractionResult()) {
                        throw new IllegalStateException("Expected PtmFinderFractionResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case PTM_FINDER_SUMMARIZE: {
                    final PtmFinderSummarizeResult ptmFinderSummarizeResult = result.getPtmFinderSummarizeResult();
                    final IdSummarizeTaskResult idSummarizeTaskResult = ptmFinderSummarizeResult.getIdSummarizationResult();
                    final PtmFinderSummarizeStepOutput output = new PtmFinderSummarizeStepOutputBuilder()
                        .fdrs(FdrMessageAdaptor.fromFdr(idSummarizeTaskResult.getFdrsList()))
                        .fdrMapping(idSummarizeTaskResult.getFdrMappingMap())
                        .build();
                    workEvent = WorkEventFactory.taskCompletedJson(taskId, workerId, realTimeMs, cpuTimeMs, output);
                    break;
                }
                case SPIDER_BATCH_SEARCH: {
                    if (!result.hasSpiderFractionResult()) {
                        throw new IllegalStateException("Expected SpiderFractionResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case SPIDER_SUMMARIZE: {
                    final SpiderSummarizeResult ptmFinderSummarizeResult = result.getSpiderSummarizeResult();
                    final IdSummarizeTaskResult idSummarizeTaskResult = ptmFinderSummarizeResult.getIdSummarizationResult();
                    final SpiderSummarizeStepOutput output = new SpiderSummarizeStepOutputBuilder()
                        .fdrs(FdrMessageAdaptor.fromFdr(idSummarizeTaskResult.getFdrsList()))
                        .fdrMapping(idSummarizeTaskResult.getFdrMappingMap())
                        .build();
                    workEvent = WorkEventFactory.taskCompletedJson(taskId, workerId, realTimeMs, cpuTimeMs, output);
                    break;
                }
                case LFQ_RETENTION_TIME_ALIGNMENT: {
                    if (!result.hasRetentionTimeAlignmentResult()) {
                        throw new IllegalStateException("Expected Retetion time alignemt result instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case LFQ_FEATURE_ALIGNMENT: {
                    if (!result.hasFeatureAlignmentResult()) {
                        throw new IllegalStateException("Expected FeatureAlignmentResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case LFQ_SUMMARIZE: {
                    if (!result.hasLabelFreeSummarizeResult()) {
                        throw new IllegalStateException("Expected LabelFreeSummarizeResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case REPORTER_ION_Q_BATCH_CALCULATION: {
                    if (!result.hasReporterIonQFractionResult()) {
                        throw new IllegalStateException("Expected ReporterIonQFractionResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case REPORTER_ION_Q_FILTER_SUMMARIZATION: {
                    if (!result.hasReporterIonQSummarizationFilterResult()) {
                        throw new IllegalStateException("Expected ReporterIonQSummarizationFilterResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case REPORTER_ION_Q_NORMALIZATION:{
                    if (!result.hasReporterIonQNormalizationResult()) {
                        throw new IllegalStateException("Expected ReporterIonQNormalizationResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DENOVO_FILTER: {
                    if (!result.hasDenovoSummarizeResult()) {
                        throw new IllegalStateException("Expected DenovoFilterResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DB_FILTER_SUMMARIZATION:
                case PTM_FINDER_FILTER_SUMMARIZATION:
                case SPIDER_FILTER_SUMMARIZATION: {
                    if (!result.hasDbProteinSummarizationFilterResult()) {
                        throw new IllegalStateException("Expected DbProteinSummarizationResult instead of " + result.getResultCase());
                    }
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                case DIA_LFQ_FILTER_SUMMARIZATION:
                case LFQ_FILTER_SUMMARIZATION: {
                    if (!result.hasLfqSummarizationFilterResult()) {
                        throw new IllegalStateException("Expected LfqSummarizationResult instead of " + result.getResultCase());
                    }
                    final LfqSummarizationFilterResult lfqSummarizationFilterResult = result.getLfqSummarizationFilterResult();
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs, b -> b.setLfqSummarizationFilterResult(lfqSummarizationFilterResult));
                    break;
                }
                case DIA_DB_FILTER_SUMMARIZE:
                case SL_FILTER_SUMMARIZATION: {
                    if (!result.hasSlFilterSummarizationResult()) {
                        throw new IllegalStateException("Expected SlFilterSummarizationResult instead of " + result.getResultCase());
                    }
                    final SlFilterSummarizationResult slFilterSummarizationResult = result.getSlFilterSummarizationResult();
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs, b -> b.setSlFilterSummarizationResult(slFilterSummarizationResult));
                    break;
                }
                case DIA_LFQ_RETENTION_TIME_ALIGNMENT:
                case DIA_LFQ_FEATURE_EXTRACTION:
                case DIA_LFQ_SUMMARIZE:
                case SL_SEARCH:
                case SL_PEPTIDE_SUMMARIZATION:
                case SILAC_RT_ALIGNMENT:
                case SILAC_FEATURE_ALIGNMENT:
                case SILAC_FEATURE_VECTOR_SEARCH:
                case SILAC_PEPTIDE_SUMMARIZE:
                case SILAC_FILTER_SUMMARIZE:
                case DDA_SL_SEARCH:
                case DIA_DB_SEARCH:
                case DIA_DB_PEPTIDE_SUMMARIZE:
                case DIA_DB_DATA_REFINE:
                case DIA_DATA_REFINE:
                case DIA_DB_PRE_SEARCH: {
                    validateResultClass(() -> stepTypeVerificationMap.get(stepType).test(result), result, stepType);
                    workEvent = WorkEventFactory.taskCompleted(taskId, workerId, realTimeMs, cpuTimeMs);
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected Task Type " + stepType);
            }
        } catch (Exception e) {
            log.error(e, "Unable to handle task result for task: {}", taskId);
            throw new RuntimeException(e);
        }
        log.info("Worker {} finished task {}, update master and become waiting for ack", workerId, taskId);
        results.put(UUID.fromString(result.getTaskId()), Pair.create(workEvent, Instant.now()));
        tellMaster(workEvent);
    }

    private void validateResultClass(BooleanSupplier check, ServiceTaskResult result, StepType stepType) {
        if (!check.getAsBoolean()) {
            throw new IllegalStateException("Expected result for step " + stepType.name() +  " instead of " + result.getResultCase().name());
        }
    }

    private void doWork(TaskWrapper task) {
        final UUID taskId = task.taskId();
        final String service = task.service();
        log.info("Start processing task {}", taskId);


        // debug mode check
        if (debugMode) {
            // release resource right away if stop here
            ResourceRelease prematureRelease = ResourceRelease.newBuilder().setTaskId(taskId.toString()).build();

            Config debugConfig = context().system().settings().config().getConfig("peaks.worker.debug");
            Config servicesConfig = debugConfig.getConfig("services");
            ConfigList serviceActions = servicesConfig.hasPath(service) ?
                servicesConfig.getList(service) : servicesConfig.getList("all");

            int actionIndex = debugCounters.get(service);
            if (actionIndex < serviceActions.size()) {
                Config action = serviceActions.get(actionIndex).atPath("service");
                debugCounters.put(service, actionIndex + 1);
                if (action.hasPath("service.throw") && action.getBoolean("service.throw")) {
                    log.info(task.service() + " task " + taskId + " is failed intentionally by debugging instruction");
                    fail(taskId, new RuntimeException("Mock exception thrown from debug mode"));
                    self().tell(prematureRelease, self());
                    return;
                } else if (action.hasPath("service.delay")) {
                    java.time.Duration interval = action.getDuration("service.delay");
                    try {
                        log.info("Worker is being intentionally delayed for " + interval.toMillis() + " milliseconds");
                        Thread.sleep(interval.toMillis());
                        log.info("Worker is waking up from the delay");
                    } catch (InterruptedException e) {
                        fail(taskId, e);
                        self().tell(prematureRelease, self());
                        return;
                    }
                } else {
                    fail(taskId, new IllegalStateException("Illegal debug mode action"));
                    self().tell(prematureRelease, self());
                    return;
                }
            }
        }

        // actually do the work
        Pair<TaskWrapper, List<String>> taskAndGPUs = useResource(task);

        if (systemConfig.isDebug()) {
            taskAndGPUs.first().saveTask(storageFactory.getSystemStorage().getTaskLogRepository()).whenComplete((done, error) -> {
                if (error != null) {
                    log.error(error, "Unable to save next task {} for service {} to database",
                        taskId, service);
                } else {
                    log.debug("Worker saved task {} for service {} to database for future reference",
                        taskId, service);
                }
            });
        }

        if (detached) {
            offloadTask(service, taskAndGPUs.first(), taskAndGPUs.second());
        } else {
            doTask(service, taskAndGPUs.first(), taskAndGPUs.second());
        }
    }

    private Pair<TaskWrapper, List<String>> useResource(TaskWrapper task) {
        TaskWrapper finalTask = task;
        curCPU += task.taskDescription.getCpu();
        curMemory += task.taskDescription.getMemory();


        // TODO: handle gpu usage on task type
        List<String> assignedGpus = new ArrayList<>();

        if (task.taskDescription.getGpu() > 0 && task.protoTask.isPresent()) {
            TaskParameters parameters = task.protoTask.get();
            if (parameters.hasDiaPreSearchTaskParameters()) {
                // only pre search could use GPU, always use the first
                String gpuName = availableGPUs().get(0);
                assignedGpus.add(gpuName);
                TaskParameters parametersOverride = parameters.toBuilder().setDiaPreSearchTaskParameters(
                    parameters.getDiaPreSearchTaskParameters().toBuilder().setGpuName(gpuName)
                ).build();
                try {
                    finalTask = new TaskWrapper(
                        task.taskDescription.toBuilder().setParameters(parametersOverride).build()
                    );
                } catch (Exception e) {
                    throw new RuntimeException("Unable to override gpu settings on task " + task.taskId());
                }
            }
        }

        usedGPUs.addAll(assignedGpus);
        return Pair.create(finalTask, assignedGpus);
    }

    private void doTask(String service, TaskWrapper task, List<String> assignedGPUs) {
        // for attached mode, run in the same jvm
        ActorRef attachedService = getContext().watch(getContext().actorOf(
            Props.create(WorkerDependency.SERVICE_ACTORS.get(service), getSelf(), storageFactory, task.taskDescription.getCpu()),
            service + UUID.randomUUID()
        ));
        actors.put(task.taskId(), Tuple3.create(attachedService, service, Instant.now()));
        tasks.put(task.taskId(), Tuple3.create(task, null, assignedGPUs));
        attachedService.tell(task.get(), self());
    }

    private void offloadTask(String service, TaskWrapper task, List<String> assignedGPUs) {
        CompletableFuture.supplyAsync(() -> {
            int cpu = task.taskDescription.getCpu();
            int memory = task.taskDescription.getMemory();
            List<String> cmd = ImmutableList.of(
                "java", "-Djava.library.path=.",
                "-cp", System.getProperty("java.class.path"),
                "-XX:+UseG1GC", "-XX:SoftRefLRUPolicyMSPerMB=100", "-XX:MaxGCPauseMillis=500",
                "-XX:G1HeapRegionSize=32m", "-XX:+CrashOnOutOfMemoryError",
                "-Xms" + memory + "G", "-Xmx" + memory + "G",
                Launcher.class.getName(),
                "-c", Launcher.confFile,
                "-p", task.taskId().toString() + "|" + service + "|" + cpu
            );
            ProcessBuilder builder = new ProcessBuilder(cmd);
            builder.environment().put("PATH", "./mllib/pytorch");
            builder.environment().put("LD_LIBRARY_PATH", "./mllib/pytorch");
            builder.environment().put("OMP_NUM_THREADS", "8");
            builder.environment().put("MKL_NUM_THREADS", "8");

            try {
                log.info("Starting sub process for task " + task.taskId() + ": " + String.join(" ", cmd));
                Process process = builder.inheritIO().start();
                tasks.put(task.taskId(), Tuple3.create(task, process, assignedGPUs));
                int code = process.waitFor();
                log.info("Process for task {} exited with code {}", task.taskId(), code);
                ResourceRelease release = ResourceRelease.newBuilder().setTaskId(task.taskId().toString()).build();
                self().tell(release, self());
                if (code < 0) {
                    fail(task.taskId(), new IllegalStateException("Worker sub process exited with code " + code + ", possibly timed out"));
                } else if (code > 0) {
                    fail(task.taskId(), new IllegalStateException("Worker sub process exited with code " + code + ", possibly OOM"));
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            return Done.getInstance();
        });
    }

    private void tellMaster(Object message) {
        if (message instanceof WorkCommand) {
            final WorkCommand command = (WorkCommand) message;
            if (command.hasWorkRequest()) {
                WorkRequest request = command.getWorkRequest();
                log.info("Worker {} send a work request to master, available CPU/MEMORY/GPU: "
                    + request.getAvailableCPU() + "/" + request.getAvailableMemory() + "/" + request.getAvailableGPU(), workerId);
            }
        }
        clusterClient.tell(new ClusterClient.SendToAll(Master.PROXY_PATH, message), getSelf());
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("Worker {} is starting", workerId);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        cleanupTask.cancel();
        log.info("Worker {} is stopped", workerId);
        getContext().getSystem().terminate();
    }

    protected FI.UnitApply<WorkerCommand> commandAckOnFailure(FI.UnitApply<WorkerCommand> next) {
        return command -> {
            try {
                final UUID workerId = UUID.fromString(command.getWorkerId());
                if (!workerId.equals(workerId)) {
                    throw new IllegalArgumentException("WorkerCommand does has different workerId {}" + command.getWorkerId());
                }
                next.apply(command);
            } catch (Throwable throwable) {
                log.error(throwable, "Failed validating command {} from sender {}", command.getCommandCase(), sender().getClass().getSimpleName());
                tellMaster(messageFactory.ackReject(command, throwable.getMessage()));
            }
        };
    }

    /**
     * This class is a temprorey structure meant to support tasks as both immutables and protos
     *  the ideal we only use protos for tasks.
     */
    public static final class TaskWrapper {
        private final Optional<Task> immutableTask;
        private final Optional<TaskParameters> protoTask;
        private final TaskDescription taskDescription;

        public TaskWrapper(TaskDescription taskDescription) throws IOException, ClassNotFoundException {
            this.taskDescription = taskDescription;
            switch(taskDescription.getTaskCase()) {
                case JSONPARAMATERS:
                    this.immutableTask = Optional.of(Helper.convertParameters(taskDescription.getJsonParamaters()));
                    this.protoTask = Optional.empty();
                    break;
                case PARAMETERS:
                    this.immutableTask = Optional.empty();
                    this.protoTask = Optional.of(taskDescription.getParameters());
                    break;
                default:
                    throw new IllegalStateException("Unable to create task wrapper");
            }
        }

        public UUID taskId() {
            if (immutableTask.isPresent()) {
                return immutableTask.get().taskId();
            }
            return UUID.fromString(protoTask.get().getTaskId());
        }

        public String service() {
            if (immutableTask.isPresent()) {
                return immutableTask.get().service();
            }
            return protoTask.get().getService();
        }

        public CompletionStage<Done> saveTask(TaskLogRepository logRepository) {
            if (immutableTask.isPresent()) {
                return logRepository.saveTaskJson(immutableTask.get());
            }
            return logRepository.saveTaskProto(protoTask.get());
        }

        public Object get() {
            if (immutableTask.isPresent()) {
                return immutableTask.get();
            }
            return protoTask.get();
        }

        public String className() {
            if (immutableTask.isPresent()) {
                return immutableTask.get().getClass().getSimpleName();
            }
            return protoTask.get().getParametersCase().toString();
        }

        public TaskDescription getTaskDescription() {
            return taskDescription;
        }
    }
}
