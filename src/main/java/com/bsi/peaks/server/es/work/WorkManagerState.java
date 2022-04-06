package com.bsi.peaks.server.es.work;

import akka.japi.Pair;
import akka.japi.tuple.Tuple3;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.work.SubmitTask;
import com.bsi.peaks.event.work.TaskRegistration;
import com.bsi.peaks.event.work.WorkManagerSnapshot;
import com.bsi.peaks.event.work.WorkerRegistration;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.StepType;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.immutables.value.Value;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;
import org.pcollections.PSet;
import org.pcollections.TreePVector;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;

@PeaksImmutable
@JsonDeserialize(builder = WorkManagerStateBuilder.class)
public interface WorkManagerState {

    default int threadLimit() {
        return 16;
    }

    default String softwareVersion() {
        return "";
    }

    @Value.Lazy
    default int currentThreadCount() {
        // when counting thread, do not include data loaders
        return workers().values().stream()
            .filter(w -> !w.isDataLoader())
            .mapToInt(WorkerRef::threadCount).sum();
    }

    default PMap<UUID, Instant> workersLastContact() {
        return HashTreePMap.empty();
    }

    default PMap<UUID, WorkerRef> workers() {
        return HashTreePMap.empty();
    }

    default PMap<UUID, Set<StepType>> workerSupportsStepTypes() {
        return HashTreePMap.empty();
    }

    /**
     * List of known tasks. Once processed, they will be removed from map.
     * Key = taskId
     * Value = (queued, priority, task)
     */
    default PMap<UUID, Tuple3<Instant, Integer, TaskRegistration>> tasks() {
        return HashTreePMap.empty();
    }

    default PSet<UUID> readyTasks() {
        return HashTreePSet.empty();
    }

    default PMap<UUID, PMap<UUID, Instant>> workerTasks() {
        return HashTreePMap.empty();
    }

    default TreePVector<SubmitTask> queuedTasks() { return TreePVector.empty(); }

    // Auto generated
    WorkManagerState withThreadLimit(int threadLimit);
    WorkManagerState withSoftwareVersion(String softwareVersion);
    WorkManagerState withWorkers(PMap<UUID, WorkerRef> workers);
    WorkManagerState withWorkersLastContact(PMap<UUID, Instant> workers);
    WorkManagerState withWorkerSupportsStepTypes(PMap<UUID, Set<StepType>> workerSupportsStepTypes);
    WorkManagerState withTasks(PMap<UUID, Tuple3<Instant, Integer, TaskRegistration>> tasks);
    WorkManagerState withReadyTasks(PSet<UUID> tasks);
    WorkManagerState withWorkerTasks(PMap<UUID, PMap<UUID, Instant>> workerTasks);
    WorkManagerState withQueuedTasks(TreePVector<SubmitTask> queuedTasks);

    default WorkManagerState updateWorker(UUID workerId, WorkerRef workerRef) {
        return this.withWorkers(workers().plus(workerId, workerRef));
    }

    default WorkManagerState plusWorker(UUID workerId, WorkerRef workerRef, Set<StepType> supportsStepTypes) {
        return this
            .withWorkers(workers().plus(workerId, workerRef))
            .withWorkerTasks(workerTasks().plus(workerId, workerTasks().getOrDefault(workerId, HashTreePMap.empty())))
            .withWorkerSupportsStepTypes(workerSupportsStepTypes().plus(workerId, supportsStepTypes));
    }

    default WorkManagerState minusWorker(UUID workerId) {
        return this
            .withWorkers(workers().minus(workerId))
            .withWorkerTasks(workerTasks().minus(workerId))
            .withWorkerSupportsStepTypes(workerSupportsStepTypes().minus(workerId));
    }

    default WorkManagerState plusTask(UUID taskId, TaskRegistration task, int weight, Instant queued) {
        return this
            .withTasks(tasks().plus(taskId, Tuple3.create(queued, weight, task)))
            .withReadyTasks(readyTasks().plus(taskId));
    }

    default WorkManagerState minusTask(UUID taskId) {
        return this.withTasks(tasks().minus(taskId));
    }

    default WorkManagerState removeTaskFromWorker(UUID workerId, UUID taskId) {
        if (workerTasks().containsKey(workerId)) {
            PMap<UUID, Instant> updatedTasks = workerTasks().get(workerId).minus(taskId);
            return this.withWorkerTasks(workerTasks().plus(workerId, updatedTasks));
        }
        else {
            return this;
        }
    }

    default WorkManagerState assignWorkerToTask(UUID workerId, UUID taskId, Instant started) {
        PMap<UUID, Instant> tasks = workerTasks().getOrDefault(workerId, HashTreePMap.empty());
        return this.withWorkerTasks(workerTasks().plus(workerId, tasks.plus(taskId, started)));
    }

    default WorkManagerState minusReadyTask(UUID taskId) {
        return this.withReadyTasks(readyTasks().minus(taskId));
    }

    default WorkManagerState plusWorkerContactNow(UUID workerId) {
        return withWorkersLastContact(workersLastContact().plus(workerId, Instant.now()));
    }

    default Set<UUID> workersWithIdleGPU(TaskDescription task) {
        // don't need task but make it not caching values
        return workers().entrySet().stream()
            .filter(entry -> entry.getValue().availableGPU() > 0)
            .map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    default Set<UUID> idleWorkers(TaskDescription task) {
        final Set<UUID> allWorkerIds = workers().keySet();
        final Set<UUID> busyWorkerIds;
        if (task == null) {
            busyWorkerIds = workerTasks().entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(Map.Entry::getKey).collect(Collectors.toSet());
        } else {
            busyWorkerIds = workers().entrySet().stream()
                .filter(entry -> entry.getValue().availableThread() < task.getCpu()
                    || entry.getValue().availableGPU() < task.getGpu()
                    || entry.getValue().availableMemory() < task.getMemory()
                )
                .map(Map.Entry::getKey).collect(Collectors.toSet());
        }

        return Sets.difference(allWorkerIds, busyWorkerIds);
    }

    default WorkManagerState queueTask(SubmitTask task) {
        TreePVector<SubmitTask> submitTasks = queuedTasks().plus(task);
        return withQueuedTasks(submitTasks);
    }

    default WorkManagerState dequeueTask(SubmitTask task){
        TreePVector<SubmitTask> submitTasks = queuedTasks().minus(task);
        return withQueuedTasks(submitTasks);
    }

    default void snapshot(WorkManagerSnapshot.Builder builder) {
        final PMap<UUID, Set<StepType>> workerSupportsStepTypes = workerSupportsStepTypes();
        builder.setSoftwareVersion(softwareVersion());
        builder.setThreadLimit(threadLimit());
        workers().forEach((workerId, workerRef) -> {
            builder.addWorkersBuilder()
                .setWorkerId(workerId.toString())
                .setIpAddress(workerRef.ipAddress())
                .setProcessId(workerRef.processId())
                .setActorPath(workerRef.workerActorPath().toSerializationFormat())
                .setVersion(workerRef.version())
                .setThreadCount(workerRef.threadCount())
                .setGpuCount(workerRef.gpuCount())
                .setMemoryAmount(workerRef.memoryAmount())
                .setAvailableThread(workerRef.availableThread())
                .setAvailableGPU(workerRef.availableGPU())
                .setAvailableMemory(workerRef.availableMemory())
                .addAllSupportsStepTypes(workerSupportsStepTypes.get(workerId));
        });

        final Map<UUID, Pair<UUID, Instant>> taskWorkerStartMap = new HashMap<>();
        workerTasks().forEach((workerId, tasks) -> tasks.forEach((taskId, time) ->
            taskWorkerStartMap.put(taskId, Pair.create(workerId, time))));

        queuedTasks().forEach(builder::addSubmitTasks);

        tasks().forEach((taskId, t) -> {
            final WorkManagerSnapshot.Task.Builder taskBuilder = builder.addTasksBuilder()
                .setQueued(CommonFactory.convert(t.t1()))
                .setPriority(t.t2())
                .setTaskRegistration(t.t3());
            final Pair<UUID, Instant> workerStart = taskWorkerStartMap.get(taskId);
            if (workerStart != null) {
                taskBuilder
                    .setWorkerId(uuidToByteString(workerStart.first()))
                    .setStart(CommonFactory.convert(workerStart.second()));
            }
        });
    }

    static WorkManagerState from(WorkManagerSnapshot snapshot) {
        PMap<UUID, WorkerRef> workers = HashTreePMap.empty();
        PMap<UUID, Set<StepType>> workerSupportsStepTypes = HashTreePMap.empty();
        PMap<UUID, Tuple3<Instant, Integer, TaskRegistration>> tasks = HashTreePMap.empty();
        PSet<UUID> readyTasks = HashTreePSet.empty();
        PMap<UUID, PMap<UUID, Instant>> workerTasks = HashTreePMap.empty();
        PMap<UUID, Instant> workersLastContact = HashTreePMap.empty();

        for (WorkManagerSnapshot.Task task : snapshot.getTasksList()) {
            final TaskRegistration taskRegistration = task.getTaskRegistration();
            final UUID taskId = UUID.fromString(taskRegistration.getTaskId());
            final Instant queued = CommonFactory.convert(task.getQueued());
            tasks = tasks.plus(taskId, Tuple3.create(queued, task.getPriority(), taskRegistration));
            if (task.hasStart()) {
                final UUID workerId = ModelConversion.uuidFrom(task.getWorkerId());
                final Instant start = CommonFactory.convert(task.getStart());
                PMap<UUID, Instant> tasksByWorker = workerTasks.getOrDefault(workerId, HashTreePMap.empty());
                workerTasks = workerTasks.plus(workerId, tasksByWorker.plus(taskId, start));
            } else {
                readyTasks = readyTasks.plus(taskId);
            }
        }

        final Instant now = Instant.now();
        for (WorkerRegistration reg : snapshot.getWorkersList()) {
            final UUID workerId = UUID.fromString(reg.getWorkerId());
            workers = workers.plus(workerId, WorkerRef.from(reg));
            workerSupportsStepTypes = workerSupportsStepTypes.plus(workerId, ImmutableSet.copyOf(reg.getSupportsStepTypesList()));
            workersLastContact = workersLastContact.plus(workerId, now);
        }

        return new WorkManagerStateBuilder()
            .threadLimit(snapshot.getThreadLimit())
            .softwareVersion(snapshot.getSoftwareVersion())
            .workers(workers)
            .workerSupportsStepTypes(workerSupportsStepTypes)
            .tasks(tasks)
            .readyTasks(readyTasks)
            .workerTasks(workerTasks)
            .workersLastContact(workersLastContact)
            .queuedTasks(TreePVector.from(snapshot.getSubmitTasksList()))
            .build();
    }
}
