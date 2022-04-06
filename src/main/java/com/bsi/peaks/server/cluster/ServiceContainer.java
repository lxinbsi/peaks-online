package com.bsi.peaks.server.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.TaskParameters;
import com.bsi.peaks.messages.service.CancelService;
import com.bsi.peaks.messages.worker.WorkerCommand;
import com.bsi.peaks.messages.worker.WorkerProcessState;
import com.bsi.peaks.model.system.Task;
import com.bsi.peaks.server.dependency.WorkerDependency;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.FutureConverters;

import java.lang.management.ManagementFactory;
import java.sql.Date;
import java.text.DateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class ServiceContainer extends AbstractActor {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceContainer.class);
    private static final String CONTAINER_CLEANUP_TICK = "CONTAINER_CLEANUP";
    private final OperatingSystemMXBean operatingSystemMXBean;
    private final ActorRef serviceActor;
    private final UUID taskId;
    private final Duration timeout;
    private final Cancellable cleanupTask;
    private Instant lastPinged;

    private ServiceContainer(String taskService, UUID taskId, ActorRef worker, ApplicationStorageFactory storageFactory, int threads) {
        this.taskId = taskId;
        operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        timeout = context().system().settings().config().getDuration("peaks.worker.heartbeat-timeout");
        serviceActor = context().system().actorOf(
            Props.create(WorkerDependency.SERVICE_ACTORS.get(taskService), worker, storageFactory, threads),
            taskService
        );

        lastPinged = Instant.now();
        Duration interval = Duration.ofSeconds(10);
        cleanupTask = getContext().system().scheduler().schedule(
            interval,
            interval,
            getSelf(),
            CONTAINER_CLEANUP_TICK,
            getContext().dispatcher(),
            getSelf()
        );
    }

    public static Props props(String taskService, UUID taskId, ActorRef worker, ApplicationStorageFactory storageFactory, int threads) {
        return Props.create(ServiceContainer.class, () -> new ServiceContainer(taskService, taskId, worker, storageFactory, threads));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(TaskDescription.class, description -> {
                Worker.TaskWrapper wrapped = new Worker.TaskWrapper(description);
                forwardToService(wrapped.get());
            })
            .match(CancelService.class, this::forwardToService)
            .match(WorkerCommand.class, WorkerCommand::hasQueryState, this::handleWorkerStateQuery)
            .matchEquals(Worker.PING, ignore -> pong())
            .matchEquals(CONTAINER_CLEANUP_TICK, ignore -> cleanup())
            .build();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        cleanupTask.cancel();
    }

    private void pong() {
        lastPinged = Instant.now();
        sender().tell(new ServiceContainerUpdateBuilder().taskId(taskId).build(), self());
    }

    private void cleanup() {
        if (Instant.now().isAfter(lastPinged.plusNanos(timeout.toNanos()))) {
            LOG.warn("Service container last contacted at " + DateFormat.getTimeInstance().format(Date.from(lastPinged)));
            LOG.warn("Passed timeout and will shut self down");
            FutureConverters.toJava(context().system().terminate()).whenComplete((done, error) -> System.exit(1));
        }
    }

    private void handleWorkerStateQuery(WorkerCommand command) {
        final float cpuLoad = (float)operatingSystemMXBean.getProcessCpuLoad();
        final long totalMem = Runtime.getRuntime().totalMemory();
        final long freeMem = Runtime.getRuntime().freeMemory();
        final long usedMem = totalMem - freeMem;

        WorkerProcessState state = WorkerProcessState.newBuilder()
            .setTaskId(taskId.toString())
            .setUsedMemoryBytes(usedMem)
            .setAvailableMemoryBytes(totalMem)
            .setFreeMemoryBytes(freeMem)
            .setCpuUsagePercent(cpuLoad)
            .build();

        sender().tell(state, self());
    }

    private void forwardToService(Object msg) {
        serviceActor.tell(msg, sender());
    }
}
