package com.bsi.peaks.server.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.bsi.peaks.message.WorkerMessageFactory;
import com.bsi.peaks.messages.worker.WorkerProcessState;
import com.bsi.peaks.messages.worker.WorkerState;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public class WorkerStateAggregator extends AbstractActor {
    private final static long DEADLINE_MILLIS = 300;
    private final static String TIMEOUT = "timeout";
    private final ActorRef destination;
    private final UUID workerId;
    private final WorkerState.Builder stateBuilder;
    private final int numProcesses;
    private int processesAdded;
    private WorkerStateAggregator(ActorRef destination, UUID workerId, WorkerState.Builder stateBuilder, int numProcesses) {
        this.destination = destination;
        this.workerId = workerId;
        this.stateBuilder = stateBuilder;
        this.numProcesses = numProcesses;
        this.processesAdded = 0;
        if (numProcesses == 0) {
            destination.tell(WorkerMessageFactory.worker(workerId).workerStateResponse(stateBuilder.build()), ActorRef.noSender());
            context().stop(self());
        } else {
            context().system().scheduler()
                .scheduleOnce(Duration.ofMillis(DEADLINE_MILLIS), self(), TIMEOUT, context().system().dispatcher(), self());
        }
    }

    static Props props(ActorRef destination, UUID workerId, WorkerState.Builder stateBuilder, int numProcesses) {
        return Props.create(WorkerStateAggregator.class, () -> new WorkerStateAggregator(destination, workerId, stateBuilder, numProcesses));
    }
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals(TIMEOUT, ignore -> {
                destination.tell(WorkerMessageFactory.worker(workerId).workerStateResponse(stateBuilder.build()), ActorRef.noSender());
                context().stop(self());
            })
            .match(WorkerProcessState.class, state -> {
                final List<WorkerProcessState> processesList = stateBuilder.getProcessesList();
                int foundIndex = -1;
                for(int i = 0; i < processesList.size(); i++) {
                    final WorkerProcessState workerProcessState = processesList.get(i);
                    if (workerProcessState.getTaskId().equals(state.getTaskId())) {
                        foundIndex = i;
                    }
                }
                if(foundIndex == -1) {
                    throw new RuntimeException("No default WorkerProcessState found for task id: " + state.getTaskId());
                } else {
                    stateBuilder.removeProcesses(foundIndex);
                    stateBuilder.addProcesses(state);
                }
                processesAdded++;
                if (processesAdded == numProcesses) {
                    destination.tell(WorkerMessageFactory.worker(workerId).workerStateResponse(stateBuilder.build()), ActorRef.noSender());
                    context().stop(self());
                }
            })
            .build();
    }
}
