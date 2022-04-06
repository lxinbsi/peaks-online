package com.bsi.peaks.server.es.work;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import com.bsi.peaks.event.work.WorkerRegistration;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.StepType;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@PeaksImmutable
@JsonDeserialize(builder = WorkerRefBuilder.class)
public interface WorkerRef {
    ActorPath workerActorPath();
    int threadCount();
    int gpuCount();
    int memoryAmount();
    long processId();
    String ipAddress();
    String version();
    boolean isDataLoader();
    default int availableThread() {
        return 0;
    };
    default int availableGPU() {
        return 0;
    };
    default int availableMemory() {
        return 0;
    };

    static WorkerRef from(WorkerRegistration reg) {
        ActorPath actorPath = ActorPaths.fromString(reg.getActorPath());
        // it's possible the worker's ip address which is from worker hostname is not the ip the worker is binding to
        // so we try to get the ip from address first, and fallback to the hostname if address doesn't contain host ip
        String ip = actorPath.address().host().getOrElse(reg::getIpAddress);
        return new WorkerRefBuilder()
            .threadCount(reg.getThreadCount())
            .gpuCount(reg.getGpuCount())
            .memoryAmount(reg.getMemoryAmount())
            .availableThread(reg.getAvailableThread())
            .availableGPU(reg.getAvailableGPU())
            .availableMemory(reg.getAvailableMemory())
            .processId(reg.getProcessId())
            .ipAddress(ip)
            .workerActorPath(actorPath)
            .version(reg.getVersion())
            .isDataLoader(reg.getSupportsStepTypesList().contains(StepType.DATA_LOADING))
            .build();
    }

    default WorkerRef update(int availableThread, int availableGPU, int availableMemory) {
        return new WorkerRefBuilder().from(this)
            .availableThread(availableThread)
            .availableGPU(availableGPU)
            .availableMemory(availableMemory)
            .build();
    }
}
