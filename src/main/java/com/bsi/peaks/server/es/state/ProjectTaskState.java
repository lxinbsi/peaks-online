package com.bsi.peaks.server.es.state;

import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.system.Progress;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFactory;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.UUID;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;

@PeaksImmutable
@JsonDeserialize(builder = ProjectTaskStateBuilder.class)
public interface ProjectTaskState {

    UUID taskPlanId();

    UUID taskId();

    Progress progress();

    default DataState dataState() {
        return DataState.NONE;
    }

    int priority();

    @Nullable
    ProjectAggregateEvent.TaskDone taskDone();

    Instant queuedTimestamp();

    @Nullable
    Instant startTimestamp();

    @Nullable
    Instant completedTimestamp();

    default float processingProgress() {
        return 0f;
    }

    @Nullable
    String workerIpAddress();

    @Nullable
    UUID workerId();

    default void proto(ProjectAggregateState.TaskState.Builder builder) {
        builder.setTaskPlanId(uuidToByteString(taskPlanId()));
        builder.setTaskId(uuidToByteString(taskId()));
        builder.setPriority(priority());
        builder.setProcessingProgress(processingProgress());
        builder.setQueuedTimestamp(CommonFactory.convert(queuedTimestamp()));

        final Instant startTimestamp = startTimestamp();
        if (startTimestamp != null) builder.setStartTimestamp(CommonFactory.convert(startTimestamp));

        final Instant completedTimestamp = completedTimestamp();
        if (completedTimestamp != null) builder.setCompletedTimestamp(CommonFactory.convert(completedTimestamp));

        switch (progress()) {
            case PENDING:
                builder.setProgress(ProjectAggregateState.Progress.PROGRESS_PENDING);
                break;
            case QUEUED:
                builder.setProgress(ProjectAggregateState.Progress.PROGRESS_QUEUED);
                break;
            case IN_PROGRESS:
                builder.setProgress(ProjectAggregateState.Progress.PROGRESS_IN_PROGRESS);
                break;
            case DONE:
                builder.setProgress(ProjectAggregateState.Progress.PROGRESS_DONE);
                break;
            case FAILED:
                builder.setProgress(ProjectAggregateState.Progress.PROGRESS_FAILED);
                break;
            case STOPPING:
                builder.setProgress(ProjectAggregateState.Progress.PROGRESS_STOPPING);
                break;
            case CANCELLED:
                builder.setProgress(ProjectAggregateState.Progress.PROGRESS_CANCELLED);
                break;
            default:
                throw new IllegalArgumentException("Unexpected progress " + progress());
        }

        switch (dataState()) {
            case NONE:
                builder.setDataState(ProjectAggregateState.TaskState.DataState.NONE);
                break;
            case BUILDING:
                builder.setDataState(ProjectAggregateState.TaskState.DataState.BUILDING);
                break;
            case READY:
                builder.setDataState(ProjectAggregateState.TaskState.DataState.READY);
                break;
            case DELETING:
                builder.setDataState(ProjectAggregateState.TaskState.DataState.DELETING);
                break;
            case DELETED:
                builder.setDataState(ProjectAggregateState.TaskState.DataState.DELETED);
                break;
            default:
                throw new IllegalArgumentException("Unexpected data state " + dataState());
        }

        final ProjectAggregateEvent.TaskDone taskDone = taskDone();
        if (taskDone != null) builder.setTaskDone(taskDone);

        final String workerIpAddress = workerIpAddress();
        if (workerIpAddress != null) builder.setWorkerIpAddress(workerIpAddress);

        final UUID workerId = workerId();
        if (workerId != null) builder.setWorkerId(uuidToByteString(workerId));
    }

    static ProjectTaskState from(ProjectAggregateState.TaskState proto, MemoryOptimizedFactory memoryOptimizedFactory) {
        final ProjectTaskStateBuilder builder = new ProjectTaskStateBuilder()
            .taskPlanId(memoryOptimizedFactory.uuid(proto.getTaskPlanId()))
            .taskId(memoryOptimizedFactory.uuid(proto.getTaskId()))
            .priority(proto.getPriority())
            .queuedTimestamp(CommonFactory.convert(proto.getQueuedTimestamp()))
            .processingProgress(proto.getProcessingProgress())
            .workerIpAddress(Strings.emptyToNull(proto.getWorkerIpAddress()));

        ModelConversion.tryUuidFrom(proto.getWorkerId())
            .map(memoryOptimizedFactory::intern)
            .ifPresent(builder::workerId);

        switch (proto.getProgress()) {
            case PROGRESS_PENDING:
                builder.progress(Progress.PENDING);
                break;
            case PROGRESS_QUEUED:
                builder.progress(Progress.QUEUED);
                break;
            case PROGRESS_IN_PROGRESS:
                builder.progress(Progress.IN_PROGRESS);
                break;
            case PROGRESS_DONE:
                builder.progress(Progress.DONE);
                break;
            case PROGRESS_FAILED:
                builder.progress(Progress.FAILED);
                break;
            case PROGRESS_STOPPING:
                builder.progress(Progress.STOPPING);
                break;
            case PROGRESS_CANCELLED:
                builder.progress(Progress.CANCELLED);
                break;
            default:
                throw new IllegalArgumentException("Unexpected progress " + proto.getProgress());
        }

        switch (proto.getDataState()) {
            case NONE:
                builder.dataState(DataState.NONE);
                break;
            case BUILDING:
                builder.dataState(DataState.BUILDING);
                break;
            case READY:
                builder.dataState(DataState.READY);
                break;
            case DELETING:
                builder.dataState(DataState.DELETING);
                break;
            case DELETED:
                builder.dataState(DataState.DELETED);
                break;
            default:
                throw new IllegalArgumentException("Unexpected data state " + proto.getDataState());
        }

        if (proto.hasTaskDone()) {
            builder.taskDone(proto.getTaskDone());
        }

        if (proto.hasStartTimestamp()) {
            builder.startTimestamp(CommonFactory.convert(proto.getStartTimestamp()));
        }

        if (proto.hasCompletedTimestamp()) {
            builder.completedTimestamp(CommonFactory.convert(proto.getCompletedTimestamp()));
        }

        return builder.build();
    }

    // Auto generated withMethods.
    ProjectTaskState withProgress(Progress progress);
    ProjectTaskState withProcessingProgress(float progress);
    ProjectTaskState withDataState(DataState dataState);

    enum DataState{
        NONE,
        BUILDING,
        READY,
        DELETING,
        DELETED
    }

}
