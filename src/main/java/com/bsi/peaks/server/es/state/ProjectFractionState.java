package com.bsi.peaks.server.es.state;

import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFactory;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;
import static com.bsi.peaks.model.ModelConversion.uuidsFrom;
import static com.bsi.peaks.model.ModelConversion.uuidsToByteString;

@PeaksImmutable
@JsonDeserialize(builder = ProjectFractionStateBuilder.class)
public interface ProjectFractionState {

    static ProjectFractionStateBuilder builder(ProjectFractionState oldSampleState) {
        final ProjectFractionStateBuilder builder = new ProjectFractionStateBuilder();
        if (oldSampleState != null) {
            builder.from(oldSampleState);
        }
        return builder;
    }

    UUID fractionID();

    Instant lastUpdateTimeStamp();

    @Nullable
    UUID uploadId();

    @Nullable
    String remotePathName();

    @Nullable
    String subPath();

    default UploadState uploadState() {
        return UploadState.PENDING;
    }

    String srcFile();

    default PMap<StepType, UUID> stepTaskPlanId() {
        return HashTreePMap.empty();
    }

    default void proto(ProjectAggregateState.FractionState.Builder builder) {
        builder.setFractionId(uuidToByteString(fractionID()));

        final UUID uploadId = uploadId();
        final String remotePathName = remotePathName();
        final String subPath = subPath();
        if (uploadId != null) builder.setUploadId(uuidToByteString(uploadId));
        if (remotePathName != null) builder.setRemotePathName(remotePathName);
        if (subPath != null) builder.setSubPath(subPath);

        switch (uploadState()) {
            case PENDING:
                builder.setUploadState(ProjectAggregateState.FractionState.UploadState.PENDING);
                break;
            case QUEUED:
                builder.setUploadState(ProjectAggregateState.FractionState.UploadState.QUEUED);
                break;
            case STARTED:
                builder.setUploadState(ProjectAggregateState.FractionState.UploadState.STARTED);
                break;
            case COMPLETED:
                builder.setUploadState(ProjectAggregateState.FractionState.UploadState.COMPLETED);
                break;
            case FAILED:
                builder.setUploadState(ProjectAggregateState.FractionState.UploadState.FAILED);
                break;
            case TIMEOUT:
                builder.setUploadState(ProjectAggregateState.FractionState.UploadState.TIMEOUT);
                break;
            default:
                throw new IllegalArgumentException("Unexpected upload state " + uploadState());
        }

        builder.setSrcFile(srcFile());

        final PMap<StepType, UUID> stepTaskPlanId = stepTaskPlanId();
        List<UUID> taskPlanIds = new ArrayList(stepTaskPlanId.size());
        stepTaskPlanId.forEach((stepType, taskPlanId) -> {
            taskPlanIds.add(taskPlanId);
            builder.addSteps(stepType);
        });
        builder.setTaskPlanIds(uuidsToByteString(taskPlanIds));
    }

    // Auto generated methods
    ProjectFractionState withUploadState(UploadState uploadState);
    ProjectFractionState withStepTaskPlanId(PMap<StepType, UUID> stepTaskPlanId);
    ProjectFractionState withLastUpdateTimeStamp(Instant value);
    ProjectFractionState withSrcFile(String value);

    default ProjectFractionState plusStepTaskPlanId(StepType stepType, UUID taskPlanId) {
        final PMap<StepType, UUID> stepTaskPlanId = stepTaskPlanId();
        if (stepTaskPlanId.containsKey(stepType)) {
            throw new IllegalStateException("Attempt to add taskPlanId for step " + stepType + " that already exists");
        }
        return withStepTaskPlanId(stepTaskPlanId.plus(stepType, taskPlanId));
    }

    static ProjectFractionState from(ProjectAggregateState.FractionState proto, MemoryOptimizedFactory memoryOptimizedFactory) {
        final ProjectFractionStateBuilder builder = new ProjectFractionStateBuilder()
            .fractionID(memoryOptimizedFactory.uuid(proto.getFractionId()))
            .lastUpdateTimeStamp(CommonFactory.convert(proto.getLastUpdateTimeStamp()))
            .srcFile(proto.getSrcFile());

        ModelConversion.tryUuidFrom(proto.getUploadId())
            .map(memoryOptimizedFactory::intern)
            .ifPresent(builder::uploadId);

        String remotePathName = proto.getRemotePathName();
        String subPath = proto.getSubPath();
        if (!Strings.isNullOrEmpty(subPath)) builder.remotePathName(remotePathName);
        if (!Strings.isNullOrEmpty(subPath)) builder.subPath(subPath);

        switch (proto.getUploadState()) {
            case PENDING:
                builder.uploadState(UploadState.PENDING);
                break;
            case QUEUED:
                builder.uploadState(UploadState.QUEUED);
                break;
            case STARTED:
                builder.uploadState(UploadState.STARTED);
                break;
            case COMPLETED:
                builder.uploadState(UploadState.COMPLETED);
                break;
            case FAILED:
                builder.uploadState(UploadState.FAILED);
                break;
            case TIMEOUT:
                builder.uploadState(UploadState.TIMEOUT);
                break;
            default:
                throw new IllegalArgumentException("Unexpected upload state " + proto.getUploadState());
        }

        PMap<StepType, UUID> stepTaskPlanId = HashTreePMap.empty();
        final Iterator<UUID> taskPlanIds = uuidsFrom(proto.getTaskPlanIds()).iterator();
        for (StepType stepType : proto.getStepsList()) {
            stepTaskPlanId = stepTaskPlanId.plus(stepType, taskPlanIds.next());
        }

        return builder
            .stepTaskPlanId(stepTaskPlanId)
            .build();
    }
}
