package com.bsi.peaks.server.es.state;

import akka.japi.Function;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState.SampleState.SampleStatus;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.ActivationMethod;
import com.bsi.peaks.model.dto.Sample;
import com.bsi.peaks.model.proto.Enzyme;
import com.bsi.peaks.model.proto.Instrument;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFactory;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;

@PeaksImmutable
@JsonDeserialize(builder = ProjectSampleStateBuilder.class)
public interface ProjectSampleState {

    Logger LOG = LoggerFactory.getLogger(ProjectSampleState.class);

    static ProjectSampleStateBuilder builder(UUID sampleId, ProjectSampleState oldSampleState) {
        final ProjectSampleStateBuilder builder = new ProjectSampleStateBuilder();
        if (oldSampleState != null) {
            builder.from(oldSampleState);
        } else {
            builder.sampleId(sampleId);
        }
        return builder;
    }

    UUID sampleId();

    default Instant createdTimeStamp() {
        return Instant.now();
    }

    default Instant lastUpdatedTimeStamp() {
        return Instant.now();
    }

    default String name() {
        return "";
    }

    default int priority() { return 3; }

    default Instrument instrument() {
        return Instrument.getDefaultInstance();
    }

    default Enzyme enzyme() {
        return Enzyme.getDefaultInstance();
    }

    default ActivationMethod activationMethod() {
        return ActivationMethod.UNDEFINED;
    }

    default Sample.AcquisitionMethod acquisitionMethod() {
        return Sample.AcquisitionMethod.DDA;
    }

    List<WorkflowStep> steps();

    default PMap<UUID, ProjectFractionState> fractionState() {
        return HashTreePMap.empty();
    }

    List<UUID> fractionIds();

    default boolean hasFailedTask() {
        return false;
    }

    default boolean isCancelled() {
        return false;
    }

    default boolean allTasksDone() {
        return false;
    }

    @Value.Lazy
    default Status status() {
        if (isCancelled()) return Status.CANCELLED;
        if (hasFailedTask()) return Status.FAILED;
        if (allTasksDone()) {
            if (!fractionState().values().stream().allMatch(fraction -> fraction.uploadState() == UploadState.COMPLETED)) {
                LOG.error("Inconsistent state in sample " + sampleId() + ": allTasksDone but some upload not complete.");
            }
            return Status.DONE;
        }
        boolean processing = false;
        for (ProjectFractionState fraction : fractionState().values()) {
            switch (fraction.uploadState()) {
                case FAILED:
                case TIMEOUT:
                    return Status.FAILED;
                case QUEUED:
                case STARTED:
                case COMPLETED:
                    processing = true;
            }
        }
        if (processing) return Status.PROCESSING;
        return Status.CREATED;
    }

    default void proto(ProjectAggregateState.SampleState.Builder builder) {
        builder.setSampleId(uuidToByteString(sampleId()));
        builder.setCreatedTimestamp(CommonFactory.convert(createdTimeStamp()));
        builder.setLastUpdatedTimeStamp(CommonFactory.convert(lastUpdatedTimeStamp()));
        builder.setName(name());
        builder.setPriority(priority());
        builder.setInstrument(instrument());
        builder.setEnyme(enzyme());
        builder.setActivationMethod(activationMethod());
        builder.setAcquisitionMethod(acquisitionMethod());
        if (isCancelled())  {
            builder.setStatus(SampleStatus.CANCELLED);
        } else  if (hasFailedTask()) {
            builder.setStatus(SampleStatus.FAILED);
        } else if (allTasksDone()) {
            builder.setStatus(SampleStatus.DONE);
        } else {
            builder.setStatus(SampleStatus.CREATED);
        }

        steps().forEach(builder::addSteps);

        final PMap<UUID, ProjectFractionState> fractions = fractionState();
        fractionIds().forEach(fractionId -> {
            final ProjectFractionState fractionState = fractions.get(fractionId);
            if (fractionState == null) {
                builder.addFractionStatesBuilder().setFractionId(uuidToByteString(fractionId));
            } else {
                fractionState.proto(builder.addFractionStatesBuilder());
            }
        });
    }

    static ProjectSampleState from(ProjectAggregateState.SampleState proto, MemoryOptimizedFactory memoryOptimizedFactory) {
        final ProjectSampleStateBuilder builder = new ProjectSampleStateBuilder()
            .sampleId(memoryOptimizedFactory.uuid(proto.getSampleId()))
            .createdTimeStamp(CommonFactory.convert(proto.getCreatedTimestamp()))
            .lastUpdatedTimeStamp(CommonFactory.convert(proto.getLastUpdatedTimeStamp()))
            .name(proto.getName())
            .priority(proto.getPriority())
            .instrument(memoryOptimizedFactory.internOther(proto.getInstrument()))
            .enzyme(memoryOptimizedFactory.internOther(proto.getEnyme()))
            .activationMethod(proto.getActivationMethod())
            .acquisitionMethod(proto.getAcquisitionMethod())
            .steps(proto.getStepsList());

        switch (proto.getStatus()) {
            case PENDING:
            case CREATED:
            case PROCESSING:
                // Ignore, these states are derived.
                break;
            case DONE:
                builder.allTasksDone(true);
                break;
            case CANCELLED:
                builder.isCancelled(true);
                break;
            case FAILED:
                builder.hasFailedTask(true);
                break;
            default:
                throw new IllegalArgumentException("Unexpected sample status " + proto.getStatus());
        }

        PMap<UUID, ProjectFractionState> fractionStateMap = HashTreePMap.empty();
        for (ProjectAggregateState.FractionState fractionProto : proto.getFractionStatesList()) {
            final ProjectFractionState fractionState = ProjectFractionState.from(fractionProto, memoryOptimizedFactory);
            final UUID fractionId = fractionState.fractionID();
            builder.addFractionIds(fractionId);
            fractionStateMap = fractionStateMap.plus(fractionId, fractionState);
        }

        return builder
            .fractionState(fractionStateMap)
            .build();
    }

    // Auto generated withMethods.
    ProjectSampleState withName(String name);

    ProjectSampleState withCreatedTimeStamp(Instant value);

    ProjectSampleState withLastUpdatedTimeStamp(Instant value);

    ProjectSampleState withFractionState(PMap<UUID, ProjectFractionState> entries);

    ProjectSampleState withHasFailedTask(boolean value);

    ProjectSampleState withIsCancelled(boolean value);

    ProjectSampleState withAllTasksDone(boolean value);

    ProjectSampleState withPriority(int priority);

    default ProjectSampleState plusFraction(UUID fractionId, Function<ProjectFractionState, ProjectFractionState> transform) throws Exception {
        final PMap<UUID, ProjectFractionState> fractionStateMap = fractionState();
        final ProjectFractionState fractionState = fractionStateMap.get(fractionId);
        if (fractionState == null) {
            throw new IllegalArgumentException("FractionId " + fractionId + " does not exist");
        }
        return withFractionState(fractionStateMap.plus(fractionId, transform.apply(fractionState)));
    }

    enum Status {
        CREATED,
        PROCESSING,
        DONE,
        CANCELLED,
        FAILED
    }
}
