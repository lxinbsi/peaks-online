package com.bsi.peaks.server.es.state;

import com.bsi.peaks.analysis.parameterModels.dto.AnalysisAcquisitionMethod;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState.AnalysisState.AnalysisStatus;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.internal.task.SilacFilterSummarizationParameters;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.FilterList;
import com.bsi.peaks.model.dto.LfqFilter;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParameters;
import com.bsi.peaks.model.parameters.RetentionTimeAlignmentParameters;
import com.bsi.peaks.model.proto.SimplifiedFastaSequenceDatabaseInfo;
import com.bsi.peaks.model.query.IdentificationType;
import com.bsi.peaks.model.system.OrderedSampleFractions;
import com.bsi.peaks.model.system.OrderedSampleFractionsBuilder;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFactory;
import com.bsi.peaks.server.es.tasks.Helper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import org.immutables.value.Value;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;
import static com.bsi.peaks.model.ModelConversion.uuidsFrom;
import static com.bsi.peaks.model.ModelConversion.uuidsToByteString;

@PeaksImmutable
@JsonDeserialize(builder = ProjectAnalysisStateBuilder.class)
public interface ProjectAnalysisState {

    static ProjectAnalysisStateBuilder builder(UUID analysisId, ProjectAnalysisState oldAnalaysis) {
        final ProjectAnalysisStateBuilder builder = new ProjectAnalysisStateBuilder();
        if (oldAnalaysis == null) {
            return builder.analysisId(analysisId);
        } else {
            return builder.from(oldAnalaysis);
        }
    }

    UUID analysisId();

    default Instant createdTimeStamp() {
        return Instant.now();
    }

    default Instant lastUpdatedTimeStamp() {
        return Instant.now();
    }

    default Status status() {
        return Status.PENDING;
    }

    default String name() {
        return "";
    }

    default String description() {
        return "";
    }

    default int priority() { return 3; }

    List<WorkflowStep> steps();

    List<OrderedSampleFractions> samples();

    List<SimplifiedFastaSequenceDatabaseInfo> databases();

    @Value.Lazy
    default AnalysisAcquisitionMethod analysisAcquisitionMethod() {
        for (WorkflowStep step : steps()) {
            switch (step.getStepType()) {
                case SL_SEARCH:
                case DIA_DB_SEARCH:
                case DIA_DB_PRE_SEARCH:
                case DIA_DATA_REFINE:
                    return AnalysisAcquisitionMethod.DIA;
            }
        }
        return AnalysisAcquisitionMethod.DDA;
    }

    default PSet<UUID> targetDatabaseIds() {
        return HashTreePSet.empty();
    }

    default PSet<UUID> contaminantDatabaseIds() {
        return HashTreePSet.empty();
    }

    default PMap<StepType, PSet<UUID>> stepTaskPlanId() {
        return HashTreePMap.empty();
    }

    default PMap<WorkFlowType, FilterList> workflowTypeFilter() {
        return HashTreePMap.empty();
    }

    default void proto(ProjectAggregateState.AnalysisState.Builder builder) {
        builder.setAnalysisId(uuidsToByteString(analysisId()));
        builder.setCreatedTimestamp(CommonFactory.convert(createdTimeStamp()));
        builder.setLastUpdatedTimeStamp(CommonFactory.convert(lastUpdatedTimeStamp()));
        switch (status()) {
            case PENDING:
                builder.setStatus(AnalysisStatus.PENDING);
                break;
            case CREATED:
                builder.setStatus(AnalysisStatus.CREATED);
                break;
            case PROCESSING:
                builder.setStatus(AnalysisStatus.PROCESSING);
                break;
            case DONE:
                builder.setStatus(AnalysisStatus.DONE);
                break;
            case CANCELLED:
                builder.setStatus(AnalysisStatus.CANCELLED);
                break;
            case FAILED:
                builder.setStatus(AnalysisStatus.FAILED);
                break;
            default:
                throw new IllegalArgumentException("Unexpected analysis status " + status());
        }
        builder.setName(name());
        builder.setDescription(description());
        builder.setPriority(priority());
        builder.addAllDatabases(databases());
        builder.setTargetDatabaseIds(ModelConversion.uuidsToByteString(targetDatabaseIds()));
        builder.setContaminantDatabaseIds(ModelConversion.uuidsToByteString(contaminantDatabaseIds()));

        for (OrderedSampleFractions sample : samples()) {
            builder.addSamples(uuidToByteString(sample.sampleId()).concat(uuidsToByteString(sample.fractionIds())));
        }

        final PMap<StepType, PSet<UUID>> stepTaskPlanId = stepTaskPlanId();
        for (WorkflowStep step : steps()) {
            builder.addSteps(step);
            final Set<UUID> taskPlanIds = stepTaskPlanId.get(step.getStepType());
            builder.addTaskPlanIds(taskPlanIds == null ? ByteString.EMPTY : uuidsToByteString(taskPlanIds));
        }

        workflowTypeFilter().forEach((workFlowType, filterList) -> {
            builder.addWorkFlowTypes(workFlowType);
            builder.addFilters(filterList);
        });
    }

    static ProjectAnalysisState from(ProjectAggregateState.AnalysisState proto, MemoryOptimizedFactory memoryOptimizedFactory) {

        PSet<UUID> targetDatabaseIds = HashTreePSet.empty();
        for (UUID targetDatabaseId : ModelConversion.uuidsFrom(proto.getTargetDatabaseIds())) {
            targetDatabaseIds = targetDatabaseIds.plus(targetDatabaseId);
        }

        PSet<UUID> contaminantDatabaseIds = HashTreePSet.empty();
        for (UUID contaminantDatabaseId : ModelConversion.uuidsFrom(proto.getContaminantDatabaseIds())) {
            contaminantDatabaseIds = contaminantDatabaseIds.plus(contaminantDatabaseId);
        }

        final ProjectAnalysisStateBuilder builder = new ProjectAnalysisStateBuilder()
            .analysisId(memoryOptimizedFactory.uuid(proto.getAnalysisId()))
            .createdTimeStamp(CommonFactory.convert(proto.getCreatedTimestamp()))
            .lastUpdatedTimeStamp(CommonFactory.convert(proto.getLastUpdatedTimeStamp()))
            .name(proto.getName())
            .description(proto.getDescription())
            .priority(proto.getPriority())
            .addAllDatabases(proto.getDatabasesList())
            .targetDatabaseIds(targetDatabaseIds)
            .contaminantDatabaseIds(contaminantDatabaseIds);

        switch (proto.getStatus()) {
            case PENDING:
                builder.status(Status.PENDING);
                break;
            case CREATED:
                builder.status(Status.CREATED);
                break;
            case PROCESSING:
                builder.status(Status.PROCESSING);
                break;
            case DONE:
                builder.status(Status.DONE);
                break;
            case CANCELLED:
                builder.status(Status.CANCELLED);
                break;
            case FAILED:
                builder.status(Status.FAILED);
                break;
            default:
                throw new IllegalArgumentException("Unexpected analysis status " + proto.getStatus());
        }

        for (ByteString sampleFractionProto : proto.getSamplesList()) {
            final Iterator<UUID> uuidIterator = ModelConversion.uuidsFrom(sampleFractionProto).iterator();
            final UUID sampleId = memoryOptimizedFactory.intern(uuidIterator.next());
            final ImmutableList<UUID> fractionIds = ImmutableList.copyOf(Iterators.transform(uuidIterator, memoryOptimizedFactory::intern));
            builder.addSamples(new OrderedSampleFractionsBuilder().sampleId(sampleId).fractionIds(fractionIds).build());
        }

        PMap<StepType, PSet<UUID>> stepTaskPlanId = HashTreePMap.empty();

        final int stepsCount = proto.getStepsCount();
        for (int i = 0; i < stepsCount; i++) {
            final WorkflowStep workflowStep = proto.getSteps(i);
            builder.addSteps(workflowStep);
            PSet<UUID> taskPlanIds = HashTreePSet.empty();
            for (UUID taskPlanId : uuidsFrom(proto.getTaskPlanIds(i))) {
                taskPlanIds = taskPlanIds.plus(memoryOptimizedFactory.intern(taskPlanId));
            }
            stepTaskPlanId = stepTaskPlanId.plus(workflowStep.getStepType(), taskPlanIds);
        }

        PMap<WorkFlowType, FilterList> workflowTypeFilter = HashTreePMap.empty();
        final int workFlowTypesCount = proto.getWorkFlowTypesCount();
        for (int i = 0; i < workFlowTypesCount; i++) {
            final WorkFlowType workFlowType = proto.getWorkFlowTypes(i);
            final FilterList filterList = proto.getFilters(i);
            workflowTypeFilter = workflowTypeFilter.plus(workFlowType, filterList);
        }

        return builder
            .stepTaskPlanId(stepTaskPlanId)
            .workflowTypeFilter(workflowTypeFilter)
            .build();
    }

    @JsonIgnore
    @Value.Lazy
    default int numberOfFractions() {
        return samples().stream()
            .mapToInt(x -> x.fractionIds().size())
            .sum();
    }


    @JsonIgnore
    @Value.Lazy
    default boolean hasQuantification() {
        return steps().stream()
                .map(WorkflowStep::getStepType)
                .anyMatch(step -> StepType.REPORTER_ION_Q_FILTER_SUMMARIZATION.equals(step) || StepType.LFQ_SUMMARIZE.equals(step));
    }

    @JsonIgnore
    @Value.Lazy
    default List<StepType> stepTypes() {
        return steps().stream().map(WorkflowStep::getStepType).collect(Collectors.toList());
    }

    @JsonIgnore
    @Value.Lazy
    default IdentificationType lastIdentificationType() {
        for (WorkflowStep step : Lists.reverse(steps())) {
            switch (step.getStepType()) {
                case DB_SUMMARIZE:
                    return IdentificationType.DB;
                case PTM_FINDER_SUMMARIZE:
                    return IdentificationType.PTM_FINDER;
                case SPIDER_SUMMARIZE:
                    return IdentificationType.SPIDER;
                case SL_FILTER_SUMMARIZATION:
                    return IdentificationType.SPECTRAL_LIBRARY;
                case DIA_DB_SEARCH:
                case DIA_DB_PRE_SEARCH:
                    return IdentificationType.DIA_DB;
            }
        }
        return IdentificationType.NONE;
    }

    default WorkflowStep findStep(StepType find) {
        for (WorkflowStep step : steps()) {
            if (step.getStepType().equals(find)) {
                return step;
            }
        }
        throw new IllegalStateException("Unable to find step:" + find);
    }

    default boolean containsStep(StepType find) {
        for (WorkflowStep step : steps()) {
            if (step.getStepType().equals(find)) {
                return true;
            }
        }
        return false;
    }

    @Value.Lazy
    default Map<UUID, UUID> fractionIdToSampleId() {
        final ImmutableMap.Builder<UUID, UUID> builder = ImmutableMap.builder();
        for (OrderedSampleFractions sample : samples()) {
            final UUID sampleId = sample.sampleId();
            for (UUID fractionId : sample.fractionIds()) {
                builder.put(fractionId, sampleId);
            }
        }
        return builder.build();
    }

    // Auto generated withMethods.
    ProjectAnalysisState withName(String name);

    ProjectAnalysisState withCreatedTimeStamp(Instant value);

    ProjectAnalysisState withLastUpdatedTimeStamp(Instant value);

    ProjectAnalysisState withDescription(String description);

    ProjectAnalysisState withPriority(int priority);

    ProjectAnalysisState withStatus(Status status);

    ProjectAnalysisState withStepTaskPlanId(PMap<StepType, PSet<UUID>> stepTaskPlanId);

    ProjectAnalysisState withWorkflowTypeFilter(PMap<WorkFlowType, FilterList> workflowTypeFilter);

    ProjectAnalysisState withTargetDatabaseIds(PSet<UUID> targetDatabaseIds);

    ProjectAnalysisState withContaminantDatabaseIds(PSet<UUID> contaminantDatabaseIds);

    default ProjectAnalysisState plusTaskPlan(StepType stepType, UUID taskPlanId) {
        final PMap<StepType, PSet<UUID>> stepTypePSetPMap = stepTaskPlanId();
        final PSet<UUID> taskPlanIds = stepTypePSetPMap.getOrDefault(stepType, HashTreePSet.empty()).plus(taskPlanId);
        return withStepTaskPlanId(stepTypePSetPMap.plus(stepType, taskPlanIds));
    }

    default ProjectAnalysisState plusWorkflowTypeFilter(WorkFlowType workflowType, FilterList filter) {
        return withWorkflowTypeFilter(workflowTypeFilter().plus(workflowType, filter));
    }

    default ProjectAnalysisState plusTargetContaminantDatabaseIds(List<UUID> targetDatabaseIds, List<UUID> contaminantDatabaseIds) {
        ProjectAnalysisState state = this;
        if (!targetDatabaseIds.isEmpty()) {
            state = state.withTargetDatabaseIds(targetDatabaseIds().plusAll(targetDatabaseIds));
        }
        if (!contaminantDatabaseIds.isEmpty()) {
            state = state.withContaminantDatabaseIds(contaminantDatabaseIds().plusAll(contaminantDatabaseIds));
        }
        return state;
    }

    default int totalTasks(AnalysisTaskInformation taskInfo,  StepType stepType) {
        final TaskDescription.LevelCase levelCase = ModelConversion.taskLevel(stepType);
        switch (levelCase) {
            case FRACTIONLEVELTASK: {
                return calculateFractionLevelTaskCount(taskInfo, stepType);
            }
            case SAMPLELEVELTASK:
                return samples().size();
            case INDEXEDFRACTIONLEVELTASK:
                return calculateFractionIndexLevelTaskCount(taskInfo, stepType);
            case ANALYSISLEVELTASK:
            case ANALYSISDATAINDEPENDENTLEVELTASK:
                return 1;
            default:
                throw new IllegalStateException("Unexpected level " + levelCase);
        }
    }

    default int calculateFractionLevelTaskCount(AnalysisTaskInformation taskInfo, StepType stepType) {
        if (stepType == StepType.LFQ_FEATURE_ALIGNMENT) {
            //lfq feature alignment should exclude samples excluded
            WorkflowStep filterStep = taskInfo.workflowStep().get(StepType.LFQ_SUMMARIZE);
            final LabelFreeQuantificationParameters parameters = (LabelFreeQuantificationParameters) Helper.workflowStepParameters(filterStep);

            WorkflowStep rtStep = taskInfo.workflowStep().get(StepType.LFQ_RETENTION_TIME_ALIGNMENT);
            final RetentionTimeAlignmentParameters rtParameters = (RetentionTimeAlignmentParameters) Helper.workflowStepParameters(rtStep);


            Set<UUID> samplesInLfq = Streams.concat(
                    parameters.groups().stream().flatMap(g -> g.sampleIds().stream()),
                    rtParameters.sampleIdsForIdTransfer().stream()
                )
                .collect(Collectors.toSet());

            return (int) samples().stream()
                .filter(s -> samplesInLfq.contains(s.sampleId()))
                .flatMap(p -> p.fractionIds().stream())
                .count();
        } else if (stepType == StepType.SILAC_FEATURE_ALIGNMENT || stepType == StepType.SILAC_FEATURE_VECTOR_SEARCH) {
            WorkflowStep filterStep = taskInfo.workflowStep().get(StepType.SILAC_FILTER_SUMMARIZE);
            final SilacFilterSummarizationParameters parameters = (SilacFilterSummarizationParameters) Helper.workflowStepParameters(filterStep);
            Set<UUID> samplesFeatureAligned = parameters.getSilacParameters().getGroupsList().stream()
                .flatMap(g -> g.getSamplesList().stream())
                .map(sample -> UUID.fromString(sample.getSampleId()))
                .collect(Collectors.toSet());
            return (int) samples().stream()
                .filter(s -> samplesFeatureAligned.contains(s.sampleId()))
                .flatMap(p -> p.fractionIds().stream())
                .count();
        } else if (stepType == StepType.DIA_LFQ_FEATURE_EXTRACTION) {
            WorkflowStep filterStep = taskInfo.workflowStep().get(StepType.DIA_LFQ_FILTER_SUMMARIZATION);
            LfqFilter lfqFilter = (LfqFilter) Helper.workflowStepParameters(filterStep);
            Set<UUID> samplesInLfq = lfqFilter.getLabelFreeQueryParameters().getGroupsList().stream()
                .flatMap(g -> g.getSampleIndexesList().stream())
                .map(idx -> samples().get(idx).sampleId())
                .collect(Collectors.toSet());

            return (int) samples().stream()
                .filter(s -> samplesInLfq.contains(s.sampleId()))
                .flatMap(p -> p.fractionIds().stream())
                .count();

        } else {
            return fractionIdToSampleId().size();
        }
    }

    default int calculateFractionIndexLevelTaskCount(AnalysisTaskInformation taskInfo, StepType stepType) {
        if (stepType == StepType.SILAC_RT_ALIGNMENT) {
            WorkflowStep filterStep = taskInfo.workflowStep().get(StepType.SILAC_FILTER_SUMMARIZE);
            final SilacFilterSummarizationParameters parameters = (SilacFilterSummarizationParameters) Helper.workflowStepParameters(filterStep);
            Set<UUID> samplesFeatureAligned = parameters.getSilacParameters().getGroupsList().stream()
                .flatMap(g -> g.getSamplesList().stream())
                .map(sample -> UUID.fromString(sample.getSampleId()))
                .collect(Collectors.toSet());
            int fractionCount = (int) samples().stream()
                .filter(s -> samplesFeatureAligned.contains(s.sampleId()))
                .flatMap(p -> p.fractionIds().stream())
                .count();
            return fractionCount / samplesFeatureAligned.size();
        } else if (stepType == StepType.LFQ_RETENTION_TIME_ALIGNMENT || stepType == StepType.LFQ_SUMMARIZE) {
            WorkflowStep rtStep = taskInfo.workflowStep().get(StepType.LFQ_RETENTION_TIME_ALIGNMENT);
            final RetentionTimeAlignmentParameters parameters = (RetentionTimeAlignmentParameters) Helper.workflowStepParameters(rtStep);

            Set<UUID> samplesInLfq = parameters.groups().stream()
                .flatMap(g -> g.sampleIds().stream())
                .collect(Collectors.toSet());

            int fractionCount = (int) samples().stream()
                .filter(s -> samplesInLfq.contains(s.sampleId()))
                .flatMap(p -> p.fractionIds().stream())
                .count();
            return fractionCount / samplesInLfq.size();
        } else if (stepType == StepType.DIA_LFQ_RETENTION_TIME_ALIGNMENT || stepType == StepType.DIA_LFQ_SUMMARIZE) {
            WorkflowStep filterStep = taskInfo.workflowStep().get(StepType.DIA_LFQ_FILTER_SUMMARIZATION);
            LfqFilter lfqFilter = (LfqFilter) Helper.workflowStepParameters(filterStep);
            Set<UUID> samplesInLfq = lfqFilter.getLabelFreeQueryParameters().getGroupsList().stream()
                .flatMap(g -> g.getSampleIndexesList().stream())
                .map(idx -> samples().get(idx).sampleId())
                .collect(Collectors.toSet());

            int fractionCount = (int) samples().stream()
                .filter(s -> samplesInLfq.contains(s.sampleId()))
                .flatMap(p -> p.fractionIds().stream())
                .count();
            return fractionCount / samplesInLfq.size();
        } else {
            return fractionIdToSampleId().size() / samples().size();
        }
    }

    enum Status {
        PENDING,
        CREATED,
        PROCESSING,
        CANCELLED,
        FAILED,
        DONE;

        public com.bsi.peaks.model.dto.Progress.State progressState() {
            switch (this) {
                case DONE:
                    return com.bsi.peaks.model.dto.Progress.State.DONE;
                case PENDING:
                    return com.bsi.peaks.model.dto.Progress.State.PENDING;
                case FAILED:
                    return com.bsi.peaks.model.dto.Progress.State.FAILED;
                case PROCESSING:
                    return  com.bsi.peaks.model.dto.Progress.State.PROGRESS;
                case CANCELLED:
                    return com.bsi.peaks.model.dto.Progress.State.CANCELLED;
                case CREATED:
                    return com.bsi.peaks.model.dto.Progress.State.PENDING;
                default:
                    return com.bsi.peaks.model.dto.Progress.State.UNRECOGNIZED;
            }
        }
    }
}
