package com.bsi.peaks.server.es.project;

import akka.Done;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.model.applicationgraph.GraphTaskIds;
import com.bsi.peaks.model.applicationgraph.SampleFractions;
import com.bsi.peaks.model.core.AcquisitionMethod;
import com.bsi.peaks.model.dto.IonMobilityType;
import com.bsi.peaks.model.dto.Sample;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.system.Progress;
import com.bsi.peaks.server.dto.DtoAdaptorHelpers;
import com.bsi.peaks.server.es.state.ProjectAggregateState;
import com.bsi.peaks.server.es.state.ProjectFractionState;
import com.bsi.peaks.server.es.state.ProjectSampleState;
import com.bsi.peaks.server.es.state.ProjectTaskState;
import org.pcollections.PMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ProjectAggregateSampleQuery {

    private final ProjectAggregateQueryConfig config;

    public ProjectAggregateSampleQuery(ProjectAggregateQueryConfig config) {
        this.config = config;
    }

    public CompletionStage<ProjectAggregateQueryResponse.Builder>  querySample(ProjectAggregateQueryRequest.SampleQueryRequest queryRequest, ProjectAggregateState data) {
        final UUID sampleId = uuidFrom(queryRequest.getSampleId());
        final ProjectSampleState sample = data.sampleIdToSample().get(sampleId);
        if (sample == null) {
            return completedFuture(
                ProjectAggregateQueryResponse.newBuilder()
                    .setFailure(ProjectAggregateQueryResponse.QueryFailure.newBuilder()
                        .setReason(ProjectAggregateQueryResponse.QueryFailure.Reason.NOT_FOUND)
                        .build()
                    )
            );
        }
        final ProjectAggregateQueryResponse.SampleQueryResponse.Builder sampleBuilder = ProjectAggregateQueryResponse.SampleQueryResponse.newBuilder();
        switch (queryRequest.getQueryCase()) {
            case GRAPHTASKIDS: {
                queryGraphTaskIds(data, sample, sampleBuilder.getGraphTaskIdsBuilder());
                break;
            }
            default:
                throw new IllegalArgumentException("Unexpected sample query " + queryRequest.getQueryCase());
        }
        return completedFuture(ProjectAggregateQueryResponse.newBuilder()
            .setSampleQueryResponse(sampleBuilder.setSampleId(queryRequest.getSampleId()))
        );
    }

    public void progress(
        ProjectAggregateState data,
        ProjectAggregateQueryResponse.ProgressList.Builder progressListBuilder,
        UUID sampleId,
        boolean isCassandraFull
    ) {
        final ProjectSampleState sampleState = data.sampleIdToSample().get(sampleId);
        final int numberOfSteps = sampleState.steps().size() + 1; // Add step for upload.
        final int totalSampleTasks = numberOfSteps * sampleState.fractionIds().size();
        final List<Progress> sampleProgressList = new ArrayList<>();
        for (ProjectFractionState fractionState : sampleState.fractionState().values()) {
            final Map<StepType, UUID> fractionStepTaskPlanId = fractionState.stepTaskPlanId();
            final String fractionId = fractionState.fractionID().toString();
            final com.bsi.peaks.model.system.Progress uploadProgress;
            final com.bsi.peaks.model.dto.Progress fractionProgress;

            switch (fractionState.uploadState()) {
                case PENDING:
                case QUEUED:
                    uploadProgress = com.bsi.peaks.model.system.Progress.PENDING;
                    fractionProgress = com.bsi.peaks.model.dto.Progress.newBuilder()
                        .setId(fractionId)
                        .setState(com.bsi.peaks.model.dto.Progress.State.PENDING)
                        .build();
                    break;
                case STARTED:
                    uploadProgress = com.bsi.peaks.model.system.Progress.IN_PROGRESS;
                    fractionProgress = com.bsi.peaks.model.dto.Progress.newBuilder()
                        .setId(fractionId)
                        .setState(com.bsi.peaks.model.dto.Progress.State.PENDING)
                        .build();
                    break;
                case COMPLETED:
                    uploadProgress = com.bsi.peaks.model.system.Progress.DONE;
                    fractionProgress = Optional.ofNullable(fractionStepTaskPlanId.get(StepType.DATA_LOADING))
                        .flatMap(data::taskState)
                        .map(taskState -> {
                            final com.bsi.peaks.model.dto.Progress.Builder builder = com.bsi.peaks.model.dto.Progress.newBuilder().setId(fractionId);
                            switch (taskState.progress()) {
                                case PENDING:
                                    builder.setState(com.bsi.peaks.model.dto.Progress.State.PENDING);
                                    break;
                                case QUEUED:
                                    builder.setState(com.bsi.peaks.model.dto.Progress.State.PROGRESS);
                                    builder.setQueued(1f);
                                    break;
                                case FAILED:
                                    builder.setState(com.bsi.peaks.model.dto.Progress.State.FAILED);
                                    break;
                                case DONE:
                                    builder.setState(com.bsi.peaks.model.dto.Progress.State.DONE);
                                    builder.setDone(1f);
                                    break;
                                case IN_PROGRESS:
                                    builder.setState(com.bsi.peaks.model.dto.Progress.State.PROGRESS);
                                    builder.setInprogress(1f - taskState.processingProgress());
                                    builder.setDone(taskState.processingProgress());
                                    break;
                            }
                            return builder.build();
                        })
                        .orElseGet(() -> com.bsi.peaks.model.dto.Progress.newBuilder()
                            .setId(fractionId)
                            .setState(com.bsi.peaks.model.dto.Progress.State.PENDING)
                            .build());
                    break;
                case FAILED:
                case TIMEOUT:
                    uploadProgress = com.bsi.peaks.model.system.Progress.FAILED;
                    fractionProgress = com.bsi.peaks.model.dto.Progress.newBuilder()
                        .setId(fractionId)
                        .setState(com.bsi.peaks.model.dto.Progress.State.FAILED)
                        .build();
                    break;
                default:
                    throw new IllegalStateException("Unexpected upload state " + fractionState.uploadState());
            }
            sampleProgressList.add(uploadProgress);
            fractionStepTaskPlanId.values()
                .stream()
                .map(data::taskState)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(ProjectTaskState::progress)
                .forEach(sampleProgressList::add);
            progressListBuilder.addProgress(fractionProgress);
        }
        final float[] progressCount = DtoAdaptorHelpers.progressCount(sampleProgressList);
        progressListBuilder.addProgress(DtoAdaptorHelpers.progress(sampleId, progressCount, totalSampleTasks, isCassandraFull));
    }

    private void queryGraphTaskIds(ProjectAggregateState data, ProjectSampleState sample, GraphTaskIds.Builder builder) {
        builder.setKeyspace(config.keyspace());
        builder.setProjectId(config.projectId().toString());

        final SampleFractions.Builder sampleBuilder = builder.addSamplesBuilder()
            .setName(sample.name())
            .setSampleId(sample.sampleId().toString())
            .setEnzyme(sample.enzyme())
            .setInstrument(sample.instrument())
            .setActivationMethod(sample.activationMethod());

        final PMap<UUID, ProjectFractionState> fractionStateMap = sample.fractionState();
        for (UUID fractionId : sample.fractionIds()) {
            final ProjectFractionState fraction = fractionStateMap.get(fractionId);
            final String fractionIdString = fractionId.toString();
            builder.addFractionIds(fractionIdString);
            sampleBuilder.addFractionsIds(fractionIdString);
            sampleBuilder.addFractionSrcFile(fraction.srcFile());
            Optional.ofNullable(fraction.stepTaskPlanId().get(StepType.DATA_LOADING))
                .flatMap(data::taskState)
                .filter(task -> task.progress() == com.bsi.peaks.model.system.Progress.DONE)
                .ifPresent(task -> builder.putFractionIdToDataLoadingTaskId(fractionIdString, task.taskId().toString()));
        }
    }

    CompletionStage<Sample.Builder> sampleDto(ProjectSampleState sample) {
        final Sample.Builder builder = Sample.newBuilder()
            .setId(sample.sampleId().toString())
            .setName(sample.name())
            .setEnzyme(sample.enzyme().getName())
            .setInstrument(sample.instrument().getName())
            .setActivationMethod(sample.activationMethod())
            .setAcquisitionMethod(sample.acquisitionMethod())
            .setHasIonMobility(sample.instrument().getIonMobillityType() == IonMobilityType.TIMS);
        final List<UUID> fractionIds = sample.fractionIds();
        final Map<UUID, ProjectFractionState> fractionStateMap = sample.fractionState();
        if (fractionIds.isEmpty()) {
            return completedFuture(builder);
        }
        final Iterator<UUID> fractionIdIterator = fractionIds.iterator();
        CompletionStage<Done> future = builerFraction(builder.addFractionsBuilder(), fractionStateMap.get(fractionIdIterator.next()));
        while (fractionIdIterator.hasNext()) {
            final Sample.Fraction.Builder fractionBuilder = builder.addFractionsBuilder();
            final ProjectFractionState fractionState = fractionStateMap.get(fractionIdIterator.next());
            future = future.thenCompose(ignore -> builerFraction(fractionBuilder, fractionState));
        }
        return future.thenApply(ignore-> builder);
    }

    private CompletionStage<Done> builerFraction(Sample.Fraction.Builder fractionBuilder, ProjectFractionState fractionState) {
        final UUID fractionId = fractionState.fractionID();
        return config.fractionCache().get(fractionId)
            .thenApply(fraction -> {
                fractionBuilder
                    .setId(fractionId.toString())
                    .setSourceFile(fractionState.srcFile());

                fraction.ifPresent(fractionCacheEntry -> {
                    if (fractionCacheEntry.acquisitionMethod() == AcquisitionMethod.DIA) {
                        fractionBuilder.setAcquisitionMethod(Sample.AcquisitionMethod.DIA);
                    } else {
                        fractionBuilder.setAcquisitionMethod(Sample.AcquisitionMethod.DDA);
                    }
                    int ms2Count;
                    if (fractionCacheEntry.hasIonMobility() && fractionCacheEntry.acquisitionMethod() == AcquisitionMethod.DIA) {
                        ms2Count = fractionCacheEntry.frameCountByMsLevel(2);
                    } else {
                        ms2Count = fractionCacheEntry.numberOfScansByMsLevel(2);
                    }
                    fractionBuilder
                        .setMaxRetentionTime(fractionCacheEntry.maxRetentionTime().getSeconds())
                        .setMs1ScanCount(fractionCacheEntry.hasIonMobility() ?
                            fractionCacheEntry.frameCountByMsLevel(1) : fractionCacheEntry.numberOfScansByMsLevel(1))
                        .setMs2ScanCount(ms2Count)
                        .addAllFaimsCvs(fractionCacheEntry.faimsCvs());
                });
                return Done.getInstance();
            });
    }

    public CompletionStage<Set<Float>> faimsCvsFoundInFractions(Iterable<UUID> fractionIds) {
        return Source.from(fractionIds)
            .runFoldAsync(new HashSet<>(), (set, fractionId) ->
                    config.fractionCache().get(fractionId)
                        .thenCompose(opt -> {
                            if (opt.isPresent()) {
                                set.addAll(opt.get().faimsCvs());
                            }
                            return completedFuture(set);
                        }),
                config.materializer());
    }

    static void querySampleFractions(ProjectAggregateState data, ProjectAggregateQueryResponse.ProjectQueryResponse.SampleFractionsList.Builder builder) {
        for (ProjectSampleState sample : data.orderSamples()) {
            final SampleFractions.Builder sampleFractionsBuilder = builder.addSampleFractionsBuilder()
                .setSampleId(sample.sampleId().toString())
                .setName(sample.name())
                .setEnzyme(sample.enzyme())
                .setInstrument(sample.instrument())
                .setAcquisitionMethod(sample.acquisitionMethod())
                .setActivationMethod(sample.activationMethod());
            final Map<UUID, ProjectFractionState> fractionStateMap = sample.fractionState();
            for (UUID fractionId : sample.fractionIds()) {
                final ProjectFractionState fraction = fractionStateMap.get(fractionId);
                sampleFractionsBuilder.addFractionsIds(fractionId.toString());
                sampleFractionsBuilder.addFractionSrcFile(fraction.srcFile());
            }
        }
    }

}
