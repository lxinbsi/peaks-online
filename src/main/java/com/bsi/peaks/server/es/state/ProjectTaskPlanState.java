package com.bsi.peaks.server.es.state;


import com.bsi.peaks.event.projectAggregate.ProjectAggregateState;
import com.bsi.peaks.event.tasks.JsonTaskParameters;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.TaskParameters;
import com.bsi.peaks.internal.task.FDBProteinCandidateTaskParameters;
import com.bsi.peaks.internal.task.SlFilterSummarizationTaskParameters;
import com.bsi.peaks.internal.task.SlPeptideSummarizationTaskParameters;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.system.levels.LevelTask;
import com.bsi.peaks.model.system.levels.MemoryOptimizedAnalysisDataIndependentLevelTask;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFactory;
import com.bsi.peaks.model.system.levels.MemoryOptimizedIndexedFractionLevelTask;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Iterables;
import io.vertx.core.json.JsonObject;
import org.immutables.value.Value;
import org.pcollections.PVector;
import org.pcollections.TreePVector;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;
import static com.bsi.peaks.model.ModelConversion.uuidToByteString;
import static com.bsi.peaks.model.ModelConversion.uuidsFrom;
import static com.bsi.peaks.model.ModelConversion.uuidsToByteString;

@PeaksImmutable
@JsonDeserialize(builder = ProjectTaskPlanStateBuilder.class)
public interface ProjectTaskPlanState {

    UUID taskPlanId();
    StepType stepType();
    LevelTask level();

    @Nullable
    JsonTaskParameters jsonTaskParamaters();

    @Nullable
    TaskParameters taskParameters();

    default PVector<UUID> taskIds() {
        return TreePVector.empty();
    }
    int processAttemptsRemaining();

    @Value.Lazy
    default Optional<UUID> latestTaskId() {
        final List<UUID> taskIds = taskIds();
        return taskIds.isEmpty() ? Optional.empty() : Optional.of(taskIds.get(taskIds.size() - 1));
    }

    default List<UUID> referencedTargetDatabaseIds() {
        switch (stepType()) {
            case DB_TAG_SEARCH:
                final JsonObject json = new JsonObject(jsonTaskParamaters().getTaskParametersJson());
                if (json.containsKey("targetDatabaseSources")) {
                    return json.getJsonArray("targetDatabaseSources").stream()
                        .map(JsonObject.class::cast)
                        .map(databaseSource -> {
                            String databaseId = databaseSource.getString("databaseid");
                            return ModelConversion.uuidFrom(Base64.getDecoder().decode(databaseId));
                        })
                        .collect(Collectors.toList());
                } else if (json.containsKey("dbInfo")) {
                    // DbInfo is for backward compatibility.
                    return json.getJsonArray("dbInfo").stream()
                        .map(JsonObject.class::cast)
                        .filter(databaseSource -> !databaseSource.getBoolean("isContaminant"))
                        .map(databaseSource -> UUID.fromString(databaseSource.getJsonObject("database").getString("id")))
                        .collect(Collectors.toList());
                } else {
                    return Collections.emptyList();
                }
            case FDB_PROTEIN_CANDIDATE_FROM_FASTA:
                FDBProteinCandidateTaskParameters fastDBProteinCandidateParameters = taskParameters().getFastDBProteinCandidateParameters();
                return fastDBProteinCandidateParameters.getTargetDatabaseSourcesList()
                    .stream()
                    .map(databaseSource -> ModelConversion.uuidFrom(databaseSource.getDatabaseId()))
                    .collect(Collectors.toList());
            case SL_PEPTIDE_SUMMARIZATION:
                SlPeptideSummarizationTaskParameters slPeptideSummarizationTaskParameters = taskParameters().getSlPeptideSummarizationTaskParameters();
                return slPeptideSummarizationTaskParameters.getTargetDatabaseSourcesList()
                    .stream()
                    .map(databaseSource -> ModelConversion.uuidFrom(databaseSource.getDatabaseId()))
                    .collect(Collectors.toList());
            default:
                return Collections.emptyList();
        }
    }

    default List<UUID> referencedContaminantDatabaseIds() {
        switch (stepType()) {
            case DB_TAG_SEARCH:
                final JsonObject json = new JsonObject(jsonTaskParamaters().getTaskParametersJson());
                if (json.containsKey("contaminantDatabaseSources")) {
                    return json.getJsonArray("contaminantDatabaseSources").stream()
                        .map(JsonObject.class::cast)
                        .map(databaseSource -> {
                            String databaseId = databaseSource.getString("databaseid");
                            return ModelConversion.uuidFrom(Base64.getDecoder().decode(databaseId));
                        })
                        .collect(Collectors.toList());
                } else if (json.containsKey("dbInfo")) {
                    // DbInfo is for backward compatibility.
                    return json.getJsonArray("dbInfo").stream()
                        .map(JsonObject.class::cast)
                        .filter(databaseSource -> databaseSource.getBoolean("isContaminant"))
                        .map(databaseSource -> UUID.fromString(databaseSource.getJsonObject("database").getString("id")))
                        .collect(Collectors.toList());
                } else {
                    return Collections.emptyList();
                }
            case FDB_PROTEIN_CANDIDATE_FROM_FASTA:
                FDBProteinCandidateTaskParameters fastDBProteinCandidateParameters = taskParameters().getFastDBProteinCandidateParameters();
                return fastDBProteinCandidateParameters.getContaminantDatabaseSourcesList()
                    .stream()
                    .map(databaseSource -> ModelConversion.uuidFrom(databaseSource.getDatabaseId()))
                    .collect(Collectors.toList());
            default:
                return Collections.emptyList();
        }
    }

    ProjectTaskPlanState withTaskIds(PVector<UUID> taskIds);
    ProjectTaskPlanState withProcessAttemptsRemaining(int processAttemptsRemaining);
    ProjectTaskPlanState withJsonTaskParamaters(JsonTaskParameters jsonTaskParamaters);
    ProjectTaskPlanState withTaskParameters(TaskParameters taskParameters);

    default ProjectTaskPlanState plusTaskIds(UUID taskId) {
        return withTaskIds(taskIds().plus(taskId));
    }

    default void proto(ProjectAggregateState.TaskPlanState.Builder builder) {
        builder.setProcessAttemptsRemaining(processAttemptsRemaining());
        builder.setTaskPlanId(uuidToByteString(taskPlanId()));
        builder.setTaskIds(uuidsToByteString(taskIds()));
        builder.setReferencedTargetDatabaseIds(uuidsToByteString(referencedTargetDatabaseIds()));
        builder.setReferencedContaminantDatabaseIds(uuidsToByteString(referencedContaminantDatabaseIds()));

        final TaskDescription.Builder taskDescriptionBuilder = builder.getTaskDescriptionBuilder().setStepType(stepType());
        final LevelTask level = level();
        switch (level.levelCase()) {
            case FRACTIONLEVELTASK:
                taskDescriptionBuilder.getFractionLevelTaskBuilder()
                    .setSampleId(level.sampleIds().get(0).toString())
                    .setFractionId(level.fractionIds().get(0).toString());
                break;
            case SAMPLELEVELTASK:
                taskDescriptionBuilder.getSampleLevelTaskBuilder()
                    .setSampleId(level.sampleIds().get(0).toString())
                    .addAllFractionIds(Iterables.transform(level.fractionIds(), UUID::toString));
                break;
            case ANALYSISLEVELTASK:
                taskDescriptionBuilder.getAnalysisLevelTaskBuilder()
                    .addAllSampleIds(Iterables.transform(level.sampleIds(), UUID::toString))
                    .addAllFractionIds(Iterables.transform(level.fractionIds(), UUID::toString));
                break;
            case INDEXEDFRACTIONLEVELTASK:
                taskDescriptionBuilder.getIndexedFractionLevelTaskBuilder()
                    .setFractionIndex(((MemoryOptimizedIndexedFractionLevelTask) level).getFractionIndex())
                    .addAllSampleIds(Iterables.transform(level.sampleIds(), UUID::toString))
                    .addAllFractionIds(Iterables.transform(level.fractionIds(), UUID::toString));
                break;
            case ANALYSISDATAINDEPENDENTLEVELTASK:
                taskDescriptionBuilder.getAnalysisDataIndependentLevelTaskBuilder()
                    .setStep(((MemoryOptimizedAnalysisDataIndependentLevelTask) level).stepType());
                break;
        }

        final JsonTaskParameters jsonParamaters = jsonTaskParamaters();
        if (jsonParamaters != null) {
            taskDescriptionBuilder.setJsonParamaters(jsonParamaters);
        } else {
            final TaskParameters taskParameters = taskParameters();
            if (taskParameters != null) taskDescriptionBuilder.setParameters(taskParameters);
        }


    }

    static ProjectTaskPlanState from(ProjectAggregateState.TaskPlanState proto, MemoryOptimizedFactory memoryOptimizedFactory) {
        final TaskDescription taskDescription = proto.getTaskDescription();

        final ProjectTaskPlanStateBuilder builder = new ProjectTaskPlanStateBuilder()
            .taskPlanId(uuidFrom(proto.getTaskPlanId()))
            .stepType(taskDescription.getStepType())
            .processAttemptsRemaining(proto.getProcessAttemptsRemaining())
            .level(memoryOptimizedFactory.internLevelTask(taskDescription))
            .referencedTargetDatabaseIds(uuidsFrom(proto.getReferencedTargetDatabaseIds()))
            .referencedContaminantDatabaseIds(uuidsFrom(proto.getReferencedContaminantDatabaseIds()));

        TreePVector<UUID> taskIds = TreePVector.empty();
        for (UUID taskId : uuidsFrom(proto.getTaskIds())) {
            taskIds = taskIds.plus(taskId);
        }
        builder.taskIds(taskIds);

        switch (taskDescription.getTaskCase()) {
            case JSONPARAMATERS:
                builder.jsonTaskParamaters(taskDescription.getJsonParamaters());
                break;
            case PARAMETERS:
                builder.taskParameters(taskDescription.getParameters());
                break;
        }

        return builder.build();
    }
}
