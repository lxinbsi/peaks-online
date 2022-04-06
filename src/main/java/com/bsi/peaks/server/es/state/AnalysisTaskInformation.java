package com.bsi.peaks.server.es.state;

import akka.japi.Pair;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.messages.service.LfqSummarizationFilterResult;
import com.bsi.peaks.messages.service.SlFilterSummarizationResult;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.dto.ActivationMethod;
import com.bsi.peaks.model.dto.ReporterIonQFilterSummarization;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.proto.FastaHeaderParser;
import com.bsi.peaks.model.proto.FastaSequenceDatabaseSource;
import com.bsi.peaks.model.proto.SimplifiedFastaSequenceDatabaseInfo;
import com.bsi.peaks.model.system.OrderedSampleFractions;
import com.bsi.peaks.model.system.Progress;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFractionLevelTask;
import com.bsi.peaks.model.system.levels.MemoryOptimizedIndexedFractionLevelTask;
import com.bsi.peaks.model.system.levels.MemoryOptimizedSampleLevelTask;
import com.bsi.peaks.model.system.steps.DbSummarizeStepOutput;
import com.bsi.peaks.model.system.steps.PtmFinderSummarizeStepOutput;
import com.bsi.peaks.model.system.steps.SpiderSummarizeStepOutput;
import com.bsi.peaks.server.es.tasks.Helper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import org.pcollections.PMap;
import org.pcollections.PSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;
import static com.bsi.peaks.server.es.WorkManager.RESOURCE_OVERRIDE_PATTERN;

public final class AnalysisTaskInformation {
    protected static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new Jdk8Module())
        .registerModule(new ProtobufModule());

    private final UUID projectId;
    private final ProjectAggregateState state;
    private final ProjectAnalysisState analysis;
    private final List<WorkflowStep> steps;
    private final Map<StepType, Object> stepParameters;
    private final Map<UUID, Integer> fractionIdToIndex;
    private final Map<UUID, List<UUID>> sampleIdToFractionIds;
    private final Map<StepType, WorkflowStep> workflowStepMap;
    private final String keyspace;
    private final String resourceOverride;

    public static AnalysisTaskInformation create(String keyspace, ProjectAggregateState state, UUID projectId, UUID analysisId) {
        final ProjectAnalysisState analysisState = state.analysisIdToAnalysis().get(analysisId);
        if (analysisState == null) {
            throw new IllegalStateException("Analysis " + analysisId + " does not exist");
        }
        final String resourceOverride = analysisState.description().trim().matches(RESOURCE_OVERRIDE_PATTERN) ?
            analysisState.description().trim().replace("<<<", "").replace(">>>", "") : "";

        return new AnalysisTaskInformation(
            keyspace, resourceOverride,
            projectId,
            state,
            analysisState
        );
    }

    protected AnalysisTaskInformation(
        String keyspace, String resourceOverride,
        UUID projectId,
        ProjectAggregateState state,
        ProjectAnalysisState analysis
    ) {
        this.keyspace = keyspace;
        this.resourceOverride = resourceOverride;
        this.projectId = projectId;
        this.state = state;
        this.analysis = analysis;
        steps = analysis.steps();

        workflowStepMap = new HashMap<>();
        for (WorkflowStep step : steps) {
            workflowStepMap.put(step.getStepType(), step);
        }
        stepParameters = Maps.transformValues(workflowStepMap, Helper::workflowStepParameters);

        Map<UUID, Integer> fIdToIndex = new HashMap<>();
        Map<UUID, List<UUID>> sampleIdToFractionIds = new HashMap<>();
        for(OrderedSampleFractions sample : analysis.samples()) {
            for(int i = 0; i < sample.fractionIds().size(); i++) {
                fIdToIndex.put(sample.fractionIds().get(i), i);
            }
            sampleIdToFractionIds.put(sample.sampleId(), sample.fractionIds());
        }
        this.fractionIdToIndex = fIdToIndex;
        this.sampleIdToFractionIds = sampleIdToFractionIds;
    }

    public ProjectAnalysisState analysis() {
        return analysis;
    }

    public String keyspace() {
        return keyspace;
    }

    public UUID projectId() {
        return projectId;
    }

    public UUID analysisId() {
        return analysis.analysisId();
    }

    public String analysisName() {
        return analysis.name();
    }

    public String resourceOverride() {
        return resourceOverride;
    }

    public List<OrderedSampleFractions> samples() {
        return analysis.samples();
    }

    public Iterable<FastaSequenceDatabaseSource> targetDatabaseSources() {
        List<SimplifiedFastaSequenceDatabaseInfo> databases = analysis.databases();
        if (databases.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(fastaSequenceDatabaseSource(databases.get(0)));
        }
    }

    public Iterable<FastaSequenceDatabaseSource> contaminantDatabaseSources() {
        List<SimplifiedFastaSequenceDatabaseInfo> databases = analysis.databases();
        if (databases.size() < 2) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(fastaSequenceDatabaseSource(databases.get(1)));
        }
    }

    private FastaSequenceDatabaseSource fastaSequenceDatabaseSource(SimplifiedFastaSequenceDatabaseInfo db) {
        return FastaSequenceDatabaseSource.newBuilder()
            .setDatabaseId(uuidToByteString(UUID.fromString(db.getId())))
            .addAllTaxonomyIds(db.getTaxonomyIdsList())
            .addAllTaxonomyHash(db.getTaxonomyHashList())
            .setHash(db.getHash())
            .build();
    }

    public Iterable<FastaHeaderParser> fastaHeaderParsers() {
        List<SimplifiedFastaSequenceDatabaseInfo> databases = analysis.databases();
        List<FastaHeaderParser> parsers = new ArrayList<>();
        for (SimplifiedFastaSequenceDatabaseInfo db : databases) {
            UUID dbId = UUID.fromString(db.getId());
            FastaHeaderParser.Builder parser = FastaHeaderParser.newBuilder()
                .setIdPatternStringBytes(db.getIdPatternBytes())
                .setDescriptionPatternStringBytes(db.getDescriptionPatternBytes());
            if (db.getIsContaminant()) {
                for (UUID databaseId : analysis.contaminantDatabaseIds().plus(dbId)) {
                    parsers.add(
                        parser.setDatabaseId(uuidToByteString(databaseId))
                            .setIsContaminant(true)
                            .build());
                }
            } else {
                for (UUID databaseId : analysis.targetDatabaseIds().plus(dbId)) {
                    parsers.add(
                        parser.setDatabaseId(uuidToByteString(databaseId))
                            .setIsContaminant(false)
                            .build());
                }
            }
        }
        return parsers;
    }

    public Map<UUID, UUID> dataLoadingStepFractionIdToTaskId() {
        PMap<UUID, ProjectSampleState> samples = state.sampleIdToSample();
        Map<UUID, ProjectFractionState> map = new HashMap<>();
        for (OrderedSampleFractions sampleFraction : analysis.samples()) {
            final UUID sampleId = sampleFraction.sampleId();
            final Map<UUID, ProjectFractionState> fractionStateMap = samples.get(sampleId).fractionState();
            for (UUID fractionId : sampleFraction.fractionIds()) {
                map.put(fractionId, fractionStateMap.get(fractionId));
            }
        }
        return Maps.transformValues(map, fractionState ->
            Optional.ofNullable(fractionState.stepTaskPlanId().get(StepType.DATA_LOADING))
                .flatMap(taskPlanId -> state.taskState(taskPlanId))
                .filter(task -> task.progress() == Progress.DONE)
                .map(task -> task.taskId())
                .orElse(null)
        );
    }

    public Map<UUID, UUID> dataRefineStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DATA_REFINE);
    }

    public Map<UUID, UUID> featureDetectionStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.FEATURE_DETECTION);
    }

    public Map<UUID, UUID> denovoStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DENOVO);
    }

    public Map<UUID, UUID> tagSearchStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DB_TAG_SEARCH);
    }

    public Map<UUID, UUID> dbPreSearchStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DB_PRE_SEARCH);
    }

    public Map<UUID, UUID> dbSearchStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DB_BATCH_SEARCH);
    }

    public Map<UUID, UUID> ptmSearchStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.PTM_FINDER_BATCH_SEARCH);
    }

    public Map<UUID, UUID> spiderSearchStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.SPIDER_BATCH_SEARCH);
    }

    public Map<UUID, UUID> tagSummarizeStepSampleIdToTaskId() {
        return sampleLevelStepTaskIds(StepType.DB_TAG_SUMMARIZE);
    }

    public Map<UUID, UUID> featureAlignmentFractionIndexToTaskId() {
        return fractionLevelStepTaskIds(StepType.LFQ_FEATURE_ALIGNMENT);
    }

    public Map<UUID, UUID> diaFeatureExtractorFractionIndexToTaskId() {
        return fractionLevelStepTaskIds(StepType.DIA_LFQ_FEATURE_EXTRACTION);
    }

    public Map<Integer, UUID> retentionTimeAlignmentFractionIndexToTaskId() {
        return indexedFractionLevelStepTaskIds(StepType.LFQ_RETENTION_TIME_ALIGNMENT);
    }

    public Map<Integer, UUID> diaLfqRtAlignmentFractionIndexToTaskId() {
        return indexedFractionLevelStepTaskIds(StepType.DIA_LFQ_RETENTION_TIME_ALIGNMENT);
    }

    public Map<Integer, UUID> lfqSummarizeFractionIndexToTaskId() {
        return indexedFractionLevelStepTaskIds(StepType.LFQ_SUMMARIZE);
    }

    public Map<Integer, UUID> diaLfqSummarizeFractionIndexToTaskId() {
        return indexedFractionLevelStepTaskIds(StepType.DIA_LFQ_SUMMARIZE);
    }

    public Map<UUID, UUID> reporterIonQStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.REPORTER_ION_Q_BATCH_CALCULATION);
    }

    public Map<UUID, UUID> reporterIonQNormalizationSampleIdToTaskId() {
        return sampleLevelStepTaskIds(StepType.REPORTER_ION_Q_NORMALIZATION);
    }

    public Map<UUID, UUID> dbDenovoOnlySearchStepSampleIdToTaskId() {
        return sampleLevelStepTaskIds(StepType.DB_DENOVO_ONLY_TAG_SEARCH);
    }

    public Map<UUID, UUID> ptmFinderDenovoOnlySearchStepSampleIdToTaskId() {
        return sampleLevelStepTaskIds(StepType.PTM_FINDER_DENOVO_ONLY_TAG_SEARCH);
    }

    public Map<UUID, UUID> spiderDenovoOnlySearchStepSampleIdToTaskId() {
        return sampleLevelStepTaskIds(StepType.SPIDER_DENOVO_ONLY_TAG_SEARCH);
    }

    public Map<UUID, UUID> slSearchStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.SL_SEARCH);
    }

    public Map<UUID, UUID> diaDbSearchStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DIA_DB_SEARCH);
    }

    public Map<UUID, UUID> diaDenovoDataRefineStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DIA_DATA_REFINE);
    }

    public Map<UUID, UUID> diaDbDataRefineStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DIA_DB_DATA_REFINE);
    }

    public Map<UUID, UUID> ddaSlSearchStepFractionIdToTaskId() {
        return fractionLevelStepTaskIds(StepType.DDA_SL_SEARCH);
    }

    public Optional<UUID> dbSearchSummarizeTaskId() {
        return analysisLevelStepTaskId(StepType.DB_SUMMARIZE);
    }

    public Optional<UUID> ptmSearchSummarizeTaskId() {
        return analysisLevelStepTaskId(StepType.PTM_FINDER_SUMMARIZE);
    }

    public Optional<UUID> spiderSearchSummarizeTaskId() {
        return analysisLevelStepTaskId(StepType.SPIDER_SUMMARIZE);
    }

    public Map<UUID, UUID> sampleIdToDenovoFilterTaskId() {
        return sampleLevelStepTaskIds(StepType.DENOVO_FILTER);
    }

    public Optional<UUID> dbFilterSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.DB_FILTER_SUMMARIZATION);
    }

    public Optional<UUID> peptideLibraryPrepareTaskId() {
        return analysisLevelStepTaskId(StepType.PEPTIDE_LIBRARY_PREPARE);
    }

    public Optional<UUID> fDbProteinCandidateFromFastaTaskId() {
        return analysisLevelStepTaskId(StepType.FDB_PROTEIN_CANDIDATE_FROM_FASTA);
    }

    public Optional<UUID> reporterIonQSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.REPORTER_ION_Q_FILTER_SUMMARIZATION);
    }

    public Optional<UUID> ptmFilterSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.PTM_FINDER_FILTER_SUMMARIZATION);
    }

    public Optional<UUID> spiderFilterSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.SPIDER_FILTER_SUMMARIZATION);
    }

    public Optional<UUID> slPeptideSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.SL_PEPTIDE_SUMMARIZATION);
    }

    public Optional<UUID> diaDbPeptideSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.DIA_DB_PEPTIDE_SUMMARIZE);
    }

    public Optional<UUID> slFilterSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.SL_FILTER_SUMMARIZATION);
    }

    public Optional<UUID> diaDbFilterSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.DIA_DB_FILTER_SUMMARIZE);
    }

    public Optional<UUID> lfqFilterSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.LFQ_FILTER_SUMMARIZATION);
    }

    public Optional<UUID> diaLfqFilterSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.DIA_LFQ_FILTER_SUMMARIZATION);
    }

    public Map<Integer, UUID> silacRtAlignmentFractionIndexToTaskId() {
        return indexedFractionLevelStepTaskIds(StepType.SILAC_RT_ALIGNMENT);
    }

    public Map<UUID, UUID> silacFeatureAlignmentFractionIndexToTaskId() {
        return fractionLevelStepTaskIds(StepType.SILAC_FEATURE_ALIGNMENT);
    }

    public Map<UUID, UUID>  silacFeatureVectorSearchTaskId() {
        return fractionLevelStepTaskIds(StepType.SILAC_FEATURE_VECTOR_SEARCH);
    }

    public Optional<UUID> silacPeptideSummarizeTaskId() {
        return analysisLevelStepTaskId(StepType.SILAC_PEPTIDE_SUMMARIZE);
    }

    public Optional<UUID> silacFilterSummarizationTaskId() {
        return analysisLevelStepTaskId(StepType.SILAC_FILTER_SUMMARIZE);
    }

    public Optional<UUID> diaDbPreSearchTaskId() {
        return analysisLevelStepTaskId(StepType.DIA_DB_PRE_SEARCH);
    }

    public Optional<DbSummarizeStepOutput> dbSummarizeStepOutput() {
        return dbSearchSummarizeTaskId().map(taskId -> readTaskOutput(taskId, DbSummarizeStepOutput.class));
    }

    public Optional<PtmFinderSummarizeStepOutput> ptmFinderSummarizeStepOutput() {
        return ptmSearchSummarizeTaskId().map(taskId -> readTaskOutput(taskId, PtmFinderSummarizeStepOutput.class));
    }

    public Optional<SpiderSummarizeStepOutput> spiderSummarizeStepOutput() {
        return spiderSearchSummarizeTaskId().map(taskId -> readTaskOutput(taskId, SpiderSummarizeStepOutput.class));
    }

    public Optional<LfqSummarizationFilterResult> lfqSummarizationFilterResult() {
        Optional<UUID> filterSummarizationTaskId = lfqFilterSummarizationTaskId().isPresent() ?
            lfqFilterSummarizationTaskId() : diaLfqFilterSummarizationTaskId();
        if (filterSummarizationTaskId.isPresent() && state.taskIdToTask().get(filterSummarizationTaskId.get()).taskDone() != null) {
            return Optional.of(state.taskIdToTask().get(filterSummarizationTaskId.get()).taskDone().getLfqSummarizationFilterResult());
        } else {
            return Optional.empty();
        }
    }

    public Optional<LfqSummarizationFilterResult> diaLfqSummarizationFilterResult() {
        return diaLfqFilterSummarizationTaskId().map(taskId -> state.taskIdToTask().get(taskId).taskDone().getLfqSummarizationFilterResult());
    }

    public Optional<SlFilterSummarizationResult> slFilterSummarizationResult() {
        return slFilterSummarizationTaskId().map(taskId -> state.taskIdToTask().get(taskId).taskDone().getSlFilterSummarizationResult());
    }

    public Optional<SlFilterSummarizationResult> diaFilterSummarizationResult() {
        return diaDbFilterSummarizationTaskId().map(taskId -> state.taskIdToTask().get(taskId).taskDone().getSlFilterSummarizationResult());
    }

    public Collection<UUID> fractionIds() {
        return fractionIdToIndex.keySet();
    }

    public UUID fractionIdToRtAlignmentTaskId(UUID fractionId) {
        return retentionTimeAlignmentFractionIndexToTaskId().get(fractionIdToIndex.get(fractionId));
    }

    public UUID fractionIdToDiaRtAlignmentTaskId(UUID fractionId) {
        return diaLfqRtAlignmentFractionIndexToTaskId().get(fractionIdToIndex.get(fractionId));
    }

    public UUID fractionIdToSilacRtAlignmentTaskId(UUID fractionId) {
        return silacRtAlignmentFractionIndexToTaskId().get(fractionIdToIndex.get(fractionId));
    }

    private Stream<Pair<UUID, ProjectTaskPlanState>> doneTasksForStep(StepType step) {
        final PSet<UUID> taskPlanIds = analysis.stepTaskPlanId().get(step);
        if (taskPlanIds == null) {
            return Stream.empty();
        }
        PMap<UUID, ProjectTaskPlanState> taskPlans = state.taskPlanIdToTaskPlan();
        return taskPlanIds.stream()
            .map(taskPlanId -> {
                ProjectTaskPlanState taskPlanState = taskPlans.get(taskPlanId);
                return state.taskState(taskPlanState)
                    .filter(taskState -> taskState.progress() == Progress.DONE)
                    .map(taskState -> Pair.create(taskState.taskId(), taskPlanState));
            })
            .filter(Optional::isPresent)
            .map(Optional::get);
    }

    private Map<UUID, UUID> fractionLevelStepTaskIds(StepType step) {
        return doneTasksForStep(step).collect(Collectors.toMap(
            p -> ((MemoryOptimizedFractionLevelTask) p.second().level()).getFractionId(),
            Pair::first)
        );
    }

    private Map<UUID, UUID> sampleLevelStepTaskIds(StepType step) {
        return doneTasksForStep(step).collect(Collectors.toMap(
            p -> ((MemoryOptimizedSampleLevelTask) p.second().level()).getSampleId(),
            Pair::first)
        );
    }

    private Map<Integer, UUID> indexedFractionLevelStepTaskIds(StepType step) {
        return doneTasksForStep(step).collect(Collectors.toMap(
            p -> ((MemoryOptimizedIndexedFractionLevelTask) p.second().level()).getFractionIndex(),
            Pair::first)
        );
    }

    private Optional<UUID> analysisLevelStepTaskId(StepType step) {
        final PSet<UUID> taskPlanIds = analysis.stepTaskPlanId().get(step);
        if (taskPlanIds == null) {
            return Optional.empty();
        }
        return taskPlanIds.stream()
            .map(taskPlanId -> state.taskState(taskPlanId)
                .filter(taskState -> taskState.progress() == Progress.DONE)
                .map(taskState -> taskState.taskId())
            )
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
    }

    private <T> T readTaskOutput(UUID taskId, Class<T> clazz) {
        try {
            return OM.readValue(state.taskIdToTask().get(taskId).taskDone().getTaskOutputJson(), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<UUID> stepQueuedFractionIds(StepType stepType) {
        final Set<UUID> taskPlanIds = analysis.stepTaskPlanId().get(stepType);
        if (taskPlanIds == null) {
            return Collections.emptySet();
        }
        return taskPlanIds.stream()
            .map(state.taskPlanIdToTaskPlan()::get)
            .flatMap(taskPlan -> taskPlan.level().fractionIds().stream())
            .collect(Collectors.toSet());
    }

    public ReporterIonQFilterSummarization stepParametersReporterIonQFilterSummarization() {
        final Object value = stepParameters().get(StepType.REPORTER_ION_Q_FILTER_SUMMARIZATION);
        Objects.requireNonNull(value);
        return (ReporterIonQFilterSummarization) value;
    }

    public List<WorkflowStep> steps() {
        return steps;
    }

    public boolean stepQueued(StepType stepType) {
        // There is definitely a more efficient way of doing this ...
        return stepQueuedFractionIds(stepType).containsAll(analysis.fractionIdToSampleId().keySet());
    }

    public boolean hasPtm() {
        return hasStep(StepType.PTM_FINDER_SUMMARIZE);
    }

    public boolean hasDenovo() {
        return hasStep(StepType.DENOVO);
    }

    public boolean hasSpider() {
        return hasStep(StepType.SPIDER_SUMMARIZE);
    }

    public boolean hasDb() {
        return hasStep(StepType.DB_SUMMARIZE);
    }

    public boolean hasSlSearch() {
        return hasStep(StepType.SL_SEARCH);
    }

    public boolean hasDiaDb() {
        return hasStep(StepType.DIA_DB_SEARCH);
    }

    public boolean hasDiaDenovo() {
        return hasStep(StepType.DIA_DATA_REFINE);
    }

    private boolean hasStep(StepType stepType) {
        return stepParameters().containsKey(stepType);
    }

    public Map<StepType, Object> stepParameters() {
        return stepParameters;
    }

    public Map<StepType, WorkflowStep> workflowStep() {
        return workflowStepMap;
    }

    public boolean hasSlProteinInference() {
        return stepParameters().containsKey(StepType.SL_PEPTIDE_SUMMARIZATION) && !analysis.databases().isEmpty();
    }

    public List<UUID> fractionIdsForSample(UUID sampleId) {
        return sampleIdToFractionIds.get(sampleId);
    }

    public boolean isAnalysisWorkflowTypeStepDone(WorkFlowType workflowType) {
        final int numberOfSamples = samples().size();

        final PMap<StepType, PSet<UUID>> stepTaskPlanId = analysis.stepTaskPlanId();

        if (!Iterables.any(stepTaskPlanId.keySet(), x -> ModelConversion.workflowType(x) == workflowType)) {
            return false;
        }

        // Reverse order is more efficient since last task is likely last to complete.
        return Lists.reverse(analysis.steps()).stream()
            .map(WorkflowStep::getStepType)
            .filter(step -> ModelConversion.workflowType(step) == workflowType)
            .allMatch(step -> {
                final PSet<UUID> taskPlanIds = stepTaskPlanId.get(step);
                if (taskPlanIds == null) return false;
                switch (ModelConversion.taskLevel(step)) {
                    case FRACTIONLEVELTASK:
                        if (taskPlanIds.size() < analysis.calculateFractionLevelTaskCount(this, step)) return false;
                        break;
                    case SAMPLELEVELTASK:
                        if (taskPlanIds.size() < numberOfSamples) return false;
                        break;
                    case INDEXEDFRACTIONLEVELTASK:
                        if (taskPlanIds.size() < analysis.calculateFractionIndexLevelTaskCount(this, step)) return false;
                        break;
                    case ANALYSISLEVELTASK:
                    case ANALYSISDATAINDEPENDENTLEVELTASK:
                        if (taskPlanIds.isEmpty()) return false;
                        break;
                }
                return Iterables.all(taskPlanIds, taskPlanId -> state.taskState(taskPlanId)
                    .map(ProjectTaskState::progress)
                    .orElse(com.bsi.peaks.model.system.Progress.PENDING) == com.bsi.peaks.model.system.Progress.DONE
                );
            });
    }

    public ActivationMethod getActivationMethodFromSample(UUID sampleId){
        return state.sampleIdToSample().get(sampleId).activationMethod();
    }

}
