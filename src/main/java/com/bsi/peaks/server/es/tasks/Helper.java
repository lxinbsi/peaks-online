package com.bsi.peaks.server.es.tasks;

import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.graph.SampleFractions;
import com.bsi.peaks.event.tasks.JsonTaskParameters;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.internal.task.AnalysisLevelTask;
import com.bsi.peaks.internal.task.FractionLevelTask;
import com.bsi.peaks.internal.task.IndexedFractionLevelTask;
import com.bsi.peaks.internal.task.SampleLevelTask;
import com.bsi.peaks.model.core.ActivationMethod;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.model.parameters.TagSearchParameters;
import com.bsi.peaks.model.proteomics.Enzyme;
import com.bsi.peaks.model.proteomics.Instrument;
import com.bsi.peaks.model.query.DbSearchQuery;
import com.bsi.peaks.model.query.DenovoQuery;
import com.bsi.peaks.model.query.LabelFreeQuery;
import com.bsi.peaks.model.query.PtmFinderQuery;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.query.ReporterIonQMethodQuery;
import com.bsi.peaks.model.query.SilacQuery;
import com.bsi.peaks.model.query.SlQuery;
import com.bsi.peaks.model.query.SpiderQuery;
import com.bsi.peaks.model.system.Task;
import com.bsi.peaks.model.system.steps.DataLoadingStep;
import com.bsi.peaks.model.system.steps.DataRefineStep;
import com.bsi.peaks.model.system.steps.DbBatchSearchStep;
import com.bsi.peaks.model.system.steps.DbPreSearchStep;
import com.bsi.peaks.model.system.steps.DbSummarizeStep;
import com.bsi.peaks.model.system.steps.DbTagSearchStep;
import com.bsi.peaks.model.system.steps.DbTagSummarizeStep;
import com.bsi.peaks.model.system.steps.DenovoStep;
import com.bsi.peaks.model.system.steps.FeatureDetectionStep;
import com.bsi.peaks.model.system.steps.LfqFeatureAlignmentStep;
import com.bsi.peaks.model.system.steps.LfqSummarizeStep;
import com.bsi.peaks.model.system.steps.PeptideLibraryPrepareStep;
import com.bsi.peaks.model.system.steps.PtmFinderBatchStep;
import com.bsi.peaks.model.system.steps.PtmFinderSummarizeStep;
import com.bsi.peaks.model.system.steps.RetentionTimeAlignmentStep;
import com.bsi.peaks.model.system.steps.SpiderBatchStep;
import com.bsi.peaks.model.system.steps.SpiderSummarizeStep;
import com.bsi.peaks.model.system.steps.Step;
import com.bsi.peaks.model.system.steps.WorkflowStepStep;
import com.bsi.peaks.model.system.steps.WorkflowStepStepBuilder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Helper {

    public static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new ProtobufModule())
        .registerModule(new Jdk8Module());

    public static Iterable<UUID> fractionIds(TaskDescription taskDescription) {
        switch (taskDescription.getLevelCase()) {
            case FRACTIONLEVELTASK:
                return ImmutableList.of(
                    UUID.fromString(taskDescription.getFractionLevelTask().getFractionId())
                );
            case SAMPLELEVELTASK:
                return Iterables.transform(
                    taskDescription.getSampleLevelTask().getFractionIdsList(),
                    UUID::fromString
                );
            case ANALYSISLEVELTASK:
                return Iterables.transform(
                    taskDescription.getAnalysisLevelTask().getFractionIdsList(),
                    UUID::fromString
                );
            case INDEXEDFRACTIONLEVELTASK:
                return Iterables.transform(
                    taskDescription.getIndexedFractionLevelTask().getFractionIdsList(),
                    UUID::fromString
                );
            case ANALYSISDATAINDEPENDENTLEVELTASK:
                return Collections.emptyList();
            default:
                throw new IllegalArgumentException("Unexpected Level");
        }
    }

    public static List<FractionLevelTask> fractionLevelTasks(List<SampleFractions> sampleFractionsList) {
        return sampleFractionsList.stream()
            .flatMap(sampleFractions -> {
                String sampleId = sampleFractions.sampleId().toString();
                return sampleFractions.fractionIds().stream()
                    .map(fractionId -> FractionLevelTask.newBuilder()
                        .setSampleId(sampleId)
                        .setFractionId(fractionId.toString())
                        .build()
                    );
            })
            .collect(Collectors.toList());
    }

    public static List<SampleLevelTask> sampleLevelTasks(List<SampleFractions> sampleFractionsList) {
        return sampleFractionsList.stream().
            map(sampleFractions -> SampleLevelTask.newBuilder()
                .setSampleId(sampleFractions.sampleId().toString())
                .addAllFractionIds(Iterables.transform(sampleFractions.fractionIds(), UUID::toString))
                .build()
            )
            .collect(Collectors.toList());
    }

    public static AnalysisLevelTask analysisLevelTasks(List<SampleFractions> sampleFractionsList) {
        final AnalysisLevelTask.Builder builder = AnalysisLevelTask.newBuilder();
        for (SampleFractions sampleFractions : sampleFractionsList) {
            builder.addSampleIds(sampleFractions.sampleId().toString());
            builder.addAllSampleIds(Iterables.transform(sampleFractions.fractionIds(), UUID::toString));
        }
        return builder.build();
    }

    public static List<IndexedFractionLevelTask> indexedFractionLevelTasks(List<SampleFractions> sampleFractionsList) {
        int fractionCount = sampleFractionsList.get(0).fractionIds().size();

        IndexedFractionLevelTask.Builder[] builders = IntStream.range(0, fractionCount)
            .mapToObj(i -> IndexedFractionLevelTask.newBuilder().setFractionIndex(i))
            .toArray(IndexedFractionLevelTask.Builder[]::new);

        for (SampleFractions sampleFractions : sampleFractionsList) {
            final String sampleId = sampleFractions.sampleId().toString();
            if (sampleFractions.fractionIds().size() != fractionCount) {
                throw new IllegalArgumentException("All samples must have equal number of fractions");
            }
            final List<UUID> fractionIds = sampleFractions.fractionIds();
            for (int i = 0; i < fractionCount; i++) {
                builders[i]
                    .addSampleIds(sampleId)
                    .addFractionIds(fractionIds.get(i).toString());
            }
        }

        return Arrays.stream(builders)
            .map(IndexedFractionLevelTask.Builder::build)
            .collect(Collectors.toList());
    }

    public static Step.Type convertStepType(StepType type) {
        switch (type) {
            case DATA_LOADING:
                return Step.Type.DATA_LOADING;
            case DATA_REFINE:
                return Step.Type.DATA_REFINE;
            case FEATURE_DETECTION:
                return Step.Type.FEATURE_DETECTION;
            case DENOVO:
                return Step.Type.DENOVO;
            case DENOVO_FILTER:
                return Step.Type.DENOVO_FILTER;
            case PEPTIDE_LIBRARY_PREPARE:
                return Step.Type.PEPTIDE_LIBRARY_PREPARE;
            case DB_TAG_SEARCH:
                return Step.Type.DB_TAG_SEARCH;
            case DB_TAG_SUMMARIZE:
                return Step.Type.DB_TAG_SUMMARIZE;
            case DB_PRE_SEARCH:
                return Step.Type.DB_PRE_SEARCH;
            case DB_BATCH_SEARCH:
                return Step.Type.DB_BATCH_SEARCH;
            case DB_SUMMARIZE:
                return Step.Type.DB_SUMMARIZE;
            case DB_FILTER_SUMMARIZATION:
                return Step.Type.DB_FILTER_SUMMARIZATION;
            case PTM_FINDER_BATCH_SEARCH:
                return Step.Type.PTM_FINDER_BATCH_SEARCH;
            case PTM_FINDER_SUMMARIZE:
                return Step.Type.PTM_FINDER_SUMMARIZE;
            case PTM_FINDER_FILTER_SUMMARIZATION:
                return Step.Type.PTM_FINDER_FILTER_SUMMARIZATION;
            case SPIDER_BATCH_SEARCH:
                return Step.Type.SPIDER_BATCH_SEARCH;
            case SPIDER_SUMMARIZE:
                return Step.Type.SPIDER_SUMMARIZE;
            case SPIDER_FILTER_SUMMARIZATION:
                return Step.Type.SPIDER_FILTER_SUMMARIZATION;
            case LFQ_RETENTION_TIME_ALIGNMENT:
                return Step.Type.LFQ_RETENTION_TIME_ALIGNMENT;
            case LFQ_FEATURE_ALIGNMENT:
                return Step.Type.LFQ_FEATURE_ALIGNMENT;
            case LFQ_SUMMARIZE:
                return Step.Type.LFQ_SUMMARIZE;
            case LFQ_FILTER_SUMMARIZATION:
                return Step.Type.LFQ_FILTER_SUMMARIZATION;
            case REPORTER_ION_Q_BATCH_CALCULATION:
                return Step.Type.REPORTER_ION_Q_BATCH_CALCULATION;
            case REPORTER_ION_Q_NORMALIZATION:
                return Step.Type.REPORTER_ION_Q_NORMALIZATION;
            case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                return Step.Type.REPORTER_ION_Q_FILTER_SUMMARIZATION;
            default:
                throw new IllegalArgumentException("Unexpected step:" + type.name());
        }
    }

    public static Object workflowStepParameters(WorkflowStep workflowStep) {
        try {
            switch (workflowStep.getStepType()) {
                case DATA_LOADING:
                    return OM.readValue(workflowStep.getStepParametersJson(), DataLoadingStep.class).parameters();
                case DATA_REFINE:
                    return OM.readValue(workflowStep.getStepParametersJson(), DataRefineStep.class).parameters();
                case FEATURE_DETECTION:
                    return OM.readValue(workflowStep.getStepParametersJson(), FeatureDetectionStep.class).parameters();
                case DENOVO:
                    return OM.readValue(workflowStep.getStepParametersJson(), DenovoStep.class).parameters();
                case PEPTIDE_LIBRARY_PREPARE:
                    return OM.readValue(workflowStep.getStepParametersJson(), PeptideLibraryPrepareStep.class).parameters();
                case DB_TAG_SEARCH:
                    return getTagSearchParameters(workflowStep);
                case DB_TAG_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), DbTagSummarizeStep.class).parameters();
                case DB_PRE_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), DbPreSearchStep.class).parameters();
                case DB_BATCH_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), DbBatchSearchStep.class).parameters();
                case DB_SUMMARIZE:
                    return getDbSummarizeParameters(workflowStep);
                case PTM_FINDER_BATCH_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), PtmFinderBatchStep.class).parameters();
                case PTM_FINDER_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), PtmFinderSummarizeStep.class).parameters();
                case SPIDER_BATCH_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), SpiderBatchStep.class).parameters();
                case SPIDER_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), SpiderSummarizeStep.class).parameters();
                case LFQ_RETENTION_TIME_ALIGNMENT:
                    return OM.readValue(workflowStep.getStepParametersJson(), RetentionTimeAlignmentStep.class).parameters();
                case LFQ_FEATURE_ALIGNMENT:
                    return OM.readValue(workflowStep.getStepParametersJson(), LfqFeatureAlignmentStep.class).parameters();
                case LFQ_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), LfqSummarizeStep.class).parameters();
                case DENOVO_FILTER:
                    return workflowStep.getDenovoFilterParameters();
                case FDB_PROTEIN_CANDIDATE_FROM_FASTA:
                    return workflowStep.getFastDBProteinCandidateParameters();
                case DB_FILTER_SUMMARIZATION:
                    return workflowStep.getDbFilterSummarizationParameters();
                case PTM_FINDER_FILTER_SUMMARIZATION:
                    return workflowStep.getPtmFilterSummarizationParameters();
                case SPIDER_FILTER_SUMMARIZATION:
                    return workflowStep.getSpiderFilterSummarizationParameters();
                case LFQ_FILTER_SUMMARIZATION:
                case DIA_LFQ_FILTER_SUMMARIZATION:
                    return workflowStep.getLfqFilter();
                case REPORTER_ION_Q_BATCH_CALCULATION:
                    return workflowStep.getReporterIonQBatchCalculationParameters();
                case REPORTER_ION_Q_NORMALIZATION:
                    return null;
                case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                    return workflowStep.getReporterIonQFilterSummarizationParameters();
                case SL_SEARCH:
                case DDA_SL_SEARCH:
                    return workflowStep.getSlSearchParameters();
                case DIA_DB_PEPTIDE_SUMMARIZE:
                case SL_PEPTIDE_SUMMARIZATION:
                    return workflowStep.getSlPeptideSummarizationParameters();
                case DIA_DB_FILTER_SUMMARIZE:
                case SL_FILTER_SUMMARIZATION:
                    return workflowStep.getSlFilterSummarizationParameters();
                case SILAC_RT_ALIGNMENT:
                    return workflowStep.getSilacRtAlignmentParameters();
                case SILAC_FEATURE_ALIGNMENT:
                    return workflowStep.getSilacFeatureAlignmentParameters();
                case SILAC_FEATURE_VECTOR_SEARCH:
                    return workflowStep.getSilacSeacrhParameters();
                case SILAC_PEPTIDE_SUMMARIZE:
                    return workflowStep.getSilacSummarizeParameters();
                case SILAC_FILTER_SUMMARIZE:
                    return workflowStep.getSilacFilterSummarizationParamaters();
                case DIA_DB_PRE_SEARCH:
                    return workflowStep.getDiaDbPreSearchParameters();
                case DIA_DB_SEARCH:
                    return workflowStep.getDiaDbSearchParameters();
                case DIA_DB_DATA_REFINE:
                case DIA_DATA_REFINE:
                    return workflowStep.getDiaDataRefineParameters();
                case DIA_LFQ_RETENTION_TIME_ALIGNMENT:
                    return workflowStep.getDiaRtAlignmentParameters();
                case DIA_LFQ_FEATURE_EXTRACTION:
                    return workflowStep.getDiaFeatureExtractionParameters();
                case DIA_LFQ_SUMMARIZE:
                    return workflowStep.getDiaLfqSummarizationParameters();
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        throw new IllegalArgumentException("Unexpected step:" + workflowStep.getStepType());
    }

    public static DBSearchParameters getDbSummarizeParameters(WorkflowStep workflowStep) throws IOException {
        return OM.readValue(workflowStep.getStepParametersJson(), DbSummarizeStep.class).parameters();
    }

    public static TagSearchParameters getTagSearchParameters(WorkflowStep workflowStep) throws IOException {
        // TODO Make backward compatible.
        return OM.readValue(workflowStep.getStepParametersJson(), DbTagSearchStep.class).parameters();
    }

    public static Step step(WorkflowStep workflowStep) {
        try {
            switch (workflowStep.getStepType()) {
                case DATA_LOADING:
                    return OM.readValue(workflowStep.getStepParametersJson(), DataLoadingStep.class);
                case DATA_REFINE:
                    return OM.readValue(workflowStep.getStepParametersJson(), DataRefineStep.class);
                case FEATURE_DETECTION:
                    return OM.readValue(workflowStep.getStepParametersJson(), FeatureDetectionStep.class);
                case DENOVO:
                    return OM.readValue(workflowStep.getStepParametersJson(), DenovoStep.class);
                case PEPTIDE_LIBRARY_PREPARE:
                    return OM.readValue(workflowStep.getStepParametersJson(), PeptideLibraryPrepareStep.class);
                case DB_TAG_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), DbTagSearchStep.class);
                case DB_TAG_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), DbTagSummarizeStep.class);
                case DB_PRE_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), DbPreSearchStep.class);
                case DB_BATCH_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), DbBatchSearchStep.class);
                case DB_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), DbSummarizeStep.class);
                case PTM_FINDER_BATCH_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), PtmFinderBatchStep.class);
                case PTM_FINDER_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), PtmFinderSummarizeStep.class);
                case SPIDER_BATCH_SEARCH:
                    return OM.readValue(workflowStep.getStepParametersJson(), SpiderBatchStep.class);
                case SPIDER_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), SpiderSummarizeStep.class);
                case LFQ_RETENTION_TIME_ALIGNMENT:
                    return OM.readValue(workflowStep.getStepParametersJson(), RetentionTimeAlignmentStep.class);
                case LFQ_FEATURE_ALIGNMENT:
                    return OM.readValue(workflowStep.getStepParametersJson(), LfqFeatureAlignmentStep.class);
                case LFQ_SUMMARIZE:
                    return OM.readValue(workflowStep.getStepParametersJson(), LfqSummarizeStep.class);
                default:
                    return new WorkflowStepStepBuilder().parameters(workflowStep).build();
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static String convertWorkflowStep(WorkflowStep workflowStep) throws IOException {
        final Step step;
        switch (workflowStep.getStepType()) {
            case DATA_LOADING:
            case DATA_REFINE:
            case FEATURE_DETECTION:
            case DENOVO:
            case DB_TAG_SEARCH:
            case DB_TAG_SUMMARIZE:
            case DB_PRE_SEARCH:
            case DB_BATCH_SEARCH:
            case DB_SUMMARIZE:
            case PTM_FINDER_BATCH_SEARCH:
            case PTM_FINDER_SUMMARIZE:
            case SPIDER_BATCH_SEARCH:
            case SPIDER_SUMMARIZE:
            case LFQ_RETENTION_TIME_ALIGNMENT:
            case LFQ_FEATURE_ALIGNMENT:
            case LFQ_SUMMARIZE:
                return workflowStep.getStepParametersJson();
            case DENOVO_FILTER:
            case DB_FILTER_SUMMARIZATION:
            case PTM_FINDER_FILTER_SUMMARIZATION:
            case SPIDER_FILTER_SUMMARIZATION:
            case LFQ_FILTER_SUMMARIZATION:
            case REPORTER_ION_Q_BATCH_CALCULATION:
            case REPORTER_ION_Q_NORMALIZATION:
            case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                step = new WorkflowStepStepBuilder().parameters(workflowStep).build();
                break;
            default:
                throw new IllegalStateException("Unable to convert workflow step" + workflowStep.getParametersCase());
        }
        return OM.writeValueAsString(step);
    }

    public static Step convertStep(StepType type, String stepParametersJson) throws IOException {
        switch (type) {
            case DATA_LOADING:
                return OM.readValue(stepParametersJson, DataLoadingStep.class);
            case DATA_REFINE:
                return OM.readValue(stepParametersJson, DataRefineStep.class);
            case FEATURE_DETECTION:
                return OM.readValue(stepParametersJson, FeatureDetectionStep.class);
            case DENOVO:
                return OM.readValue(stepParametersJson, DenovoStep.class);
            case PEPTIDE_LIBRARY_PREPARE:
                return OM.readValue(stepParametersJson, PeptideLibraryPrepareStep.class);
            case DB_TAG_SEARCH:
                return OM.readValue(stepParametersJson, DbTagSearchStep.class);
            case DB_TAG_SUMMARIZE:
                return OM.readValue(stepParametersJson, DbTagSummarizeStep.class);
            case DB_PRE_SEARCH:
                return OM.readValue(stepParametersJson, DbPreSearchStep.class);
            case DB_BATCH_SEARCH:
                return OM.readValue(stepParametersJson, DbBatchSearchStep.class);
            case DB_SUMMARIZE:
                return OM.readValue(stepParametersJson, DbSummarizeStep.class);
            case PTM_FINDER_BATCH_SEARCH:
                return OM.readValue(stepParametersJson, PtmFinderBatchStep.class);
            case PTM_FINDER_SUMMARIZE:
                return OM.readValue(stepParametersJson, PtmFinderSummarizeStep.class);
            case SPIDER_BATCH_SEARCH:
                return OM.readValue(stepParametersJson, SpiderBatchStep.class);
            case SPIDER_SUMMARIZE:
                return OM.readValue(stepParametersJson, SpiderSummarizeStep.class);
            case LFQ_RETENTION_TIME_ALIGNMENT:
                return OM.readValue(stepParametersJson, RetentionTimeAlignmentStep.class);
            case LFQ_FEATURE_ALIGNMENT:
                return OM.readValue(stepParametersJson, LfqFeatureAlignmentStep.class);
            case LFQ_SUMMARIZE:
                return OM.readValue(stepParametersJson, LfqSummarizeStep.class);
            case DENOVO_FILTER:
            case DB_FILTER_SUMMARIZATION:
            case PTM_FINDER_FILTER_SUMMARIZATION:
            case SPIDER_FILTER_SUMMARIZATION:
            case LFQ_FILTER_SUMMARIZATION:
            case REPORTER_ION_Q_BATCH_CALCULATION:
            case REPORTER_ION_Q_NORMALIZATION:
            case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                return OM.readValue(stepParametersJson, WorkflowStepStep.class);
            default:
                throw new IllegalArgumentException("Unexpected step:" + type.name());
        }
    }

    public static StepType convertStep(Step.Type type) {
        return StepType.forNumber(type.value);
    }

    public static Task convertParameters(JsonTaskParameters jsonTaskParameters) throws ClassNotFoundException, IOException {
        return (Task) OM.readValue(jsonTaskParameters.getTaskParametersJson(), Class.forName(jsonTaskParameters.getTaskParametersClassName()));
    }

    public static Enzyme convertEnzyme(String enzymeJson) throws IOException {
        return OM.readValue(enzymeJson, Enzyme.class);
    }

    public static ActivationMethod convertActivationMethod(String activationMethodJson) throws IOException {
        return OM.readValue(activationMethodJson, ActivationMethod.class);
    }

    public static Instrument convertInstrument(String instrumentJson) throws IOException {
        return OM.readValue(instrumentJson, Instrument.class);
    }

    public static Task convertTask(String taskClassName, String taskJson) throws ClassNotFoundException, IOException {
        return (Task) OM.readValue(taskJson, Class.forName(taskClassName));
    }

    public static Query convertQuery(String json, WorkFlowType workFlowType) throws IOException {
        switch (workFlowType) {
            case DB_SEARCH:
                return OM.readValue(json, DbSearchQuery.class);
            case REPORTER_ION_Q:
                return OM.readValue(json, ReporterIonQMethodQuery.class);
            case SPIDER:
                return OM.readValue(json, SpiderQuery.class);
            case DENOVO:
                return OM.readValue(json, DenovoQuery.class);
            case LFQ:
                return OM.readValue(json, LabelFreeQuery.class);
            case PTM_FINDER:
                return OM.readValue(json, PtmFinderQuery.class);
            case SPECTRAL_LIBRARY:
            case DIA_DB_SEARCH:
                return OM.readValue(json, SlQuery.class);
            case SILAC:
                return OM.readValue(json, SilacQuery.class);
            case DATA_REFINEMENT:
            default:
                throw new IllegalStateException("Unexpected workflow type: " + workFlowType.name());
        }
    }

    public static void prune(ObjectNode node) {
        // The sections parameter was mangled in the past, since the property was not annoated as derived, and so overwritten when using the "from" method on builder.
        // For this reason we should ignore it, also it has no influence on processing as far as I can tell.
        Optional.ofNullable((ObjectNode) node.get("parameters"))
            .ifPresent(parameters -> parameters.remove("section"));

        // The should be considered temporary, since it only applies to unreleased builds (e.g 907).
        // This was added to support reuse of internal data. Should be removed later.
        // July 8, 2019.
        node.remove("hasDenovo");

        pruneEmptyAndNull(node);
    }

    public static boolean pruneEmptyAndNull(JsonNode node) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            if (objectNode.size() == 0) {
                return true;
            }
            List<String> pruned = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> iterator = objectNode.fields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> nodeEntry = iterator.next();
                if (pruneEmptyAndNull(nodeEntry.getValue())) {
                    pruned.add(nodeEntry.getKey());
                }
            }
            pruned.forEach(objectNode::remove);
            return objectNode.size() == 0;
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            if (arrayNode.size() == 0) {
                return true;
            }
            arrayNode.forEach(Helper::pruneEmptyAndNull);
            return false;
        } else {
            return node.isNull();
        }
    }
}
