package com.bsi.peaks.server.dto;

import akka.japi.Pair;
import com.bsi.peaks.model.system.Progress;
import com.bsi.peaks.model.system.steps.Step;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DtoAdaptorHelpers {

    /**
     * Calculates progress
     * @param id reference to entity that progress represents
     * @param progressCount map of progress to a count of tasks in the progress state
     * @param totalTasks total tasks
     * @return progress
     */
    public static com.bsi.peaks.model.dto.Progress progress(UUID id, float[] progressCount, int totalTasks, boolean isCassandraFull) {
        final com.bsi.peaks.model.dto.Progress.Builder builder = com.bsi.peaks.model.dto.Progress.newBuilder()
            .setId(id.toString());

        float numOfQueued = progressCount[Progress.QUEUED.ordinal()];
        float numOfInProgress = progressCount[Progress.IN_PROGRESS.ordinal()];
        float numOfDone = progressCount[Progress.DONE.ordinal()];

        builder
            .setCassandraFull(isCassandraFull)
            .setQueued(numOfQueued / totalTasks)
            .setInprogress(numOfInProgress / totalTasks)
            .setDone(numOfDone / totalTasks);

        if (numOfDone == totalTasks) {
            return builder.setState(com.bsi.peaks.model.dto.Progress.State.DONE).build();
        }

        final boolean failed = progressCount[Progress.FAILED.ordinal()] > 0
            || progressCount[Progress.CANCELLED.ordinal()] > 0
            || progressCount[Progress.STOPPING.ordinal()] > 0;
        if (failed) {
            return builder.setState(com.bsi.peaks.model.dto.Progress.State.FAILED).build();
        }

        if (progressCount[Progress.IN_PROGRESS.ordinal()] > 0
            || progressCount[Progress.QUEUED.ordinal()] > 0
            || progressCount[Progress.DONE.ordinal()] > 0
        ) {
            return builder.setState(com.bsi.peaks.model.dto.Progress.State.PROGRESS).build();
        }

        return builder.setState(com.bsi.peaks.model.dto.Progress.State.PENDING).build();
    }

    /**
     * Calculates progress
     * @param id reference to entity that progress represents
     * @param progressCount map of progress to a count of tasks in the progress state
     * @param totalTasks total tasks
     * @return progress
     */
    public static com.bsi.peaks.model.dto.Progress progress(
        com.bsi.peaks.model.dto.Progress.State state,
        UUID id,
        float[] progressCount,
        int totalTasks
    ) {
        float numOfQueued = progressCount[Progress.QUEUED.ordinal()];
        float numOfInProgress = progressCount[Progress.IN_PROGRESS.ordinal()];
        float numOfDone = progressCount[Progress.DONE.ordinal()];
        final float floatTotalTasks = totalTasks;

        return com.bsi.peaks.model.dto.Progress.newBuilder()
            .setId(id.toString())
            .setQueued(numOfQueued / floatTotalTasks)
            .setInprogress(numOfInProgress / floatTotalTasks)
            .setDone(numOfDone / floatTotalTasks)
            .setState(state)
            .build();
   }

    public static int calculateTotalTasks(Iterable<Step.Type> steps, List<Pair<UUID, List<UUID>>> samples) {
        final int fractionCount = samples.stream()
            .map(Pair::second)
            .mapToInt(List::size)
            .sum();
        final int sampleCount = samples.size();
        final int fractionsPerSample = samples.size();
        return calculateTotalTasks(steps, fractionCount, sampleCount, fractionsPerSample);
    }

    private static int calculateTotalTasks(Iterable<Step.Type> steps, int fractionCount, int sampleCount, int fractionsPerSample) {
        int sum = 0;
        for (Step.Type type : steps) {
            switch (type) {
                case FEATURE_DETECTION:
                case DATA_REFINE:
                case DENOVO:
                case DB_TAG_SEARCH:
                case DB_PRE_SEARCH:
                case DB_BATCH_SEARCH:
                case PTM_FINDER_BATCH_SEARCH:
                case SPIDER_BATCH_SEARCH:
                case REPORTER_ION_Q_BATCH_CALCULATION:
                case DIA_LFQ_FEATURE_EXTRACTION:
                case LFQ_FEATURE_ALIGNMENT:
                    // Fraction level
                    sum += fractionCount;
                    break;
                case DB_TAG_SUMMARIZE:
                case REPORTER_ION_Q_NORMALIZATION:
                    // Sample level
                    sum += sampleCount;
                    break;
                case DENOVO_FILTER:
                case DB_SUMMARIZE:
                case DB_FILTER_SUMMARIZATION:
                case PTM_FINDER_SUMMARIZE:
                case PTM_FINDER_FILTER_SUMMARIZATION:
                case SPIDER_FILTER_SUMMARIZATION:
                case SPIDER_SUMMARIZE:
                case PEPTIDE_LIBRARY_PREPARE:
                case DIA_LFQ_FILTER_SUMMARIZATION:
                case LFQ_FILTER_SUMMARIZATION:
                case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                    // Analysis level
                    sum += 1;
                    break;
                case DIA_LFQ_RETENTION_TIME_ALIGNMENT:
                case DIA_LFQ_SUMMARIZE:
                case LFQ_RETENTION_TIME_ALIGNMENT:
                case LFQ_SUMMARIZE:
                    // Fraction Index level
                    sum += fractionCount / fractionsPerSample;
                    break;
                default:
                    throw new IllegalStateException("Unexpected task: " + type.name());
            }
        }
        return sum;
    }

    public static float[] progressCounter(List<UUID> ids, Map<UUID, com.bsi.peaks.model.system.Progress> idsToProgress, Map<UUID, Float> idToProcessingProgressFinished) {
        float[] progressCount = progressCount(Iterables.transform(ids, idsToProgress::get));

        double inProgressFinished = ids.stream()
            .filter(id -> idsToProgress.get(id) == Progress.IN_PROGRESS)
            .mapToDouble(id -> idToProcessingProgressFinished.getOrDefault(id, 0f))
            .sum();

        progressCount[Progress.DONE.ordinal()] += inProgressFinished;
        progressCount[Progress.IN_PROGRESS.ordinal()] -= inProgressFinished;

        return progressCount;
    }

    public static float[] progressCount(Iterable<Progress> progressCollection) {
        float[] progressCount = new float[Progress.values().length];
        for (Progress progress : progressCollection) {
            progressCount[progress.ordinal()] += 1f;
        }
        return progressCount;
    }
}
