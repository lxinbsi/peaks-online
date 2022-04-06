package com.bsi.peaks.server.cluster;

import akka.Done;
import akka.dispatch.Futures;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.tasks.TaskParameters;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.system.levels.LevelTask;
import com.bsi.peaks.server.es.TaskDeleter;
import com.bsi.peaks.server.es.tasks.DataLoadingTask;
import com.bsi.peaks.service.services.DeleteTaskData;
import com.bsi.peaks.service.services.dbsearch.DBPreSearchDelete;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbPreSearchDelete;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbSearchDelete;
import com.bsi.peaks.service.services.dbsearch.DbIdentificationSummaryFilterServiceDelete;
import com.bsi.peaks.service.services.dbsearch.DbSearchDelete;
import com.bsi.peaks.service.services.dbsearch.DbSearchSummarizeDelete;
import com.bsi.peaks.service.services.dbsearch.DeNovoOnlyTagSearchDelete;
import com.bsi.peaks.service.services.dbsearch.FDBProteinCandidateFromFastaDelete;
import com.bsi.peaks.service.services.dbsearch.PeptideLibraryPrepareDelete;
import com.bsi.peaks.service.services.dbsearch.TagSearchDelete;
import com.bsi.peaks.service.services.dbsearch.TagSearchSummarizeDelete;
import com.bsi.peaks.service.services.denovo.DenovoDelete;
import com.bsi.peaks.service.services.denovo.DenovoSummarizeDelete;
import com.bsi.peaks.service.services.featuredetection.FeatureDetectionDelete;
import com.bsi.peaks.service.services.labelfreequantification.FeatureAlignmentDelete;
import com.bsi.peaks.service.services.labelfreequantification.LabelFreeSummarizeDelete;
import com.bsi.peaks.service.services.labelfreequantification.LfqSummaryFilterServiceDelete;
import com.bsi.peaks.service.services.labelfreequantification.RetentionTimeAlignmentDelete;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaFeatureExtractorDelete;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaLfqSummarizeDelete;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaRententionTimeAlignmentDelete;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQDelete;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQNormalizationDelete;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQSummarizeDelete;
import com.bsi.peaks.service.services.preprocess.DataRefineDelete;
import com.bsi.peaks.service.services.preprocess.dia.DiaDataRefineDelete;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderDelete;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderSummarizeDelete;
import com.bsi.peaks.service.services.silac.SilacFeatureVectorSearhDelete;
import com.bsi.peaks.service.services.silac.SilacFilterSummarizationDelete;
import com.bsi.peaks.service.services.silac.SilacPeptideSummarizationDelete;
import com.bsi.peaks.service.services.silac.SilacRtAlignmentDelete;
import com.bsi.peaks.service.services.spectralLibrary.SlFilterSummarizationDelete;
import com.bsi.peaks.service.services.spectralLibrary.SlPeptideSummarizationDelete;
import com.bsi.peaks.service.services.spectralLibrary.SlSearchDelete;
import com.bsi.peaks.service.services.spectralLibrary.dda.DdaSlSearchDelete;
import com.bsi.peaks.service.services.spider.SpiderDelete;
import com.bsi.peaks.service.services.spider.SpiderSummarizeDelete;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

@Singleton
public class EventSourceTaskDeleter implements TaskDeleter {
    private final ApplicationStorageFactory storageFactory;

    @Inject
    public EventSourceTaskDeleter(ApplicationStorageFactory storageFactory) {
        this.storageFactory = storageFactory;
    }


    @Override
    public CompletionStage<Done> deleteTask(String keyspace, StepType stepType, UUID taskId, LevelTask level) {
        ApplicationStorage appStorage = storageFactory.getStorage(keyspace);

        final DeleteTaskData<DataLoadingTask> dataLoadingDelete = (id, taskLevel) -> {
            final UUID fractionId = Iterables.getOnlyElement(taskLevel.fractionIds());
            return appStorage.getScanRepository().delete(fractionId, id)
                .thenCompose(done -> appStorage.getFractionRepository().delete(fractionId, id));
        };

        try {
            switch (stepType) {
                case DATA_LOADING:
                    return dataLoadingDelete.delete(taskId, level);
                case DATA_REFINE:
                    return new DataRefineDelete(appStorage).delete(taskId, level);
                case FEATURE_DETECTION:
                    return new FeatureDetectionDelete(appStorage).delete(taskId, level);
                case DENOVO:
                    return new DenovoDelete(appStorage).delete(taskId, level);
                case DENOVO_FILTER:
                    return new DenovoSummarizeDelete(appStorage).delete(taskId, level);
                case FDB_PROTEIN_CANDIDATE_FROM_FASTA:
                    return new FDBProteinCandidateFromFastaDelete(appStorage).delete(taskId, level);
                case DB_TAG_SEARCH:
                    return new TagSearchDelete(appStorage).delete(taskId, level);
                case DB_TAG_SUMMARIZE:
                    return new TagSearchSummarizeDelete(appStorage).delete(taskId, level);
                case DB_PRE_SEARCH:
                    return new DBPreSearchDelete(appStorage).delete(taskId, level);
                case DB_BATCH_SEARCH:
                    return new DbSearchDelete(appStorage).delete(taskId, level);
                case DB_SUMMARIZE:
                    return new DbSearchSummarizeDelete(appStorage).delete(taskId, level);
                case DB_FILTER_SUMMARIZATION:
                case PTM_FINDER_FILTER_SUMMARIZATION:
                case SPIDER_FILTER_SUMMARIZATION:
                    return new DbIdentificationSummaryFilterServiceDelete (appStorage).delete(taskId, level);
                case DB_DENOVO_ONLY_TAG_SEARCH:
                case PTM_FINDER_DENOVO_ONLY_TAG_SEARCH:
                case SPIDER_DENOVO_ONLY_TAG_SEARCH:
                    return new DeNovoOnlyTagSearchDelete(appStorage).delete(taskId, level);
                case PTM_FINDER_BATCH_SEARCH:
                    return new PtmFinderDelete(appStorage).delete(taskId, level);
                case PTM_FINDER_SUMMARIZE:
                    return new PtmFinderSummarizeDelete(appStorage).delete(taskId, level);
                case SPIDER_BATCH_SEARCH:
                    return new SpiderDelete(appStorage).delete(taskId, level);
                case SPIDER_SUMMARIZE:
                    return new SpiderSummarizeDelete(appStorage).delete(taskId, level);
                case LFQ_FEATURE_ALIGNMENT:
                    return new FeatureAlignmentDelete(appStorage).delete(taskId, level);
                case LFQ_RETENTION_TIME_ALIGNMENT:
                    return new RetentionTimeAlignmentDelete(appStorage).delete(taskId, level);
                case LFQ_SUMMARIZE:
                    return new LabelFreeSummarizeDelete(appStorage).delete(taskId, level);
                case LFQ_FILTER_SUMMARIZATION:
                case DIA_LFQ_FILTER_SUMMARIZATION:
                    return new LfqSummaryFilterServiceDelete(appStorage).delete(taskId, level);
                case PEPTIDE_LIBRARY_PREPARE:
                    return new PeptideLibraryPrepareDelete(appStorage).delete(taskId, level);
                case REPORTER_ION_Q_BATCH_CALCULATION:
                    return new ReporterIonQDelete(appStorage).delete(taskId, level);
                case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                    return new ReporterIonQSummarizeDelete(appStorage).delete(taskId, level);
                case REPORTER_ION_Q_NORMALIZATION:
                    return new ReporterIonQNormalizationDelete(appStorage).delete(taskId, level);
                case SL_SEARCH:
                    return new SlSearchDelete(appStorage).delete(taskId, level);
                case DIA_DB_PEPTIDE_SUMMARIZE:
                case SL_PEPTIDE_SUMMARIZATION:
                    return new SlPeptideSummarizationDelete(appStorage).delete(taskId, level);
                case DIA_DB_FILTER_SUMMARIZE:
                case SL_FILTER_SUMMARIZATION:
                    return new SlFilterSummarizationDelete(appStorage).delete(taskId, level);
                case SILAC_RT_ALIGNMENT:
                    return new SilacRtAlignmentDelete(appStorage).delete(taskId, level);
                case SILAC_FEATURE_ALIGNMENT:
                    return new SilacFeatureVectorSearhDelete(appStorage).delete(taskId, level);
                case SILAC_FEATURE_VECTOR_SEARCH:
                    return new SilacFeatureVectorSearhDelete(appStorage).delete(taskId, level);
                case SILAC_FILTER_SUMMARIZE:
                    return new SilacPeptideSummarizationDelete(appStorage).delete(taskId, level);
                case SILAC_PEPTIDE_SUMMARIZE:
                    return new SilacFilterSummarizationDelete(appStorage).delete(taskId, level);
                case DDA_SL_SEARCH:
                    return new DdaSlSearchDelete(appStorage).delete(taskId, level);
                case DIA_DB_SEARCH:
                    return new DiaDbSearchDelete(appStorage).delete(taskId, level);
                case DIA_DB_DATA_REFINE:
                case DIA_DATA_REFINE:
                    return new DiaDataRefineDelete(appStorage).delete(taskId, level);
                case DIA_DB_PRE_SEARCH:
                    return new DiaDbPreSearchDelete(appStorage).delete(taskId, level);
                case DIA_LFQ_RETENTION_TIME_ALIGNMENT:
                    return new DiaRententionTimeAlignmentDelete(appStorage).delete(taskId, level);
                case DIA_LFQ_FEATURE_EXTRACTION:
                    return new DiaFeatureExtractorDelete(appStorage).delete(taskId, level);
                case DIA_LFQ_SUMMARIZE:
                    return new DiaLfqSummarizeDelete(appStorage).delete(taskId, level);
                default:
                    throw new IllegalArgumentException("Unexpected StepType " + stepType);
            }
        } catch (Exception e) {
            return Futures.failedCompletionStage(e);
        }
    }

    private static TaskParameters paramatersWithTaskId(TaskDescription taskDescription, UUID taskId) {
        return taskDescription.getParameters().toBuilder()
            .setTaskId(taskId.toString())
            .build();
    }

    @Override
    public CompletionStage<Done> deleteProject(UUID projectId) {
        return storageFactory.getSystemStorage().getTaskPlanLookupRepository().delete(projectId);
    }
}
