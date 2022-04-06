package com.bsi.peaks.server.cluster;

import akka.japi.Pair;
import com.bsi.peaks.algorithm.module.peaksdb.decoy.DecoyInfo;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.cache.FractionCache;
import com.bsi.peaks.event.tasks.TaskParameters;
import com.bsi.peaks.internal.task.SilacFeatureAlignmentTaskParameters;
import com.bsi.peaks.internal.task.SilacRTAlignmentTaskParameters;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.proto.FastaSequenceDatabaseSource;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.service.common.DecoyInfoWrapper;
import com.bsi.peaks.service.messages.tasks.*;
import com.bsi.peaks.service.services.dbsearch.*;
import com.bsi.peaks.service.services.dbsearch.DIA.*;
import com.bsi.peaks.service.services.denovo.DenovoService;
import com.bsi.peaks.service.services.denovo.DenovoServiceResourceCalculator;
import com.bsi.peaks.service.services.denovo.DenovoSummarizeService;
import com.bsi.peaks.service.services.denovo.DenovoSummarizeServiceResourceCalculator;
import com.bsi.peaks.service.services.featuredetection.FeatureDetectionService;
import com.bsi.peaks.service.services.featuredetection.FeatureDetectionServiceResourceCalculator;
import com.bsi.peaks.service.services.labelfreequantification.*;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaFeatureExtractorResourceCalculator;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaFeatureExtractorService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaLfqSummarizeResourceCalculator;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaLfqSummarizeService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaRetentionTimeAlignmentService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaRetentionTimeResourceCalculator;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.*;
import com.bsi.peaks.service.services.preprocess.DataRefineService;
import com.bsi.peaks.service.services.preprocess.DataRefineServiceResourceCalculator;
import com.bsi.peaks.service.services.preprocess.dia.DiaDataRefineResourceCalculator;
import com.bsi.peaks.service.services.preprocess.dia.DiaDataRefineService;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderService;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderServiceResourceCalculator;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderSummarizeService;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderSummarizeServiceResourceCalculator;
import com.bsi.peaks.service.services.silac.*;
import com.bsi.peaks.service.services.spectralLibrary.*;
import com.bsi.peaks.service.services.spectralLibrary.dda.DdaSlSearchService;
import com.bsi.peaks.service.services.spectralLibrary.dda.DdaSlSearchServiceResourceCalculator;
import com.bsi.peaks.service.services.spider.SpiderService;
import com.bsi.peaks.service.services.spider.SpiderServiceResourceCalculator;
import com.bsi.peaks.service.services.spider.SpiderSummarizeService;
import com.bsi.peaks.service.services.spider.SpiderSummarizeServiceResourceCalculator;
import com.github.racc.tscg.TypesafeConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import javafx.concurrent.Task;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;


@Singleton
public class PeaksTaskResourceCalculator {
    private final ApplicationStorageFactory storageFactory;
    private final FractionCache fractionCache;
    private final LoadingCache<Pair<String,UUID>, Integer> peptideCountCache;
    private final LoadingCache<Pair<String,UUID>, Integer> psmCountCache;
    private final FastaDatabaseService fastaDatabaseService;
    private final int diaDbSearchThreads;

    @Inject
    public PeaksTaskResourceCalculator(
        @TypesafeConfig("peaks.server.resource-override.dia-db-search-threads") int diaDbSearchThreads,
        ApplicationStorageFactory storageFactory,
        FractionCache fractionCache,
        FastaDatabaseService fastaDatabaseService) {
        this.diaDbSearchThreads = diaDbSearchThreads;
        this.storageFactory = storageFactory;
        this.fractionCache = fractionCache;
        this.fastaDatabaseService = fastaDatabaseService;
        peptideCountCache = CacheBuilder.newBuilder()
            .softValues()
            .maximumSize(100)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(CacheLoader.from(this::getPeptideCountFromDecoyInfo));
        psmCountCache = CacheBuilder.newBuilder()
            .softValues()
            .maximumSize(100)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(CacheLoader.from(this::getPSMCountFromDecoyInfo));
    }

    private int getPSMCountFromDecoyInfo (Pair<String,UUID> keyspaceUUID) {
        try {
            ApplicationStorage storage = storageFactory.getStorage(keyspaceUUID.first());
            final CompletionStage<Optional<DecoyInfo>> decoyInfoFromDecoyInfo = storage.getPsmDecoyInfoRepository().load(keyspaceUUID.second());
            CompletionStage<Optional<DecoyInfo>> decoyInfoFromTaskOutput = storage
                .getTaskOutputRepository()
                .load(keyspaceUUID.second(), com.bsi.peaks.model.proto.DecoyInfo::parseFrom)
                .thenApply(o -> o.map(DecoyInfoWrapper::decoyInfoFromProto));

            DecoyInfo decoyInfo = decoyInfoFromDecoyInfo.thenCompose(o -> {
                if (o.isPresent()) return CompletableFuture.completedFuture(o);
                return decoyInfoFromTaskOutput;
            })
                .thenApply(optional -> optional.orElseThrow(NoSuchElementException::new))
                .toCompletableFuture().join();

            return decoyInfo.getForwardScore().length;
        } catch (Exception e) {
            return 0;
        }

    }


    private int getPeptideCountFromDecoyInfo (Pair<String,UUID> keyspaceUUID) {
        try {
            ApplicationStorage storage = storageFactory.getStorage(keyspaceUUID.first());
            DecoyInfo decoyInfo = storage.getPeptideDecoyInfoRepository().load(keyspaceUUID.second()).thenApply(optional -> optional.orElseThrow(NoSuchElementException::new))
                .toCompletableFuture().join();
            return decoyInfo.getForwardScore().length;
        } catch (Exception e) {
            return 0;
        }
    }

    public <T> int calculateGPUs(String service, String keyspace, T task) {
        ApplicationStorage storage = storageFactory.getStorage(keyspace);

        if (service.equals(DiaDbPreSearchService.NAME)) {
            FastaSequenceDatabaseSource targetDb = ((TaskParameters)task).getDiaPreSearchTaskParameters().getTargetDatabaseSourcesList().get(0);
            UUID dbId = ModelConversion.uuidFrom(targetDb.getDatabaseId());
            CompletionStage<Double> doubleCompletionStage = fastaDatabaseService.queryFastaDatabaseTaxonomyCount(UUID.fromString(ADMIN_USERID), dbId, targetDb.getTaxonomyIdsList());
            return new DiaDbPreSearchResourceCalculator(storage, fractionCache, doubleCompletionStage.toCompletableFuture().join()).calculateGPUAmount((TaskParameters) task);
        } else {
            return 0;
        }
    }

    public <T> int calculateCPUThreads(String service, String keyspace, T task, int originalThreads) {
        ApplicationStorage storage = storageFactory.getStorage(keyspace);
        switch (service) {
            case DataRefineService.NAME:
                return new DataRefineServiceResourceCalculator(storage).calculateCPUThreads((DataRefineTask) task, originalThreads);
            case FeatureDetectionService.NAME:
                return new FeatureDetectionServiceResourceCalculator(storage, fractionCache).calculateCPUThreads((FeatureDetectionTask) task, originalThreads);
            case DenovoService.NAME:
                return new DenovoServiceResourceCalculator(storage).calculateCPUThreads((DenovoTask) task, originalThreads);
            case DenovoSummarizeService.NAME:
                return new DenovoSummarizeServiceResourceCalculator(storage).calculateCPUThreads((TaskParameters) task, originalThreads);
            case FDBProteinCandidateFromFastaService.NAME:
                return new FDBProteinCandidateFromFastaServiceResourceCalculator(storage).calculateCPUThreads((TaskParameters) task, originalThreads);
            case TagSearchService.NAME:
                return new TagSearchServiceResourceCalculator(storage).calculateCPUThreads((TagSearchTask) task, originalThreads);
            case TagSearchSummarizeService.NAME:
                return new TagSearchSummarizeServiceResourceCalculator(storage).calculateCPUThreads((TagSearchSummarizeTask) task, originalThreads);
            case DBPreSearchService.NAME:
                return new DBPreSearchServiceResourceCalculator(storage,fractionCache).calculateCPUThreads((DBPreSearchTask) task, originalThreads);
            case DBSearchService.NAME:
                return new DBSearchServiceResourceCalculator(storage).calculateCPUThreads((DBSearchTask) task, originalThreads);
            case DBSearchSummarizeService.NAME:
                return new DBSearchSummarizeServiceResourceCalculator(storage, fractionCache).calculateCPUThreads((DBSearchSummarizeTask) task, originalThreads);
            case DbIdentificationSummaryFilterService.NAME:
                return new DbIdentificationSummaryFilterServiceResourceCalculator(storage).calculateCPUThreads((TaskParameters) task, originalThreads);
            case DeNovoOnlyTagSearchService.NAME:
                return new DeNovoOnlyTagSearchServiceResourceCalculator(storage).calculateCPUThreads((TaskParameters) task, originalThreads);
            case PtmFinderService.NAME:
                return new PtmFinderServiceResourceCalculator(storage).calculateCPUThreads((TaskParameters) task, originalThreads);
            case PtmFinderSummarizeService.NAME:
                return new PtmFinderSummarizeServiceResourceCalculator(storage, fractionCache).calculateCPUThreads((TaskParameters) task, originalThreads);
            case SpiderService.NAME:
                return new SpiderServiceResourceCalculator(storage).calculateCPUThreads((SpiderTask) task, originalThreads);
            case SpiderSummarizeService.NAME:
                return new SpiderSummarizeServiceResourceCalculator(storage, fractionCache).calculateCPUThreads((SpiderSummarizeTask) task, originalThreads);
            case SlSearchService.NAME:
                return new SlSearchServiceResourceCalculator(storage, fractionCache).calculateCPUThreads((TaskParameters) task, originalThreads);
            case DdaSlSearchService.NAME:
                return new DdaSlSearchServiceResourceCalculator(storage, fractionCache).calculateCPUThreads((TaskParameters) task, originalThreads);
            case SlPeptideSummarizationService.NAME:
                return new SlPeptideSummarizationServiceResourceCalculator(storage).calculateCPUThreads((TaskParameters) task, originalThreads);
            case SlFilterSummarizationService.NAME:
                return new SlFilterSummarizationServiceResourceCalculator(storage).calculateCPUThreads((TaskParameters) task, originalThreads);
            case DiaDataRefineService.NAME:
                return new DiaDataRefineResourceCalculator(storage, fractionCache).calculateCPUThreads((TaskParameters)task, originalThreads);
            case DiaDbSearchService.NAME:
                int calculatedThreads = new DiaDbSearchResourceCalculator(storage, fractionCache).calculateCPUThreads((TaskParameters)task, originalThreads);
                return Math.min(diaDbSearchThreads, calculatedThreads);
            case DiaDbPreSearchService.NAME:
                FastaSequenceDatabaseSource targetDb = ((TaskParameters)task).getDiaPreSearchTaskParameters().getTargetDatabaseSourcesList().get(0);
                UUID dbId = ModelConversion.uuidFrom(targetDb.getDatabaseId());
                CompletionStage<Double> doubleCompletionStage = fastaDatabaseService.queryFastaDatabaseTaxonomyCount(UUID.fromString(ADMIN_USERID), dbId, targetDb.getTaxonomyIdsList());
                return new DiaDbPreSearchResourceCalculator(storage, fractionCache, doubleCompletionStage.toCompletableFuture().join()).calculateCPUThreads((TaskParameters)task, originalThreads);
            default:
                return originalThreads;

        }
    }

    public <T> int calculateMemoryAmount(StepType stepType, String keyspace, T task, int threads) {
        ApplicationStorage storage = storageFactory.getStorage(keyspace);
        switch (stepType) {
            case DATA_REFINE:
                return new DataRefineServiceResourceCalculator(storage).calculateMemoryAmount((DataRefineTask)task, threads);
            case FEATURE_DETECTION:
                return new FeatureDetectionServiceResourceCalculator(storage, fractionCache).calculateMemoryAmount((FeatureDetectionTask)task, threads);
            case DENOVO:
                return new DenovoServiceResourceCalculator(storage).calculateMemoryAmount((DenovoTask) task, threads);
            case DENOVO_FILTER:
                return new DenovoSummarizeServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case FDB_PROTEIN_CANDIDATE_FROM_FASTA:
                return new FDBProteinCandidateFromFastaServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case DB_TAG_SEARCH:
                return new TagSearchServiceResourceCalculator(storage).calculateMemoryAmount((TagSearchTask)task, threads);
            case DB_TAG_SUMMARIZE:
                return new TagSearchSummarizeServiceResourceCalculator(storage).calculateMemoryAmount((TagSearchSummarizeTask)task, threads);
            case DB_PRE_SEARCH:
                return new DBPreSearchServiceResourceCalculator(storage,fractionCache).calculateMemoryAmount((DBPreSearchTask)task, threads);
            case DB_BATCH_SEARCH:
                return new DBSearchServiceResourceCalculator(storage).calculateMemoryAmount((DBSearchTask)task, threads);
            case DB_SUMMARIZE:
                return new DBSearchSummarizeServiceResourceCalculator(storage, fractionCache).calculateMemoryAmount((DBSearchSummarizeTask)task, threads);
            case DB_FILTER_SUMMARIZATION:
            case PTM_FINDER_FILTER_SUMMARIZATION:
            case SPIDER_FILTER_SUMMARIZATION:
                return new DbIdentificationSummaryFilterServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case DB_DENOVO_ONLY_TAG_SEARCH:
            case PTM_FINDER_DENOVO_ONLY_TAG_SEARCH:
            case SPIDER_DENOVO_ONLY_TAG_SEARCH:
                return new DeNovoOnlyTagSearchServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case PTM_FINDER_BATCH_SEARCH:
                return new PtmFinderServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case PTM_FINDER_SUMMARIZE:
                return new PtmFinderSummarizeServiceResourceCalculator(storage, fractionCache).calculateMemoryAmount((TaskParameters)task, threads);
            case SPIDER_BATCH_SEARCH:
                return new SpiderServiceResourceCalculator(storage).calculateMemoryAmount((SpiderTask)task, threads);
            case SPIDER_SUMMARIZE:
                return new SpiderSummarizeServiceResourceCalculator(storage, fractionCache).calculateMemoryAmount((SpiderSummarizeTask)task, threads);
            case LFQ_FEATURE_ALIGNMENT:
                int peptideCount;
                try {
                    peptideCount = peptideCountCache.get(Pair.create(keyspace,((FeatureAlignmentTask)task).idSummarizeTaskId()));
                } catch (Exception e) {
                    peptideCount = 0;
                }
                return new FeatureAlignmentServiceResourceCalculator(storage, fractionCache, peptideCount).calculateMemoryAmount((FeatureAlignmentTask)task, threads);
            case LFQ_RETENTION_TIME_ALIGNMENT:
                int psmCount;
                try {
                    psmCount = psmCountCache.get(Pair.create(keyspace,((RetentionTimeAlignmentTask)task).idSummarizeTaskId()));
                } catch (Exception e) {
                    psmCount = 0;
                }
                return new RetentionTimeAlignmentServiceResourceCalculator(storage, psmCount).calculateMemoryAmount((RetentionTimeAlignmentTask)task, threads);
            case LFQ_SUMMARIZE:
                return new LabelFreeSummarizeServiceResourceCalculator(storage).calculateMemoryAmount((LabelFreeSummarizeTask)task, threads);
            case LFQ_FILTER_SUMMARIZATION:
                return new LfqSummaryFilterServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case REPORTER_ION_Q_BATCH_CALCULATION:
                return new ReporterIonQServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                return new ReporterIonQSummarizeServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case REPORTER_ION_Q_NORMALIZATION:
                return new ReporterIonQNormalizationServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case SL_SEARCH:
                return new SlSearchServiceResourceCalculator(storage, fractionCache).calculateMemoryAmount((TaskParameters)task, threads);
            case SL_PEPTIDE_SUMMARIZATION:
            case DIA_DB_PEPTIDE_SUMMARIZE:
                return new SlPeptideSummarizationServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case SL_FILTER_SUMMARIZATION:
            case DIA_DB_FILTER_SUMMARIZE:
                return new SlFilterSummarizationServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case SILAC_RT_ALIGNMENT:
                int silacPsmCount;
                try {
                    SilacRTAlignmentTaskParameters silacTask = ((TaskParameters) task).getSilacRTAlignmentTaskParameters();
                    silacPsmCount = peptideCountCache.get(Pair.create(keyspace, ModelConversion.uuidFrom(silacTask.getIdSummarizeTaskId())));
                } catch (Exception e) {
                    silacPsmCount = 0;
                }
                return new SilacRtAlignmentResourceCalculator(storage, silacPsmCount).calculateMemoryAmount((TaskParameters)task, threads);
            case SILAC_FEATURE_ALIGNMENT:
                int silacPeptideCount;
                try {
                    SilacFeatureAlignmentTaskParameters silacTask = ((TaskParameters) task).getSilacFeatureAlignmentTaskParameters();
                    silacPeptideCount = peptideCountCache.get(Pair.create(keyspace, ModelConversion.uuidFrom(silacTask.getPeptideSummarizeTaskId())));
                } catch (Exception e) {
                    silacPeptideCount = 0;
                }
                return new SilacFeatureAlignmentResourceCalculator(storage, fractionCache, silacPeptideCount).calculateMemoryAmount((TaskParameters)task, threads);
            case SILAC_FEATURE_VECTOR_SEARCH:
                return new SilacFeatureVectorSearchServiceResourceCalculator(storage, fractionCache).calculateMemoryAmount((TaskParameters)task, threads);
            case SILAC_FILTER_SUMMARIZE:
                return new SilacFilterSummarizationServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case SILAC_PEPTIDE_SUMMARIZE:
                return new SilacPeptideSummarizationServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters)task, threads);
            case DIA_LFQ_FILTER_SUMMARIZATION:
                return new LfqSummaryFilterServiceResourceCalculator(storage).calculateMemoryAmount((TaskParameters) task, threads);
            case DIA_LFQ_SUMMARIZE:
                return new DiaLfqSummarizeResourceCalculator(storage).calculateMemoryAmount((TaskParameters) task, threads);
            case DIA_LFQ_FEATURE_EXTRACTION:
                return new DiaFeatureExtractorResourceCalculator(storage, fractionCache).calculateMemoryAmount((TaskParameters)task, threads);
            case DIA_LFQ_RETENTION_TIME_ALIGNMENT:
                return new DiaRetentionTimeResourceCalculator(storage).calculateMemoryAmount((TaskParameters) task, threads);
            case DIA_DB_SEARCH:
                return new DiaDbSearchResourceCalculator(storage,fractionCache).calculateMemoryAmount((TaskParameters) task, threads);
            case DIA_DB_PRE_SEARCH:
                TaskParameters taskParameters = (TaskParameters) task;
                FastaSequenceDatabaseSource targetDb = taskParameters.getDiaPreSearchTaskParameters().getTargetDatabaseSourcesList().get(0);
                UUID dbId = ModelConversion.uuidFrom(targetDb.getDatabaseId());
                CompletionStage<Double> doubleCompletionStage = fastaDatabaseService.queryFastaDatabaseTaxonomyCount(UUID.fromString(ADMIN_USERID), dbId, targetDb.getTaxonomyIdsList());
                return new DiaDbPreSearchResourceCalculator(storage, fractionCache, doubleCompletionStage.toCompletableFuture().join()).calculateMemoryAmount(taskParameters, threads);
            case DIA_DB_DATA_REFINE:
            case DIA_DATA_REFINE:
                return new DiaDataRefineResourceCalculator(storage, fractionCache).calculateMemoryAmount((TaskParameters)task, threads);
            default:
                return 2 * threads + 2;
        }
    }
}
