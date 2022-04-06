package com.bsi.peaks.server.dependency;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.client.ClusterClient;
import akka.cluster.client.ClusterClientSettings;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.cache.FractionCache;
import com.bsi.peaks.data.storage.application.cassandra.CassandraStorageFactory;
import com.bsi.peaks.server.Launcher;
import com.bsi.peaks.server.cluster.Master;
import com.bsi.peaks.server.cluster.PeaksTaskResourceCalculator;
import com.bsi.peaks.service.services.ServiceActor;
import com.bsi.peaks.service.services.dbsearch.DBPreSearchService;
import com.bsi.peaks.service.services.dbsearch.DBSearchService;
import com.bsi.peaks.service.services.dbsearch.DBSearchSummarizeService;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbPreSearchService;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbSearchService;
import com.bsi.peaks.service.services.dbsearch.DbIdentificationSummaryFilterService;
import com.bsi.peaks.service.services.dbsearch.DeNovoOnlyTagSearchService;
import com.bsi.peaks.service.services.dbsearch.FDBProteinCandidateFromFastaService;
import com.bsi.peaks.service.services.dbsearch.PeptideLibraryPrepareService;
import com.bsi.peaks.service.services.dbsearch.TagSearchService;
import com.bsi.peaks.service.services.dbsearch.TagSearchSummarizeService;
import com.bsi.peaks.service.services.denovo.DenovoService;
import com.bsi.peaks.service.services.denovo.DenovoSummarizeService;
import com.bsi.peaks.service.services.featuredetection.FeatureDetectionService;
import com.bsi.peaks.service.services.labelfreequantification.FeatureAlignmentService;
import com.bsi.peaks.service.services.labelfreequantification.LabelFreeSummarizeService;
import com.bsi.peaks.service.services.labelfreequantification.LfqSummaryFilterService;
import com.bsi.peaks.service.services.labelfreequantification.RetentionTimeAlignmentService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaFeatureExtractorService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaLfqSummarizeService;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaRetentionTimeAlignmentService;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQNormalizationService;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQService;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQSummarizeService;
import com.bsi.peaks.service.services.preprocess.DataRefineService;
import com.bsi.peaks.service.services.preprocess.dia.DiaDataRefineService;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderService;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderSummarizeService;
import com.bsi.peaks.service.services.silac.SilacFeatureAlignService;
import com.bsi.peaks.service.services.silac.SilacFeatureVectorSearchService;
import com.bsi.peaks.service.services.silac.SilacFilterSummarizationService;
import com.bsi.peaks.service.services.silac.SilacPeptideSummarizationService;
import com.bsi.peaks.service.services.silac.SilacRtAlignmentService;
import com.bsi.peaks.service.services.spectralLibrary.SlFilterSummarizationService;
import com.bsi.peaks.service.services.spectralLibrary.SlPeptideSummarizationService;
import com.bsi.peaks.service.services.spectralLibrary.SlSearchService;
import com.bsi.peaks.service.services.spectralLibrary.dda.DdaSlSearchService;
import com.bsi.peaks.service.services.spider.SpiderService;
import com.bsi.peaks.service.services.spider.SpiderSummarizeService;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * @author span
 *         Created by span on 6/6/2017.
 */
public class WorkerDependency extends AbstractModule {
    private final Config config;
    public final static Map<String, Class<? extends ServiceActor<?>>> SERVICE_ACTORS = new HashMap<>();
    static {
        SERVICE_ACTORS.put(DataRefineService.NAME, DataRefineService.class);
        SERVICE_ACTORS.put(FeatureDetectionService.NAME, FeatureDetectionService.class);
        //Denovo
        SERVICE_ACTORS.put(DenovoService.NAME, DenovoService.class);
        SERVICE_ACTORS.put(DenovoSummarizeService.NAME, DenovoSummarizeService.class);
        //Peptide library search
        SERVICE_ACTORS.put(PeptideLibraryPrepareService.NAME, PeptideLibraryPrepareService.class);
        //Db
        SERVICE_ACTORS.put(FDBProteinCandidateFromFastaService.NAME, FDBProteinCandidateFromFastaService.class);
        SERVICE_ACTORS.put(TagSearchService.NAME, TagSearchService.class);
        SERVICE_ACTORS.put(TagSearchSummarizeService.NAME, TagSearchSummarizeService.class);
        SERVICE_ACTORS.put(DBPreSearchService.NAME, DBPreSearchService.class);
        SERVICE_ACTORS.put(DBSearchService.NAME, DBSearchService.class);
        SERVICE_ACTORS.put(DBSearchSummarizeService.NAME, DBSearchSummarizeService.class);
        SERVICE_ACTORS.put(DbIdentificationSummaryFilterService.NAME, DbIdentificationSummaryFilterService.class);
        SERVICE_ACTORS.put(DeNovoOnlyTagSearchService.NAME, DeNovoOnlyTagSearchService.class);
        //Ptm
        SERVICE_ACTORS.put(PtmFinderService.NAME, PtmFinderService.class);
        SERVICE_ACTORS.put(PtmFinderSummarizeService.NAME, PtmFinderSummarizeService.class);
        //Spider
        SERVICE_ACTORS.put(SpiderService.NAME, SpiderService.class);
        SERVICE_ACTORS.put(SpiderSummarizeService.NAME, SpiderSummarizeService.class);
        //Spectral Library
        SERVICE_ACTORS.put(SlSearchService.NAME, SlSearchService.class);
        SERVICE_ACTORS.put(SlPeptideSummarizationService.NAME, SlPeptideSummarizationService.class);
        SERVICE_ACTORS.put(SlFilterSummarizationService.NAME, SlFilterSummarizationService.class);
        //DDA LIBRARY SEARCH
        SERVICE_ACTORS.put(DdaSlSearchService.NAME, DdaSlSearchService.class);
        //DIA DB SEARCH
        SERVICE_ACTORS.put(DiaDbSearchService.NAME, DiaDbSearchService.class);
        SERVICE_ACTORS.put(DiaDbPreSearchService.NAME, DiaDbPreSearchService.class);
        SERVICE_ACTORS.put(DiaDataRefineService.NAME, DiaDataRefineService.class);
    }

    public WorkerDependency(Config config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        bind(ApplicationStorageFactory.class).to(CassandraStorageFactory.class).asEagerSingleton();
        bind(PeaksTaskResourceCalculator.class);
        bind(FractionCache.class);
        ActorSystem system = ActorSystem.create("worker", config);
        bind(ActorSystem.class).toInstance(system);
        if (Launcher.launcherType == Launcher.TYPE.WORKER) {
            ActorRef clusterClient = system.actorOf(ClusterClient.props(ClusterClientSettings.create(system)), Master.CLUSTER_CLIENT);
            bind(ActorRef.class).annotatedWith(Names.named(Master.CLUSTER_CLIENT)).toInstance(clusterClient);
        }
    }
}
