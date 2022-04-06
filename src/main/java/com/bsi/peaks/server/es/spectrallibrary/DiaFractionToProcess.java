package com.bsi.peaks.server.es.spectrallibrary;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.SLPSMRepository;
import com.bsi.peaks.internal.task.SlSearchParameters;
import com.bsi.peaks.model.filter.SLPsmFilter;
import com.bsi.peaks.model.filter.SlPeptideFilter;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.service.services.libgenerator.AdjustPsmRetentionTimeFunction;

import java.time.Duration;
import java.util.*;

public class DiaFractionToProcess implements FractionToProcess {
    public final String projectKeyspace;
    public final UUID fractionId;
    public final UUID dataLoadingTaskId;
    public final UUID dbSearchTaskId;
    public final SlSearchParameters slSearchParameters;
    public final SLPsmFilter slPsmFilter;
    public final SlPeptideFilter slPeptideFilter;
    public final List<Modification> modifications;
    protected final boolean isTims;

    public DiaFractionToProcess(
            String projectKeyspace,
            UUID fractionId,
            UUID dataLoadingTaskId,
            UUID dbSearchTaskId,
            SlSearchParameters slSearchParameters,
            SLPsmFilter slPsmFilter,
            SlPeptideFilter slPeptideFilter,
            List<Modification> modifications,
            boolean isTims
    ) {
        this.projectKeyspace = projectKeyspace;
        this.fractionId = fractionId;
        this.dataLoadingTaskId = dataLoadingTaskId;
        this.dbSearchTaskId = dbSearchTaskId;
        this.slSearchParameters = slSearchParameters;
        this.slPsmFilter = slPsmFilter;
        this.slPeptideFilter = slPeptideFilter;
        this.modifications = modifications;
        this.isTims = isTims;
    }

    @Override
    public FractionToProcessWithRtFunction withFractionToProcessWithRtFunction(int psmCount, AdjustPsmRetentionTimeFunction psmRtFunction) {
        if (isTims) {
            return new TimsDiaFractionToProcessWithRtFunction(projectKeyspace, fractionId, dataLoadingTaskId, dbSearchTaskId, slSearchParameters, slPsmFilter, slPeptideFilter, modifications, psmCount, psmRtFunction);
        } else {
            return new DiaFractionToProcessWithRtFunction(projectKeyspace, fractionId, dataLoadingTaskId, dbSearchTaskId, slSearchParameters, slPsmFilter, slPeptideFilter, modifications, psmCount, psmRtFunction);
        }
    }

    @Override
    public UUID fractionId() {
        return fractionId;
    }

    @Override
    public String projectKeyspace() {
        return projectKeyspace;
    }

    @Override
    public UUID dataLoadingTaskId() {
        return dataLoadingTaskId;
    }

    @Override
    public Source<Map<String, List<Duration>>, NotUsed> createFractionToProcessWithRtFunctionSource(ApplicationStorageFactory storageFactory) {
        SLPSMRepository slPsmRepository = storageFactory.getStorage(projectKeyspace).getSLPSMRespository();
        //TODO This only needs the Retention Times, and needs to be optimized using Cassandra queries
        return slPsmRepository.source(fractionId, dbSearchTaskId, slPsmFilter)
                .map(psm -> Pair.create(psm.getSequence().getSequence(), Duration.ofMillis(psm.getData().getRetentionTimeMillisecond())))
                .fold(new HashMap<>(), (l, psm) -> {
                    l.computeIfAbsent(psm.first(), arg -> new ArrayList<>()).add(psm.second());
                    return l;
                });
    }
}
