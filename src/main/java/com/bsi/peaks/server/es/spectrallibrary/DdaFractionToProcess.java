package com.bsi.peaks.server.es.spectrallibrary;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.PSMRepository;
import com.bsi.peaks.model.filter.PeptideFilter;
import com.bsi.peaks.model.filter.PsmFilter;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.service.services.libgenerator.AdjustPsmRetentionTimeFunction;

import java.time.Duration;
import java.util.*;

public class DdaFractionToProcess implements FractionToProcess{
    public final String projectKeyspace;
    public final UUID fractionId;
    public final UUID dataLoadingTaskId;
    public final UUID dbSearchTaskId;
    public final DBSearchParameters dbSearchParameters;
    public final PsmFilter psmFilter;
    public final PeptideFilter peptideFilter;

    public DdaFractionToProcess(
            String projectKeyspace,
            UUID fractionId,
            UUID dataLoadingTaskId,
            UUID dbSearchTaskId,
            DBSearchParameters dbSearchParameters,
            PsmFilter psmFilter,
            PeptideFilter peptideFilter
    ) {
        this.projectKeyspace = projectKeyspace;
        this.fractionId = fractionId;
        this.dataLoadingTaskId = dataLoadingTaskId;
        this.dbSearchTaskId = dbSearchTaskId;
        this.dbSearchParameters = dbSearchParameters;
        this.psmFilter = psmFilter;
        this.peptideFilter = peptideFilter;
    }

    @Override
    public FractionToProcessWithRtFunction withFractionToProcessWithRtFunction(int psmCount, AdjustPsmRetentionTimeFunction psmRtFunction) {
        return new DdaFractionToProcessWithRtFunction(projectKeyspace, fractionId, dataLoadingTaskId, dbSearchTaskId, dbSearchParameters, psmFilter, peptideFilter, psmCount, psmRtFunction);
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
        PSMRepository psmRepository = storageFactory.getStorage(projectKeyspace).getPSMRepository();
        return psmRepository.createRtSource(fractionId, dbSearchTaskId, psmFilter)
                .fold(new HashMap<>(), (l, psm) -> {
                    l.computeIfAbsent(psm.first(), arg -> new ArrayList<>()).add(psm.second());
                    return l;
                });
    }
}
