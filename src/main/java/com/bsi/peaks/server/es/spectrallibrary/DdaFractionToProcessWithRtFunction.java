package com.bsi.peaks.server.es.spectrallibrary;

import akka.NotUsed;
import akka.japi.Pair;
import akka.japi.tuple.Tuple5;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.parameterModels.dto.MassParameter;
import com.bsi.peaks.analysis.parameterModels.dto.MassUnit;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.PSMRepository;
import com.bsi.peaks.data.utils.StreamHelper;
import com.bsi.peaks.model.filter.PeptideFilter;
import com.bsi.peaks.model.filter.PsmFilter;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.service.services.PsmRtAdjustable;
import com.bsi.peaks.service.services.libgenerator.AdjustPsmRetentionTimeFunction;
import com.bsi.peaks.service.services.libgenerator.AdjustableRtPsm;

import java.util.UUID;

public class DdaFractionToProcessWithRtFunction extends DdaFractionToProcess implements FractionToProcessWithRtFunction {
    public final AdjustPsmRetentionTimeFunction adjustPsmRetentionTimeFunction;
    public final int psmCount;

    public DdaFractionToProcessWithRtFunction(
            String projectKeyspace,
            UUID fractionId,
            UUID dataLoadingTaskId,
            UUID dbSearchTaskId,
            DBSearchParameters dbSearchParameters,
            PsmFilter psmFilter,
            PeptideFilter peptideFilter,
            int psmCount,
            AdjustPsmRetentionTimeFunction psmRtFunction
    ) {
        super(projectKeyspace, fractionId, dataLoadingTaskId, dbSearchTaskId, dbSearchParameters, psmFilter, peptideFilter);
        this.psmCount = psmCount;
        this.adjustPsmRetentionTimeFunction = psmRtFunction;
    }

    @Override
    public Source<PsmForScanQuerying, NotUsed> createFilteredPsmRtAdjustableSource(ApplicationStorageFactory storageFactory) {
        final MassParameter massParameter = MassParameter.newBuilder()
            .setParam(dbSearchParameters.fragmentTol())
            .setUnit(dbSearchParameters.isPPMForFragmentTol() ? MassUnit.PPM : MassUnit.DA)
            .build();
        final ApplicationStorage storage = storageFactory.getStorage(projectKeyspace);
        return storageFactory.getStorage(projectKeyspace).getPSMRepository()
                .createSourceByPeptide(fractionId, dbSearchTaskId, psmFilter)
                .via(StreamHelper.splitAndGroupBy(PSMRepository.PeptidePsm::peptide))
                .filter(peptidePsms -> {
                    if (peptideFilter.minPValue() != 0) {
                        return peptideFilter.predicate(new PeptideFilter.PeptideFilterable() {
                            @Override
                            public float pValue() {
                                return peptidePsms.second().stream().map(p -> p.psm().pValue()).max(Float::compareTo)
                                        .orElseThrow(() -> new IllegalStateException("No psm for peptide " + peptidePsms.first()));
                            }
                        });
                    }
                    return true; //no peptide filter
                })
                .map(Pair::second)
                .mapConcat(p -> p)
                .map(peptidePsm -> {
                    AdjustableRtPsm psm = new AdjustableRtPsm(peptidePsm.psm());
                    return new PsmForScanQueryingBuilder()
                        .peptide(peptidePsm.peptide())
                        .psm(psm)
                        .massParameter(massParameter)
                        .queryScan(storage.getScanRepository().get(psm.fractionId(), dataLoadingTaskId, psm.apexScan()))
                        .build();
                })
                .buffer(64, OverflowStrategy.backpressure());
    }

    @Override
    public AdjustPsmRetentionTimeFunction adjustPsmRetentionTimeFunction() {
        return adjustPsmRetentionTimeFunction;
    }

    @Override
    public int psmCount() {
        return psmCount;
    }
}
