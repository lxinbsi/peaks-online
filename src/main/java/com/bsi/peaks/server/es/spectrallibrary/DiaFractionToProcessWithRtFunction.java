package com.bsi.peaks.server.es.spectrallibrary;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.parameterModels.dto.MassParameter;
import com.bsi.peaks.analysis.parameterModels.dto.MassUnit;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.utils.StreamHelper;
import com.bsi.peaks.internal.task.SlSearchParameters;
import com.bsi.peaks.model.filter.SLPsmFilter;
import com.bsi.peaks.model.filter.SlPeptideFilter;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.model.proto.SLPSM;
import com.bsi.peaks.service.services.libgenerator.AdjustPsmRetentionTimeFunction;
import com.bsi.peaks.service.services.libgenerator.AdjustableRtSlPsm;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class DiaFractionToProcessWithRtFunction extends DiaFractionToProcess implements FractionToProcessWithRtFunction {
    public final AdjustPsmRetentionTimeFunction adjustPsmRetentionTimeFunction;
    public final int psmCount;

    public DiaFractionToProcessWithRtFunction(
            String projectKeyspace,
            UUID fractionId,
            UUID dataLoadingTaskId,
            UUID dbSearchTaskId,
            SlSearchParameters slSearchParameters,
            SLPsmFilter slPsmFilter,
            SlPeptideFilter slPeptideFilter,
            List<Modification> modifications,
            int psmCount,
            AdjustPsmRetentionTimeFunction psmRtFunction
    ) {
        this(projectKeyspace, fractionId, dataLoadingTaskId, dbSearchTaskId, slSearchParameters, slPsmFilter, slPeptideFilter, modifications, psmCount, psmRtFunction, false);
    }

    protected DiaFractionToProcessWithRtFunction(
            String projectKeyspace,
            UUID fractionId,
            UUID dataLoadingTaskId,
            UUID dbSearchTaskId,
            SlSearchParameters slSearchParameters,
            SLPsmFilter slPsmFilter,
            SlPeptideFilter slPeptideFilter,
            List<Modification> modifications,
            int psmCount,
            AdjustPsmRetentionTimeFunction psmRtFunction,
            boolean isTims
    ) {
        super(projectKeyspace, fractionId, dataLoadingTaskId, dbSearchTaskId, slSearchParameters, slPsmFilter, slPeptideFilter, modifications, isTims);
        this.psmCount = psmCount;
        this.adjustPsmRetentionTimeFunction = psmRtFunction;
    }

    @Override
    public Source<PsmForScanQuerying, NotUsed>createFilteredPsmRtAdjustableSource(ApplicationStorageFactory storageFactory) {
        Map<String, com.bsi.peaks.model.proteomics.Modification> modificationMap = new HashMap<>();
        for (Modification m: modifications) {
            modificationMap.put(m.getName(), com.bsi.peaks.model.proteomics.Modification.from(m));
        }
        final ApplicationStorage storage = storageFactory.getStorage(projectKeyspace);
        return Source.fromCompletionStage(getMassParameter(storage))
            .flatMapConcat(massParameter -> createSlPsmSource(storage)
            .map(psm -> {
                AdjustableRtSlPsm adjustableRtSlPsm = new AdjustableRtSlPsm(psm, fractionId, modificationMap);
                return new PsmForScanQueryingBuilder()
                    .peptide(psm.getSequence().getSequence())
                    .psm(adjustableRtSlPsm)
                    .massParameter(convertMassParmaToDalton(massParameter, psm))
                    .queryScan(storage.getScanRepository().get(fractionId, dataLoadingTaskId, adjustableRtSlPsm.apexScan()))
                    .build();
            })
            .buffer(64, OverflowStrategy.backpressure())
        );
    }

    protected CompletionStage<MassParameter> getMassParameter(ApplicationStorage storage) {
        if (slSearchParameters.getSlSearchSharedParameters().getEnableToleranceOptimization()) {
            storage.getSlSearchRoundParametersRepository().load(dbSearchTaskId)
                    .thenApply(opt -> opt.map(slSearchRoundParameters -> slSearchRoundParameters.getFragmentTolerance())
                            .orElse(slSearchParameters.getSlSearchSharedParameters().getFragmentTolerance()))
                    .toCompletableFuture().join();
        }
        return CompletableFuture.completedFuture(slSearchParameters.getSlSearchSharedParameters().getFragmentTolerance());
    }

    @NotNull
    protected Source<SLPSM, NotUsed> createSlPsmSource(ApplicationStorage storage) {
        return storage.getSLPSMRespository()
                .sourceByPeptide(fractionId, dbSearchTaskId, slPsmFilter)
                .via(StreamHelper.splitAndGroupBy(p -> p.getSequence().getSequence()))
                .filter(peptidePsms -> {
                    if (slPeptideFilter.minPValue() != 0) {
                        return slPeptideFilter.predicate(new SlPeptideFilter.SlPeptideFilterable() {
                            @Override
                            public float pValue() {
                                return peptidePsms.second().stream()
                                        .map(p -> p.getData().getPValue())
                                        .max(Float::compareTo)
                                        .orElseThrow(() -> new IllegalStateException("No psm for peptide " + peptidePsms.first()));
                            }
                        });
                    }
                    return true; //no peptide filter
                })
                .map(Pair::second)
                .mapConcat(p -> p);
    }

    protected MassParameter convertMassParmaToDalton(MassParameter massParameter, com.bsi.peaks.model.proto.SLPSM psm) {
        switch (massParameter.getUnit()) {
            case DA:
                return massParameter;
            case PPM: {
                double ppm = massParameter.getParam();
                float featureMz = psm.getData().getFeatureMz();

                double daTol = featureMz * ppm / 1e6;
                MassParameter daltonParam = MassParameter.newBuilder()
                        .setParam(daTol)
                        .setUnit(MassUnit.DA)
                        .build();
                return daltonParam;
            }
            default:
                throw new IllegalStateException("Unrecognized unit " + massParameter.getUnit());
        }
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
