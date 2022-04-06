package com.bsi.peaks.server.es.spectrallibrary;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.parameterModels.dto.MassParameter;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.ScanRepository;
import com.bsi.peaks.internal.task.SlSearchParameters;
import com.bsi.peaks.io.writer.dto.DtoConversion;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.core.ms.Scan;
import com.bsi.peaks.model.core.ms.ScanWithPeaks;
import com.bsi.peaks.model.core.ms.scan.PeaksData;
import com.bsi.peaks.model.core.ms.scan.PeaksStats;
import com.bsi.peaks.model.core.ms.scan.ScanAttributes;
import com.bsi.peaks.model.core.ms.scan.ScanAttributesBuilder;
import com.bsi.peaks.model.filter.SLPsmFilter;
import com.bsi.peaks.model.filter.SlPeptideFilter;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.model.proto.TimstofFrame;
import com.bsi.peaks.service.services.PsmRtAdjustable;
import com.bsi.peaks.service.services.libgenerator.AdjustPsmRetentionTimeFunction;
import com.bsi.peaks.service.services.libgenerator.AdjustableRtSlPsm;
import com.bsi.peaks.service.services.spectralLibrary.SpectralLibrarySearch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class TimsDiaFractionToProcessWithRtFunction extends DiaFractionToProcessWithRtFunction {
    private final static Logger LOG = LoggerFactory.getLogger(TimsDiaFractionToProcessWithRtFunction.class);

    public TimsDiaFractionToProcessWithRtFunction(String projectKeyspace, UUID fractionId, UUID dataLoadingTaskId, UUID dbSearchTaskId, SlSearchParameters slSearchParameters, SLPsmFilter slPsmFilter, SlPeptideFilter slPeptideFilter, List<Modification> modifications, int psmCount, AdjustPsmRetentionTimeFunction psmRtFunction) {
        super(projectKeyspace, fractionId, dataLoadingTaskId, dbSearchTaskId, slSearchParameters, slPsmFilter, slPeptideFilter, modifications, psmCount, psmRtFunction, true);
    }

    private CompletionStage<ScanWithPeaks> grabScan(ScanRepository scanRepository, List<Float> frameIonMobilities, UUID dataLoadingTaskId, PsmRtAdjustable psm, MassParameter tolerance) {
        return scanRepository.getFrameByLevel(psm.fractionId(), dataLoadingTaskId, 2, psm.apexScan())
            .thenApply(optTimsFrame -> {
                TimstofFrame timsFrame = optTimsFrame.get();
                Optional<float[][]> optPeaks = SpectralLibrarySearch.generatePeaksFromFrame(
                        frameIonMobilities,
                        timsFrame,
                        psm.featureMz(),
                        psm.ccsStart(),
                        psm.ccsEnd()
                );
                if (timsFrame.getFrameId() == psm.apexScan() && !optPeaks.isPresent()) {
                    // if the apex frame is filtered out this is an issue with our algorithm
                    LOG.warn("RT Apex frame was filtered out.");
                    ScanAttributes scanAttributes = new ScanAttributesBuilder()
                            .scanNum(psm.apexScan())
                            .retentionTime(Duration.ofNanos(timsFrame.getHeader().getRetentionTime()))
                            .activationMethod(com.bsi.peaks.model.core.ActivationMethod.from(timsFrame.getActivationMethod()))
                            .msLevel(2)
                            .build();
                    return Scan.from(psm.fractionId(), scanAttributes, PeaksStats.generate(PeaksData.EMPTY))
                            .withPeaksData(PeaksData.EMPTY);
                }
                return optPeaks.map(peaks -> DtoConversion.slTimsCombinedScan(psm.fractionId(), peaks[0], peaks[1], timsFrame))
                        .orElseThrow(() -> new IllegalStateException("Unable to generate spectral library"));
            });
    }

    @Override
    public Source<PsmForScanQuerying, NotUsed> createFilteredPsmRtAdjustableSource(ApplicationStorageFactory storageFactory) {
        Map<String, com.bsi.peaks.model.proteomics.Modification> modificationMap = new HashMap<>();
        for (Modification m: modifications) {
            modificationMap.put(m.getName(), com.bsi.peaks.model.proteomics.Modification.from(m));
        }
        final ApplicationStorage storage = storageFactory.getStorage(projectKeyspace);
        CompletionStage<Pair<List<Float>, MassParameter>> ionMobilitiesMassParamFuture = storage.getFractionRepository().getAttributesById(fractionId())
            .thenApply(attributes -> attributes.orElseThrow(() -> new IllegalStateException("Unable to find fraction " + fractionId()))
                .frameIonMobilities()
            )
            .thenCombine(getMassParameter(storage), Pair::create);
        return Source.fromCompletionStage(ionMobilitiesMassParamFuture)
            .flatMapConcat(ionMobilitiesMassParamPair -> {
                final List<Float> frameIonMobilities = ionMobilitiesMassParamPair.first();
                final MassParameter massParameter = ionMobilitiesMassParamPair.second();
                return createSlPsmSource(storage)
                    .map(psm -> {
                        AdjustableRtSlPsm adjustableRtSlPsm = new AdjustableRtSlPsm(psm, fractionId, modificationMap);
                        MassParameter daltonParam = convertMassParmaToDalton(massParameter, psm);
                        return new PsmForScanQueryingBuilder()
                            .peptide(psm.getSequence().getSequence())
                            .psm(adjustableRtSlPsm)
                            .massParameter(daltonParam)
                            .queryScan(grabScan(storage.getScanRepository(), frameIonMobilities, dataLoadingTaskId, adjustableRtSlPsm, massParameter))
                            .build();
                    })
                    .buffer(64, OverflowStrategy.backpressure());
                });
    }
}
