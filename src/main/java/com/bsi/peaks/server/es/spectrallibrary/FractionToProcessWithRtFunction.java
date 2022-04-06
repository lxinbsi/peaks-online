package com.bsi.peaks.server.es.spectrallibrary;

import akka.NotUsed;
import akka.japi.tuple.Tuple5;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.parameterModels.dto.MassParameter;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.ScanRepository;
import com.bsi.peaks.model.PeaksImmutable2;
import com.bsi.peaks.model.core.ms.ScanWithPeaks;
import com.bsi.peaks.service.services.PsmRtAdjustable;
import com.bsi.peaks.service.services.libgenerator.AdjustPsmRetentionTimeFunction;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface FractionToProcessWithRtFunction extends FractionToProcess {
    Source<PsmForScanQuerying, NotUsed> createFilteredPsmRtAdjustableSource(ApplicationStorageFactory storageFactory);
    AdjustPsmRetentionTimeFunction adjustPsmRetentionTimeFunction();
    int psmCount();

    @PeaksImmutable2
    interface PsmForScanQuerying {
        String peptide();
        MassParameter massParameter();
        PsmRtAdjustable psm();
        CompletionStage<ScanWithPeaks> queryScan();
    }
}
