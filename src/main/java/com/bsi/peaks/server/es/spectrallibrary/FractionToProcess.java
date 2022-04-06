package com.bsi.peaks.server.es.spectrallibrary;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.service.services.libgenerator.AdjustPsmRetentionTimeFunction;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface FractionToProcess {
    FractionToProcessWithRtFunction withFractionToProcessWithRtFunction(int psmCount, AdjustPsmRetentionTimeFunction psmRtFunction);
    UUID fractionId();
    String projectKeyspace();
    UUID dataLoadingTaskId();
    Source<Map<String, List<Duration>>, NotUsed> createFractionToProcessWithRtFunctionSource(ApplicationStorageFactory storageFactory);
}
