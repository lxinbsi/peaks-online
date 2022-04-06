package com.bsi.peaks.server.service;

import akka.Done;
import akka.NotUsed;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.InstrumentDaemonCommandFactory;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonResponse;
import com.bsi.peaks.model.dto.InstrumentDaemon;
import com.bsi.peaks.model.dto.InstrumentDaemonSampleInfo;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.server.es.communication.InstrumentDaemonCommunication;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class InstrumentDaemonService {
    private static final Logger LOG = LoggerFactory.getLogger(InstrumentDaemonService.class);
    private final InstrumentDaemonCommunication instrumentDaemonCommunication;

    @Inject
    public InstrumentDaemonService(
        final InstrumentDaemonCommunication instrumentDaemonCommunication
    ) {
        this.instrumentDaemonCommunication = instrumentDaemonCommunication;
    }

    public Source<InstrumentDaemon, NotUsed> queryAllDaemon(UUID actingAsUserId) {
        InstrumentDaemonCommandFactory commandFactory = InstrumentDaemonCommandFactory.actingAsUserId(actingAsUserId);
        return Source.fromSourceCompletionStage(instrumentDaemonCommunication.query(commandFactory.queryAllInstrumentDaemons())
            .thenApply(response -> Source.from(response.getInstrumentDaemons().getInstrumentDaemonList()))
        ).mapMaterializedValue(ignore -> NotUsed.getInstance());
    }

    public CompletionStage<InstrumentDaemonResponse.CopyProgresses> queryCopySample(UUID actingAsUserId, UUID projectId) {
        InstrumentDaemonCommandFactory commandFactory = InstrumentDaemonCommandFactory.actingAsUserId(actingAsUserId);
        return instrumentDaemonCommunication.query(commandFactory.querySampleCopyStatus(projectId))
            .thenApply(InstrumentDaemonResponse::getCopyProgresses);
    }

    public CompletionStage<Boolean> checkUploadPermission(UUID actingAsUserId, String name, String apiKey) {
        InstrumentDaemonCommandFactory commandFactory = InstrumentDaemonCommandFactory.actingAsUserId(actingAsUserId);
        return instrumentDaemonCommunication.query(commandFactory.checkUploadPermission(name, apiKey))
            .thenApply(InstrumentDaemonResponse::getHasPermission);
    }

    public CompletionStage<Done> updateInstrumentDaemonFileCopyStatus(
        UUID actingAsUserId,
        UUID projectId,
        UUID sampleId,
        UUID fractionId,
        Progress.State copyState
    ) {
        InstrumentDaemonCommandFactory commandFactory = InstrumentDaemonCommandFactory.actingAsUserId(actingAsUserId);
        return instrumentDaemonCommunication.command(commandFactory.updateInstrumentDaemonFileCopyStatus(projectId, sampleId, fractionId, copyState));
    }

    public CompletionStage<Done> delete(UUID actingAsUserId, String name) {
        InstrumentDaemonCommandFactory commandFactory = InstrumentDaemonCommandFactory.actingAsUserId(actingAsUserId);
        return instrumentDaemonCommunication.command(commandFactory.deleteInstrumentDaemon(name));
    }

    public CompletionStage<Done> update(UUID actingAsUserId, InstrumentDaemon instrumentDaemon) {
        InstrumentDaemonCommandFactory commandFactory = InstrumentDaemonCommandFactory.actingAsUserId(actingAsUserId);
        return instrumentDaemonCommunication.command(commandFactory.updateInstrumentDaemon(instrumentDaemon));
    }

    public CompletionStage<Done> uploadDaemonSample(
        UUID actingAsUserId,
        InstrumentDaemonSampleInfo InstrumentDaemonSampleInfo,
        UUID projectId,
        List<UUID> sampleIds,
        List<UUID> fractionIds
    ) {
        InstrumentDaemonCommandFactory commandFactory = InstrumentDaemonCommandFactory.actingAsUserId(actingAsUserId);
        return instrumentDaemonCommunication.command(commandFactory.instrumentDaemonUploadSample(InstrumentDaemonSampleInfo, projectId, sampleIds, fractionIds));
    }
}
