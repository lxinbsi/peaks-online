package com.bsi.peaks.server.archiver;

import akka.Done;
import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.model.core.AcquisitionMethod;
import com.bsi.peaks.model.core.ms.*;
import com.bsi.peaks.model.core.ms.Fraction;
import com.bsi.peaks.model.core.ms.scan.*;
import com.bsi.peaks.model.dto.ProjectTaskListing;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.proteomics.Enzyme;
import com.bsi.peaks.model.proteomics.Instrument;
import com.bsi.peaks.model.proto.*;
import com.bsi.peaks.server.cluster.Loader;
import com.bsi.peaks.service.common.ArchiveHelpers;
import com.bsi.peaks.service.services.TaskArchiver;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class DataLoadingArchiver extends TaskArchiver {
    private final boolean discardPeaks;
    public DataLoadingArchiver(ApplicationStorage storage, boolean discardPeaks) {
        super(storage, StepType.DATA_LOADING);
        this.discardPeaks = discardPeaks;
    }

    @Override
    protected void serializeToStream(ProjectTaskListing task, OutputStream out) {
        UUID dataLoadingTaskId = UUID.fromString(task.getTaskId());
        UUID fractionId = UUID.fromString(task.getFractionId(0));

        // first write fraction, we need its information for deserialization
        Fraction fraction = storage.getFractionRepository().getById(fractionId)
            .toCompletableFuture().join().orElseThrow(NoSuchFieldError::new);

        try {
            Fraction.compressFraction(fraction).writeDelimitedTo(out);
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize fraction", e);
        }

        // then followed by scans
        if (fraction.fractionStats().getVersion() == 2 && fraction.hasIonMobility()) {
            // version 2 timstof data is saved as frames
            Source<TimstofFrame, NotUsed> frameSource = storage.getScanRepository()
                .createTimsFrameSourceByLevel(fractionId, dataLoadingTaskId, 1)
                .concat(storage.getScanRepository().createTimsFrameSourceByLevel(fractionId, dataLoadingTaskId, 2));
            if (discardPeaks) {
                frameSource = frameSource.map(frame -> frame.toBuilder()
                    .setPeaksMzs(ByteString.copyFrom(new byte[0]))
                    .setPeaksIntensities(ByteString.copyFrom(new byte[0]))
                    .build()
                );
            }

            frameSource.async()
                .map(frame -> DataLoadingArchivingData.newBuilder().setCompressedFrame(frame).build())
                .runForeach(frame -> frame.writeDelimitedTo(out), storage.getMaterializer())
                .toCompletableFuture().join();
        } else {
            // others are saved as scans
            Source<ScanWithPeaks, NotUsed> scanSource;
            if (discardPeaks) {
                scanSource = storage.getScanRepository().createSourceNoPeaks(fractionId, dataLoadingTaskId)
                    .map(scan -> scan.withPeaksData(PeaksData.EMPTY));
            } else {
                scanSource = storage.getScanRepository().createSource(fractionId, dataLoadingTaskId);
            }

            scanSource.async()
                .map(scan -> DataLoadingArchivingData.newBuilder()
                    .setCompressedScan(scan.proto(CompressedScan.newBuilder(), 1, discardPeaks))
                    .build()
                )
                .runForeach(scan -> scan.writeDelimitedTo(out), storage.getMaterializer())
                .toCompletableFuture().join();
        }
    }

    @Override
    protected void deserializeFromStream(ProjectTaskListing task, InputStream in) {
        UUID dataLoadingTaskId = UUID.fromString(task.getTaskId());
        UUID fractionId = UUID.fromString(task.getFractionId(0));

        final CompressedFraction fraction;
        try {
            fraction = CompressedFraction.parseDelimitedFrom(in);
            storage.getFractionRepository().create(fractionId, Fraction.decompressFraction(fractionId, fraction))
                .toCompletableFuture().join();
        } catch (Exception e) {
            throw new RuntimeException("Unable to deserialize fraction", e);
        }

        Sink<DataLoadingArchivingData, CompletionStage<Done>> scanSink = Flow.<DataLoadingArchivingData>create()
            .async()
            .map(data -> Loader.adaptDDAScan(ScanWithPeaks.from(fractionId, data.getCompressedScan(), 1)))
            .async()
            .toMat(storage.getScanRepository().createSink(dataLoadingTaskId, fraction.getHasIonMobility()), Keep.right());

        Sink<DataLoadingArchivingData, CompletionStage<Done>> frameSink = Flow.<DataLoadingArchivingData>create()
            .async()
            .map(DataLoadingArchivingData::getCompressedFrame)
            .toMat(storage.getScanRepository().createTimsFrameSink(fractionId, dataLoadingTaskId), Keep.right());

        ArchiveHelpers.createProtoSource(in, DataLoadingArchivingData::parseDelimitedFrom)
            .divertToMat(frameSink, DataLoadingArchivingData::hasCompressedFrame, Keep.right())
            .divertToMat(scanSink, DataLoadingArchivingData::hasCompressedScan, (a, b) -> a.thenCompose(ignore -> b))
            .toMat(Sink.ignore(), (a, b) -> a.thenCompose(ignore -> b))
            .run(storage.getMaterializer())
            .toCompletableFuture().join();
    }


}
