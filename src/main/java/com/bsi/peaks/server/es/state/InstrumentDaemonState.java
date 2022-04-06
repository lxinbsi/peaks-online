package com.bsi.peaks.server.es.state;

import akka.japi.Pair;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonEvent;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonFileCopyStatusUpdated;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonUpdated;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.InstrumentDaemon;
import com.bsi.peaks.model.dto.Progress;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

@PeaksImmutable
@JsonDeserialize(builder = InstrumentDaemonStateBuilder.class)
public interface InstrumentDaemonState {
    static InstrumentDaemonStateBuilder builder() {
        return new InstrumentDaemonStateBuilder();
    }

    Logger LOG = LoggerFactory.getLogger(InstrumentDaemonStateBuilder.class);

    default PMap<String, InstrumentDaemon> nameToInstrumentDaemon() {
        return HashTreePMap.empty();
    }

    default PMap<UUID, Progress.State> fractionIdsToProgress() { return HashTreePMap.empty(); }

    default PMap<UUID, ImmutableList<UUID>> projectToFractionIds() { return HashTreePMap.empty(); }

    static InstrumentDaemonState getDefaultInstance() {
        return builder().nameToInstrumentDaemon(HashTreePMap.empty()).build();
    }

    InstrumentDaemonState withNameToInstrumentDaemon(PMap<String, InstrumentDaemon> value);

    InstrumentDaemonState withFractionIdsToProgress(PMap<UUID, Progress.State> value);

    InstrumentDaemonState withProjectToFractionIds(PMap<UUID, ImmutableList<UUID>> value);

    default InstrumentDaemonState update(InstrumentDaemonEvent event) {
        try {
            switch(event.getEventCase()) {
                case INSTRUMENTDAEMONUPDATED:
                    return withDaemonUpdated(event);
                case INSTRUMENTDAEMONDELETED:
                    return withDaemonDeleted(event);
                case INSTRUMENTDAEMONPINGED:
                    return withDaemonPinged(event);
                case INSTRUMENTDAEMONFILECOPYSTATUSUPDATED:
                    return withDaemonFileCopyStatusUpdated(event);
                default:
                    throw new IllegalArgumentException("Unexpected event " + event.getEventCase());
            }
        } catch (Exception e) {
            LOG.error("Unable to update instrument daemon manager state , event {}", event, e);
        }
        return this;
    }

    default InstrumentDaemonState withDaemonUpdated(InstrumentDaemonEvent event) {
        InstrumentDaemonUpdated instrumentDaemonUpdated = event.getInstrumentDaemonUpdated();
        String name = instrumentDaemonUpdated.getName();
        InstrumentDaemon instrumentDaemon = instrumentDaemonUpdated.getInstrumentDaemon();
        return plus(name, instrumentDaemon);
    }

    default InstrumentDaemonState withDaemonDeleted(InstrumentDaemonEvent event) {
        return minus(event.getInstrumentDaemonDeleted().getName());
    }

    default InstrumentDaemonState withDaemonPinged(InstrumentDaemonEvent event) {
        InstrumentDaemon instrumentDaemon = event.getInstrumentDaemonPinged().getInstrumentDaemon();
        InstrumentDaemon.Builder builder = InstrumentDaemon.newBuilder();
        String name = instrumentDaemon.getName();
        if (nameToInstrumentDaemon().containsKey(name)) {
            builder.mergeFrom(nameToInstrumentDaemon().get(name))
                .setLastTimeCommunicated(instrumentDaemon.getLastTimeCommunicated())
                .build();
            return plus(name, builder.build());
        } else {
            return plus(name, instrumentDaemon);
        }
    }

    default InstrumentDaemonState withDaemonFileCopyStatusUpdated(InstrumentDaemonEvent event) {
        InstrumentDaemonFileCopyStatusUpdated copyStatusUpdated = event.getInstrumentDaemonFileCopyStatusUpdated();

        UUID projectId = ModelConversion.uuidFrom(copyStatusUpdated.getProjectId());
        UUID fractionId = ModelConversion.uuidFrom(copyStatusUpdated.getFractionId());

        ImmutableList<UUID> fractionIds = projectToFractionIds().getOrDefault(projectId, ImmutableList.of());
        ImmutableList<UUID> newFractionIds = new ImmutableList.Builder<UUID>().addAll(fractionIds).add(fractionId).build();

        return withFractionIdsToProgress(fractionIdsToProgress().plus(fractionId, copyStatusUpdated.getCopyingProgress()))
            .withProjectToFractionIds(projectToFractionIds().plus(projectId, newFractionIds));
    }


    default InstrumentDaemonState plus(String name, InstrumentDaemon instrumentDaemon) {
        PMap<String, InstrumentDaemon> nameInstrumentDaemonPMap = nameToInstrumentDaemon().plus(name, instrumentDaemon);
        return withNameToInstrumentDaemon(nameInstrumentDaemonPMap);
    }

    default InstrumentDaemonState minus(String name) {
        PMap<String, InstrumentDaemon> nameInstrumentDaemonPMap = nameToInstrumentDaemon().minus(name);
        return withNameToInstrumentDaemon(nameInstrumentDaemonPMap);
    }
}
