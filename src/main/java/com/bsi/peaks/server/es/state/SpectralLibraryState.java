package com.bsi.peaks.server.es.state;

import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.spectralLibrary.SLEvent;
import com.bsi.peaks.event.spectralLibrary.SLMetaData;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.SLProgressState;
import com.bsi.peaks.server.es.PeaksCommandPersistentActor;
import com.bsi.peaks.server.es.spectrallibrary.SLInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.jetbrains.annotations.NotNull;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;

@PeaksImmutable
@JsonDeserialize(builder = SpectralLibraryStateBuilder.class)
public interface SpectralLibraryState {
    static SpectralLibraryStateBuilder builder() {
        return new SpectralLibraryStateBuilder();
    }

    Logger LOG = LoggerFactory.getLogger(PeaksCommandPersistentActor.class);

    default PMap<UUID, SLInfo> slIdName() {
        return HashTreePMap.empty();
    }

    SpectralLibraryState withSlIdName(PMap<UUID, SLInfo> value);

    static SpectralLibraryState getDefaultInstance() {
        return builder().slIdName(HashTreePMap.empty()).build();
    }

    default SpectralLibraryState update(SLEvent event) {
        try {
            switch(event.getEventCase()) {
                case CREATED:
                    return withNewSL(event);
                case PROCESS:
                    SLEvent.Process process = event.getProcess();
                    return withProcess(process);
                case NAMESET:
                    String newName = event.getNameSet().getNewName();
                    UUID nameSetSLId = uuidFrom(event.getNameSet().getSlId());
                    return withUpdateName(nameSetSLId, newName);
                case PROCESSINGSTATUS:
                    SLProgressState progressState = event.getProcessingStatus().getState();
                    UUID processingStatusSLId = uuidFrom(event.getProcessingStatus().getSlId());
                    switch (progressState) {
                        case DELETED:
                            return minus(processingStatusSLId);
                        default:
                            return withUpdateState(processingStatusSLId, progressState);
                    }
                case PERMISSIONGRANTED:
                    UUID newOwnerId = uuidFrom(event.getPermissionGranted().getNewOwnerId());
                    UUID permissionSLId = uuidFrom(event.getPermissionGranted().getSlId());
                    return withUpdateOwner(permissionSLId, newOwnerId);
                case ENTRYCOUNTSET:
                    UUID entryCountSetSlid = uuidFrom(event.getEntryCountSet().getSlId());
                    int count = event.getEntryCountSet().getCount();
                    return withUpdateEntryCount(entryCountSetSlid, count);

                default:
                    throw new IllegalArgumentException("Unexpected event " + event.getEventCase());
            }
        } catch (Exception e) {
            LOG.error("Unable to update SL state, event {}", event, e);
        }
        return this;
    }

    @NotNull
    default SpectralLibraryState withNewSL(SLEvent event) {
        SLEvent.Created eventCreated = event.getCreated();
        SLMetaData meta = eventCreated.getMeta();
        SLInfo newSLInfo = SLInfo.builder()
            .creationTime(CommonFactory.convert(event.getTimeStamp()))
            .entryNumber(0)
            .name(meta.getName())
            .ownerId(uuidFrom(meta.getOwnerId()))
            .slId(uuidFrom(eventCreated.getSlId()))
            .databaseNames(meta.getDatabaseList())
            .projectName(meta.getProjectName())
            .analysisName(meta.getAnalysisName())
            .state(SLProgressState.QUEUED)
            .metaJson(meta.getJson())
            .spectralLibraryType(meta.getType())
            .useRtAsIRt(meta.getUseRtAsIRt())
            .acquisitionMethod(eventCreated.getMeta().getAcquisitionMethod())
            .build();
        return plus(newSLInfo.slId(), newSLInfo);
    }

    default SpectralLibraryState withProcess(SLEvent.Process processEvent) {
        return this;
    }

    default SpectralLibraryState withUpdateName(UUID uuid, String newName) {
        SLInfo slInfo = slIdName().get(uuid);
        return plus(uuid, slInfo.withName(newName));
    }

    default SpectralLibraryState withUpdateState(UUID uuid, SLProgressState newState) {
        SLInfo slInfo = slIdName().get(uuid);
        return plus(uuid, slInfo.withState(newState));
    }

    default SpectralLibraryState withUpdateOwner(UUID slId, UUID newOwnerId) {
        SLInfo slInfo = slIdName().get(slId);
        return plus(slId, slInfo.withOwnerId(newOwnerId));
    }

    default SpectralLibraryState withUpdateEntryCount(UUID slId, int newCount) {
        SLInfo slInfo = slIdName().get(slId);
        return plus(slId, slInfo.withEntryNumber(newCount));
    }

    default SpectralLibraryState plus(UUID id, SLInfo newSLInfo) {
        PMap<UUID, SLInfo> slidsInfoPMap = slIdName().plus(id, newSLInfo);
        return withSlIdName(slidsInfoPMap);
    }

    default SpectralLibraryState minus(UUID uuid) {
        PMap<UUID, SLInfo> slidsInfoPMap = slIdName().minus(uuid);
        return withSlIdName(slidsInfoPMap);
    }
}
