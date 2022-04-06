package com.bsi.peaks.server.es.spectrallibrary;

import com.bsi.peaks.analysis.parameterModels.dto.AnalysisAcquisitionMethod;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.SLProgress;
import com.bsi.peaks.model.dto.SLProgressState;
import com.bsi.peaks.model.dto.SpectralLibraryListItem;
import com.bsi.peaks.model.dto.SpectralLibraryType;
import com.bsi.peaks.model.proto.AcquisitionMethod;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import org.immutables.value.Value;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;

@PeaksImmutable
@JsonDeserialize(builder = SLInfoBuilder.class)
public interface SLInfo {
    static SLInfoBuilder builder() {
        return  new SLInfoBuilder();
    }

    UUID slId();

    String name();

    String projectName();

    String analysisName();

    UUID ownerId();

    SLProgressState state();

    Collection<String> databaseNames();

    Instant creationTime();

    String metaJson();

    SpectralLibraryType spectralLibraryType();

    default AcquisitionMethod acquisitionMethod() { return AcquisitionMethod.DDA; }

    default int entryNumber() { return 0; }

    default boolean useRtAsIRt() { return false; }

    @Value.Check
    default void check() {
        Preconditions.checkState(metaJson().equals("{}") || !databaseNames().isEmpty(),
            "'databaseNames' should have at least one database");
    }

    // Automatically generated methods.
    SLInfo withName(String name);
    SLInfo withOwnerId(UUID uuid);
    SLInfo withState(SLProgressState state);
    SLInfo withEntryNumber(int value);
    SLInfo withSpectralLibraryType(SpectralLibraryType type);

    @Nonnull
    default SpectralLibraryListItem to(double progress) {
        SpectralLibraryListItem.Builder builder = SpectralLibraryListItem.newBuilder()
                .setSlId(slId().toString())
                .setSlName(name())
                .setOwnerId(ownerId().toString())
                .setAnalysisName(analysisName())
                .setProjectName(projectName())
                .setSlProgress(SLProgress.newBuilder()
                                         .setProgress(progress)
                                         .setState(state()))
                .setType(spectralLibraryType())
                .setUseRtAsIRt(useRtAsIRt())
                .setAcquisitionMethod(acquisitionMethod() == AcquisitionMethod.DIA ? AnalysisAcquisitionMethod.DIA : AnalysisAcquisitionMethod.DDA);
        return builder.setCreatedTimestamp(creationTime().getEpochSecond())
                .addAllDatabaseNames(databaseNames())
                .setEntryCount(entryNumber())
                .build();
    }
}
