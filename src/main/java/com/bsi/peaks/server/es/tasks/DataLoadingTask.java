package com.bsi.peaks.server.es.tasks;


import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.core.AcquisitionMethod;
import com.bsi.peaks.model.core.ActivationMethod;
import com.bsi.peaks.model.proteomics.Enzyme;
import com.bsi.peaks.model.proteomics.Instrument;
import com.bsi.peaks.model.system.Task;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

import java.util.UUID;

@SuppressWarnings("javadoc")
@PeaksImmutable
@JsonDeserialize(builder = DataLoadingTaskBuilder.class)
public interface DataLoadingTask extends Task {
    @Override
    @Value.Derived
    default String service() {
        return "DATA_LOADING";
    }

    String uploadedFile();
    String sourceFile();
    ActivationMethod activationMethod();
    Enzyme enzyme();
    Instrument instrument();

    // default acquisition method is DDA
    default AcquisitionMethod acquisitionMethod() {
        return AcquisitionMethod.DDA;
    }

    UUID fractionId();
}
