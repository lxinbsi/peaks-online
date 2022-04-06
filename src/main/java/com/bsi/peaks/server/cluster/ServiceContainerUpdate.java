package com.bsi.peaks.server.cluster;

import com.bsi.peaks.common.messaging.PeaksMessage;
import com.bsi.peaks.model.PeaksImmutable;

import java.util.UUID;

@PeaksImmutable
public interface ServiceContainerUpdate extends PeaksMessage {
    UUID taskId();
}
