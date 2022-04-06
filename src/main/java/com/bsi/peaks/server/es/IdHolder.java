package com.bsi.peaks.server.es;

import java.util.UUID;

public class IdHolder {
    public static IdHolder fractionIndexedLevel(Integer index, UUID taskId) {
        return new IdHolder(taskId, null, null, index);
    }

    public static IdHolder fractionLevel(UUID fractionId, UUID sampleId, UUID taskId) {
        return new IdHolder(taskId, fractionId, sampleId, null);
    }

    public static IdHolder sampleLevel(UUID sampleId, UUID taskId) {
        return new IdHolder(taskId, null, sampleId, null);
    }

    public static IdHolder analysisLevel(UUID taskId) {
        return new IdHolder(taskId, null, null,null);
    }

    private IdHolder(UUID taskId, UUID fractionId, UUID sampleId, Integer fracIndex) {
        this.taskId = taskId;
        this.fractionIndex = fracIndex;
        this.fractionId = fractionId;
        this.sampleId = sampleId;
    }

    public final Integer fractionIndex;
    public final UUID fractionId;
    public final UUID sampleId;
    public final UUID taskId;
}
