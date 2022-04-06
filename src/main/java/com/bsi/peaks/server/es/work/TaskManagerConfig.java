package com.bsi.peaks.server.es.work;

import com.bsi.peaks.model.PeaksImmutable;

@PeaksImmutable
public interface TaskManagerConfig {
    default Integer parallelism() {return 8;}
    default long timeout() {return 1000;}
}
