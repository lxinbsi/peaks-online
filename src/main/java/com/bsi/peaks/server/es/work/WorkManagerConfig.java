package com.bsi.peaks.server.es.work;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.function.Function;
import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.server.es.ProjectAggregateEntity;
import com.google.common.collect.ImmutableList;
import org.immutables.value.Value;

import java.time.Duration;
import java.util.Map;

@PeaksImmutable
public interface WorkManagerConfig {
    String checkVersion();
    default Integer minTaskThreads() {
        return 8;
    }
    default Integer maxTaskThreads() {
        return 64;
    }
    default Integer taskThreadsIncrement() {
        return 8;
    }
    default Duration unreachableDisconnect() {
        return Duration.ofHours(1);
    }
    default Integer parallelism() {return 8;}
    default long timeout() {return 1000;}
    default Function<ActorSystem, ActorRef> projectActorRef() {
        return ProjectAggregateEntity::instance;
    }
}
