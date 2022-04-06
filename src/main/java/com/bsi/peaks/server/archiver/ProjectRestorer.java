package com.bsi.peaks.server.archiver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.sharding.ClusterSharding;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractPersistentActor;
import com.bsi.peaks.event.project.ProjectEventToRestore;
import com.bsi.peaks.server.es.ProjectAggregateEntity;

import java.text.MessageFormat;
import java.util.UUID;

public class ProjectRestorer extends AbstractPersistentActor {
    private final UUID projectId;

    private ProjectRestorer(UUID projectId) {
        this.projectId = projectId;
    }

    public static ActorRef instance(ActorSystem system) {
        return ClusterSharding.get(system).shardRegion(ProjectAggregateEntity.CLUSTER_NAMESPACE);
    }

    @Override
    public Receive createReceiveRecover() {
        // don't do anything
        return ReceiveBuilder.create().build();
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
            .match(ProjectEventToRestore.class, this::restore)
            .build();
    }

    private void restore(ProjectEventToRestore events) {
        // persist all events and die
        persistAll(events.getEventsList(), null);
        context().stop(self());
    }

    @Override
    public String persistenceId() {
        return MessageFormat.format("{0}-{1}", ProjectAggregateEntity.CLUSTER_NAMESPACE, projectId);
    }
}
