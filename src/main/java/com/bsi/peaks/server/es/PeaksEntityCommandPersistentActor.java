package com.bsi.peaks.server.es;

import akka.actor.PoisonPill;
import akka.actor.ReceiveTimeout;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.cluster.sharding.ShardRegion;
import akka.japi.Effect;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.DeleteMessagesSuccess;
import akka.persistence.journal.Tagged;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public abstract class PeaksEntityCommandPersistentActor<S, E extends GeneratedMessageV3, C extends GeneratedMessageV3> extends PeaksCommandPersistentActor<S, E, C> {

    protected static final int UUID_LENGTH = 36;
    private final int passivateIdleTimeout;

    public PeaksEntityCommandPersistentActor(Class<E> eventClass, int passivateIdleTimeout) {
        super(eventClass);
        this.passivateIdleTimeout = passivateIdleTimeout;
    }

    protected abstract String eventTag(E event);

    @Override
    protected void persist(E event, Effect afterPersist) throws Exception {
        String tag = eventTag(event);
        final Tagged tagged = new Tagged(event, ImmutableSet.of(tag));
        setState(nextState(data, event));
        persist(tagged, ignore -> {
            mediator.tell(new DistributedPubSubMediator.Publish(persistenceId(), event), getSelf());
            mediator.tell(new DistributedPubSubMediator.Publish(tag, event), getSelf());
            if (afterPersist != null) {
                afterPersist.apply();
            }
        });
        saveSnapshotInterval();
    }

    @Override
    public final String persistenceId() {
        return getSelf().path().name();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        getContext().setReceiveTimeout(Duration.create(passivateIdleTimeout, TimeUnit.MINUTES));
    }

    @Override
    protected ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .matchEquals(ReceiveTimeout.getInstance(), msg -> passivate())
            .match(DeleteMessagesSuccess.class, msg -> {
                log.info("{}: deleted events to sequenceNr {}. Will now passivate entity.", persistenceId(), msg.toSequenceNr());
                getContext().getParent().tell(new ShardRegion.Passivate(PoisonPill.getInstance()), getSelf());
            });
    }

    private void passivate() {
        if (!saveSnapshot()) {
            // We wait until snapshot success/failure messages have arrived before passivising.
            log.debug("Passivate " + persistenceId());
            getContext().getParent().tell(new ShardRegion.Passivate(PoisonPill.getInstance()), getSelf());
        }
    }

}
