package com.bsi.peaks.server.es;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Effect;
import akka.japi.function.Function;
import akka.japi.function.Predicate;
import akka.japi.function.Procedure;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractPersistentActorWithAtLeastOnceDelivery;
import akka.persistence.AtLeastOnceDelivery;
import akka.persistence.DeleteSnapshotFailure;
import akka.persistence.DeleteSnapshotSuccess;
import akka.persistence.DeleteSnapshotsFailure;
import akka.persistence.DeleteSnapshotsSuccess;
import akka.persistence.RecoveryCompleted;
import akka.persistence.SaveSnapshotFailure;
import akka.persistence.SaveSnapshotSuccess;
import akka.persistence.SnapshotOffer;
import akka.persistence.SnapshotSelectionCriteria;
import akka.persistence.journal.Tagged;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.common.CommandAck;
import com.bsi.peaks.event.work.WorkCommandAck;
import com.bsi.peaks.model.system.User;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public abstract class PeaksCommandPersistentActor<S, E extends GeneratedMessageV3, C extends GeneratedMessageV3> extends AbstractPersistentActorWithAtLeastOnceDelivery {
    protected final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    protected final Class<E> eventClass;
    protected final int snapshotInterval;
    protected S data;
    protected ActorRef mediator;
    private long confirmedSaveSnapshotSequenceNr = -1;
    private long saveSnapshotSequenceNr = 0;

    public PeaksCommandPersistentActor(Class<E> eventClass) {
        this.eventClass = eventClass;
        data = defaultState();
        mediator = DistributedPubSub.get(getContext().system()).mediator();

        final Config config = context().system().settings().config();
        snapshotInterval = config.getInt("peaks.server.snapshot-interval");
    }

    protected abstract S defaultState();

    protected void sendSelf(Object msg) {
        this.getSelf().tell(msg, null);
    }

    protected void sendSelfEvent(E msg) {
        this.getSelf().tell(msg, getSelf());
    }

    protected void removeUndeliveredMessages(ActorPath actorPath) {
        try {
            getDeliverySnapshot().getUnconfirmedDeliveries().stream()
                .filter(destination -> {
                    final ActorPath destinationPath = destination.destination();
                    if (destinationPath.equals(actorPath)) {
                        log.info("Removing undelivered message to {}", destinationPath);
                        return true;
                    }
                    return false;
                })
                .map(AtLeastOnceDelivery.UnconfirmedDelivery::deliveryId)
                .forEach(this::confirmDelivery);
        } catch (Exception e) {
            log.error(e, "Attempt to remove undelivered messages of " + actorPath);
        }
    }

    protected abstract S nextState(S state, E event) throws Exception;

    protected ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return receiveBuilder;
    }

    protected void setState(S data) {
        this.data = data;
    }

    protected abstract long deliveryId(C command);

    protected final CommandAck ackAccept(C command) {
        return CommonFactory.ackAccept(deliveryId((command)));
    }

    protected final CommandAck ackReject(C command, String rejectDescription) {
        return CommonFactory.ackReject(deliveryId(command), rejectDescription);
    }

    private List<E> recoveryEvents = new ArrayList<>();

    @Override
    public Receive createReceiveRecover() {
        return receiveBuilder()
            .match(SnapshotOffer.class, snapshotOffer -> {
                try {
                    onSnapShotOffer(snapshotOffer);
                } catch (Throwable throwable) {
                    if (snapshotOffer.snapshot() instanceof GeneratedMessageV3) {
                        GeneratedMessageV3 proto = (GeneratedMessageV3) snapshotOffer.snapshot();
                        log.error(throwable, "Exception during recovery with snapshot: " + JsonFormat.printer().omittingInsignificantWhitespace().print(proto));
                    }
                    log.error(throwable,"During recovery, snapshot deleted, due to exception.");
                    deleteSnapshot(snapshotOffer.metadata().sequenceNr());
                    throw throwable; // Let actor die, and recover without snapshot, since we just deleted it.
                }
                saveSnapshotSequenceNr = confirmedSaveSnapshotSequenceNr = snapshotOffer.metadata().sequenceNr();
            })
            .match(eventClass, event -> {
                if (!this.ignoreEventOnRecovery(event)) {
                    return;
                }
                recoveryEvents.add(event);
                try {
                    setState(nextState(data, event));
                } catch (Throwable exception) {
                    log.error(exception,"During recovery, event {} ignored, due to exception.", event);
                }
            })
            .match(Tagged.class, tagged -> {
                final E event = eventClass.cast(tagged.payload());
                recoveryEvents.add(event);
                try {
                    setState(nextState(data, event));
                } catch (Throwable exception) {
                    log.error(exception,"During recovery, event {} ignored, due to exception.", event);
                }
            })
            .match(RecoveryCompleted.class, ignore -> {
                try {
                    final int recoveryEventsSize = recoveryEvents.size();
                    onRecoveryComplete(recoveryEvents);
                    if (recoveryEventsSize > 0) {
                        log.info("{}: Recovered {} events", persistenceId(), recoveryEventsSize);
                        saveSnapshot();
                    }
                } catch (Exception e) {
                    log.error(e, "Exception thrown onRecoveryComplete");
                }
                recoveryEvents = null;
            })
            .matchAny(object -> {
                log.warning("Invalid message {} encountered during recovery of {}", object.getClass().getSimpleName(), persistenceId());
            })
            .build();
    }

    protected boolean ignoreEventOnRecovery(E e) {
        return true;
    }

    protected void onSnapShotOffer(SnapshotOffer snapshotOffer) {
        throw new IllegalStateException("Unexpected snapshot offer");
    }

    public void reply(Object msg) {
        tell(msg, getSender());
    }

    public void replyFuture(CompletionStage<?> furureMsg) {
        final ActorRef sender = getSender();
        furureMsg.whenComplete((msg, throwable) -> {
            if (throwable == null) {
                tell(msg, sender);
            } else {
                log.error(throwable, "Future message cannot be sent due to error.");
            }
        });
    }

    public void tell(Object msg, ActorRef sender) {
        if (sender == null) {
            log.debug("{}: Cannot reply to sender NULL. {}", persistenceId(), msg);
        } else if (sender.equals(context().system().deadLetters())) {
            log.debug("{}: Cannot reply to deadLetters. {}", persistenceId(), msg);
        } else if (sender.isTerminated()) {
            log.warning("{}: Cannot reply to terminated sender {}. {}", persistenceId(), sender.path(), log.isDebugEnabled() ? msg : msg.getClass());
        } else {
            sender.tell(msg, getSelf());
        }
    }

    protected void onRecoveryComplete(List<E> recoveryEvents) throws Exception {
    }

    protected final void persist(E event) throws Exception {
        persist(event, (Effect) null);
    }

    @Override
    public final Receive createReceive() {
        return createReceive(receiveBuilder())
            .match(CommandAck.class, this::ackCommand)
            .match(WorkCommandAck.class, this::ackWorkCommand)
            .match(AtLeastOnceDelivery.UnconfirmedWarning.class, this::unconfirmWarning)
            .match(Throwable.class, this::unexpectedException)
            .match(SaveSnapshotSuccess.class, success -> {
                final long newSnapshotSequenceNr = success.metadata().sequenceNr();
                log.info("{}: Snapshot {} saved.", persistenceId(), newSnapshotSequenceNr);
                if (confirmedSaveSnapshotSequenceNr >= 0) {
                    deleteSnapshot(confirmedSaveSnapshotSequenceNr);
                }
                confirmedSaveSnapshotSequenceNr = newSnapshotSequenceNr;
            })
            .match(SaveSnapshotFailure.class, failure -> {
                log.error(failure.cause(), "{}: Snapshot {} failed to save.", persistenceId(), failure.metadata().sequenceNr());
                // Revert due to save failure.
                saveSnapshotSequenceNr = confirmedSaveSnapshotSequenceNr;
            })
            .match(DeleteSnapshotSuccess.class, success -> log.info("{}: Snapshot {} deleted.", persistenceId(), success.metadata().sequenceNr()))
            .match(DeleteSnapshotFailure.class, failure -> log.error(failure.cause(), "{}: Snapshot {} failed to delete.", persistenceId(), failure.metadata().sequenceNr()))
            .match(DeleteSnapshotsSuccess.class, success -> log.info("{}: Snapshots deleted", persistenceId()))
            .match(DeleteSnapshotsFailure.class, failure -> log.error(failure.cause(), "{}: Snapshots failed to delete", persistenceId()))
            .matchAny(this::invalidMessage)
            .build();
    }

    /**
     * Perform saving of snapshot
     * @return true if snapshot save was required.
     */
    protected final boolean saveSnapshot() {
        if (lastSequenceNr() > confirmedSaveSnapshotSequenceNr) {
            final GeneratedMessageV3 snapShot = snapShotFromState(data);
            if (snapShot == null) return false;
            saveSnapshot(snapShot);
            saveSnapshotSequenceNr = lastSequenceNr();
            return true;
        }
        return false;
    }

    protected final boolean saveSnapshotInterval() {
        if (lastSequenceNr() >= saveSnapshotSequenceNr + snapshotInterval) {
            final GeneratedMessageV3 snapShot = snapShotFromState(data);
            if (snapShot == null) return false;
            saveSnapshot(snapShot);
            saveSnapshotSequenceNr = lastSequenceNr();
            return true;
        }
        return false;
    }

    protected void deleteSelf() {
        deleteSnapshots(SnapshotSelectionCriteria.create(lastSequenceNr(), Long.MAX_VALUE));
        deleteMessages(lastSequenceNr());
        this.data = defaultState();
    }

    protected GeneratedMessageV3 snapShotFromState(S state) {
        return null;
    }

    private void unconfirmWarning(AtLeastOnceDelivery.UnconfirmedWarning msg) {
        for (AtLeastOnceDelivery.UnconfirmedDelivery unconfirmedDelivery : msg.getUnconfirmedDeliveries()) {
            log.error("{}: Deleting unconfirmed message {} to {}: ", persistenceId(), unconfirmedDelivery.getMessage().getClass(), unconfirmedDelivery.destination());
            confirmDelivery(unconfirmedDelivery.deliveryId());
        }

    }

    protected void unexpectedException(Throwable throwable) {
        log.error(throwable,"{}: Unexpected exception from {}", persistenceId(), getSender().path());
    }

    protected void invalidMessage(Object object) {
        log.warning("{}: Invalid message {} from {}", persistenceId(), object.getClass().getSimpleName(), getSender().path());
        IllegalStateException stateException = new IllegalStateException("Invalid message:" + object.getClass().getSimpleName());
        reply(stateException);
    }

    protected User.Role userRole(UUID userId) {
        // TODO ask user actor to verify role
        if (userId.toString().equals(UserManager.ADMIN_USERID)) {
            return User.Role.SYS_ADMIN;
        } else {
            return User.Role.USER;
        }
    }

    protected void ackCommand(CommandAck ack) {
        final long deliveryId = ack.getDeliveryId();
        switch (ack.getAckStatus()) {
            case REJECT:
                this.getDeliverySnapshot().getUnconfirmedDeliveries()
                    .stream()
                    .filter(unconfirmedDelivery -> unconfirmedDelivery.deliveryId() == deliveryId)
                    .findFirst()
                    .ifPresent(unconfirmedDelivery -> {
                        Class<?> aClass = unconfirmedDelivery.getMessage().getClass();
                        log.error("{}: Failed sending {}", persistenceId(), aClass);
                    });
        }
        confirmDelivery(deliveryId);
    }

    protected void ackWorkCommand(WorkCommandAck ack) {
        switch (ack.getAckStatus()) {
            case REJECT:
                try {
                    onWorkCommandAckReject(ack);
                } catch (Exception e) {
                    log.error(e, "{}: onWorkCommandAckReject threw exception", persistenceId());
                }
                break;
            case ACCEPT:
                onWorkCommandAckAccept(ack);
                break;
        }
        confirmDelivery(ack.getDeliveryId());
    }

    protected void onWorkCommandAckReject(WorkCommandAck ack) {

    }

    protected void onWorkCommandAckAccept(WorkCommandAck workCommandAck) {

    }

    protected abstract String commandTag(C command);

    protected void persist(E event, Effect afterPersist) throws Exception {
        setState(nextState(data, event));
        persist(event, ignore -> {
            mediator.tell(new DistributedPubSubMediator.Publish(persistenceId(), event), getSelf());
            if (afterPersist != null) {
                afterPersist.apply();
            }
        });
        saveSnapshotInterval();
    }

    protected FI.UnitApply<C> commandThenAck(Procedure<C> runCommand) {
        return command -> {
            try {
                runCommand.apply(command);
                defer(ackAccept(command), this::reply);
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating command {} from sender {}", persistenceId(), commandTag(command), sender().path());
                reply(ackReject(command, throwable.toString()));
            }
        };
    }

    protected FI.UnitApply<C> ignoreThenAckOr(Predicate<C> ignore, Function<C, CommandAck> next) {
        return command -> {
            try {
                if (ignore.test(command)) {
                    log.info("{}: Ignoring command {} from sender {}", persistenceId(), commandTag(command), sender().path());
                    reply(ackAccept(command));
                } else {
                    defer(next.apply(command), this::reply);
                }
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating command {} from sender {}", persistenceId(), commandTag(command), sender().path());
                reply(ackReject(command, throwable.toString()));
            }
        };
    }

    protected FI.UnitApply<C> validateCommandThen(Procedure<C> validate, Function<C, CommandAck> next) {
        return command -> {
            try {
                validate.apply(command);
                defer(next.apply(command), this::reply);
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), commandTag(command), sender().path());
                reply(ackReject(command, throwable.toString()));
            }
        };
    }

    protected FI.UnitApply<C> commandAckOnFailure(FI.UnitApply<C> next) {
        return command -> {
            try {
                next.apply(command);
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating command {} from sender {}", persistenceId(), commandTag(command), sender().path());
                reply(ackReject(command, throwable.toString()));
            }
        };
    }

    @Override
    public abstract String persistenceId();

    public void persistOnlyFromSelf(E event) throws Exception {
        if (self().equals(sender())) {
            persist(event);
        } else {
            log.warning("{}: Only events from self will be persisted", persistenceId());
        }
    }
}
