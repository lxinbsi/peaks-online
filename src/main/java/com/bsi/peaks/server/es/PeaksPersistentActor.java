package com.bsi.peaks.server.es;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.japi.Effect;
import akka.japi.function.Predicate;
import akka.japi.function.Procedure;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AtLeastOnceDelivery;
import akka.persistence.journal.Tagged;
import com.bsi.peaks.event.work.WorkCommand;
import com.bsi.peaks.event.work.WorkCommandAck;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.FiniteDuration;

import java.util.Optional;
import java.util.Set;

public abstract class PeaksPersistentActor<S, E extends GeneratedMessageV3, EA extends GeneratedMessageV3, C extends GeneratedMessageV3>
    extends PeaksCommandPersistentActor<S, E, C> {

    public static final String LOGGING_TICK = "LOGGING_TICK";
    protected static final ObjectMapper OM = new ObjectMapper().registerModule(new Jdk8Module());

    public PeaksPersistentActor(Class<E> eventClass) {
        this(eventClass, false);
    }

    public PeaksPersistentActor(Class<E> eventClass, boolean enableLogging) {
        super(eventClass);
        if (enableLogging) {
            FiniteDuration logDuration = FiniteDuration.fromNanos(
                ConfigFactory.load().getDuration("peaks.data.es.logging-interval").toNanos()
            );
            if (logDuration.toSeconds() > 0) {
                context().system().scheduler().schedule(
                    logDuration,
                    logDuration,
                    self(),
                    LOGGING_TICK,
                    context().dispatcher(),
                    self()
                );
            }
        }
    }

    @Deprecated
    protected EA ackAcceptEvent(E event) {
        throw new RuntimeException("Events not accepted from outside actors");
    }

    @Deprecated
    protected EA ackRejectEvent(E event, String rejectDescription) {
        throw new RuntimeException("Events not accepted from outside actors");
    }

    protected void persistAck(E event) {
        final Set<String> tags = ImmutableSet.of(eventTag(event));
        final Tagged tagged = new Tagged(event, tags);
        try {
            persist(tagged, ignore -> {
                try {
                    setState(nextState(data, event));
                    reply(ackAcceptEvent(event));
                } catch (Throwable throwable) {
                    log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                    reply(ackRejectEvent(event, throwable.toString()));
                }
            });
        } catch (Throwable throwable) {
            log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), eventTag(event), sender().path());
            reply(ackRejectEvent(event, throwable.toString()));
        }
    }

    protected void persistAckThen(E event, Effect afterPersist) {
        try {
            persistAckThen(afterPersist).apply(event);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    protected FI.UnitApply<E> persistAckThen(Effect afterPersist) {
        return persistAckThen(ignore -> afterPersist.apply());
    }

    protected FI.UnitApply<E> persistAckThen(Procedure<E> afterPersist) {
        return event -> {
            final Set<String> tags = ImmutableSet.of(eventTag(event));
            final Tagged tagged = new Tagged(event, tags);
            try {
                persist(tagged, ignore -> {
                    try {
                        setState(nextState(data, event));
                        if (afterPersist != null) {
                            afterPersist.apply(event);
                        }
                        reply(ackAcceptEvent(event));
                    } catch (Throwable throwable) {
                        log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                        reply(ackRejectEvent(event, throwable.toString()));
                    }
                });
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                reply(ackRejectEvent(event, throwable.toString()));
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

    protected FI.UnitApply<E> ignoreThenAckOr(Predicate<E> ignore, FI.UnitApply<E> next) {
        return event -> {
            try {
                if (ignore.test(event)) {
                    log.info("{}: Ignoring event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                    reply(ackAcceptEvent(event));
                } else {
                    next.apply(event);
                }
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                reply(ackRejectEvent(event, throwable.toString()));
            }
        };
    }

    protected FI.UnitApply<E> ignoreThenAckOrDebug(Predicate<E> ignore, FI.UnitApply<E> next) {
        return event -> {
            try {
                if (ignore.test(event)) {
                    log.debug("{}: Ignoring event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                    reply(ackAcceptEvent(event));
                } else {
                    next.apply(event);
                }
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                reply(ackRejectEvent(event, throwable.toString()));
            }
        };
    }

    protected FI.UnitApply<E> validateThen(Procedure<E> validate, FI.UnitApply<E> next) {
        return event -> {
            try {
                validate.apply(event);
                next.apply(event);
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                reply(ackRejectEvent(event, throwable.toString()));
            }
        };
    }

    protected FI.UnitApply<E> ignoreThenAckOrValidateThen(Predicate<E> ignore, Procedure<E> validate, FI.UnitApply<E> next) {
        return event -> {
            try {
                if (ignore.test(event)) {
                    log.info("{}: Ignoring event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                    reply(ackAcceptEvent(event));
                } else {
                    validate.apply(event);
                    next.apply(event);
                }
            } catch (Throwable throwable) {
                log.error(throwable, "{}: Failed validating event {} from sender {}", persistenceId(), eventTag(event), sender().path());
                reply(ackRejectEvent(event, throwable.toString()));
            }
        };
    }

    @Override
    protected void onWorkCommandAckReject(WorkCommandAck ack) {
        final long deliveryId = ack.getDeliveryId();
        final String rejectDescription = ack.getAckStatusDescription();
        log.error("{}: Failed sending WorkCommand {}", persistenceId(), rejectDescription);
    }

    @Override
    protected void onWorkCommandAckAccept(WorkCommandAck workCommandAck) {

    }

    protected ActorRef workManagerActor() {
        return WorkManager.instance(getContext().getSystem());
    }

    protected void sendWorkManagerCommand(WorkCommand command) {
        final ActorPath workManagerActor = workManagerActor().path();
        this.deliver(workManagerActor, (Long deliveryId) -> command.toBuilder()
            .setDeliveryId(deliveryId)
            .build());
    }

    protected Optional<Object> getUnconfirmedDelivery(long deliveryId) {
        return getDeliverySnapshot().getUnconfirmedDeliveries().stream()
            .filter(unconfirmedDelivery -> unconfirmedDelivery.deliveryId() == deliveryId)
            .findFirst()
            .map(AtLeastOnceDelivery.UnconfirmedDelivery::getMessage);
    }

    protected abstract String eventTag(E event);

    @Override
    protected abstract String commandTag(C command);

    @Override
    protected void persist(E event, Effect afterPersist) throws Exception {
        final Set<String> tags = ImmutableSet.of(eventTag(event));
        final Tagged tagged = new Tagged(event, tags);
        setState(nextState(data, event));
        persist(tagged, ignore -> {
            if (afterPersist != null) {
                afterPersist.apply();
            }
        });
        saveSnapshotInterval();
    }

    @Override
    protected ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .matchEquals(LOGGING_TICK, ignore -> this.logState());
    }

    private void logState() {
        try {
            log.info(OM.writeValueAsString(data));
        } catch (JsonProcessingException e) {
            log.warning("Unable to serialize work manager state: {}", e);
        }
    }
}
