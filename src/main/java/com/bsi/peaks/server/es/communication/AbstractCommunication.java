package com.bsi.peaks.server.es.communication;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.pattern.PatternsCS;
import com.bsi.peaks.event.common.CommandAck;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.protobuf.GeneratedMessageV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public abstract class AbstractCommunication<C extends GeneratedMessageV3, Q extends GeneratedMessageV3, R extends GeneratedMessageV3> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCommunication.class);

    private final Duration askTimeout;
    private final Class<R> queryResponseClass;
    protected final ActorSystem system;
    private final Function<ActorSystem, ActorRef> actorRefProvidor;

    public AbstractCommunication(
        final ActorSystem system,
        final Class<R> queryResponseClass,
        final Function<ActorSystem, ActorRef> actorRefProvidor
    ) {
        this.queryResponseClass = queryResponseClass;
        this.system = system;
        this.askTimeout = system.settings().config().getDuration("peaks.server.ask-timeout");
        this.actorRefProvidor = actorRefProvidor;
    }

    protected CompletionStage<Object> ask(Object msg) {
        return ask(msg, askTimeout);
    }

    protected CompletionStage<Object> ask(Object msg, Duration specifiedTimeout) {
        return PatternsCS.ask(actorRef(), msg, specifiedTimeout);
    }

    protected <T> CompletionStage<T> askThrowException(Object message, Class<T> clazz) {
        return PatternsCS.ask(actorRef(), message, askTimeout)
            .thenApply(ack -> {
                if (ack instanceof Throwable) {
                    throw Throwables.propagate((Throwable) ack);
                }
                return clazz.cast(ack);
            });
    }

    protected final ActorRef actorRef() {
        return actorRefProvidor.apply(system);
    }

    protected abstract String tagFromCommand(C command);

    public CompletionStage<Done> command(
        C command
    ) {
        return ask(command).thenCompose(new akka.japi.pf.PFBuilder<Object, CompletionStage<Done>>()
            .match(CommandAck.class, ack -> {
                switch (ack.getAckStatus()) {
                    case ACCEPT:
                        return CompletableFuture.completedFuture(Done.getInstance());
                    case REJECT:
                    default:
                        String ackStatusDescription = ack.getAckStatusDescription();
                        if (Strings.isNullOrEmpty(ackStatusDescription)) {
                            ackStatusDescription = "Command failed: " + command.toString();
                        }
                        throw new Error("Ack REJECT " + tagFromCommand(command) + ": " + ackStatusDescription);
                }
            })
            .match(Throwable.class, Futures::failedCompletionStage)
            .matchAny(unexpected -> {
                final String message = MessageFormat.format("Command received unexpected response type {0}", unexpected.getClass());
                LOG.error(message);
                return Futures.failedCompletionStage(new Error(message));
            })
            .build()::apply);
    }

    public CompletionStage<CommandAck> commandWithAck(
            C command
    ) {
        return ask(command).thenCompose(new akka.japi.pf.PFBuilder<Object, CompletionStage<CommandAck>>()
                .match(CommandAck.class, ack -> CompletableFuture.completedFuture(ack))
                .match(Throwable.class, Futures::failedCompletionStage)
                .matchAny(unexpected -> {
                    final String message = MessageFormat.format("Command received unexpected response type {0}", unexpected.getClass());
                    LOG.error(message);
                    return Futures.failedCompletionStage(new Error(message));
                })
                .build()::apply);
    }

    public CompletionStage<R> query(
        Q query
    ) {
        return ask(query).thenCompose(v1 -> new akka.japi.pf.PFBuilder<Object, CompletionStage<R>>()
            .match(queryResponseClass, CompletableFuture::completedFuture)
            .match(Throwable.class, Futures::failedCompletionStage)
            .matchAny(unexpected -> {
                final String message = MessageFormat.format("Query received unexpected response type {0}", unexpected.getClass());
                LOG.error(message);
                return Futures.failedCompletionStage(new Error(message));
            })
            .build().apply(v1));
    }

}
