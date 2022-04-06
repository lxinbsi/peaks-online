package com.bsi.peaks.server.handlers.helper;

import akka.Done;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;

import java.util.concurrent.CompletableFuture;

public class AckingReceiver extends AbstractActor {
    public final static String NAME = "ackingReceiver";
    public final static String INITIALIZED = "INITIALIZED";
    public final static String COMPLETED = "COMPLETED";
    public final static String ACK = "ACK";
    public final static String CONTINUE = "CONTINUE";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final ServerWebSocket webSocket;
    private final CompletableFuture<Done> futureDone;
    private ActorRef upstream;

    public static class StreamFailure {
        private final Throwable cause;
        public StreamFailure(Throwable cause) {
            this.cause = cause;
        }
        public Throwable getCause() {
            return cause;
        }
    }

    public static Props props(ServerWebSocket webSocket, CompletableFuture<Done> futureDone) {
        return Props.create(AckingReceiver.class, () -> new AckingReceiver(webSocket, futureDone));
    }

    private AckingReceiver(ServerWebSocket webSocket, CompletableFuture<Done> futureDone) {
        this.webSocket = webSocket;
        this.futureDone = futureDone;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals(INITIALIZED, init -> {
                log.debug("Web socket acking receiver initialized");
                upstream = sender();
                upstream.tell(ACK, self());
            })
            .matchEquals(COMPLETED, completed -> {
                log.debug("Web socket acking receiver completed");
                futureDone.complete(Done.getInstance());
            })
            .matchEquals(CONTINUE, clientContinue -> {
                // this is from client
                upstream.tell(ACK, self());
            })
            .match(StreamFailure.class, failure -> {
                log.error(failure.getCause().getMessage());
                futureDone.completeExceptionally(failure.getCause());
            })
            .match(Buffer.class, buffer -> {
                try {
                    webSocket.writeFinalBinaryFrame(buffer);
                } catch (Exception e) {
                    futureDone.completeExceptionally(e);
                }
            })
            .build();
    }
}
