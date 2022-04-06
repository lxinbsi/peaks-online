package com.bsi.peaks.server.service;

import akka.actor.ActorRef;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.google.common.base.Throwables;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AskHelpers {
    public static CompletionStage<?> askInOrder(ActorRef actor, Timeout timeout, List<?> messages) {
        if(messages.size() == 0) {
            return CompletableFuture.completedFuture(null);
        }

        Queue<Object> messageQueue = new LinkedList<>();
        for(int i = 0; i < messages.size(); i++) {
            messageQueue.add(messages.get(i));
        }
        return askInOrder(actor, timeout, messageQueue);
    }

    private static CompletionStage<?> askInOrder(ActorRef actor, Timeout timeout, Queue<Object> messageQueue) {
        Object poll = messageQueue.poll();
        if (messageQueue.size() == 0) {
            return PatternsCS.ask(actor, poll, timeout);
        }

        return PatternsCS.ask(actor, poll, timeout).thenCompose(ignore -> askInOrder(actor, timeout, messageQueue));
    }

    public static CompletionStage<?> AskThrowException(ActorRef actor, Object message, Timeout timeout) {
        return PatternsCS.ask(actor, message, timeout)
            .thenApply(ack -> {
                if (ack instanceof Throwable) {
                    throw Throwables.propagate((Throwable) ack);
                }
                return ack;
            });
    }
}
