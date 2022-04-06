package com.bsi.peaks.server.es.communication;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonCommand;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonQuery;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonResponse;
import com.bsi.peaks.server.es.InstrumentDaemonManager;
import com.google.inject.Inject;

import java.util.function.Function;

public class InstrumentDaemonCommunication extends AbstractCommunication<InstrumentDaemonCommand,
    InstrumentDaemonQuery, InstrumentDaemonResponse> {

    @Inject
    public InstrumentDaemonCommunication(ActorSystem system) {
        this(system, InstrumentDaemonManager::instance);
    }

    public InstrumentDaemonCommunication(ActorSystem system, ActorRef actorRef) {
        this(system, ignore -> actorRef);
    }

    public InstrumentDaemonCommunication(ActorSystem system, Function<ActorSystem, ActorRef> actorRefSupplier) {
        super(system, InstrumentDaemonResponse.class, actorRefSupplier);
    }

    @Override
    protected String tagFromCommand(InstrumentDaemonCommand command) {
        return  "InstrumentDaemonCommand " + command.getCommandCase().name();
    }

}