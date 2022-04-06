package com.bsi.peaks.server.es.communication;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.bsi.peaks.server.es.CassandraMonitorManager;
import com.google.inject.Inject;
import peaksdata.event.cassandraMonitor.CassandraMonitor.CassandraMonitorCommand;
import peaksdata.event.cassandraMonitor.CassandraMonitor.CassandraMonitorQuery;
import peaksdata.event.cassandraMonitor.CassandraMonitor.CassandraMonitorResponse;

import java.util.function.Function;

public class CassandraMonitorCommunication extends AbstractCommunication<CassandraMonitorCommand,
    CassandraMonitorQuery, CassandraMonitorResponse> {

    @Inject
    public CassandraMonitorCommunication(ActorSystem system) {
        this(system, CassandraMonitorManager::instance);
    }

    public CassandraMonitorCommunication(ActorSystem system, ActorRef actorRef) {
        this(system, ignore -> actorRef);
    }

    public CassandraMonitorCommunication(ActorSystem system, Function<ActorSystem, ActorRef> actorRefSupplier) {
        super(system, CassandraMonitorResponse.class, actorRefSupplier);
    }

    @Override
    protected String tagFromCommand(CassandraMonitorCommand command) {
        return  "CassandraMonitorCommand " + command.getCommandCase().name();
    }

}