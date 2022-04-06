package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import com.bsi.peaks.event.fasta.FastaDatabaseCommand;
import com.bsi.peaks.event.fasta.FastaDatabaseQuery;
import com.bsi.peaks.event.fasta.FastaDatabaseQueryResponse;
import com.bsi.peaks.server.es.FastaDatabaseEntity;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class FastaDatabaseCommunication extends AbstractCommunication<FastaDatabaseCommand, FastaDatabaseQuery, FastaDatabaseQueryResponse> {

    @Inject
    public FastaDatabaseCommunication(ActorSystem system) {
        super(system, FastaDatabaseQueryResponse.class, FastaDatabaseEntity::instance);
    }

    @Override
    protected String tagFromCommand(FastaDatabaseCommand command) {
        return "FastaDatabaseCommand " + command.getCommandCase().name();
    }

}
