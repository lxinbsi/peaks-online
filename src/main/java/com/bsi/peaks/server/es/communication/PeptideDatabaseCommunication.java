package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import com.bsi.peaks.event.modification.PeptideDatabaseCommand;
import com.bsi.peaks.event.modification.PeptideDatabaseQueryRequest;
import com.bsi.peaks.event.modification.PeptideDatabaseQueryResponse;
import com.bsi.peaks.server.es.PeptideDatabaseManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PeptideDatabaseCommunication
    extends AbstractCommunication<PeptideDatabaseCommand, PeptideDatabaseQueryRequest, PeptideDatabaseQueryResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(PeptideDatabaseCommunication.class);

    @Inject
    public PeptideDatabaseCommunication(
        final ActorSystem system
    ) {
        super(system, PeptideDatabaseQueryResponse.class, PeptideDatabaseManager::instance);
    }

    @Override
    protected String tagFromCommand(PeptideDatabaseCommand command) {
        return "PeptideDatabaseCommand " + command.getCommandCase().name();
    }
}
