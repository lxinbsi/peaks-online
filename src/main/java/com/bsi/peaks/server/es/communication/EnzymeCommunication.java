package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import com.bsi.peaks.event.enzyme.EnzymeCommand;
import com.bsi.peaks.event.enzyme.EnzymeQuery;
import com.bsi.peaks.event.enzyme.EnzymeResponse;
import com.bsi.peaks.server.es.EnzymeManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class EnzymeCommunication extends AbstractCommunication<EnzymeCommand, EnzymeQuery, EnzymeResponse> {

    @Inject
    public EnzymeCommunication(
        final ActorSystem system
    ) {
        super(system, EnzymeResponse.class, EnzymeManager::instance);
    }

    @Override
    protected String tagFromCommand(EnzymeCommand command) {
        return "EnzymeCommand  " + command.getCommandCase().name();
    }

}
