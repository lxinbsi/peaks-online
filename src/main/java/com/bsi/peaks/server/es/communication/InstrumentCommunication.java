package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import com.bsi.peaks.event.instrument.InstrumentCommand;
import com.bsi.peaks.event.instrument.InstrumentQuery;
import com.bsi.peaks.event.instrument.InstrumentResponse;
import com.bsi.peaks.server.es.InstrumentManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class InstrumentCommunication extends AbstractCommunication<InstrumentCommand, InstrumentQuery, InstrumentResponse> {

    @Inject
    public InstrumentCommunication(
        final ActorSystem system
    ) {
        super(system, InstrumentResponse.class, InstrumentManager::instance);
    }

    @Override
    protected String tagFromCommand(InstrumentCommand command) {
        return "InstrumentCommand  " + command.getCommandCase().name();
    }

}
