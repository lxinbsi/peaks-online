package com.bsi.peaks.server.es.communication;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.bsi.peaks.event.spectralLibrary.SLCommand;
import com.bsi.peaks.event.spectralLibrary.SLQuery;
import com.bsi.peaks.event.spectralLibrary.SLResponse;
import com.bsi.peaks.server.es.SpectralLibraryManager;
import com.google.inject.Inject;

import java.util.function.Function;

public class SpectralLibraryCommunication extends AbstractCommunication<SLCommand, SLQuery, SLResponse> {

    @Inject
    public SpectralLibraryCommunication(ActorSystem system) {
        this(system, SpectralLibraryManager::instance);
    }

    public SpectralLibraryCommunication(ActorSystem system, ActorRef actorRef) {
        this(system, ignore -> actorRef);
    }

    public SpectralLibraryCommunication(ActorSystem system, Function<ActorSystem, ActorRef> actorRefSupplier) {
        super(system, SLResponse.class, actorRefSupplier);
    }

    @Override
    protected String tagFromCommand(SLCommand command) {
        return  "SpectralLibraryCommand " + command.getCommandCase().name();
    }

}
