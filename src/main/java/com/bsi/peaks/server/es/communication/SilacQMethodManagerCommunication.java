package com.bsi.peaks.server.es.communication;

 import akka.actor.ActorSystem;
import com.bsi.peaks.event.silacqmethod.SilacQMethodCommand;
import com.bsi.peaks.event.silacqmethod.SilacQMethodQuery;
import com.bsi.peaks.event.silacqmethod.SilacQMethodResponse;
import com.bsi.peaks.server.es.SilacQMethodManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class SilacQMethodManagerCommunication extends AbstractCommunication<SilacQMethodCommand, SilacQMethodQuery, SilacQMethodResponse> {

    @Inject
    public SilacQMethodManagerCommunication(ActorSystem system) {
        super(system, SilacQMethodResponse.class, SilacQMethodManager::instance);
    }

    @Override
    protected String tagFromCommand(SilacQMethodCommand command) {
        return "SilacQMethodCommand " + command.getCommandCase().name();
    }
}
