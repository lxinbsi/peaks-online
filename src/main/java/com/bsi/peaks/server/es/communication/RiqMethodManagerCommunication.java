package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodCommand;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodQuery;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodResponse;
import com.bsi.peaks.server.es.ReporterIonQMethodManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class RiqMethodManagerCommunication extends AbstractCommunication<ReporterIonQMethodCommand, ReporterIonQMethodQuery, ReporterIonQMethodResponse> {

    @Inject
    public RiqMethodManagerCommunication(ActorSystem system) {
        super(system, ReporterIonQMethodResponse.class, ReporterIonQMethodManager::instance);
    }

    @Override
    protected String tagFromCommand(ReporterIonQMethodCommand command) {
        return "ReporterIonQMethodCommand " + command.getCommandCase().name();
    }
}
