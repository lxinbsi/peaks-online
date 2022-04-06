package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import com.bsi.peaks.event.modification.ModificationCommand;
import com.bsi.peaks.event.modification.ModificationQuery;
import com.bsi.peaks.event.modification.ModificationResponse;
import com.bsi.peaks.server.es.ModificationEntity;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class ModificationCommunication extends AbstractCommunication<ModificationCommand, ModificationQuery, ModificationResponse> {

    @Inject
    public ModificationCommunication(
        final ActorSystem system
    ) {
        super(system, ModificationResponse.class, ModificationEntity::instance);
    }

    @Override
    protected String tagFromCommand(ModificationCommand command) {
        return "ModificationCommand  " + command.getCommandCase().name();
    }

}
