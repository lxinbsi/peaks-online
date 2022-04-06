package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import com.bsi.peaks.event.work.WorkCommand;
import com.bsi.peaks.event.work.WorkQueryRequest;
import com.bsi.peaks.event.work.WorkQueryResponse;
import com.bsi.peaks.server.es.WorkManager;
import com.google.inject.Inject;

public class WorkManagerCommunication extends AbstractCommunication<WorkCommand, WorkQueryRequest, WorkQueryResponse> {

    @Inject
    public WorkManagerCommunication(
        final ActorSystem system
    ) {
        super(system, WorkQueryResponse.class, WorkManager::instance);
    }

    @Override
    protected String tagFromCommand(WorkCommand command) {
        return "UserManagerCommand " + command.getCommandCase().name();
    }

}
