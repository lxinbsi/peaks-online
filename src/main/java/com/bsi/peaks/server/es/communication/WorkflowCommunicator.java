package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import com.bsi.peaks.event.workflow.WorkflowCommand;
import com.bsi.peaks.event.workflow.WorkflowQuery;
import com.bsi.peaks.event.workflow.WorkflowResponse;
import com.bsi.peaks.server.es.WorkflowManager;
import com.google.inject.Inject;

public class WorkflowCommunicator extends AbstractCommunication<WorkflowCommand, WorkflowQuery, WorkflowResponse> {

    @Inject
    public WorkflowCommunicator(
        final ActorSystem system
    ) {
        super(system, WorkflowResponse.class, WorkflowManager::instance);
    }

    @Override
    protected String tagFromCommand(WorkflowCommand command) {
        return "WorkflowCommand " + command.getCommandCase().name();
    }

}
