package com.bsi.peaks.server.es;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.BackoffSupervisor;
import com.bsi.peaks.event.WorkflowEventFactory;
import com.bsi.peaks.event.common.AckStatus;
import com.bsi.peaks.event.workflow.Create;
import com.bsi.peaks.event.workflow.Delete;
import com.bsi.peaks.event.workflow.GrantPermission;
import com.bsi.peaks.event.workflow.Update;
import com.bsi.peaks.event.workflow.WorkflowCommand;
import com.bsi.peaks.event.workflow.WorkflowCommandAck;
import com.bsi.peaks.event.workflow.WorkflowEvent;
import com.bsi.peaks.event.workflow.WorkflowQuery;
import com.bsi.peaks.event.workflow.WorkflowResponse;
import com.bsi.peaks.event.workflow.WorkflowState;
import com.bsi.peaks.model.dto.Workflow;

import java.time.Duration;
import java.util.UUID;

import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;

public class WorkflowManager extends PeaksCommandPersistentActor<WorkflowState, WorkflowEvent, WorkflowCommand> {
    public static final String NAME = "Workflow";
    public static final String PERSISTENCE_ID = "WorkflowManager";
    protected final LoggingAdapter log = Logging.getLogger(getContext().system(), this);


    public static void start(ActorSystem system) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(WorkflowManager.class),
                    NAME + "Instance",
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(10),
                    0.2
                ),
                PoisonPill.getInstance(),
                ClusterSingletonManagerSettings.create(system)
            ),
            NAME
        );
        system.actorOf(
            ClusterSingletonProxy.props("/user/" + NAME, ClusterSingletonProxySettings.create(system)),
            NAME + "Proxy"
        );
    }


    public static ActorRef instance(ActorSystem system) {
        return system.actorFor("/user/" + NAME + "Proxy");
    }

    public WorkflowManager() {
        super(WorkflowEvent.class);
    }

    @Override
    protected WorkflowState defaultState() {
        return WorkflowState.getDefaultInstance();
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(WorkflowCommand.class, WorkflowCommand::hasCreate, commandHandler(this::handleCreate))
            .match(WorkflowCommand.class, WorkflowCommand::hasDelete, commandHandler(this::handleDelete))
            .match(WorkflowCommand.class, WorkflowCommand::hasUpdate, commandHandler(this::handleUpdate))
            .match(WorkflowCommand.class, WorkflowCommand::hasGrantPermission, commandHandler(this::handleGrantPermission))
            .match(WorkflowQuery.class, WorkflowQuery::hasAll, queryHandler(this::queryAll))
            .match(WorkflowQuery.class, WorkflowQuery::hasById, queryHandler(this::queryById))
            .matchAny(obj -> {
                System.out.println("wtf?");
            });
    }

    @Override
    protected long deliveryId(WorkflowCommand command) {
        return command.getDeliveryId();
    }

    @Override
    protected WorkflowState nextState(WorkflowState state, WorkflowEvent event) throws Exception {
        final String userId = event.getUserId();
        switch (event.getEventCase()) {
            case CREATE: {
                final Create create = event.getCreate();
                final Workflow workflow = create.getWorkflow();
                return data.toBuilder()
                    .putEntries(workflow.getId(), workflow)
                    .build();
            }
            case UPDATE: {
                final Update update = event.getUpdate();
                final Workflow workflow = update.getWorkflow();
                return data.toBuilder()
                    .removeEntries(workflow.getId())
                    .putEntries(workflow.getId(), workflow)
                    .build();
            }
            case DELETE: {
                final Delete delete = event.getDelete();
                return data.toBuilder()
                    .removeEntries(delete.getId())
                    .build();
            }
            case GRANTPERMISSION: { //TODO is this meant to handle both shared and owner?
                final GrantPermission grantPermission = event.getGrantPermission();
                final String name = grantPermission.getName();
                return data.toBuilder()
                    .putEntries(name, data.getEntriesOrThrow(name).toBuilder()
                        .setOwnerId(grantPermission.getNewOwnerId())
                        .build()
                    )
                    .build();
            }
            default:
                return data;
        }
    }

    protected WorkflowCommandAck ackRejectQuery(WorkflowQuery query, String rejectDescription) {
        return WorkflowCommandAck.newBuilder()
            .setAckStatus(AckStatus.REJECT)
            .setAckStatusDescription(rejectDescription)
            .build();
    }

    @Override
    protected String commandTag(WorkflowCommand command) {
        return "WorkflowCommandAck." + command.getCommandCase().name();
    }

    @Override
    public String persistenceId() {
        return PERSISTENCE_ID;
    }

    private boolean hasReadPermission(String userId, Workflow workflow) {
        return userId.equalsIgnoreCase(ADMIN_USERID) || userId.equals(workflow.getOwnerId()) || workflow.getShared();
    }

    private boolean hasWritePermission(String userId, Workflow workflow) {
        return userId.equalsIgnoreCase(ADMIN_USERID) || userId.equals(workflow.getOwnerId());
    }

    private FI.UnitApply<WorkflowCommand> commandHandler(FI.UnitApply<WorkflowCommand> lamda) {
        return command -> {
            try {
                lamda.apply(command);
            } catch (Throwable e) {
                log.error("{}: Failed validating command {} from sender {}", persistenceId(), commandTag(command), sender().path());
                reply(ackReject(command, e.getMessage()));
            }
        };
    }

    private FI.UnitApply<WorkflowQuery> queryHandler(FI.UnitApply<WorkflowQuery> lamda) {
        return query -> {
            try {
                lamda.apply(query);
            } catch (Throwable e) {
                log.error("{}: Failed validating command {} from sender {}", persistenceId(), query.getQueryCase().name(), sender().path());
                reply(ackRejectQuery(query, e.getMessage()));
            }
        };
    }

    public void handleCreate(WorkflowCommand command) throws Exception {
        final Create create = command.getCreate();
        final Workflow workflow = create.getWorkflow();

        if (workflow.getName().isEmpty()) {
            throw new IllegalStateException("Workflow name cannot be empty");
        }

        if (workflow.getWorkFlowsEnabledCount() == 0) {
            throw new IllegalStateException("Workflow must contain some steps");
        }

        try {
            UUID.fromString(workflow.getId());
        } catch (Exception e) {
            throw new IllegalStateException("Workflow has invalid id");
        }

        UUID ownerId;
        try {
            ownerId = UUID.fromString(workflow.getOwnerId());
        } catch (Exception e) {
            throw new IllegalStateException("Workflow has invalid owner id");
        }

        if (isNameDuplicated(workflow.getName(), ownerId)) {
            throw new IllegalStateException("Workflow cannot have duplicate name");
        }

        persist(WorkflowEventFactory.factory(command.getUserId()).create(create), () -> reply(ackAccept(command)));
    }

    public void handleDelete(WorkflowCommand command) throws Exception {
        final Delete delete = command.getDelete();
        final String workflowId = delete.getId();
        final String userId = command.getUserId();

        if (!data.containsEntries(workflowId)) {
            reply(ackReject(command,"Cannot delete none existing workflow " + workflowId));
            return;
        }

        if (!hasWritePermission(userId, data.getEntriesOrThrow(workflowId))) {
            reply(ackReject(command, "User " + userId + " does not have permission to delete workflow " + workflowId));
            return;
        }

        persist(WorkflowEventFactory.factory(command.getUserId()).delete(delete), () -> reply(ackAccept(command)));
    }

    public void handleUpdate(WorkflowCommand command) throws Exception {
        final Update update = command.getUpdate();
        final String workflowId = update.getId();
        final String userId = command.getUserId();

        if (!data.containsEntries(workflowId)) {
            reply(ackReject(command,"Cannot update none existing workflow " + workflowId));
            return;
        }

        if (!hasWritePermission(userId, data.getEntriesOrThrow(workflowId))) {
            reply(ackReject(command, "User " + userId + " does not have permission to update workflow " + workflowId));
            return;
        }

        persist(WorkflowEventFactory.factory(command.getUserId()).update(update), () -> reply(ackAccept(command)));
    }

    //TODO not sure about this
    //TODO check permissions
    public void handleGrantPermission(WorkflowCommand command) {
        reply(ackAccept(command));
    }

    private void queryAll(WorkflowQuery query) {
        if (!query.hasAll()) throw new IllegalStateException("Internal server error attempting to query all workflow without WorkflowQuery.All message");

        final String userId = query.getUserId();

        final WorkflowResponse.Entries.Builder builder = WorkflowResponse.Entries.newBuilder();
        data.getEntriesMap().forEach((name, workflow) -> {
            if (hasReadPermission(userId, workflow)) {
                builder.addEntries(workflow);
            }
        });

        reply(WorkflowResponse.newBuilder()
            .setDeliveryId(query.getDeliveryId())
            .setEntries(builder)
            .build()
        );
    }

    private void queryById(WorkflowQuery query) {
        if (!query.hasById()) throw new IllegalStateException("Internal server error attempting to query single workflow without WorkflowQuery.ById message");

        final WorkflowQuery.ById byId = query.getById();
        final String userId = query.getUserId();
        final String workflowId = byId.getId();

        final WorkflowResponse.Entries.Builder builder = WorkflowResponse.Entries.newBuilder();
        data.getEntriesMap().forEach((name, workflow) -> {
            if (hasReadPermission(userId, workflow) && workflowId.equals(workflow.getId())) {
                builder.addEntries(workflow);
            }
        });

        reply(WorkflowResponse.newBuilder()
            .setDeliveryId(query.getDeliveryId())
            .setEntries(builder)
            .build()
        );
    }

    private boolean isNameDuplicated(String workflowName, UUID ownerId) {
        for (Workflow workflow : data.getEntriesMap().values()) {
            if (workflow.getOwnerId().equals(ownerId.toString()) && workflow.getName().equals(workflowName)) {
                return true;
            }
        }

        return false;
    }
}
