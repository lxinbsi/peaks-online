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
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.BackoffSupervisor;
import com.bsi.peaks.event.CassandraMonitorCommandFactory;
import com.bsi.peaks.event.CassandraMonitorEventFactory;
import com.bsi.peaks.model.dto.CassandraMonitorInfo;
import com.bsi.peaks.model.dto.CassandraNodeInfo;
import com.bsi.peaks.model.dto.NodeStatus;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.es.state.CassandraMonitorState;
import org.jetbrains.annotations.NotNull;
import peaksdata.event.cassandraMonitor.CassandraMonitor;
import peaksdata.event.cassandraMonitor.CassandraMonitor.CassandraMonitorCommand;
import peaksdata.event.cassandraMonitor.CassandraMonitor.CassandraMonitorEvent;
import peaksdata.event.cassandraMonitor.CassandraMonitor.CassandraMonitorQuery;
import peaksdata.event.cassandraMonitor.CassandraMonitor.CassandraMonitorResponse;
import scala.Option;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;

public class CassandraMonitorManager  extends PeaksCommandPersistentActor<CassandraMonitorState, CassandraMonitorEvent, CassandraMonitorCommand>  {
    private static final String NAME = "CassandraMonitor";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final CassandraMonitorCommandFactory systemCommandFactory;
    private final int offlineTimeLimit;

    public static void start(
        ActorSystem system,
        int offlineTimeLimit
    ) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(CassandraMonitorManager.class, offlineTimeLimit),
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

    @Override
    protected void onRecoveryComplete(List<CassandraMonitorEvent> recoveryEvents) throws Exception {
        super.onRecoveryComplete(recoveryEvents);
        data.ipAddressNodeInfo().values().forEach(systemCommandFactory::updateNodeInfo);
    }

    protected CassandraMonitorManager(int offlineTimeLimit) {
        super(CassandraMonitorEvent.class);
        this.offlineTimeLimit = offlineTimeLimit;
        systemCommandFactory = CassandraMonitorCommandFactory.actingAsUserId(UUID.fromString(UserManager.ADMIN_USERID));
    }

    //PeaksPersistentActor implementation
    @Override
    protected CassandraMonitorState defaultState() {
        return CassandraMonitorState.getDefaultInstance();
    }

    @Override
    protected CassandraMonitorState nextState(CassandraMonitorState state, CassandraMonitorEvent event) throws Exception {
        return state.update(event);
    }

    @NotNull
    public static String eventTag(CassandraMonitorEvent.EventCase eventCase) {
        return "CassandraMonitorEvent." + eventCase.name();
    }

    @Override
    protected String commandTag(CassandraMonitorCommand command) {
        return commandTag(command.getCommandCase());
    }

    @Override
    public String persistenceId() {
        return NAME;
    }

    @NotNull
    public static String commandTag(CassandraMonitorCommand.CommandCase commandCase) {
        return "CassandraMonitorCommand." + commandCase.name();
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(CassandraMonitorCommand.class, CassandraMonitorCommand::hasNodeInfo, commandAckOnFailure(this::handleNodeInfo))
            .match(CassandraMonitorCommand.class, CassandraMonitorCommand::hasDelete, commandAckOnFailure(this::handleDeleteCassandraNode))
            .match(CassandraMonitorCommand.class, CassandraMonitorCommand::hasDeleteAll, commandAckOnFailure(this::handleDeleteAllCassandraNode))
            .match(CassandraMonitorCommand.class, CassandraMonitorCommand::hasUpdateCutOff, commandAckOnFailure(this::handleUpdateCutOff))
            .match(CassandraMonitorCommand.class, CassandraMonitorCommand::hasUpdateIgnoreMonitor, commandAckOnFailure(this::handleUpdateIgnoreMonitor))
            .match(CassandraMonitorQuery.class, this::queryCassandraMonitor);
    }

    @Override
    protected long deliveryId(CassandraMonitorCommand command) {
        return command.getDeliveryId();
    }

    @Override
    public void onRecoveryFailure(Throwable cause, Option<Object> event) {
        log.info("Recovery failed for cassandra monitor manager, will delete all related events. \nPlease wait a couple of minutes before accessing the cassandra monitor page.");
        deleteMessages(Long.MAX_VALUE);
    }

    // commands
    private void handleNodeInfo(CassandraMonitorCommand command) throws Exception {
        UUID creatorId = uuidFrom(command.getActingUserId());
        CassandraMonitorEvent nodeInfoUpdated = CassandraMonitorEventFactory.nodeInfoUpdated(command.getNodeInfo().getNodeInfo(), creatorId);
        persist(nodeInfoUpdated, () -> reply(CassandraMonitorCommandFactory.ackAccept(command)));
    }

    private void handleUpdateCutOff(CassandraMonitorCommand command) throws Exception {
        UUID creatorId = uuidFrom(command.getActingUserId());
        isAdmin(userRole(creatorId));
        CassandraMonitorCommand.UpdateCutOff updateCutOff = command.getUpdateCutOff();
        float cutOff = updateCutOff.getCutOff();

        CassandraMonitorEvent cutOffUpdated = CassandraMonitorEventFactory.cutOffUpdated(cutOff, creatorId);
        persist(cutOffUpdated, () -> reply(CassandraMonitorCommandFactory.ackAccept(command)));
    }

    private void handleUpdateIgnoreMonitor(CassandraMonitorCommand command) throws Exception {
        UUID creatorId = uuidFrom(command.getActingUserId());
        isAdmin(userRole(creatorId));
        CassandraMonitorCommand.UpdateIgnoreMonitor updateIgnoreMonitor = command.getUpdateIgnoreMonitor();
        boolean ignoreMonitor = updateIgnoreMonitor.getIgnoreMonitor();

        CassandraMonitorEvent ignoreMonitorUpdated = CassandraMonitorEventFactory.ignoreMonitorUpdated(ignoreMonitor, creatorId);
        persist(ignoreMonitorUpdated, () -> reply(CassandraMonitorCommandFactory.ackAccept(command)));
    }

    private void handleDeleteCassandraNode(CassandraMonitorCommand command) throws Exception {
        CassandraMonitorCommand.Delete delete = command.getDelete();
        String ipAddress = delete.getIpAddress();
        UUID actingUserId = uuidFrom(command.getActingUserId());

        if (!validateExistenceAndPermission(ipAddress, actingUserId, command)) {
            return;
        }
        CassandraMonitorEvent cassandraMonitorEvent = CassandraMonitorEventFactory.nodeDeleted(ipAddress, actingUserId);
        persist(cassandraMonitorEvent, () -> reply(CassandraMonitorCommandFactory.ackAccept(command)));
    }

    private void handleDeleteAllCassandraNode(CassandraMonitorCommand command) throws Exception {
        UUID actingUserId = uuidFrom(command.getActingUserId());
        CassandraMonitorEvent deleteAll = CassandraMonitorEventFactory.deletedAll(actingUserId);
        persist(deleteAll, () -> reply(CassandraMonitorCommandFactory.ackAccept(command)));
    }

    //queries
    private void queryCassandraMonitor(CassandraMonitorQuery cassandraMonitorQuery) {
        try {
            final User.Role role = userRole(uuidFrom(cassandraMonitorQuery.getActingUserId()));
            final CassandraMonitor.CassandraMonitorResponse reply;
            switch (cassandraMonitorQuery.getQueryCase()) {
                case MONITORINFO:
                    isAdmin(role);
                    CassandraMonitorInfo.Builder builder = CassandraMonitorInfo.newBuilder();
                    this.data.ipAddressNodeInfo().values()
                        .forEach(node -> {
                            if(Instant.now().getEpochSecond() - node.getLastTimeCommunicated() > offlineTimeLimit) {
                                CassandraNodeInfo updatedStatus = CassandraNodeInfo.newBuilder().mergeFrom(node)
                                    .setNodeStatus(NodeStatus.OFFLINE)
                                    .build();
                                builder.addCassandraNodeInfos(updatedStatus);
                            } else {
                                builder.addCassandraNodeInfos(node);
                            }
                        });
                    builder.setCutoffThresholdPercent(this.data.cutOff())
                        .setIgnoreMonitor(this.data.ignoreMonitor());
                    CassandraMonitorInfo cassandraMonitorInfo = builder.build();
                    reply = CassandraMonitorResponse.newBuilder()
                        .setCassandraMonitorInfo(cassandraMonitorInfo)
                        .build();
                    break;
                case CHECKFULL: {
                    float cutOff = this.data.cutOff();
                    if (this.data.ignoreMonitor()) {
                        reply = CassandraMonitorResponse.newBuilder()
                            .setIsFull(false)
                            .build();
                    } else {
                        boolean isFull = this.data.ipAddressNodeInfo().size() == 0 ||
                            this.data.ipAddressNodeInfo().values()
                                .stream().anyMatch(node -> Instant.now().getEpochSecond() - node.getLastTimeCommunicated() > offlineTimeLimit ||
                                node.getNodeDriveInfosList().stream()
                                    .anyMatch(drive -> (float) drive.getUsedDiskBytes() / drive.getTotalDiskBytes() >= cutOff));
                        reply = CassandraMonitorResponse.newBuilder()
                            .setIsFull(isFull)
                            .build();
                    }
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected CassandraMonitorQuery " + cassandraMonitorQuery.getQueryCase());
            }
            reply(reply);
        } catch(Exception e) {
            log.error("Unable to CassandraMonitorQuery cassandra monitor caused by", e);
            reply(e);
        }
    }

    private void isAdmin(User.Role role) {
        if (!role.equals(User.Role.SYS_ADMIN)) {
            throw new IllegalStateException("Cassandra Node permission denied");
        }
    }

    //validations
    private boolean validateExistenceAndPermission(String ipAddress, UUID userId, CassandraMonitorCommand command) {
        boolean exists = data.ipAddressNodeInfo().get(ipAddress) != null;
        if (!exists) {
            reply(CassandraMonitorCommandFactory.ackReject(command, "Cassandra Node not found"));
            return false;
        }
        boolean hasPermission = userId.toString().equals(UserManager.ADMIN_USERID);
        if (!hasPermission) {
            reply(CassandraMonitorCommandFactory.ackReject(command, "Cassandra Node permission denied"));
            return false;
        }
        return true;
    }
}
