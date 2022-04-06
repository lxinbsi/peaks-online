package com.bsi.peaks.server.es;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.BackoffSupervisor;
import com.bsi.peaks.data.utils.LabeledQMethodReader;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.modification.ModificationCommand;
import com.bsi.peaks.event.modification.ModificationEvent;
import com.bsi.peaks.event.reportionqmethod.Create;
import com.bsi.peaks.event.reportionqmethod.Delete;
import com.bsi.peaks.event.reportionqmethod.GrantPermission;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodCommand;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodEvent;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodQuery;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodResponse;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodState;
import com.bsi.peaks.event.reportionqmethod.Update;
import com.bsi.peaks.model.dto.peptide.ReporterIonQMethod;
import com.bsi.peaks.model.dto.peptide.ReporterIonQMethodEntry;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

public class ReporterIonQMethodManager extends PeaksCommandPersistentActor<ReporterIonQMethodState, ReporterIonQMethodEvent, ReporterIonQMethodCommand> {

    public static final String NAME = "ReporterIonQMethod";
    public static final String PERSISTENCE_ID = "ReporterIonQMethodManager";

    public static void start(ActorSystem system) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(ReporterIonQMethodManager.class),
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

    protected ReporterIonQMethodManager() {
        super(ReporterIonQMethodEvent.class);
    }

    @NotNull
    public static String eventTag(ModificationEvent.EventCase eventCase) {
        return NAME + "Event." + eventCase.name();
    }

    @NotNull
    public static String commandTag(ModificationCommand.CommandCase commandCase) {
        return NAME + "Command." + commandCase.name();
    }

    @Override
    protected ReporterIonQMethodState defaultState() {
        final ReporterIonQMethodState.Builder builder = ReporterIonQMethodState.newBuilder();
        for (ReporterIonQMethod reporterIonQMethod : LabeledQMethodReader.readReportIonQMethods()) {
            builder.putEntries(
                reporterIonQMethod.getMethodName(),
                ReporterIonQMethodEntry.newBuilder()
                    .setOwnerId(UserManager.ADMIN_USERID)
                    .setBuiltIn(true)
                    .setData(reporterIonQMethod)
                    .build()
            );
        }
        return builder.build();
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(ReporterIonQMethodCommand.class, ReporterIonQMethodCommand::hasCreate, this::handleCreate)
            .match(ReporterIonQMethodCommand.class, ReporterIonQMethodCommand::hasDelete, this::handleDelete)
            .match(ReporterIonQMethodCommand.class, ReporterIonQMethodCommand::hasUpdate, this::handleUpdate)
            .match(ReporterIonQMethodCommand.class, ReporterIonQMethodCommand::hasGrantPermission, this::handleGrantPermission)
            .match(ReporterIonQMethodQuery.class, ReporterIonQMethodQuery::hasAll, this::queryAll)
            .match(ReporterIonQMethodQuery.class, ReporterIonQMethodQuery::hasByName, this::queryByName);
    }

    @Override
    protected long deliveryId(ReporterIonQMethodCommand command) {
        return command.getDeliveryId();
    }

    private boolean hasWritePermission(UUID userId, String name) {
        if (!data.containsEntries(name)) {
            return true;
        }
        return hasWritePermission(userId, data.getEntriesOrThrow(name));
    }

    private boolean hasWritePermission(UUID userId, ReporterIonQMethodEntry qMethodEntry) {
        if (qMethodEntry.getBuiltIn()) {
            return false;
        }
        switch (userRole(userId)) {
            case SYS_ADMIN:
                return true;
            default:
                return qMethodEntry.getOwnerId().equals(userId.toString());
        }
    }

    private boolean hasReadPermission(String userId, ReporterIonQMethodEntry qMethodEntry) {
        if (qMethodEntry.getBuiltIn() || userId.equals(UserManager.ADMIN_USERID)) {
            return true;
        }
        final String ownerId = qMethodEntry.getOwnerId();
        return ownerId.equals(UserManager.ADMIN_USERID)
            || ownerId.equals(userId);
    }

    private boolean isSame(ReporterIonQMethod qMethod) {
        final String name = qMethod.getMethodName();
        return data.containsEntries(name) && data.getEntriesOrThrow(name).getData().equals(qMethod);
    }

    private static ReporterIonQMethodEvent.Builder builder(String userId) {
        return ReporterIonQMethodEvent.newBuilder()
            .setUserId(userId)
            .setTimeStamp(CommonFactory.nowTimeStamp());
    }

    private void handleCreate(ReporterIonQMethodCommand command) {
        try {
            final UUID userId = UUID.fromString(command.getUserId());
            final ReporterIonQMethod qMethod = command.getCreate().getData();
            final String name = qMethod.getMethodName();
            if (!data.containsEntries(name)) {
                final ReporterIonQMethodEvent event = builder(command.getUserId())
                    .setCreate(command.getCreate())
                    .build();
                persist(event, () -> reply(ackAccept(command)));
            } else if (hasWritePermission(userId, name) && isSame(qMethod)) {
                reply(ackAccept(command)); // This will happen if same command is sent twice in row.
            } else {
                reply(ackReject(command, "Name already in use"));
            }
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void handleDelete(ReporterIonQMethodCommand command) {
        final Delete delete = command.getDelete();
        try {
            final String name = delete.getName();
            if (data.containsEntries(name)) {
                final UUID userId = UUID.fromString(command.getUserId());
                if (hasWritePermission(userId, name)) {
                    final ReporterIonQMethodEvent event = builder(command.getUserId())
                        .setDelete(delete)
                        .build();
                    persist(event, () -> reply(ackAccept(command)));
                } else {
                    reply(ackReject(command, "Permission denied"));
                }
            } else {
                reply(ackAccept(command));
            }
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void handleUpdate(ReporterIonQMethodCommand command) {
        final Update update = command.getUpdate();
        try {
            final UUID userId = UUID.fromString(command.getUserId());
            final ReporterIonQMethod qMethod = update.getData();
            final String oldName = update.getName();
            final String newName = qMethod.getMethodName();
            if (oldName.equals(newName)) {
                final String name = oldName;
                if (this.data.containsEntries(name)) {
                    if (hasWritePermission(userId, name)) {
                        if (isSame(qMethod)) {
                            reply(ackAccept(command));
                        } else {
                            final ReporterIonQMethodEvent event = builder(command.getUserId())
                                .setUpdate(update)
                                .build();
                            persist(event, () -> reply(ackAccept(command)));
                        }
                    } else {
                        reply(ackReject(command, "Permission Denied"));
                    }
                } else {
                    reply(ackReject(command, "Does not exist"));
                }
            } else { // Rename
                if (this.data.containsEntries(oldName)) {
                    if (this.data.containsEntries(newName)) {
                        reply(ackReject(command, "Name already in use"));
                    } else {
                        if (hasWritePermission(userId, oldName)) {
                            final ReporterIonQMethodEvent event = builder(command.getUserId())
                                .setUpdate(update)
                                .build();
                            persist(event, () -> reply(ackAccept(command)));
                        } else {
                            reply(ackReject(command, "Permission denied"));
                        }
                    }
                } else {
                    if (this.data.containsEntries(newName)) {
                        if (isSame(qMethod)) {
                            reply(ackAccept(command));
                        } else {
                            reply(ackReject(command, "Name already in use"));
                        }
                    } else {
                        reply(ackReject(command, "Does not exist"));
                    }
                }
            }
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void handleGrantPermission(ReporterIonQMethodCommand command) {
        final GrantPermission grantPermission = command.getGrantPermission();
        try {
            final UUID userId = UUID.fromString(command.getUserId());
            final String name = grantPermission.getName();
            if (hasWritePermission(userId, name)) {
                if (data.containsEntries(name)) {
                    if (data.getEntriesOrThrow(name).getOwnerId().equals(grantPermission.getNewOwnerId())) {
                        reply(ackAccept(command));
                    } else {
                        final ReporterIonQMethodEvent event = builder(command.getUserId())
                            .setGrantPermission(grantPermission)
                            .build();
                        persist(event, () -> reply(ackAccept(command)));
                    }
                } else {
                    reply(ackReject(command, "Does not exist"));
                }
            } else {
                reply(ackReject(command, "Permission denied"));
            }
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void queryAll(ReporterIonQMethodQuery query) {
        final String userId = query.getUserId();
        final ReporterIonQMethodResponse.Entries.Builder builder = ReporterIonQMethodResponse.Entries.newBuilder();
        data.getEntriesMap().forEach((name, qMethodEntry) -> {
            if (hasReadPermission(userId, qMethodEntry)) {
                builder.addEntries(qMethodEntry);
            }
        });

        reply(ReporterIonQMethodResponse.newBuilder()
            .setDeliveryId(query.getDeliveryId())
            .setEntries(builder)
            .build()
        );
    }

    private void queryByName(ReporterIonQMethodQuery query) {
        final String userId = query.getUserId();
        final ReporterIonQMethodQuery.ByName queryByName = query.getByName();
        final ReporterIonQMethodResponse response = ReporterIonQMethodResponse.newBuilder()
            .setDeliveryId(query.getDeliveryId())
            .setEntries(ReporterIonQMethodResponse.Entries.newBuilder()
                .addAllEntries(Optional
                    .ofNullable(data.getEntriesMap().get(queryByName.getName()))
                    .filter(qMethodEntry -> hasReadPermission(userId, qMethodEntry))
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList()))
                .build()
            ).build();

        reply(response);
    }

    @Override
    protected ReporterIonQMethodState nextState(ReporterIonQMethodState data, ReporterIonQMethodEvent event) throws Exception {
        final String userId = event.getUserId();
        switch (event.getEventCase()) {
            case CREATE: {
                final Create create = event.getCreate();
                final ReporterIonQMethod qMethod = create.getData();
                return data.toBuilder()
                    .putEntries(qMethod.getMethodName(), ReporterIonQMethodEntry.newBuilder()
                        .setOwnerId(userId)
                        .setBuiltIn(false)
                        .setData(qMethod)
                        .build()
                    )
                    .build();
            }
            case UPDATE: {
                final Update update = event.getUpdate();
                final ReporterIonQMethod qMethod = update.getData();
                final ReporterIonQMethodState.Builder builder = data.toBuilder();
                final String oldName = update.getName();
                final String newName = qMethod.getMethodName();
                builder.putEntries(newName, data.getEntriesOrThrow(oldName)
                    .toBuilder()
                    .setData(qMethod)
                    .build());
                if (!oldName.equals(newName)) {
                    builder.removeEntries(oldName);
                }
                return builder.build();
            }
            case DELETE: {
                final Delete delete = event.getDelete();
                return data.toBuilder()
                    .removeEntries(delete.getName())
                    .build();
            }
            case GRANTPERMISSION: {
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

    @Override
    protected String commandTag(ReporterIonQMethodCommand command) {
        return "ReporterIonQMethodManagerCommand " + command.getCommandCase().name();
    }

    @Override
    public String persistenceId() {
        return PERSISTENCE_ID;
    }
}
