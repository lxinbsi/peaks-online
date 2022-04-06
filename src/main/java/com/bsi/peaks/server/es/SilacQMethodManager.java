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
import com.bsi.peaks.event.silacqmethod.Create;
import com.bsi.peaks.event.silacqmethod.Delete;
import com.bsi.peaks.event.silacqmethod.GrantPermission;
import com.bsi.peaks.event.silacqmethod.SilacQMethodCommand;
import com.bsi.peaks.event.silacqmethod.SilacQMethodEvent;
import com.bsi.peaks.event.silacqmethod.SilacQMethodQuery;
import com.bsi.peaks.event.silacqmethod.SilacQMethodResponse;
import com.bsi.peaks.event.silacqmethod.SilacQMethodState;
import com.bsi.peaks.event.silacqmethod.Update;
import com.bsi.peaks.model.dto.peptide.SilacQMethod;
import com.bsi.peaks.model.dto.peptide.SilacQMethodEntry;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;

public class SilacQMethodManager extends PeaksCommandPersistentActor<SilacQMethodState, SilacQMethodEvent, SilacQMethodCommand> {

    public static final String NAME = "SilacIonQMethod";
    public static final String PERSISTENCE_ID = "SilacQMethodManager";

    public static void start(ActorSystem system) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(SilacQMethodManager.class),
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

    protected SilacQMethodManager() {
        super(SilacQMethodEvent.class);
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
    protected SilacQMethodState defaultState() {
        final SilacQMethodState.Builder builder = SilacQMethodState.newBuilder();
        for (SilacQMethod silacQMethod : LabeledQMethodReader.readSilacQMethods()) {
            builder.putEntries(
                silacQMethod.getMethodName(),
                SilacQMethodEntry.newBuilder()
                    .setOwnerId(UserManager.ADMIN_USERID)
                    .setBuiltIn(true)
                    .setData(calculateOrder(silacQMethod))
                    .build()
            );
        }
        return builder.build();
    }

    private SilacQMethod calculateOrder(SilacQMethod silacQMethod) {
        SilacQMethod.Builder builder = silacQMethod.toBuilder()
            .clearLabels()
            .clearLabelOrder();
        silacQMethod.getLabelsList().stream()
            .sorted(Comparator.comparingDouble(label -> label.hasModification() ? label.getModification().getMonoMass() : 0.0))
            .map(label -> {
                builder.addLabels(label);
                return label.getName();
            })
            .distinct()
            .forEach(builder::addLabelOrder);
        return builder.build();
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(SilacQMethodCommand.class, SilacQMethodCommand::hasCreate, this::handleCreate)
            .match(SilacQMethodCommand.class, SilacQMethodCommand::hasDelete, this::handleDelete)
            .match(SilacQMethodCommand.class, SilacQMethodCommand::hasUpdate, this::handleUpdate)
            .match(SilacQMethodCommand.class, SilacQMethodCommand::hasGrantPermission, this::handleGrantPermission)
            .match(SilacQMethodQuery.class, SilacQMethodQuery::hasAll, this::queryAll)
            .match(SilacQMethodQuery.class, SilacQMethodQuery::hasByName, this::queryByName);
    }

    @Override
    protected long deliveryId(SilacQMethodCommand command) {
        return command.getDeliveryId();
    }

    private boolean hasWritePermission(UUID userId, String name) {
        if (!data.containsEntries(name)) {
            return true;
        }
        return hasWritePermission(userId, data.getEntriesOrThrow(name));
    }

    private boolean hasWritePermission(UUID userId, SilacQMethodEntry qMethodEntry) {
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

    private boolean hasReadPermission(String userId, SilacQMethodEntry qMethodEntry) {
        if (qMethodEntry.getBuiltIn() || userId.equals(UserManager.ADMIN_USERID)) {
            return true;
        }
        final String ownerId = qMethodEntry.getOwnerId();
        return ownerId.equals(UserManager.ADMIN_USERID)
            || ownerId.equals(userId);
    }

    private boolean isSame(SilacQMethod qMethod) {
        final String name = qMethod.getMethodName();
        return data.containsEntries(name) && data.getEntriesOrThrow(name).getData().equals(qMethod);
    }

    private static SilacQMethodEvent.Builder builder(String userId) {
        return SilacQMethodEvent.newBuilder()
            .setUserId(userId)
            .setTimeStamp(CommonFactory.nowTimeStamp());
    }

    private void handleCreate(SilacQMethodCommand command) {
        final Create create = command.getCreate();
        try {
            final UUID userId = UUID.fromString(command.getUserId());
            final SilacQMethod qMethod = create.getData();
            final String name = qMethod.getMethodName();
            if (!data.containsEntries(name)) {
                final SilacQMethodEvent event = builder(command.getUserId())
                    .setCreate(create.toBuilder().setData(calculateOrder(qMethod)))
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

    private void handleDelete(SilacQMethodCommand command) {
        final Delete delete = command.getDelete();
        try {
            final String name = delete.getName();
            if (data.containsEntries(name)) {
                final UUID userId = UUID.fromString(command.getUserId());
                if (hasWritePermission(userId, name)) {
                    final SilacQMethodEvent event = builder(command.getUserId())
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

    private void handleUpdate(SilacQMethodCommand command) {
        final Update update = command.getUpdate();
        try {
            final UUID userId = UUID.fromString(command.getUserId());
            final SilacQMethod qMethod = update.getData();
            final String oldName = update.getName();
            final String newName = qMethod.getMethodName();
            if (oldName.equals(newName)) {
                final String name = oldName;
                if (this.data.containsEntries(name)) {
                    if (hasWritePermission(userId, name)) {
                        if (isSame(qMethod)) {
                            reply(ackAccept(command));
                        } else {
                            final SilacQMethodEvent event = builder(command.getUserId())
                                .setUpdate(update.toBuilder().setData(calculateOrder(qMethod)))
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
                            final SilacQMethodEvent event = builder(command.getUserId())
                                .setUpdate(update.toBuilder().setData(calculateOrder(qMethod)))
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

    private void handleGrantPermission(SilacQMethodCommand command) {
        final GrantPermission grantPermission = command.getGrantPermission();
        try {
            final UUID userId = UUID.fromString(command.getUserId());
            final String name = grantPermission.getName();
            if (hasWritePermission(userId, name)) {
                if (data.containsEntries(name)) {
                    if (data.getEntriesOrThrow(name).getOwnerId().equals(grantPermission.getNewOwnerId())) {
                        reply(ackAccept(command));
                    } else {
                        final SilacQMethodEvent event = builder(command.getUserId())
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

    private void queryAll(SilacQMethodQuery query) {
        final String userId = query.getUserId();
        final SilacQMethodResponse.Entries.Builder builder = SilacQMethodResponse.Entries.newBuilder();
        data.getEntriesMap().forEach((name, qMethodEntry) -> {
            if (hasReadPermission(userId, qMethodEntry)) {
                builder.addEntries(qMethodEntry);
            }
        });

        reply(SilacQMethodResponse.newBuilder()
            .setDeliveryId(query.getDeliveryId())
            .setEntries(builder)
            .build()
        );
    }

    private void queryByName(SilacQMethodQuery query) {
        final String userId = query.getUserId();
        final SilacQMethodQuery.ByName queryByName = query.getByName();
        final SilacQMethodResponse response = SilacQMethodResponse.newBuilder()
            .setDeliveryId(query.getDeliveryId())
            .setEntries(SilacQMethodResponse.Entries.newBuilder()
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
    protected SilacQMethodState nextState(SilacQMethodState data, SilacQMethodEvent event) throws Exception {
        final String userId = event.getUserId();
        switch (event.getEventCase()) {
            case CREATE: {
                final Create create = event.getCreate();
                final SilacQMethod qMethod = create.getData();
                return data.toBuilder()
                    .putEntries(qMethod.getMethodName(), SilacQMethodEntry.newBuilder()
                        .setOwnerId(userId)
                        .setBuiltIn(false)
                        .setData(qMethod)
                        .build()
                    )
                    .build();
            }
            case UPDATE: {
                final Update update = event.getUpdate();
                final SilacQMethod qMethod = update.getData();
                final SilacQMethodState.Builder builder = data.toBuilder();
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
    protected String commandTag(SilacQMethodCommand command) {
        return "SilacQMethodManagerCommand " + command.getCommandCase().name();
    }

    @Override
    public String persistenceId() {
        return PERSISTENCE_ID;
    }
}
