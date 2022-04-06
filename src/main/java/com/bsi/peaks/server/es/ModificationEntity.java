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
import com.bsi.peaks.data.utils.ModificationReader;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.ModificationCommandFactory;
import com.bsi.peaks.event.modification.ModificationCommand;
import com.bsi.peaks.event.modification.ModificationCommand.AddModification;
import com.bsi.peaks.event.modification.ModificationCommand.DeleteModification;
import com.bsi.peaks.event.modification.ModificationCommand.GrantPermission;
import com.bsi.peaks.event.modification.ModificationCommand.UpdateModification;
import com.bsi.peaks.event.modification.ModificationEvent;
import com.bsi.peaks.event.modification.ModificationEvent.CategorySet;
import com.bsi.peaks.event.modification.ModificationEvent.ModificationAdded;
import com.bsi.peaks.event.modification.ModificationEvent.ModificationDeleted;
import com.bsi.peaks.event.modification.ModificationEvent.ModificationUpdated;
import com.bsi.peaks.event.modification.ModificationEvent.PermissionGranted;
import com.bsi.peaks.event.modification.ModificationQuery;
import com.bsi.peaks.event.modification.ModificationResponse;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.model.proto.ModificationCategory;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.es.modification.ModificationStateWrapper;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class ModificationEntity extends PeaksCommandPersistentActor<ModificationStateWrapper, ModificationEvent, ModificationCommand> {
    public static final String NAME = "Modification";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    protected ModificationEntity() {
        super(ModificationEvent.class);
    }

    public static void start(ActorSystem system) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(ModificationEntity.class),
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

    @NotNull
    public static String eventTag(ModificationEvent.EventCase eventCase) {
        return "ModificationEvent." + eventCase.name();
    }

    @NotNull
    public static String commandTag(ModificationCommand.CommandCase commandCase) {
        return "ModificationCommand." + commandCase.name();
    }

    @Override
    protected boolean ignoreEventOnRecovery(ModificationEvent event) {
        if (!event.hasModificationAdded()) return true;
        return !event.getModificationAdded().getModificationOrBuilder().getBuiltin(); //skip built in modifications we will just read from xml on startup always
    }

    @Override
    protected ModificationStateWrapper defaultState() {
        try {
            ModificationStateWrapper state = new ModificationStateWrapper();
            Map<ModificationCategory, List<Modification>> modifications = ModificationReader.read();
            for (Map.Entry<ModificationCategory, List<Modification>> entry : modifications.entrySet()) {
                List<Modification> list = entry.getValue();
                for (Modification modification : list) {
                    ModificationEvent addEvent = eventBuilder(UserManager.ADMIN_USERID)
                        .setModificationAdded(ModificationAdded.newBuilder()
                            .setModification(modification)
                            .build()
                        ).build();
                    state = state.addModification(addEvent);
                }
            }
            return state;
        } catch (Exception e) {
            log.error("Unable to create builtin modifications", e);
            System.exit(1);
        }
        return new ModificationStateWrapper();
    }

    @Override
    protected ModificationStateWrapper nextState(ModificationStateWrapper state, ModificationEvent event) {
        return state.update(event);
    }

    @Override
    protected String commandTag(ModificationCommand command) {
        return commandTag(command.getCommandCase());
    }

    @Override
    public String persistenceId() {
        return NAME;
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(ModificationCommand.class, ModificationCommand::hasAddModification, this::handleAddModification)
            .match(ModificationCommand.class, ModificationCommand::hasDeleteModification, this::handleDeleteModification)
            .match(ModificationCommand.class, ModificationCommand::hasUpdateModification, this::handleUpdateModification)
            .match(ModificationCommand.class, ModificationCommand::hasGrantPermission, this::handleGrantPermission)
            .match(ModificationQuery.class, ModificationQuery::hasQueryModificationById, this::queryModificationsById)
            .match(ModificationQuery.class, ModificationQuery::hasQueryModificationByName, this::queryModficationsByName)
            .match(ModificationQuery.class, ModificationQuery::hasQueryAllBuiltInModification, this::queryAllBuiltInModifications);
    }

    @Override
    protected long deliveryId(ModificationCommand command) {
        return command.getDeliveryId();
    }

    private void handleAddModification(ModificationCommand command) {
        try {
            final AddModification addModification = command.getAddModification();
            final Modification modification = addModification.getModification();
            final ModificationEvent modificationAdded = eventBuilder(command.getUserId())
                .setModificationAdded(ModificationAdded.newBuilder()
                    .setModification(modification)
                    .build()
                )
                .build();

            data.validateAddModification(modificationAdded.getModificationAdded());
            persist(modificationAdded, () -> reply(ModificationCommandFactory.ackAccept(command)));
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void handleUpdateModification(ModificationCommand command) throws Exception {
        try {
            final String userId = command.getUserId();
            final UpdateModification updateModification = command.getUpdateModification();
            final Modification modification = updateModification.getModification();

            final Optional<String> optionalPermission = data.getPermissionByName(updateModification.getName());
            if (optionalPermission.isPresent()) {
                final String permission = optionalPermission.get();
                if (!permission.equals(userId) && userRole(UUID.fromString(userId)) != User.Role.SYS_ADMIN) {
                    throw new IllegalArgumentException("Not authoried to update modification");
                }
            } else {
                throw new IllegalArgumentException("Modification to update does not exist");
            }

            final Modification oldModification = data.getModificationByName(updateModification.getName())
                .orElseThrow(() -> new IllegalStateException("Modification to update does not exist"));

            if (oldModification.getBuiltin()) {
                throw new IllegalArgumentException("Modification is built in, cannot be modified");
            }

            final ModificationEvent modificaitonUpdated = eventBuilder(userId)
                .setModificationUpdated(ModificationUpdated.newBuilder()
                    .setName(updateModification.getName())
                    .setModification(modification)
                    .build())
                .build();

            persist(modificaitonUpdated, () -> reply(ModificationCommandFactory.ackAccept(command)));
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void handleDeleteModification(ModificationCommand command) {
        try {
            final DeleteModification deleteModification = command.getDeleteModification();
            final String name = deleteModification.getName();
            ModificationEvent modificationDeleted = eventBuilder(command.getUserId())
                .setModificationDeleted(ModificationDeleted.newBuilder()
                    .setName(deleteModification.getName())
                    .build())
                .build();

            data.validateDeleteModification(modificationDeleted);
            persist(modificationDeleted, () -> reply(ModificationCommandFactory.ackAccept(command)));
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void handleGrantPermission(ModificationCommand command) throws Exception {
        try {
            final GrantPermission grantPermission = command.getGrantPermission();
            final String newOwnerId = grantPermission.getNewOwnerId();
            final String name = grantPermission.getName();
            ModificationEvent permissionGrant = eventBuilder(command.getUserId())
                .setPermissionGranted(PermissionGranted.newBuilder()
                    .setName(name)
                    .setNewOwnerId(newOwnerId)
                    .build())
                .build();

            data.validateGrantPermission(permissionGrant);
            persist(permissionGrant, () -> reply(ModificationCommandFactory.ackAccept(command)));
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void queryModificationsById(ModificationQuery query) {
        try {
            UUID userId = UUID.fromString(query.getUserId());
            List<Modification> modifications = data.getModificationsByUser(userId, userRole(userId));
            Map<String, String> permissions = new HashMap<>();
            modifications.forEach(modification -> {
                String name = modification.getName();
                String creatorId = data.getPermissionByName(name).orElseThrow(() -> new IllegalStateException("Permission not found"));
                permissions.put(name, creatorId);
            });
            reply(buildResponse(modifications, permissions));
        } catch (Exception e) {
            log.error("Unable to query modifications caused by", e);
            reply(e);
        }
    }

    private void queryModficationsByName(ModificationQuery query) {
        try {
            List<Modification> modificationByName = data.getModificationByName(query.getQueryModificationByName().getNamesList());
                    Map<String, String> modificationPermissionsByName =data.getModificationPermissionsByName(query.getQueryModificationByName().getNamesList());
            reply(buildResponse(modificationByName, modificationPermissionsByName));
        } catch (Exception e) {
            log.error("Unable to query modifications caused by", e);
            reply(e);
        }
    }

    private void queryAllBuiltInModifications(ModificationQuery query) {
        try {
            reply(buildResponse(data.getAllBuiltInModifications(), data.getAllBuiltInModificationsPermissions()));
        } catch (Exception e) {
            log.error("Unable to query modifications caused by", e);
            reply(e);
        }
    }

    private static ModificationResponse buildResponse(Iterable<Modification> enzyme, Map<String, String> nameToOwnerId) {
        return ModificationResponse.newBuilder()
            .addAllModification(enzyme)
            .putAllPermission(nameToOwnerId)
            .build();
    }

    private static ModificationEvent.Builder eventBuilder(String userId) {
        Instant now = Instant.now();
        return ModificationEvent.newBuilder()
            .setTimeStamp(CommonFactory.convert(Instant.now()))
            .setUserId(userId);
    }
}