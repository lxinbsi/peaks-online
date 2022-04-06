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
import com.bsi.peaks.data.utils.EnzymeReader;
import com.bsi.peaks.event.EnzymeCommandFactory;
import com.bsi.peaks.event.EnzymeEventFactory;
import com.bsi.peaks.event.enzyme.AddEnzyme;
import com.bsi.peaks.event.enzyme.DeleteEnzyme;
import com.bsi.peaks.event.enzyme.EnzymeAdded;
import com.bsi.peaks.event.enzyme.EnzymeCommand;
import com.bsi.peaks.event.enzyme.EnzymeDeleted;
import com.bsi.peaks.event.enzyme.EnzymeEvent;
import com.bsi.peaks.event.enzyme.EnzymeEventAck;
import com.bsi.peaks.event.enzyme.EnzymeGrantPermission;
import com.bsi.peaks.event.enzyme.EnzymePermissionGranted;
import com.bsi.peaks.event.enzyme.EnzymeQuery;
import com.bsi.peaks.event.enzyme.EnzymeResponse;
import com.bsi.peaks.event.enzyme.EnzymeState;
import com.bsi.peaks.event.enzyme.EnzymeUpdated;
import com.bsi.peaks.event.enzyme.UpdateEnzyme;
import com.bsi.peaks.model.proto.Enzyme;
import com.bsi.peaks.model.system.User;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;

public class EnzymeManager extends PeaksPersistentActor<EnzymeState, EnzymeEvent, EnzymeEventAck, EnzymeCommand> {
    public static final String NAME = "Enzyme";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static void start(ActorSystem system) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(EnzymeManager.class),
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

    protected EnzymeManager(String prefix, Class<EnzymeEvent> eventClass) {
        super(eventClass);
    }

    @Override
    protected boolean ignoreEventOnRecovery(EnzymeEvent event) {
        if (!event.hasEnzymeAdded()) return true;
        return !event.getEnzymeAdded().getEnzyme().getBuiltIn();
    }

    protected EnzymeManager() {
        super(EnzymeEvent.class);
    }

    @Override
    protected EnzymeState defaultState() {
        try {
            EnzymeState state = EnzymeState.getDefaultInstance();
            List<Enzyme> builtInEnzymes = new EnzymeReader().read();
            for(Enzyme enzyme : builtInEnzymes) {
                EnzymeEvent event = EnzymeEventFactory.enzymeAdded(enzyme, ADMIN_USERID);
                state = addEnzyme(state, event);
            }
            return state;
        } catch (Exception e) {
            log.error("Unable to create builtin enzymes", e);
            System.exit(1);
        }
        return EnzymeState.getDefaultInstance();
    }

    @Override
    protected EnzymeState nextState(EnzymeState state, EnzymeEvent event) throws Exception {
        switch (event.getEventCase()) {
            case ENZYMEADDED:
                return addEnzyme(state, event);
            case ENZYMEDELETED:
                return deleteEnzyme(state, event);
            case ENZYMEUPDATED:
                return updateEnzyme(state, event);
            case ENZYMEPERMISSIONGRANTED:
                return enzymeGrantPermission(state, event);
            default:
                throw new IllegalArgumentException("Invalid enzyme event");
        }
    }

    @Override
    protected String eventTag(EnzymeEvent event) {
        return eventTag(event.getEventCase());
    }
    
    @NotNull
    public static String eventTag(EnzymeEvent.EventCase eventCase) {
        return "EnzymeEvent." + eventCase.name();
    }

    @Override
    protected String commandTag(EnzymeCommand command) {
        return commandTag(command.getCommandCase());
    }
    
    @NotNull
    public static String commandTag(EnzymeCommand.CommandCase commandCase) {
        return "EnzymeCommand." + commandCase.name();
    }

    @Override
    public String persistenceId() {
        return NAME;
    }
    
    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(EnzymeCommand.class, EnzymeCommand::hasAddEnzyme, commandAckOnFailure(this::handleAddEnzyme))
            .match(EnzymeCommand.class, EnzymeCommand::hasDeleteEnzyme, commandAckOnFailure(this::handleDeleteEnzyme))
            .match(EnzymeCommand.class, EnzymeCommand::hasUpdateEnzyme, commandAckOnFailure(this::handleUpdateEnzyme))
            .match(EnzymeCommand.class, EnzymeCommand::hasEnzymeGrantPermission, commandAckOnFailure(this::handleGrantPermission))
            .match(EnzymeQuery.class, EnzymeQuery::hasQueryEnzymeById, this::queryEnzymeById)
            .match(EnzymeQuery.class, EnzymeQuery::hasQueryEnzymeByName, this::queryEnzymeByName)
            .match(EnzymeQuery.class, EnzymeQuery::hasQueryAllEnzymes, this::queryAllEnzymes);
    }

    @Override
    protected long deliveryId(EnzymeCommand command) {
        return command.getDeliveryId();
    }

    //handlers
    private void handleAddEnzyme(EnzymeCommand command) throws Exception {
        final AddEnzyme addEnzyme = command.getAddEnzyme();
        final Enzyme enzyme = addEnzyme.getEnzyme();
        final EnzymeEvent enzymeAdded = EnzymeEventFactory.enzymeAdded(enzyme, command.getActingUserId());

        validateAddEnzyme(enzymeAdded, enzyme.getName());
        persist(enzymeAdded, () -> reply(EnzymeCommandFactory.ackAccept(command)));
    }

    private void handleUpdateEnzyme(EnzymeCommand command) throws Exception {
        final UpdateEnzyme updateEnzyme = command.getUpdateEnzyme();
        final Enzyme Enzyme = updateEnzyme.getEnzyme();
        final EnzymeEvent enzymeUpdate = EnzymeEventFactory.enzymeUpdated(Enzyme, command.getActingUserId());


        validateUpdateEnzyme(enzymeUpdate);
        persist(enzymeUpdate, () -> reply(EnzymeCommandFactory.ackAccept(command)));
    }

    private void handleDeleteEnzyme(EnzymeCommand command) throws Exception {
        final DeleteEnzyme deleteEnzyme = command.getDeleteEnzyme();
        final String name = deleteEnzyme.getName();
        EnzymeEvent EnzymeDeleted = EnzymeEventFactory.enzymeDeleted(deleteEnzyme.getName(), command.getActingUserId());

        validateDeleteEnzyme(EnzymeDeleted);
        persist(EnzymeDeleted, () -> reply(EnzymeCommandFactory.ackAccept(command)));
    }

    private void handleGrantPermission(EnzymeCommand command) throws Exception {
        final EnzymeGrantPermission grantPermission = command.getEnzymeGrantPermission();
        final String newOwnerId = grantPermission.getNewOwnerId();
        final String name = grantPermission.getName();
        EnzymeEvent permissionGrant = EnzymeEventFactory.enzymeGrantPermission(name, command.getActingUserId(), newOwnerId);

        validateGrantPermission(permissionGrant);
        persist(permissionGrant, () -> reply(EnzymeCommandFactory.ackAccept(command)));
    }

    //queries
    private void queryEnzymeById(EnzymeQuery query) {
        try {
            UUID userId = UUID.fromString(query.getActingUserId());
            List<Enzyme> enzmes = getEnzymesByUser(userId, userRole(userId));
            Map<String, String> permissions = new HashMap<>();
            enzmes.forEach(enzyme -> {
                String name = enzyme.getName();
                String creatorId = getOwnerByName(name).orElseThrow(() -> new IllegalStateException("Permission not found"));
                permissions.put(name, creatorId);
            });
            reply(buildResponse(enzmes, permissions));
        } catch(Exception e) {
            log.error("Unable to query enzymes caused by", e);
            reply(e);
        }
    }


    private void queryEnzymeByName(EnzymeQuery query) {
        try {
            reply(buildResponse(
                    getEnzymeByName(query.getQueryEnzymeByName().getNamesList()),
                    getEnzymeNameToOwnerByNames(query.getQueryEnzymeByName().getNamesList())
            ));
        } catch (Exception e) {
            log.error("Unable to query enzymes caused by", e);
            reply(e);
        }
    }

    private void queryAllEnzymes(EnzymeQuery query) {
        try {
            reply(buildResponse(data.getNameToEnzymeMap().values(), data.getNameToOwnerIdMap()));
        } catch (Exception e) {
            log.error("Unable to query enzymes caused by", e);
            reply(e);
        }
    }

    //validations
    private void validateUpdateEnzyme(EnzymeEvent event) throws IllegalStateException {
        EnzymeUpdated EnzymeUpdated = event.getEnzymeUpdated();
        List<String> Enzymes = this.getEnzymesByUser(UUID.fromString(event.getActingUserId()), null).stream()
            .map(Enzyme::getName).collect(Collectors.toList());
        if (Enzymes.contains(EnzymeUpdated.getName())) {
            EnzymeEvent EnzymeAddedEvent = EnzymeEvent.newBuilder()
                .mergeFrom(event)
                .setEnzymeAdded(EnzymeAdded.newBuilder()
                    .setEnzyme(EnzymeUpdated.getEnzyme())
                    .build())
                .build();
            validateAddEnzyme(event, EnzymeUpdated.getName());
        } else {
            throw new IllegalArgumentException("Enzyme to update does not exist");
        }
    }

    private void validateAddEnzyme(EnzymeEvent event, String name) throws IllegalStateException {
        EnzymeAdded EnzymeAdded = event.getEnzymeAdded();
        Enzyme enzyme = EnzymeAdded.getEnzyme();
        if (this.data.getNameToEnzymeMap().containsKey(enzyme.getName())) {
            if (name == null || enzyme.getName().equals(name)) {
                throw new IllegalArgumentException("Duplicate Enzyme name is not allowed");
            }
        }
    }

    private void validateDeleteEnzyme(EnzymeEvent event) throws IllegalStateException {
        EnzymeDeleted EnzymeDeleted = event.getEnzymeDeleted();
        List<String> enzymes = this.getEnzymesByUser(UUID.fromString(event.getActingUserId()), null).stream()
            .map(Enzyme::getName)
            .collect(Collectors.toList());
        if (!enzymes.contains(EnzymeDeleted.getName())) {
            throw new IllegalArgumentException("Enzyme to delete does not exist");
        }
    }

    private void validateGrantPermission(EnzymeEvent event) throws IllegalStateException {
        EnzymePermissionGranted permissionGranted = event.getEnzymePermissionGranted();
        final String name = permissionGranted.getName();
        final String oldOwnerId = event.getActingUserId();
        List<String> enzymes = this.getEnzymesByUser(UUID.fromString(oldOwnerId), null).stream()
            .map(Enzyme::getName)
            .collect(Collectors.toList());
        if (!data.getNameToEnzymeMap().containsKey(name) || !data.getNameToOwnerIdMap().containsKey(name)) {
            throw new IllegalStateException("Enzyme to grant permission does not exist");
        }
    }

    private List<Enzyme> getEnzymesByUser(UUID userId, User.Role role) {
        return this.data.getNameToEnzymeMap().values().stream()
            .filter(enzyme -> {
                return (role != null && (role.equals(User.Role.SYS_ADMIN)) ||
                    userId.toString().equals(ADMIN_USERID) ||
                    userId.toString().equals(this.data.getNameToOwnerIdMap().get(enzyme.getName())) ||
                    ADMIN_USERID.equals(this.data.getNameToOwnerIdMap().get(enzyme.getName())));
            })
            .collect(Collectors.toList());
    }

    private List<Enzyme> getAllBuiltInEnzymes() {
        return this.data.getNameToEnzymeMap().values().stream()
            .filter(enzyme -> enzyme.getBuiltIn())
            .collect(Collectors.toList());
    }

    public List<Enzyme> getEnzymeByName(List<String> names) {
        return names.stream()
            .map(name -> getEnzymeByName(name).orElseThrow(() -> new IllegalStateException("Enyzme not found")))
            .collect(Collectors.toList());
    }

    public Map<String, String> getEnzymeNameToOwnerByNames(List<String> names) {
        return names.stream()
            .map(n -> {
                getOwnerByName(n).orElseThrow(IllegalStateException::new); //verify n exists
                return n;
            })
            .collect(Collectors.toMap(n -> n, n -> data.getNameToOwnerIdMap().get(n)));
    }

    public Optional<Enzyme> getEnzymeByName(String name) {
        return Optional.of(data.getNameToEnzymeMap().get(name));
    }

    public Optional<String> getOwnerByName(String name) {
        return Optional.of(data.getNameToOwnerIdMap().get(name));
    }

    //next state transitions
    private static EnzymeState addEnzyme(EnzymeState state, EnzymeEvent event) {
        EnzymeAdded enzymeAdded = event.getEnzymeAdded();
        Enzyme enzyme = enzymeAdded.getEnzyme();
        return state.toBuilder()
            .putNameToEnzyme(enzyme.getName(), enzyme)
            .putNameToOwnerId(enzyme.getName(), event.getActingUserId())
            .build();
    }

    private static EnzymeState deleteEnzyme(EnzymeState state, EnzymeEvent event) {
        EnzymeDeleted enzymeDeleted = event.getEnzymeDeleted();
        return state.toBuilder()
            .removeNameToEnzyme(enzymeDeleted.getName())
            .removeNameToOwnerId(enzymeDeleted.getName())
            .build();
    }

    private static EnzymeState updateEnzyme(EnzymeState state, EnzymeEvent event) {
        EnzymeUpdated enzymeUpdated = event.getEnzymeUpdated();
        return state.toBuilder()
            .removeNameToEnzyme(enzymeUpdated.getName())
            .putNameToEnzyme(enzymeUpdated.getEnzyme().getName(), enzymeUpdated.getEnzyme())
            .build();
    }

    private static EnzymeState enzymeGrantPermission(EnzymeState state, EnzymeEvent event) {
        EnzymePermissionGranted permissionGranted = event.getEnzymePermissionGranted();
        return state.toBuilder()
            .removeNameToOwnerId(permissionGranted.getName())
            .putNameToOwnerId(permissionGranted.getName(), permissionGranted.getNewOwnerId())
            .build();
    }

    private static EnzymeResponse buildResponse(Iterable<Enzyme> enzyme, Map<String, String> nameToOwnerId) {
        return EnzymeResponse.newBuilder()
            .addAllEnzyme(enzyme)
            .putAllNameToOwnerId(nameToOwnerId)
            .build();
    }
}
