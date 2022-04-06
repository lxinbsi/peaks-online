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
import com.bsi.peaks.data.utils.InstrumentReader;
import com.bsi.peaks.event.InstrumentCommandFactory;
import com.bsi.peaks.event.InstrumentEventFactory;
import com.bsi.peaks.event.instrument.AddInstrument;
import com.bsi.peaks.event.instrument.DeleteInstrument;
import com.bsi.peaks.event.instrument.InstrumentAdded;
import com.bsi.peaks.event.instrument.InstrumentCommand;
import com.bsi.peaks.event.instrument.InstrumentDeleted;
import com.bsi.peaks.event.instrument.InstrumentEvent;
import com.bsi.peaks.event.instrument.InstrumentEventAck;
import com.bsi.peaks.event.instrument.InstrumentGrantPermission;
import com.bsi.peaks.event.instrument.InstrumentPermissionGranted;
import com.bsi.peaks.event.instrument.InstrumentQuery;
import com.bsi.peaks.event.instrument.InstrumentResponse;
import com.bsi.peaks.event.instrument.InstrumentState;
import com.bsi.peaks.event.instrument.InstrumentUpdated;
import com.bsi.peaks.event.instrument.UpdateInstrument;
import com.bsi.peaks.model.proto.Instrument;
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

public class InstrumentManager extends PeaksPersistentActor<InstrumentState, InstrumentEvent, InstrumentEventAck, InstrumentCommand> {
    public static final String NAME = "Instrument";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static void start(ActorSystem system) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(InstrumentManager.class),
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
    protected boolean ignoreEventOnRecovery(InstrumentEvent event) {
        if (!event.hasInstrumentAdded()) return true;
        return !event.getInstrumentAdded().getInstrument().getBuiltIn();
    }

    protected InstrumentManager(String prefix, Class<InstrumentEvent> eventClass) {
        super(eventClass);
    }


    protected InstrumentManager() {
        super(InstrumentEvent.class);
    }

    //PeaksPersistentActor implementation
    @Override
    protected InstrumentState defaultState() {
        InstrumentState state = InstrumentState.getDefaultInstance();
        try {
            List<Instrument> read = new InstrumentReader().read();
            for(Instrument instrument : read) {
                state = addInstrument(state, InstrumentEventFactory.instrumentAdded(instrument, ADMIN_USERID));
            }
            return state;
        } catch (Exception e) {
            log.error("Unable to create builtin instruments", e);
            System.exit(1);
        }
        return InstrumentState.getDefaultInstance();
    }

    @Override
    protected InstrumentState nextState(InstrumentState state, InstrumentEvent event) throws Exception {
        switch (event.getEventCase()) {
            case INSTRUMENTADDED:
                return addInstrument(state, event);
            case INSTRUMENTDELETED:
                return deleteInstrument(state, event);
            case INSTRUMENTUPDATED:
                return updateInstrument(state, event);
            case INSTRUMENTPERMISSIONGRANTED:
                return instrumentGrantPermission(state, event);
            default:
                throw new IllegalArgumentException("Invalid instrument event");
        }
    }

    @Override
    protected String eventTag(InstrumentEvent event) {
        return eventTag(event.getEventCase());
    }

    @NotNull
    public static String eventTag(InstrumentEvent.EventCase eventCase) {
        return "InstrumentEvent." + eventCase.name();
    }

    @Override
    protected String commandTag(InstrumentCommand command) {
        return commandTag(command.getCommandCase());
    }

    @NotNull
    public static String commandTag(InstrumentCommand.CommandCase commandCase) {
        return "InstrumentCommand." + commandCase.name();
    }

    @Override
    public String persistenceId() {
        return NAME;
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(InstrumentCommand.class, InstrumentCommand::hasAddInstrument, commandAckOnFailure(this::handleAddInstrument))
            .match(InstrumentCommand.class, InstrumentCommand::hasDeleteInstrument, commandAckOnFailure(this::handleDeleteInstrument))
            .match(InstrumentCommand.class, InstrumentCommand::hasUpdateInstrument, commandAckOnFailure(this::handleUpdateInstrument))
            .match(InstrumentCommand.class, InstrumentCommand::hasInstrumentGrantPermission, commandAckOnFailure(this::handleGrantPermission))
            .match(InstrumentEvent.class, InstrumentEvent::hasInstrumentAdded, validateThen(event -> validateAddInstrument(event, event.getInstrumentAdded().getInstrument().getName()), this::persistAck))
            .match(InstrumentEvent.class, InstrumentEvent::hasInstrumentDeleted, validateThen(event -> validateDeleteInstrument(event), this::persistAck))
            .match(InstrumentEvent.class, InstrumentEvent::hasInstrumentUpdated, validateThen(event -> validateUpdateInstrument(event), this::persistAck))
            .match(InstrumentEvent.class, InstrumentEvent::hasInstrumentPermissionGranted, validateThen(event -> validateGrantPermission(event), this::persistAck))
            .match(InstrumentQuery.class, InstrumentQuery::hasQueryInstrumentById, this::queryInstrumentById)
            .match(InstrumentQuery.class, InstrumentQuery::hasQueryInstrumentByName, this::queryInstrumentByName)
            .match(InstrumentQuery.class, InstrumentQuery::hasQueryAllInstruments, this::queryAllInstruments)
            .match(InstrumentQuery.class, (q) ->  {
                System.out.println("Found bad query!");
            });
    }

    @Override
    protected long deliveryId(InstrumentCommand command) {
        return command.getDeliveryId();
    }

    //handlers
    private void handleAddInstrument(InstrumentCommand command) throws Exception {
        final AddInstrument addInstrument = command.getAddInstrument();
        final Instrument instrument = addInstrument.getInstrument();
        final InstrumentEvent instrumentAdded = InstrumentEventFactory.instrumentAdded(instrument, command.getActingUserId());

        validateAddInstrument(instrumentAdded, instrument.getName());
        persist(instrumentAdded, () -> reply(InstrumentCommandFactory.ackAccept(command)));
    }

    private void handleUpdateInstrument(InstrumentCommand command) throws Exception {
        final UpdateInstrument updateInstrument = command.getUpdateInstrument();
        final Instrument Instrument = updateInstrument.getInstrument();
        final InstrumentEvent instrumentUpdate = InstrumentEventFactory.instrumentUpdated(Instrument, command.getActingUserId());


        validateUpdateInstrument(instrumentUpdate);
        persist(instrumentUpdate, () -> reply(InstrumentCommandFactory.ackAccept(command)));
    }

    private void handleDeleteInstrument(InstrumentCommand command) throws Exception {
        final DeleteInstrument deleteInstrument = command.getDeleteInstrument();
        final String name = deleteInstrument.getName();
        InstrumentEvent InstrumentDeleted = InstrumentEventFactory.instrumentDeleted(deleteInstrument.getName(), command.getActingUserId());

        validateDeleteInstrument(InstrumentDeleted);
        persist(InstrumentDeleted, () -> reply(InstrumentCommandFactory.ackAccept(command)));
    }

    private void handleGrantPermission(InstrumentCommand command) throws Exception {
        final InstrumentGrantPermission grantPermission = command.getInstrumentGrantPermission();
        final String newOwnerId = grantPermission.getNewOwnerId();
        final String name = grantPermission.getName();
        InstrumentEvent permissionGrant = InstrumentEventFactory.instrumentGrantPermission(name, command.getActingUserId(), newOwnerId);

        validateGrantPermission(permissionGrant);
        persist(permissionGrant, () -> reply(InstrumentCommandFactory.ackAccept(command)));
    }

    //queries
    private void queryInstrumentById(InstrumentQuery query) {
        try {
            UUID userId = UUID.fromString(query.getActingUserId());
            List<Instrument> instruments = getInstrumentsByUser(userId, userRole(userId));
            Map<String, String> permissions = new HashMap<>();
            instruments.forEach(instrument -> {
                String name = instrument.getName();
                String creatorId = getOwnerByName(name).orElseThrow(() -> new IllegalStateException("Permission not found"));
                permissions.put(name, creatorId);
            });
            reply(buildResponse(instruments, permissions));
        } catch(Exception e) {
            log.error("Unable to query instruments caused by", e);
            reply(e);
        }
    }


    private void queryInstrumentByName(InstrumentQuery query) {
        try {
            List<Instrument> instrumentByName = getInstrumentByName(query.getQueryInstrumentByName().getNamesList());
            Map<String, String> instrumentNameToOwnerByNames = getInstrumentNameToOwnerByNames(query.getQueryInstrumentByName().getNamesList());
            reply(buildResponse(instrumentByName, instrumentNameToOwnerByNames));
        } catch (Exception e) {
            log.error("Unable to query instruments caused by", e);
            reply(e);
        }
    }

    private void queryAllInstruments(InstrumentQuery query) {
        try {
            reply(buildResponse(data.getNameToInstrumentMap().values(), data.getNameToOwnerIdMap()));
        } catch (Exception e) {
            log.error("Unable to query instruments caused by", e);
            reply(e);
        }
    }

    //validations
    private void validateUpdateInstrument(InstrumentEvent event) throws IllegalStateException {
        InstrumentUpdated InstrumentUpdated = event.getInstrumentUpdated();
        List<String> Instruments = this.getInstrumentsByUser(UUID.fromString(event.getActingUserId()), null).stream()
            .map(Instrument::getName).collect(Collectors.toList());
        if (Instruments.contains(InstrumentUpdated.getName())) {
            InstrumentEvent InstrumentAddedEvent = InstrumentEvent.newBuilder()
                .mergeFrom(event)
                .setInstrumentAdded(InstrumentAdded.newBuilder()
                    .setInstrument(InstrumentUpdated.getInstrument())
                    .build())
                .build();
            validateAddInstrument(event, InstrumentUpdated.getName());
        } else {
            throw new IllegalArgumentException("Instrument to update does not exist");
        }
    }

    private void validateAddInstrument(InstrumentEvent event, String name) throws IllegalStateException {
        InstrumentAdded InstrumentAdded = event.getInstrumentAdded();
        Instrument instrument = InstrumentAdded.getInstrument();
        if (this.data.getNameToInstrumentMap().containsKey(instrument.getName())) {
            if (name == null || instrument.getName().equals(name)) {
                throw new IllegalArgumentException("Duplicate Instrument name is not allowed");
            }
        }
    }

    private void validateDeleteInstrument(InstrumentEvent event) throws IllegalStateException {
        InstrumentDeleted InstrumentDeleted = event.getInstrumentDeleted();
        List<String> instruments = this.getInstrumentsByUser(UUID.fromString(event.getActingUserId()), null).stream()
            .map(Instrument::getName)
            .collect(Collectors.toList());
        if (!instruments.contains(InstrumentDeleted.getName())) {
            throw new IllegalArgumentException("Instrument to delete does not exist");
        }
    }

    private void validateGrantPermission(InstrumentEvent event) throws IllegalStateException {
        InstrumentPermissionGranted permissionGranted = event.getInstrumentPermissionGranted();
        final String name = permissionGranted.getName();
        final String oldOwnerId = event.getActingUserId();
        List<String> instruments = this.getInstrumentsByUser(UUID.fromString(oldOwnerId), null).stream()
            .map(Instrument::getName)
            .collect(Collectors.toList());
        if (!data.getNameToInstrumentMap().containsKey(name) || !data.getNameToOwnerIdMap().containsKey(name)) {
            throw new IllegalStateException("Instrument to grant permission does not exist");
        }
    }

    private List<Instrument> getInstrumentsByUser(UUID userId, User.Role role) {
        return this.data.getNameToInstrumentMap().values().stream()
            .filter(instrument -> {
                return (role != null && (role.equals(User.Role.SYS_ADMIN)) ||
                    userId.toString().equals(ADMIN_USERID) ||
                    userId.toString().equals(this.data.getNameToOwnerIdMap().get(instrument.getName())) ||
                    ADMIN_USERID.equals(this.data.getNameToOwnerIdMap().get(instrument.getName())));
            })
            .collect(Collectors.toList());
    }

    private List<Instrument> getAllBuiltInInstruments() {
        return this.data.getNameToInstrumentMap().values().stream()
            .filter(instrument -> instrument.getBuiltIn())
            .collect(Collectors.toList());
    }

    public List<Instrument> getInstrumentByName(List<String> names) {
        return names.stream()
            .map(name -> getInstrumentByName(name).orElseThrow(() -> new IllegalStateException("Enyzme not found")))
            .collect(Collectors.toList());
    }

    public Map<String, String> getInstrumentNameToOwnerByNames(List<String> names) {
        return names.stream()
            .map(n -> {
                getOwnerByName(n).orElseThrow(IllegalStateException::new); //verify n exists
                return n;
            })
            .collect(Collectors.toMap(n -> n, n -> data.getNameToOwnerIdMap().get(n)));
    }

    public Optional<Instrument> getInstrumentByName(String name) {
        return Optional.of(data.getNameToInstrumentMap().get(name));
    }

    public Optional<String> getOwnerByName(String name) {
        return Optional.of(data.getNameToOwnerIdMap().get(name));
    }

    //next state transitions
    private static InstrumentState addInstrument(InstrumentState state, InstrumentEvent event) {
        InstrumentAdded instrumentAdded = event.getInstrumentAdded();
        Instrument instrument = instrumentAdded.getInstrument();
        return state.toBuilder()
            .putNameToInstrument(instrument.getName(), instrument)
            .putNameToOwnerId(instrument.getName(), event.getActingUserId())
            .build();
    }

    private static InstrumentState deleteInstrument(InstrumentState state, InstrumentEvent event) {
        InstrumentDeleted instrumentDeleted = event.getInstrumentDeleted();
        return state.toBuilder()
            .removeNameToInstrument(instrumentDeleted.getName())
            .removeNameToOwnerId(instrumentDeleted.getName())
            .build();
    }

    private static InstrumentState updateInstrument(InstrumentState state, InstrumentEvent event) {
        InstrumentUpdated instrumentUpdated = event.getInstrumentUpdated();
        return state.toBuilder()
            .removeNameToInstrument(instrumentUpdated.getName())
            .putNameToInstrument(instrumentUpdated.getInstrument().getName(), instrumentUpdated.getInstrument())
            .build();
    }

    private static InstrumentState instrumentGrantPermission(InstrumentState state, InstrumentEvent event) {
        InstrumentPermissionGranted permissionGranted = event.getInstrumentPermissionGranted();
        return state.toBuilder()
            .removeNameToOwnerId(permissionGranted.getName())
            .putNameToOwnerId(permissionGranted.getName(), permissionGranted.getNewOwnerId())
            .build();
    }

    private static InstrumentResponse buildResponse(Iterable<Instrument> instrument, Map<String, String> nameToOwnerId) {
        return InstrumentResponse.newBuilder()
            .addAllInstrument(instrument)
            .putAllNameToOwnerId(nameToOwnerId)
            .build();
    }
}
