package com.bsi.peaks.server.es;

import akka.Done;
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
import akka.stream.ActorMaterializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.bsi.peaks.data.storage.application.SpectralLibraryRepository;
import com.bsi.peaks.event.SpectralLibraryCommandFactory;
import com.bsi.peaks.event.SpectralLibraryEventFactory;
import com.bsi.peaks.event.spectralLibrary.SLCommand;
import com.bsi.peaks.event.spectralLibrary.SLEvent;
import com.bsi.peaks.event.spectralLibrary.SLMetaData;
import com.bsi.peaks.event.spectralLibrary.SLQuery;
import com.bsi.peaks.event.spectralLibrary.SLResponse;
import com.bsi.peaks.model.dto.SLProgressState;
import com.bsi.peaks.model.dto.SpectralLibraryListItem;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.es.spectrallibrary.ProcessSpectralLibrary;
import com.bsi.peaks.server.es.spectrallibrary.SLInfo;
import com.bsi.peaks.server.es.state.SpectralLibraryState;
import com.bsi.peaks.service.common.ServiceCancellationException;
import com.bsi.peaks.service.common.ServiceKiller;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;
import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;

public class SpectralLibraryManager extends PeaksCommandPersistentActor<SpectralLibraryState, SLEvent, SLCommand>  {
    private static final String NAME = "SpectralLibrary";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final SpectralLibraryRepository spectralLibraryRepository;
    private final String persistenceId;
    private final ProcessSpectralLibrary processSpectralLibrary;
    private final SourceQueueWithComplete<SLCommand.CreateProcess> workQueue;
    private final Map<UUID, Float> progressTracker;
    private final Map<UUID, ServiceKiller> processSlToCancellers;
    public static final String WORKER_LIMIT = "peaks.server.spectral-library.creation-workers";
    private final SpectralLibraryCommandFactory systemCommandFactory;

    public static void start(
        ActorSystem system,
        SpectralLibraryRepository spectralLibraryRepository,
        ProcessSpectralLibrary processSpectralLibrary
    ) {
        system.actorOf(
                ClusterSingletonManager.props(
                        BackoffSupervisor.props(
                                Props.create(SpectralLibraryManager.class, NAME, spectralLibraryRepository, processSpectralLibrary),
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
    protected void onRecoveryComplete(List<SLEvent> recoveryEvents) throws Exception {
        super.onRecoveryComplete(recoveryEvents);
        data.slIdName().values().stream().forEach(spectralLibraries -> {
            UUID slId = spectralLibraries.slId();
            switch (spectralLibraries.state()) {
                case PROCESSING:
                case IMPORTING:
                    sendSelf(systemCommandFactory.processNotification(slId, SLProgressState.FAILED));
                    break;
                case DELETING:
                    sendSelf(systemCommandFactory.delete(slId));
                    break;
                default:
                    //do nothing
            }
        });
    }

    protected SpectralLibraryManager(
        String persistenceId,
        SpectralLibraryRepository spectralLibraryRepository,
        ProcessSpectralLibrary processSpectralLibrary
    ) {
        super(SLEvent.class);
        this.persistenceId = persistenceId;
        this.spectralLibraryRepository = spectralLibraryRepository;
        this.processSpectralLibrary = processSpectralLibrary;
        this.progressTracker = new HashMap<>();
        this.processSlToCancellers = new ConcurrentHashMap<>();

        final Config config = context().system().settings().config();
        final int workerLimit = config.hasPath(WORKER_LIMIT) ? config.getInt(WORKER_LIMIT) : 1;

        systemCommandFactory = SpectralLibraryCommandFactory.actingAsUserId(UUID.fromString(UserManager.ADMIN_USERID));
        workQueue = Source.<SLCommand.CreateProcess>queue(Integer.MAX_VALUE, OverflowStrategy.fail())
                .mapAsync(workerLimit, create -> {
                    try {
                        final UUID id = uuidFrom(create.getSlId());
                        log.info("Start spectral library {} creation processing", id);
                        sendSelf(systemCommandFactory.processNotification(id, SLProgressState.PROCESSING));
                        ServiceKiller killer = new ServiceKiller();
                        processSlToCancellers.put(id, killer);
                        return createLibrary(create, killer)
                            .handle((entryCount, throwable) -> {
                                if (throwable == null) {
                                    log.info("Done spectral library {} creation processing. ({} entries)", id, entryCount);
                                    sendSelf(systemCommandFactory.setEntryCount(id, entryCount));
                                    sendSelf(systemCommandFactory.processNotification(id, SLProgressState.DONE));
                                } else {
                                    log.error(throwable, "Failed spectral library {} creation processing", id);
                                    sendSelf(systemCommandFactory.processNotification(id, SLProgressState.FAILED));
                                }
                                processSlToCancellers.remove(id);
                                return Done.getInstance();
                            });
                    } catch (Throwable e) {
                        log.error(e, "Unable to start spectral library creation processing");
                        return CompletableFuture.completedFuture(Done.getInstance());
                    }
                })
                .toMat(Sink.<Done>ignore().mapMaterializedValue(futureDone -> futureDone.whenComplete((done, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable, "Work queue terminated");
                    } else {
                        log.error("Work queue unexpectedly completed");
                    }
                    context().stop(self());
                })), Keep.left())
                .run(ActorMaterializer.create(context()));
    }

    //PeaksPersistentActor implementation
    @Override
    protected SpectralLibraryState defaultState() {
        return SpectralLibraryState.getDefaultInstance();
    }

    @Override
    protected SpectralLibraryState nextState(SpectralLibraryState state, SLEvent event) throws Exception {
        return state.update(event);
    }

    @NotNull
    public static String eventTag(SLEvent.EventCase eventCase) {
        return "SLEvent." + eventCase.name();
    }

    @Override
    protected String commandTag(SLCommand command) {
        return commandTag(command.getCommandCase());
    }

    @NotNull
    public static String commandTag(SLCommand.CommandCase commandCase) {
        return "SLCommand." + commandCase.name();
    }

    @Override
    public String persistenceId() {
        return persistenceId;
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(SLCommand.class, SLCommand::hasCreateProcess, commandAckOnFailure(this::handleCreateProcessSpectralLibrary))
            .match(SLCommand.class, SLCommand::hasCreateImport, commandAckOnFailure(this::handleCreateImportSpectralLibrary))
            .match(SLCommand.class, SLCommand::hasDelete, commandAckOnFailure(this::handleDeleteSpectralLibrary))
            .match(SLCommand.class, SLCommand::hasPersistDelete, this::handlePersistDelete)
            .match(SLCommand.class, SLCommand::hasGrantPermission, commandAckOnFailure(this::handleGrantPermission))
            .match(SLCommand.class, SLCommand::hasSetName, commandAckOnFailure(this::handleSetName))
            .match(SLCommand.class, SLCommand::hasNotification, commandAckOnFailure(this::handleNotification))
            .match(SLCommand.class, SLCommand::hasProgress, this::handleProgress)
            .match(SLCommand.class, SLCommand::hasSetEntryCount, this::handleSetEntryCount)
            .match(SLQuery.class, this::querySpectralLibrary);
    }

    @Override
    protected long deliveryId(SLCommand command) {
        return command.getDeliveryId();
    }

    // commands
    private void handleProgress(SLCommand command) {
        SLCommand.ProcessProgress progress = command.getProgress();
        try {
            progressTracker.put(uuidFrom(progress.getSlId()), progress.getProgress());
        } catch (Exception e) {
            log.error(e, "Unable to handle progress notification {}", progress);
        }
    }

    private void handleCreateProcessSpectralLibrary(SLCommand command) throws Exception {
        SLCommand.CreateProcess createSL = command.getCreateProcess();
        UUID creatorId = uuidFrom(command.getActingUserId());
        UUID slId = uuidFrom(createSL.getSlId());
        SLMetaData meta = createSL.getMeta();

        if (meta.getDatabaseCount() == 0) {
            throw new IllegalArgumentException("Databases cannot be empty");
        }
        validateAddSpectralLibrary(creatorId, slId, meta.getName());

        SLEvent libraryCreated = SpectralLibraryEventFactory.spectralLibraryCreated(slId, meta, creatorId);
        SLEvent processEvent = SpectralLibraryEventFactory.spectralLibraryProcess(slId, createSL.getProcess(), creatorId);

        persist(libraryCreated);
        persist(processEvent, () -> {
            reply(SpectralLibraryCommandFactory.ackAccept(command));
            workQueue.offer(createSL);
        });
    }

    private void handleCreateImportSpectralLibrary(SLCommand command) throws Exception {
        SLCommand.CreateImport createSL = command.getCreateImport();
        UUID creatorId = uuidFrom(command.getActingUserId());
        UUID slId = uuidFrom(createSL.getSlId());
        SLMetaData meta = createSL.getMeta();

        if (meta.getDatabaseCount() == 0 && !meta.getJson().equals("{}")) {
            throw new IllegalArgumentException("Databases cannot be empty if meta info is provided");
        }
        validateAddSpectralLibrary(creatorId, slId, meta.getName());

        SLEvent libraryCreated = SpectralLibraryEventFactory.spectralLibraryCreated(slId, meta, creatorId);
        persist(libraryCreated, () -> reply(SpectralLibraryCommandFactory.ackAccept(command)));
    }

    private void handleDeleteSpectralLibrary(SLCommand command) throws Exception {
        SLCommand.Delete deleteSL = command.getDelete();
        UUID slId = uuidFrom(deleteSL.getSlId());
        UUID actingUserId = uuidFrom(command.getActingUserId());

        if (!validateExistenceAndPermission(slId, actingUserId, command)) {
            return;
        }

        if (processSlToCancellers.get(slId) != null) {
            processSlToCancellers.get(slId).abort(new ServiceCancellationException("Spectral Library " + slId + " creation cancelled by user " + actingUserId));
        }

        SLEvent libraryDeleting = SpectralLibraryEventFactory.spectralLibraryStatusSet(SLProgressState.DELETING,
                slId, "", actingUserId);
        persist(libraryDeleting, () -> reply(SpectralLibraryCommandFactory.ackAccept(command)));

        spectralLibraryRepository.delete(slId).whenComplete((done, throwable) -> {
            if (throwable == null) {
                SLCommand persistDelete = SpectralLibraryCommandFactory.actingAsUserId(actingUserId).persistDelete(slId);
                sendSelf(persistDelete);
            } else {
                log.error(throwable, "Could not delete spectral library.", slId.toString());
            }
        });
    }

    private void handlePersistDelete(SLCommand command) throws Exception {
        SLCommand.PersistDelete persistDelete = command.getPersistDelete();
        UUID slId = uuidFrom(persistDelete.getSlId());
        UUID actingUserId = uuidFrom(command.getActingUserId());
        SLEvent libraryDeleted = SpectralLibraryEventFactory.spectralLibraryStatusSet(SLProgressState.DELETED,
                slId, "", actingUserId);
        persist(libraryDeleted);
    }

    private void handleGrantPermission(SLCommand command) throws Exception {
        SLCommand.GrantPermission grantPermission = command.getGrantPermission();
        UUID newOwnerId = uuidFrom(grantPermission.getNewOwnerId());
        UUID actingUserId = uuidFrom(command.getActingUserId());
        UUID slId = uuidFrom(grantPermission.getSlId());

        if (!validateExistenceAndPermission(slId, actingUserId, command)) {
            return;
        }
        String toGrantPermSLName = this.data.slIdName().get(slId).name();
        this.data.slIdName().values().forEach(slInfo -> {
            if (slInfo.ownerId().equals(actingUserId) && slInfo.name().equals(toGrantPermSLName)) {
                throw new IllegalStateException("Trying to take ownership of a SL with name in conflict with an owned SL");
            }
        });
        SLEvent permissionGrant = SpectralLibraryEventFactory.spectralLibraryPermissionGranted(slId, uuidFrom(command.getActingUserId()), newOwnerId);
        if(this.data.slIdName().get(slId).ownerId().equals(newOwnerId)) {
            reply(SpectralLibraryCommandFactory.ackAccept(command));
            return;
        }
        persist(permissionGrant, () -> reply(SpectralLibraryCommandFactory.ackAccept(command)));
    }

    private void handleSetName(SLCommand command) throws Exception {
        SLCommand.SetName setName = command.getSetName();
        UUID actingUserId = uuidFrom(command.getActingUserId());
        UUID slId = uuidFrom(setName.getSlId());
        final String newName = setName.getNewName();
        SLEvent nameSet = SpectralLibraryEventFactory.spectralLibraryNameSet(slId, newName, actingUserId);

        validateSpectralLibraryName(slId, Optional.empty(), newName);
        if (!validateExistenceAndPermission(slId, actingUserId, command)) {
            return;
        }
        if(this.data.slIdName().get(slId).name().equals(newName)) {
            reply(SpectralLibraryCommandFactory.ackAccept(command));
            return;
        }
        persist(nameSet, () -> reply(SpectralLibraryCommandFactory.ackAccept(command)));
    }

    private void handleNotification(SLCommand command) throws Exception {
        SLCommand.ProcessNotification notification = command.getNotification();
        UUID actingUserId = uuidFrom(command.getActingUserId());
        UUID slId = uuidFrom(notification.getSlId());

        if (!validateExistenceAndPermission(slId, actingUserId, command)) {
            return;
        }
        final SLProgressState progressState = notification.getState();
        if(this.data.slIdName().get(slId).state().equals(progressState)) {
            reply(SpectralLibraryCommandFactory.ackAccept(command));
            return;
        }

        switch (progressState) {
            case DONE:
                if (data.slIdName().get(slId).state().equals(SLProgressState.DELETING)) {
                    sendSelf(systemCommandFactory.delete(slId));
                    reply(SpectralLibraryCommandFactory.ackAccept(command));
                    break;
                }
            default:
                SLEvent statusSet = SpectralLibraryEventFactory.spectralLibraryStatusSet(progressState, slId, "", actingUserId);
                persist(statusSet, () -> reply(SpectralLibraryCommandFactory.ackAccept(command)));
        }
    }

    private void handleSetEntryCount(SLCommand command) throws Exception {
        SLCommand.SetEntryCount entryCountUpdate = command.getSetEntryCount();
        UUID actingUserId = uuidFrom(command.getActingUserId());
        UUID slId = uuidFrom(entryCountUpdate.getSlId());

        SLEvent entryCountSet = SpectralLibraryEventFactory.spectralLibraryEntryCountSet(slId, entryCountUpdate.getCount(), actingUserId);
        persist(entryCountSet);
    }

    //queries
    private void querySpectralLibrary(SLQuery query) {
        try {
            final List<SLInfo> libraries;
            final UUID userId = uuidFrom(query.getActingUserId());
            final User.Role role = userRole(userId);
            final SLResponse reply;
            switch (query.getQueryCase()) {
                case LIST:
                    SLQuery.List list = query.getList();
                    switch (list.getFilterTypeCase()) {
                        case ALL:
                            libraries = getSpectralLibraryByUser(userId, role);
                            break;
                        case SLFILTER:
                            libraries = querySlBySlFilter(list.getSlFilter(), userId, role);
                            break;
                        default:
                            throw new IllegalStateException("SLFilter is not set");
                    }
                    reply = buildResponse(libraries);
                    break;
                case META: {
                    libraries = querySlBySlFilter(SLQuery.SLFilter.newBuilder().setId(query.getMeta().getId()).build(), userId, role);
                    if (libraries.size() != 1) throw new IllegalStateException("Unable to find library");
                    reply = buildResponse(libraries.get(0).metaJson());
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected query " + query.getQueryCase());
            }
            reply(reply);
        } catch(Exception e) {
            log.error("Unable to query spectral libraries caused by", e);
            reply(e);
        }
    }

    private List<SLInfo> querySlBySlFilter(SLQuery.SLFilter filter, UUID userId, User.Role role) {
        final List<SLInfo> slInfos;
        switch (filter.getFieldCase()) {
            case ID:
                SLInfo element = this.data.slIdName().get(uuidFrom(filter.getId()));
                if (element == null) slInfos = Collections.emptyList();
                else slInfos = ImmutableList.of(element);
                break;
            case NAME:
                slInfos = this.data.slIdName().values().stream().filter(s -> s.name().equals(filter.getName())).collect(Collectors.toList());
                break;
            default:
                throw new IllegalStateException("Invalid SLFilter query");
        }

        return slInfos.stream()
            .filter(slInfo -> isSlVisibleForUser(userId, role, slInfo))
            .collect(Collectors.toList());
    }

    //validations
    private void validateAddSpectralLibrary(UUID userId, UUID slId, String slName) throws IllegalStateException {
        if (this.data.slIdName().containsKey(slId)) {
            throw new IllegalArgumentException("Duplicate Spectral Library id is not allowed");
        }
        validateSpectralLibraryName(slId, Optional.of(userId), slName);
    }

    private boolean validateExistenceAndPermission(UUID slId, UUID userId, SLCommand command) {
        boolean exists = data.slIdName().get(slId) != null;
        if (!exists) {
            reply(SpectralLibraryCommandFactory.ackReject(command, "Spectral library not found"));
            return false;
        }
        boolean hasPermission = data.slIdName().get(slId).ownerId().equals(userId) || userId.toString().equals(UserManager.ADMIN_USERID);
        if (!hasPermission) {
            reply(SpectralLibraryCommandFactory.ackReject(command, "Spectral library permission denied"));
            return false;
        }
        return true;
    }

    private List<SLInfo> getSpectralLibraryByUser(UUID userId, User.Role role) {
        return this.data.slIdName().values().stream()
                .filter(slInfo -> isSlVisibleForUser(userId, role, slInfo))
                .collect(Collectors.toList());
    }

    private boolean isSlVisibleForUser(UUID userId, User.Role role, SLInfo slInfo) {
        return (role != null && (role.equals(User.Role.SYS_ADMIN)) ||
                slInfo.ownerId().toString().equals(ADMIN_USERID) ||
                userId.equals(UUID.fromString(ADMIN_USERID)) ||
                userId.equals(slInfo.ownerId()));
    }

    private void validateSpectralLibraryName(UUID slId, Optional<UUID> actingUserId, String slName) {
        final UUID ownerId;
        if (actingUserId.isPresent()) {
            ownerId = actingUserId.get();
        } else {
            ownerId = this.data.slIdName().get(slId).ownerId();
        }
        final boolean found = this.data.slIdName().values().stream()
            .anyMatch(slInfo -> !slInfo.slId().equals(slId) && slInfo.ownerId().equals(ownerId) && slInfo.name().equals(slName));
        if (found) {
            throw new IllegalArgumentException("Duplicate Spectral Library name " + slName + " is not allowed");
        }
    }

    private SLResponse buildResponse(Iterable<SLInfo> spectralLibraries) {
        Stream<SpectralLibraryListItem> libraryListItemStream = StreamSupport.stream(spectralLibraries.spliterator(), false).map(slinfo -> {
            Float progress = this.progressTracker.getOrDefault(slinfo.slId(), 0f);
            return slinfo.to(progress);
        });
        SLResponse.Builder builder = SLResponse.newBuilder();
        builder.getListBuilder().addAllSpectralLibrary(libraryListItemStream::iterator);
        return builder.build();
    }

    private SLResponse buildResponse(String metaJson) {
        return SLResponse.newBuilder().setMeta(metaJson).build();
    }

    private CompletionStage<Integer> createLibrary(SLCommand.CreateProcess createCommand, ServiceKiller serviceKiller) throws IOException {
        final UUID spectralLibraryId = uuidFrom(createCommand.getSlId());
        return processSpectralLibrary.process(
            spectralLibraryId,
            createCommand.getProcess(),
            ActorMaterializer.create(context()),
            (float pct) -> sendSelf(systemCommandFactory.processProgress(spectralLibraryId, pct)),
            serviceKiller,
            createCommand.getUseRtAsIRt()
        );
    }
}
