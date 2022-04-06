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
import com.bsi.peaks.event.InstrumentDaemonCommandFactory;
import com.bsi.peaks.event.InstrumentDaemonEventFactory;
import com.bsi.peaks.event.instrumentDaemon.CheckUploadPermission;
import com.bsi.peaks.event.instrumentDaemon.DeleteInstrumentDaemon;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonCommand;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonEvent;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonQuery;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonResponse;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonUploadSample;
import com.bsi.peaks.event.instrumentDaemon.QuerySampleCopyStatus;
import com.bsi.peaks.event.instrumentDaemon.UpdateInstrumentDaemonFileCopyStatus;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.dto.DaemonUploadRequest;
import com.bsi.peaks.model.dto.FilePath;
import com.bsi.peaks.model.dto.InstrumentDaemon;
import com.bsi.peaks.model.dto.InstrumentDaemonSample;
import com.bsi.peaks.model.dto.InstrumentDaemonSampleInfo;
import com.bsi.peaks.model.dto.NodeStatus;
import com.bsi.peaks.model.dto.ParsingRule;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.es.state.InstrumentDaemonState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import scala.Option;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;

public class InstrumentDaemonManager extends PeaksCommandPersistentActor<InstrumentDaemonState, InstrumentDaemonEvent, InstrumentDaemonCommand> {
    public static final String NAME = "InstrumentDaemon";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final InstrumentDaemonCommandFactory systemCommandFactory;
    private final int offlineTimeLimit;
    private final Map<String, ActorRef> daemonActorRefMap = new HashMap<>();

    public static void start(ActorSystem system, int offlineTimeLimit) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(InstrumentDaemonManager.class, offlineTimeLimit),
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

    protected InstrumentDaemonManager(int offlineTimeLimit) throws Exception {
        super(InstrumentDaemonEvent.class);
        this.offlineTimeLimit = offlineTimeLimit;
        systemCommandFactory = InstrumentDaemonCommandFactory.actingAsUserId(UUID.fromString(UserManager.ADMIN_USERID));
    }


    public static ActorRef instance(ActorSystem system) {
        return system.actorFor("/user/" + NAME + "Proxy");
    }

    @Override
    protected void onRecoveryComplete(List<InstrumentDaemonEvent> recoveryEvents) throws Exception {
        super.onRecoveryComplete(recoveryEvents);
        data.nameToInstrumentDaemon().values().forEach(systemCommandFactory::updateInstrumentDaemon);
    }
    @Override
    protected InstrumentDaemonState defaultState() {
        return InstrumentDaemonState.getDefaultInstance();
    }

    @Override
    protected InstrumentDaemonState nextState(InstrumentDaemonState state, InstrumentDaemonEvent event) throws Exception {
        return state.update(event);
    }

    @Override
    protected long deliveryId(InstrumentDaemonCommand command) {
        return command.getDeliveryId();
    }

    @Override
    protected String commandTag(InstrumentDaemonCommand command) {
        return "InstrumentDaemonCommand." + command.getCommandCase().name();
    }

    @Override
    public String persistenceId() {
        return NAME;
    }

    @Override
    public ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(InstrumentDaemonCommand.class, InstrumentDaemonCommand::hasUpdateInstrumentDaemon, commandAckOnFailure(this::handleUpdateInstrumentDaemon))
            .match(InstrumentDaemonCommand.class, InstrumentDaemonCommand::hasDeleteInstrumentDaemon, commandAckOnFailure(this::handleDeleteInstrumentDaemon))
            .match(InstrumentDaemonCommand.class, InstrumentDaemonCommand::hasInstrumentDaemonUploadSample, commandAckOnFailure(this::handleUploadInstrumentDaemon))
            .match(InstrumentDaemonCommand.class, InstrumentDaemonCommand::hasPingInstrumentDaemon, commandAckOnFailure(this::handlePingInstrumentDaemon))
            .match(InstrumentDaemonCommand.class, InstrumentDaemonCommand::hasUpdateInstrumentDaemonFileCopyStatus, commandAckOnFailure(this::handleUpdateInstrumentDaemonFileCopyStatus))
            .match(InstrumentDaemonQuery.class, this::queryInstrumentDaemon);
    }

    @Override
    public void onRecoveryFailure(Throwable cause, Option<Object> event) {
        log.info("Recovery failed for instrument daemon manager, will delete all related events. \nPlease wait a couple of minutes before accessing the instrument daemon page.");
        deleteMessages(Long.MAX_VALUE);
    }

    // commands
    private void handleUpdateInstrumentDaemon(InstrumentDaemonCommand command) throws Exception {
        isAdmin(userRole(uuidFrom(command.getActingUserId())));
        UUID creatorId = uuidFrom(command.getActingUserId());
        InstrumentDaemon instrumentDaemon = command.getUpdateInstrumentDaemon().getInstrumentDaemon();
        InstrumentDaemonEvent instrumentDaemonEvent = InstrumentDaemonEventFactory.instrumentDaemonUpdated(instrumentDaemon, creatorId);
        persist(instrumentDaemonEvent, () -> reply(InstrumentDaemonCommandFactory.ackAccept(command)));
    }

    private void handleDeleteInstrumentDaemon(InstrumentDaemonCommand command) throws Exception {
        isAdmin(userRole(uuidFrom(command.getActingUserId())));
        UUID creatorId = uuidFrom(command.getActingUserId());
        DeleteInstrumentDaemon deleteInstrumentDaemon = command.getDeleteInstrumentDaemon();
        String name = deleteInstrumentDaemon.getName();
        InstrumentDaemonEvent instrumentDaemonEvent = InstrumentDaemonEventFactory.instrumentDaemonDeleted(name, creatorId);
        persist(instrumentDaemonEvent, () -> reply(InstrumentDaemonCommandFactory.ackAccept(command)));
    }

    private void handleUploadInstrumentDaemon(InstrumentDaemonCommand command) {
        InstrumentDaemonUploadSample uploadSample = command.getInstrumentDaemonUploadSample();
        InstrumentDaemonSampleInfo info = uploadSample.getInstrumentDaemonSampleInfo();
        FilePath copyLocation = info.getCopyLocation();
        ParsingRule parsingRule = info.getParsingRule();

        int samplesInProject = parsingRule.getSamplesInProject();
        int fractionsPerSample = parsingRule.getFractionsPerSample();
        List<UUID> sampleIds = Lists.newArrayList(ModelConversion.uuidsFrom(uploadSample.getSampleIds()));
        List<UUID> fractionIds = Lists.newArrayList(ModelConversion.uuidsFrom(uploadSample.getFractionIds()));

        List<InstrumentDaemonSample> daemonSamples = info.getInstrumentDaemonSamplesList();
        for(int i = 0; i < daemonSamples.size(); i++) {
            int sampleStartIdx = samplesInProject * i;
            int sampleEndIdx = samplesInProject * (i + 1);
            List<UUID> curSampleIds = sampleIds.subList(sampleStartIdx, sampleEndIdx);
            List<UUID> curFractionIds = fractionIds.subList(sampleStartIdx * fractionsPerSample, sampleEndIdx * fractionsPerSample);
            InstrumentDaemonSample daemonSample = daemonSamples.get(i);
            String name = daemonSample.getName();
            ActorRef actorRef = this.daemonActorRefMap.get(name);
            DaemonUploadRequest daemonUploadRequest = DaemonUploadRequest.newBuilder()
                .setProjectId(uploadSample.getProjectId())
                .setSampleIds(ModelConversion.uuidsToByteString(curSampleIds))
                .setFractionIds(ModelConversion.uuidsToByteString(curFractionIds))
                .setParsingRule(parsingRule)
                .setCopyLocation(copyLocation)
                .build();
            actorRef.tell(daemonUploadRequest, self());
        }
        reply(InstrumentDaemonCommandFactory.ackAccept(command));
    }

    private void handlePingInstrumentDaemon(InstrumentDaemonCommand command) throws Exception {
        UUID creatorId = uuidFrom(command.getActingUserId());
        InstrumentDaemon instrumentDaemon = command.getPingInstrumentDaemon().getInstrumentDaemon();
        this.daemonActorRefMap.put(instrumentDaemon.getName(), sender());
        InstrumentDaemonEvent instrumentDaemonEvent =
            InstrumentDaemonEventFactory.instrumentDaemonPinged(instrumentDaemon, creatorId);
        persist(instrumentDaemonEvent, () -> reply(InstrumentDaemonCommandFactory.ackAccept(command)));
    }

    private void handleUpdateInstrumentDaemonFileCopyStatus(InstrumentDaemonCommand command) throws Exception {
        UUID creatorId = uuidFrom(command.getActingUserId());
        UpdateInstrumentDaemonFileCopyStatus updateInstrumentDaemonFileCopyStatus = command.getUpdateInstrumentDaemonFileCopyStatus();
        UUID projectId = uuidFrom(updateInstrumentDaemonFileCopyStatus.getProjectId());
        UUID sampleId = uuidFrom(updateInstrumentDaemonFileCopyStatus.getSampleId());
        UUID fractionId = uuidFrom(updateInstrumentDaemonFileCopyStatus.getFractionId());
        Progress.State copyingProgress = updateInstrumentDaemonFileCopyStatus.getCopyingProgress();
        InstrumentDaemonEvent instrumentDaemonEvent =
            InstrumentDaemonEventFactory.instrumentDaemonFileCopyStatusUpdated(projectId, sampleId, fractionId, copyingProgress, creatorId);
        persist(instrumentDaemonEvent, () -> reply(InstrumentDaemonCommandFactory.ackAccept(command)));
    }

    //queries
    private void queryInstrumentDaemon(InstrumentDaemonQuery instrumentDaemonQuery) {
        try {
            final InstrumentDaemonResponse reply;
            switch (instrumentDaemonQuery.getQueryCase()) {
                case QUERYALLINSTRUMENTDAEMONS:
                    String actingUserId = uuidFrom(instrumentDaemonQuery.getActingUserId()).toString();
                    InstrumentDaemonResponse.InstrumentDaemons.Builder builder =
                        InstrumentDaemonResponse.InstrumentDaemons.newBuilder();
                    this.data.nameToInstrumentDaemon().values()
                        .stream().filter(instrumentDaemon -> instrumentDaemon.getUserIdList().contains(actingUserId))
                        .forEach(node -> {
                            NodeStatus status;
                            if (Instant.now().getEpochSecond() - node.getLastTimeCommunicated() > offlineTimeLimit) {
                                status = NodeStatus.OFFLINE;
                            } else {
                                status = NodeStatus.ONLINE;
                            }
                            builder.addInstrumentDaemon(
                                InstrumentDaemon.newBuilder().mergeFrom(node)
                                    .setStatus(status)
                                    .build());
                        });
                    reply = InstrumentDaemonResponse.newBuilder()
                        .setInstrumentDaemons(builder.build())
                        .build();
                    break;
                case CHECKUPLOADPERMISSION:
                    CheckUploadPermission checkUploadPermission = instrumentDaemonQuery.getCheckUploadPermission();
                    String name = checkUploadPermission.getName();
                    String apiKey = checkUploadPermission.getApiKey();
                    boolean found = false;
                    for(InstrumentDaemon instrumentDaemon : this.data.nameToInstrumentDaemon().values()) {
                        found |= instrumentDaemon.getName().equals(name) && instrumentDaemon.getApiKey().equals(apiKey);
                    }
                    reply = InstrumentDaemonResponse.newBuilder()
                        .setHasPermission(found)
                        .build();
                    break;
                case QUERYSAMPLECOPYSTATUS:
                    QuerySampleCopyStatus querySampleCopyStatus = instrumentDaemonQuery.getQuerySampleCopyStatus();
                    UUID projectId = uuidFrom(querySampleCopyStatus.getProjectId());
                    if (this.data.projectToFractionIds().containsKey(projectId)) {
                        ImmutableList<UUID> fractionIds = this.data.projectToFractionIds().get(projectId);
                        List<Map.Entry<UUID, Progress.State>> filteredEntries =
                            this.data.fractionIdsToProgress().entrySet().stream()
                                .filter(entry -> fractionIds.contains(entry.getKey()))
                                .collect(Collectors.toList());
                        reply = InstrumentDaemonResponse.newBuilder()
                            .setCopyProgresses(InstrumentDaemonResponse.CopyProgresses.newBuilder()
                                .setIsDataCopy(true)
                                .addAllCopyingProgresses(
                                    filteredEntries.stream()
                                        .map(Map.Entry::getValue)
                                        .collect(Collectors.toList()))
                                .setFractionIds(
                                    ModelConversion.uuidsToByteString(filteredEntries.stream()
                                        .map(Map.Entry::getKey)
                                        .collect(Collectors.toList())))
                                .build())
                            .build();
                    } else {
                        reply = InstrumentDaemonResponse.newBuilder()
                            .setCopyProgresses(InstrumentDaemonResponse.CopyProgresses.newBuilder()
                                .setIsDataCopy(false)
                                .build())
                            .build();
                    }
                    break;
                default:
                    throw new IllegalStateException("Unexpected CassandraMonitorQuery " + instrumentDaemonQuery.getQueryCase());
            }
            reply(reply);
        } catch(Exception e) {
            log.error("Unable to CassandraMonitorQuery cassandra monitor caused by", e);
            reply(e);
        }
    }

    //validations
    private void isAdmin(User.Role role) {
        if (!role.equals(User.Role.SYS_ADMIN)) {
            throw new IllegalStateException("Instrument daemon permission denied");
        }
    }
}
