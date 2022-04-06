package com.bsi.peaks.server.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorInitializationException;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.DeathPactException;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.CassandraMonitorCommandFactory;
import com.bsi.peaks.event.InstrumentDaemonCommandFactory;
import com.bsi.peaks.event.WorkEventFactory;
import com.bsi.peaks.event.instrumentDaemon.InstrumentDaemonCommand;
import com.bsi.peaks.event.work.LoaderPing;
import com.bsi.peaks.event.work.ServiceRequest;
import com.bsi.peaks.event.work.WorkCommand;
import com.bsi.peaks.event.work.WorkCommandAck;
import com.bsi.peaks.event.work.WorkEvent;
import com.bsi.peaks.event.work.WorkEventAck;
import com.bsi.peaks.messages.worker.WorkerCommandAck;
import com.bsi.peaks.messages.worker.WorkerResponse;
import com.bsi.peaks.model.dto.CassandraNodeInfo;
import com.bsi.peaks.model.dto.InstrumentDaemon;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.reader.ScanReaderFactory;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.es.WorkManager;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.server.service.UploaderService;
import peaksdata.event.cassandraMonitor.CassandraMonitor;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static akka.actor.SupervisorStrategy.escalate;
import static akka.actor.SupervisorStrategy.restart;
import static akka.actor.SupervisorStrategy.stop;
import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;

/**
 * Master actor running on master node
 *
 * @author Shengying Pan
 * Created by span on 1/19/17.
 */
public class Master extends AbstractActor {
    public static final String NAME = "master";
    public static final String CLUSTER_CLIENT = "clusterClient";
    public static final String PROXY_NAME = "masterProxy";
    public static final String MASTER_PATH = "/user/" + NAME;
    public static final String PROXY_PATH = "/user/" + PROXY_NAME;
    private static final String CLEANUP_TICK = "CLEAN_UP";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final Cancellable cleanupTask;
    private final ActorRef cassandraMonitorManager;
    private final ActorRef instrumentDaemonManager;
    private final String masterVersion;
    private final int fastaDatabaseTimeout;
    private boolean hasRecovered = false;
    private ActorRef tester;
    private ActorRef masterLoader;
    private final FastaDatabaseService fastaDatabaseService;
    private final ApplicationStorageFactory storageFactory;
    private final boolean autoKeyspaceGeneration;


    public Master(
        SystemConfig config,
        ApplicationStorageFactory storageFactory,
        ScanReaderFactory readerFactory,
        ProjectService projectService,
        FastaDatabaseService fastaDatabaseService,
        UploaderService uploaderService,
        ActorRef cassandraMonitorManager,
        ActorRef instrumentDaemonManager
    ) {
        this(config, storageFactory, readerFactory, null, projectService, fastaDatabaseService,
            uploaderService, cassandraMonitorManager, instrumentDaemonManager);
    }

    public Master(
        SystemConfig config,
        ApplicationStorageFactory storageFactory,
        ScanReaderFactory readerFactory,
        ActorRef tester,
        ProjectService projectService,
        FastaDatabaseService fastaDatabaseService,
        UploaderService uploaderService,
        ActorRef cassandraMonitorManager,
        ActorRef instrumentDaemonManager
    ) {
        this.fastaDatabaseTimeout = (int) config.getFastaDatabaseTimeout().toSeconds();
        this.masterVersion = config.getVersion();
        this.cassandraMonitorManager = cassandraMonitorManager;
        this.instrumentDaemonManager = instrumentDaemonManager;
        this.fastaDatabaseService = fastaDatabaseService;
        this.tester = tester;
        this.storageFactory = storageFactory;
        this.autoKeyspaceGeneration = config.isAutoKeyspaceGeneration();

        final ActorContext context = getContext();

        // attach work creator and data loader
        masterLoader = context.watch(context.actorOf(MasterLoader.props(config, getSelf(), uploaderService, storageFactory)));

        this.cleanupTask = scheduledCheck(config.getClusterMasterCleanupInterval());
        getSelf().tell(CLEANUP_TICK, getSelf()); // Make first message a cleanup.
    }

    public static Props props(
        SystemConfig config,
        ApplicationStorageFactory storageFactory,
        ScanReaderFactory readerFactory,
        ProjectService projectService,
        FastaDatabaseService fastaDatabaseService,
        UploaderService uploaderService,
        ActorRef cassandraMonitorManager,
        ActorRef instrumentDaemonManager
    ) {
        return Props.create(Master.class, () -> new Master(config, storageFactory, readerFactory,
            projectService, fastaDatabaseService, uploaderService, cassandraMonitorManager, instrumentDaemonManager));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals(CLEANUP_TICK, x -> cleanup())
            .match(ActorRef.class, tester -> {
                this.tester = tester;
                getSender().tell("injected", getSelf());
            })
            .match(WorkerCommandAck.class, this::forwardToWorkManager)
            .match(WorkCommand.class, this::forwardToWorkManager)
            .match(WorkCommandAck.class, ignore -> {})
            .match(WorkEvent.class, this::forwardToWorkManager)
            .match(WorkEventAck.class, ignore -> {})
            .match(WorkerResponse.class, this::forwardToWorkManager)
            .match(ServiceRequest.class, this::forwardToMasterLoader)
            .match(CassandraNodeInfo.class, this::forwardToCassandraMonitorManager)
            .match(InstrumentDaemon.class, this::forwardToInstrumentManager)
            .match(LoaderPing.class, this::forwardToMasterLoader)
            .matchAny(o -> log.warning("Unexpected message {}", o))
            .build();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("Master is starting...");
        hasRecovered = false;
    }

    @Override
    public void postStop() throws Exception {
        cleanupTask.cancel();
        super.postStop();
        log.info("Master has stopped");
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
        super.preRestart(reason, message);
        log.info("Master is going to restart for the following reason: {}", reason);
        hasRecovered = false;
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        log.info("Master has restarted");
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new OneForOneStrategy(-1, Duration.Inf(), t -> {
            log.error(t, "Master supervised actor encountered error");
            if (t instanceof ActorInitializationException) {
                return stop();
            } else if (t instanceof DeathPactException) {
                return stop();
            } else if (t instanceof Exception) {
                return restart();
            } else {
                return escalate();
            }
        });
    }

    private Cancellable scheduledCheck(FiniteDuration interval) {
        return getContext().system().scheduler().schedule(
            interval,
            interval,
            getSelf(),
            CLEANUP_TICK,
            getContext().dispatcher(),
            getSelf()
        );
    }

    private void cleanup() {
        log.debug("Master started scheduled clean up");
        if (!hasRecovered) {
            hasRecovered = true;
            setVersionOnWorkManager();
        }

        try {
            fastaDatabaseService.timeoutDatabases(fastaDatabaseTimeout);
        } catch (Exception e) {
            log.error(e, "Master experienced problem during cleanup");
        }

        if (autoKeyspaceGeneration) {
            // create keyspace automatically if this is enabled
            Project dummy = Project.newBuilder()
                .setCreatedTimestamp(Instant.now().getEpochSecond())
                .build();
            try {
                storageFactory.initStorage(storageFactory.getKeyspace(dummy));
            } catch (Exception e) {
                log.error(e, "Unable to initialize current keyspace");
            }
        }
    }

    private int getSecondsPassed(Date past) {
        return (int) (new Date().getTime() - past.getTime()) / 1000;
    }

    private void forwardToWorkManager(Object object) {
        final ActorContext context = getContext();
        WorkManager.instance(getContext().getSystem()).forward(object, context);
    }

    private void forwardToMasterLoader(Object object) {
        masterLoader.tell(object, sender());
    }

    private void setVersionOnWorkManager() {
        final ActorRef workManager = WorkManager.instance(getContext().getSystem());
        workManager.tell(WorkEventFactory.version(masterVersion), getSelf());
    }

    private void forwardToCassandraMonitorManager(Object object) {
        CassandraMonitorCommandFactory cassandraMonitorCommandFactory = CassandraMonitorCommandFactory.actingAsUserId((UUID.fromString(ADMIN_USERID)));
        if (object instanceof CassandraNodeInfo) {
            CassandraNodeInfo cassandraNodeInfo = (CassandraNodeInfo) object;
            CassandraMonitor.CassandraMonitorCommand updateNodeInfo = cassandraMonitorCommandFactory.updateNodeInfo(cassandraNodeInfo);
            cassandraMonitorManager.tell(updateNodeInfo, sender());
        } else {
            throw new IllegalStateException("Master received something unexpected from cassandra node process");
        }
    }

    private void forwardToInstrumentManager(Object object) {
        InstrumentDaemonCommandFactory instrumentDaemonCommandFactory = InstrumentDaemonCommandFactory.actingAsUserId((UUID.fromString(ADMIN_USERID)));
        if (object instanceof InstrumentDaemon) {
            InstrumentDaemon instrumentDaemon = (InstrumentDaemon) object;
            InstrumentDaemonCommand pingInstrumentDaemon = instrumentDaemonCommandFactory.pingInstrumentDaemons(instrumentDaemon);
            instrumentDaemonManager.tell(pingInstrumentDaemon, sender());
        } else {
            throw new IllegalStateException("Master received something unexpected from instrument daemon node process");
        }
    }
}