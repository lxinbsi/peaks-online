package com.bsi.peaks.server.archiver;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.japi.pf.FI;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.RecoveryCompleted;
import akka.persistence.SaveSnapshotFailure;
import akka.persistence.SaveSnapshotSuccess;
import akka.persistence.SnapshotOffer;
import akka.persistence.SnapshotSelectionCriteria;
import akka.stream.javadsl.Sink;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.common.AckStatus;
import com.bsi.peaks.event.project.ArchiveCancelled;
import com.bsi.peaks.event.project.ArchiveEventDequeued;
import com.bsi.peaks.event.project.ArchiveProject;
import com.bsi.peaks.event.project.ArchiveRestarted;
import com.bsi.peaks.event.project.ArchiveTaskState;
import com.bsi.peaks.event.project.ArchiverCommand;
import com.bsi.peaks.event.project.ArchiverEvent;
import com.bsi.peaks.event.project.ArchiverResponse;
import com.bsi.peaks.event.project.ImportArchive;
import com.bsi.peaks.event.project.ImportEventDequeued;
import com.bsi.peaks.event.project.ImportRestarted;
import com.bsi.peaks.event.project.ProgressUpdate;
import com.bsi.peaks.event.project.RestartArchiveProject;
import com.bsi.peaks.event.project.RestartImportArchive;
import com.bsi.peaks.event.project.StopArchiverMinion;
import com.bsi.peaks.event.uploader.PathsResponse;
import com.bsi.peaks.event.uploader.UploaderError;
import com.bsi.peaks.model.dto.ArchiveRequest;
import com.bsi.peaks.model.dto.ArchiveTask;
import com.bsi.peaks.model.dto.ArchivingState;
import com.bsi.peaks.model.dto.FilePath;
import com.bsi.peaks.model.dto.ImportRequest;
import com.bsi.peaks.model.dto.ImportingTask;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.model.dto.RemotePath;
import com.bsi.peaks.reader.SourceType;
import com.bsi.peaks.server.es.WorkManager;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.bsi.peaks.server.service.CassandraMonitorService;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.utilities.Detector;
import com.bsi.peaks.utilities.FTPHelper;
import com.bsi.peaks.utilities.FileBrowser;
import com.bsi.peaks.utilities.LFSHelper;
import com.bsi.peaks.utilities.RemoteType;
import com.bsi.peaks.utilities.UniversalFileAccessor;
import com.typesafe.config.Config;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;

public class Archiver extends AbstractPersistentActor {
    public static final String NAME = "archiver";
    private final static String SCHEDULED_TICK = "SCHEDULED_TICK";
    private final static long SCHEDULED_INTERVAL_NANOS = 1000000000;
    private final Cancellable scheduledTask;
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final String persistenceKey;
    private final ApplicationStorageFactory storageFactory;
    private final ProjectService projectService;
    private final CassandraMonitorService cassandraMonitorService;
    private final ProjectAggregateCommunication projectCommunication;
    private final FastaDatabaseService databaseService;
    private ArchiverState state = new ArchiverState();
    private final Map<String, ActorRef> archiveMinions = new HashMap<>();
    private final Map<String, ActorRef> importMinions = new HashMap<>();
    private final int numMinions;
    private final boolean autoRecovery;
    private final int snapshotInterval;

    @Override
    public String persistenceId() {
        return NAME + "-persistence_" + persistenceKey;
    }

    private Archiver(
        ApplicationStorageFactory storageFactory,
        ProjectService projectService,
        FastaDatabaseService databaseService,
        ProjectAggregateCommunication projectCommunication,
        String persistenceKey,
        CassandraMonitorService cassandraMonitorService
    ) {
        this.storageFactory = storageFactory;
        this.projectService = projectService;
        this.projectCommunication = projectCommunication;
        this.databaseService = databaseService;
        this.cassandraMonitorService = cassandraMonitorService;
        this.persistenceKey = persistenceKey;
        this.scheduledTask = scheduleTask(FiniteDuration.fromNanos(SCHEDULED_INTERVAL_NANOS));

        final Config config = context().system().settings().config();
        this.numMinions = config.getInt("peaks.server.archiver.minions");
        this.autoRecovery = config.getBoolean("peaks.server.archiver.auto-recovery");
        this.snapshotInterval = config.getInt("peaks.server.archiver.snapshot-interval");
    }

    private Cancellable scheduleTask(FiniteDuration interval) {
        return getContext().system().scheduler().schedule(
            interval,
            interval,
            self(),
            SCHEDULED_TICK,
            context().dispatcher(),
            self()
        );
    }

    private Archiver(
        ApplicationStorageFactory storageFactory,
        ProjectService projectService,
        FastaDatabaseService databaseService,
        ProjectAggregateCommunication projectCommunication,
        CassandraMonitorService cassandraMonitorService
    ) {
        this(storageFactory, projectService, databaseService, projectCommunication, "", cassandraMonitorService);
    }

    public static Props props(
        ApplicationStorageFactory storageFactory,
        ProjectService projectService,
        FastaDatabaseService databaseService,
        ProjectAggregateCommunication projectCommunication,
        CassandraMonitorService cassandraMonitorService
    ) {
        return Props.create(Archiver.class, () -> new Archiver(storageFactory, projectService, databaseService, projectCommunication, cassandraMonitorService));
    }

    @Override
    public void postStop() throws Exception {
        scheduledTask.cancel();
        super.postStop();
    }

    @Override
    public void onRecoveryFailure(Throwable cause, Option<Object> event) {
        log.info("Recovery failed for Archiver will delete all related events. \nPlease wait a couple of minutes before accessing the archive page and attempting to archive projects.");
        deleteMessages(Long.MAX_VALUE);
        return;
    }

    @Override
    public Receive createReceiveRecover() {
        return receiveBuilder()
            .match(SnapshotOffer.class, ss -> state.recoverFrom((ArchiverState) ss.snapshot()))
            .match(ArchiverEvent.class, state::update)
            .match(RecoveryCompleted.class, this::afterRecovery)
            .build();
    }

    private void afterRecovery(RecoveryCompleted completed) {
        if (autoRecovery) {
            state.archiveTaskSource().filter(task -> task.getState() == ArchivingState.ARCHIVING)
                .runForeach(this::recoverArchiveTask, storageFactory.getMaterializer())
                .toCompletableFuture().join();
            state.importingTaskSource().filter(task -> task.getState() == ArchivingState.IMPORTING)
                .runForeach(this::recoverImportTask, storageFactory.getMaterializer())
                .toCompletableFuture().join();
        } else {
            state.archiveTaskSource().filter(task -> task.getState() == ArchivingState.ARCHIVING)
                .runForeach(this::failArchivingTask, storageFactory.getMaterializer())
                .toCompletableFuture().join();
            state.importingTaskSource().filter(task -> task.getState() == ArchivingState.IMPORTING)
                .runForeach(this::failImportingTask, storageFactory.getMaterializer())
                .toCompletableFuture().join();
        }
    }

    private void failArchivingTask(ArchiveTask task) {
        log.info("Automatic recovery disabled, failing archiving task for project " + task.getProjectId());
        ProgressUpdate update = ProgressUpdate.newBuilder()
            .setTotalTasks(task.getTotalTasks())
            .setFinishedTasks(task.getFinishedTasks())
            .setProjectId(task.getProjectId())
            .setState(Progress.State.ARCHIVING_FAILED)
            .build();
        ArchiverCommand failed = ArchiverCommand.newBuilder().setUpdateArchiveProgress(update).build();
        self().tell(failed, self());
    }

    private void failImportingTask(ImportingTask task) {
        log.info("Automatic recovery disabled, failing importing task for project " + task.getProjectId());
        ProgressUpdate update = ProgressUpdate.newBuilder()
            .setTotalTasks(task.getTotalTasks())
            .setFinishedTasks(task.getFinishedTasks())
            .setProjectId(task.getProjectId())
            .setState(Progress.State.IMPORTING_FAILED)
            .build();
        ArchiverCommand failed = ArchiverCommand.newBuilder().setUpdateImportProgress(update).build();
        self().tell(failed, self());
    }

    private void recoverArchiveTask(ArchiveTask task) {
        ArchiveRequest request = task.getAssociatedRequest();
        UUID projectId = UUID.fromString(task.getProjectId());
        log.info("Recovering archive task for project " + projectId);
        ArchiveTaskState taskState = state.retrieveArchiveTask(projectId).second();
        RemotePath remotePath = state.getPathByName(request.getArchivePath().getRemotePathName());
        RestartArchiveProject command = RestartArchiveProject.newBuilder()
            .setArchive(ArchiveProject.newBuilder()
                .setProjectId(task.getProjectId())
                .setTargetFolder(request.getArchivePath())
                .setCreatorId(task.getCreatorId())
                .setOldProjectName(task.getProjectName())
                .setDiscardScanPeaks(request.getDiscardScanPeaks())
                .build()
            )
            .setState(taskState)
            .build();
        ActorRef minion = createMinion(remotePath);
        archiveMinions.put(projectId.toString(), minion);
        minion.forward(command, context());
    }

    private void recoverImportTask(ImportingTask task) {
        ImportRequest request = task.getAssociatedRequest();
        UUID projectId = UUID.fromString(task.getProjectId());
        log.info("Recovering import task for project " + projectId);
        ArchiveTaskState taskState = state.retrieveImportTask(projectId).second();
        RemotePath remotePath = state.getPathByName(request.getArchivePath().getRemotePathName());
        RestartImportArchive command = RestartImportArchive.newBuilder()
            .setArchive(ImportArchive.newBuilder()
                .setTargetFolder(request.getArchivePath())
                .setNewProjectName(request.getNewProjectName())
                .setProjectId(projectId.toString())
                .setNewOwnerId(task.getCreatorId())
                .build()
            )
            .setState(taskState)
            .build();
        ActorRef minion = createMinion(remotePath);
        importMinions.put(projectId.toString(), minion);
        minion.forward(command, context());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals(SCHEDULED_TICK, this::checkScheduledTasks)
            .match(SaveSnapshotSuccess.class, this::snapshotSaved)
            .match(SaveSnapshotFailure.class, this::snapshotFailedToSave)
            .match(ArchiverCommand.class, ArchiverCommand::hasGetPaths, ignore -> getPaths())
            .match(ArchiverCommand.class, ArchiverCommand::hasGetFiles, this::getFiles)
            .match(ArchiverCommand.class, ArchiverCommand::hasCreateFolder, this::createFolder)
            .match(ArchiverCommand.class, ArchiverCommand::hasGetArchiveTasks, ignore -> getArchiveTasks())
            .match(ArchiverCommand.class, ArchiverCommand::hasGetImportingTasks, ignore -> getImportingTasks())
            .match(ArchiverCommand.class, ArchiverCommand::hasAddPath, validateThenPersistAndAck(state::validateAddPath))
            .match(ArchiverCommand.class, ArchiverCommand::hasDeletePath, validateThenPersistAndAck(state::validateDeletePath))
            .match(ArchiverCommand.class, ArchiverCommand::hasUpdatePath, validateThenPersistAndAck(state::validateUpdatePath))
            .match(ArchiverCommand.class, ArchiverCommand::hasArchiveProject, validateThenPersistAndAck(state::validateStartArchive))
            .match(ArchiverCommand.class, ArchiverCommand::hasImportArchive, validateThenPersistAndAck(state::validateStartImport))
            .match(ArchiverCommand.class, ArchiverCommand::hasRestartArchive, validateThenPersistAndAck(state::validateRestartArchive))
            .match(ArchiverCommand.class, ArchiverCommand::hasRestartImport, validateThenPersistAndAck(state::validateRestartImport))
            .match(ArchiverCommand.class, ArchiverCommand::hasUpdateArchiveProgress,
                validateThenPersistAndApply(state::validateUpdateArchiveProgress, this::updateArchiveProgress, true))
            .match(ArchiverCommand.class, ArchiverCommand::hasUpdateImportProgress,
                validateThenPersistAndApply(state::validateUpdateImportProgress, this::updateImportProgress, true))
            .match(ArchiverCommand.class, ArchiverCommand::hasCancelArchive,
                validateThenPersistAndApply(state::validateCancelArchive, this::cancelArchive))
            .match(ArchiverCommand.class, ArchiverCommand::hasCancelImport,
                validateThenPersistAndApply(state::validateCancelImport, this::cancelImport))
            .match(ArchiverCommand.class, ArchiverCommand::hasBatchCancelImports,
                validateThenPersistAndApply(state::validateBatchCancelImports, this::batchCancelImports))
            .build();
    }

    private void checkScheduledTasks(String tick) {
        if (archiveMinions.size() < numMinions && state.hasQueuedArchiveEvents()) {
            ArchiverEvent dequeueEvent = ArchiverEvent.newBuilder()
                .setArchiveEventDequeued(ArchiveEventDequeued.getDefaultInstance())
                .build();
            persistWithSnapshot(dequeueEvent, (e1, e2) -> {
                if (e2 != null) {
                    if (e2.hasArchiveStarted()) {
                        startArchive(e2);
                    } else if (e2.hasArchiveRestarted()) {
                        restartArchive(e2);
                    }
                }
            });
        }

        Boolean isFull = cassandraMonitorService.checkFull(UUID.fromString(ADMIN_USERID)).toCompletableFuture().join();

        if (isFull) {
            batchCancelImports();
        }

        if (!isFull && importMinions.size() < numMinions && state.hasQueuedImportEvents()) {
            ArchiverEvent dequeueEvent = ArchiverEvent.newBuilder()
                .setImportEventDequeued(ImportEventDequeued.getDefaultInstance())
                .build();
            persistWithSnapshot(dequeueEvent, (e1, e2) -> {
                if (e2 != null) {
                    if (e2.hasImportStarted()) {
                        startImport(e2);
                    } else if (e2.hasImportRestarted()) {
                        restartImport(e2);
                    }
                }
            });
        }
    }

    private void snapshotSaved(SaveSnapshotSuccess success) {
        log.info("Snapshot saved {}, try to delete previous snapshot now", success.metadata());
        if (success.metadata().sequenceNr() > snapshotInterval) {
            // it's possible the deletion doesn't work, akka will log that for us
            // it's fine to have a few undeleted snapshots in extreme cases as they are very small.
            deleteSnapshots(SnapshotSelectionCriteria.create(
                success.metadata().sequenceNr() - 1,
                success.metadata().timestamp())
            );
        }
    }

    private void snapshotFailedToSave(SaveSnapshotFailure failure) {
        log.warning("Unable to save snapshot {}", failure.metadata());
    }

    private void restartArchive(ArchiverEvent event) {
        ArchiveRestarted restarted = event.getArchiveRestarted();
        UUID projectId = UUID.fromString(restarted.getProjectId());
        Pair<ArchiveTask, ArchiveTaskState> pair = state.retrieveArchiveTask(projectId);
        ArchiveRequest request = pair.first().getAssociatedRequest();
        RestartArchiveProject command = RestartArchiveProject.newBuilder()
            .setArchive(ArchiveProject.newBuilder()
                .setProjectId(projectId.toString())
                .setTargetFolder(request.getArchivePath())
                .setCreatorId(pair.first().getCreatorId())
                .setOldProjectName(pair.first().getProjectName())
                .setDiscardScanPeaks(request.getDiscardScanPeaks())
                .build()
            )
            .setState(pair.second())
            .build();
        ActorRef minion = createMinion(restarted.getRemotePath());
        archiveMinions.put(projectId.toString(), minion);
        minion.forward(command, context());
    }

    private void restartImport(ArchiverEvent event) {
        ImportRestarted restarted = event.getImportRestarted();
        UUID projectId = UUID.fromString(restarted.getProjectId());
        Pair<ImportingTask, ArchiveTaskState> pair = state.retrieveImportTask(projectId);
        ImportRequest request = pair.first().getAssociatedRequest();
        RestartImportArchive command = RestartImportArchive.newBuilder()
            .setArchive(ImportArchive.newBuilder()
                .setTargetFolder(request.getArchivePath())
                .setNewProjectName(request.getNewProjectName())
                .setProjectId(projectId.toString())
                .setNewOwnerId(pair.first().getCreatorId())
                .build()
            )
            .setState(pair.second())
            .build();
        ActorRef minion = createMinion(restarted.getRemotePath());
        importMinions.put(projectId.toString(), minion);
        minion.forward(command, context());
    }

    private void cancelArchive(ArchiverEvent event) {
        ArchiveCancelled cancelled = event.getArchiveCancelled();
        String projectId = cancelled.getProjectId();
        if (archiveMinions.containsKey(projectId)) {
            archiveMinions.get(projectId).tell(StopArchiverMinion.getDefaultInstance(), self());
            archiveMinions.remove(projectId);
        } else {
            // try to cleanup directory without the archive minion
            try {
                RemotePath remotePath = state.getPathByName(cancelled.getPath().getRemotePathName());
                UniversalFileAccessor.deleteDir(remotePath, cancelled.getPath().getSubPath());
            } catch (Exception e) {
                // try to delete and give out a warning, but don't cause any trouble to outside
                log.warning("Caught error {}, unable to cleanup directory " + cancelled.getPath().getSubPath(), e);
            }
        }

        // cleanup the project aggregate entity, just tell it the archiving is done
        ProjectAggregateCommandFactory.ArchiveNotificationFactory factory = ProjectAggregateCommandFactory.project(
            UUID.fromString(projectId), WorkManager.ADMIN_USERID).archive();
        projectCommunication.command(factory.cancel());
        reply();
    }

    private void batchCancelImports(ArchiverEvent event) {
        for (String projectId : event.getImportsBatchCancelled().getProjectIdsList()) {
            if (importMinions.containsKey(projectId)) {
                importMinions.get(projectId).tell(StopArchiverMinion.getDefaultInstance(), self());
                importMinions.remove(projectId);
            }
        }
        reply();
    }

    private void batchCancelImports() {
        for (String projectId : importMinions.keySet()) {
            importMinions.get(projectId).tell(StopArchiverMinion.getDefaultInstance(), self());
            importMinions.remove(projectId);
        }
    }

    private void cancelImport(ArchiverEvent event) {
        String projectId = event.getImportCancelled().getProjectId();
        if (importMinions.containsKey(projectId)) {
            importMinions.get(projectId).tell(StopArchiverMinion.getDefaultInstance(), self());
            importMinions.remove(projectId);
        }
        reply();
    }

    private void updateArchiveProgress(ArchiverEvent event) {
        ProgressUpdate update = event.getArchiveProgressUpdated();
        if (update.getState() == Progress.State.ARCHIVING_DONE) {
            archiveMinions.remove(update.getProjectId());
        } else if (update.getState() == Progress.State.ARCHIVING_FAILED) {
            archiveMinions.remove(update.getProjectId());
        }
    }

    private void updateImportProgress(ArchiverEvent event) {
        ProgressUpdate update = event.getImportProgressUpdated();
        if (update.getState() == Progress.State.IMPORTING_DONE) {
            importMinions.remove(update.getProjectId());
        } else if (update.getState() == Progress.State.IMPORTING_FAILED) {
            importMinions.remove(update.getProjectId());
        }
    }

    private void getArchiveTasks() {
        List<ArchiveTask> tasks = state.archiveTaskSource().runWith(Sink.seq(), storageFactory.getMaterializer())
            .toCompletableFuture().join();
        sender().tell(tasks, self());
    }

    private void getImportingTasks() {
        List<ImportingTask> tasks = state.importingTaskSource().runWith(Sink.seq(), storageFactory.getMaterializer())
            .toCompletableFuture().join();
        sender().tell(tasks, self());
    }

    private void startArchive(ArchiverEvent event) {
        ArchiveProject job = event.getArchiveStarted().getProjectToArchive();
        RemotePath remotePath = event.getArchiveStarted().getRemotePath();
        ActorRef minion = createMinion(remotePath);
        archiveMinions.put(job.getProjectId(), minion);
        minion.forward(job, context());
    }

    private void startImport(ArchiverEvent event) {
        ImportArchive job = event.getImportStarted().getArchiveToImport();
        RemotePath remotePath = event.getImportStarted().getRemotePath();
        ActorRef minion = createMinion(remotePath);
        importMinions.put(job.getProjectId(), minion);
        minion.forward(job, context());
    }

    private void persistWithSnapshot(ArchiverEvent event) {
        persistWithSnapshot(event, null);
    }

    /**
     * Persist and save snapshot
     * @param event event to persist
     * @param handler, binary handler (event, event), the first event is the original event, the second event
     *                 is the event returned from state (specifically used to deque previous events)
     */
    private void persistWithSnapshot(ArchiverEvent event, FI.UnitApply2<ArchiverEvent, ArchiverEvent> handler) {
        persist(event, persisted -> {
            // returned event may not be the original event, don't rely on it
            ArchiverEvent returnedEvent = state.update(persisted);
            if (lastSequenceNr() % snapshotInterval == 0 && lastSequenceNr() != 0) {
                saveSnapshot(state);
            }
            if (handler != null) {
                handler.apply(event, returnedEvent);
            }
        });
    }

    private FI.UnitApply<ArchiverCommand> validateThenPersistAndApply(Function<ArchiverCommand, ArchiverEvent> validator,
                                                                      FI.UnitApply<ArchiverEvent> eventHandler, boolean suppressError) {
        return command -> {
            try {
                ArchiverEvent event = validator.apply(command);
                persistWithSnapshot(event, (e1, e2) -> eventHandler.apply(e1));
            } catch (Exception e) {
                if (!suppressError) {
                    sendError(e);
                }
            }
        };
    }

    private FI.UnitApply<ArchiverCommand> validateThenPersistAndApply(Function<ArchiverCommand, ArchiverEvent> validator,
                                                                      FI.UnitApply<ArchiverEvent> eventHandler) {
        return validateThenPersistAndApply(validator, eventHandler, false);
    }

    private FI.UnitApply<ArchiverCommand> validateThenPersistAndAck(Function<ArchiverCommand, ArchiverEvent> validator) {
        return command -> {
            try {
                ArchiverEvent event = validator.apply(command);
                persistWithSnapshot(event);
                reply();
            } catch (Exception e) {
                sendError(e);
            }
        };
    }

    private void createFolder(ArchiverCommand command) {
        FilePath folder = command.getCreateFolder().getFolderToCreate();
        RemotePath remotePath = state.getPathByName(folder.getRemotePathName());
        String subPath = folder.getSubPath();
        if (remotePath == null) {
            sendError(new IllegalArgumentException("No matching remote repository is found for " + folder.getRemotePathName()));
        } else {
            // try to create dir
            try {
                RemoteType type = Detector.detectRemote(remotePath);
                switch (type) {
                    case LFS:
                        LFSHelper.mkdir(remotePath.getPath(), subPath);
                        break;
                    case FTP:
                        FTPHelper.mkdir(remotePath.getPath(), subPath);
                        break;
                    default:
                        throw new UnsupportedOperationException("Remote type not supported");
                }
                reply();
            } catch (Exception e) {
                sendError(e);
            }
        }
    }

    private void getPaths() {
        ArchiverResponse response = ArchiverResponse.newBuilder()
            .setPaths(
                PathsResponse.newBuilder()
                    .addAllPaths(state.getPaths())
                    .build()
            )
            .build();
        sender().tell(response, self());
    }

    private void getFiles(ArchiverCommand command) {
        RemotePath remotePath = state.getPathByName(command.getGetFiles().getDir().getRemotePathName());
        if (remotePath == null) {
            sendError(new IllegalArgumentException("Remote path name doesn't exist"));
            return;
        }
        ActorRef browser = context().actorOf(FileBrowser.props(
            remotePath,
            SourceType.nonPeaksTypes(),
            (e) -> {
                String message = e == null || e.getMessage() == null ? "Unknown error" : e.getMessage();
                return UploaderError.newBuilder().setMessage(message).build();
            }
        ));
        browser.forward(command.getGetFiles(), context());
    }

    private void sendError(Exception e) {
        log.error(e, "Archiver captured exception, forward to master");
        String message = e == null || e.getMessage() == null ? "Unknown error" : e.getMessage();
        ArchiverResponse response = ArchiverResponse.newBuilder()
            .setError(message)
            .build();
        sender().tell(response, self());
    }

    private void reply() {
        ArchiverResponse response = ArchiverResponse.newBuilder()
            .setAck(AckStatus.ACCEPT)
            .build();
        sender().tell(response, self());
    }

    private ActorRef createMinion(RemotePath remotePath) {
        return context().actorOf(
            ArchiverMinion.props(self(), storageFactory, projectService, databaseService, projectCommunication, remotePath)
        );
    }
}
