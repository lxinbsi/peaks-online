package com.bsi.peaks.server.archiver;

import akka.Done;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.javadsl.CurrentEventsByPersistenceIdQuery;
import akka.persistence.query.journal.leveldb.javadsl.LeveldbReadJournal;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.Version;
import com.bsi.peaks.common.Instant;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.ProjectAggregateCommandFactory.ArchiveNotificationFactory;
import com.bsi.peaks.event.ProjectAggregateEventFactory;
import com.bsi.peaks.event.project.ArchiveProject;
import com.bsi.peaks.event.project.ArchiveTaskProgress;
import com.bsi.peaks.event.project.ArchiveTaskState;
import com.bsi.peaks.event.project.ArchiverCommand;
import com.bsi.peaks.event.project.ImportArchive;
import com.bsi.peaks.event.project.ProgressUpdate;
import com.bsi.peaks.event.project.ProjectEventToRestore;
import com.bsi.peaks.event.project.RestartArchiveProject;
import com.bsi.peaks.event.project.RestartImportArchive;
import com.bsi.peaks.event.project.StopArchiverMinion;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.projectAggregate.ProjectInformation;
import com.bsi.peaks.event.projectAggregate.ProjectPermission;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.dto.ProjectTaskListing;
import com.bsi.peaks.model.dto.RemotePath;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.Launcher;
import com.bsi.peaks.server.es.ProjectAggregateEntity;
import com.bsi.peaks.server.es.WorkManager;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.service.common.ArchiveHelpers;
import com.bsi.peaks.service.common.ServiceCancellationException;
import com.bsi.peaks.service.common.ServiceKiller;
import com.bsi.peaks.service.services.TaskArchiver;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbFilterSummarizationFilterArchiver;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbPeptideSummarizationArchiver;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbPreSearchArchiver;
import com.bsi.peaks.service.services.dbsearch.DIA.DiaDbSearchArchiver;
import com.bsi.peaks.service.services.dbsearch.DbBatchSearchArchiver;
import com.bsi.peaks.service.services.dbsearch.DbDeNovoOnlyTagSearchArchiver;
import com.bsi.peaks.service.services.dbsearch.DbFilterSummarizationArchiver;
import com.bsi.peaks.service.services.dbsearch.DbSearchSummarizationArchiver;
import com.bsi.peaks.service.services.dbsearch.FDBProteinCandidateFromFastaArchiver;
import com.bsi.peaks.service.services.dbsearch.PreSearchArchiver;
import com.bsi.peaks.service.services.dbsearch.TagSearchArchiver;
import com.bsi.peaks.service.services.dbsearch.TagSearchSummarizeArchiver;
import com.bsi.peaks.service.services.denovo.DenovoArchiver;
import com.bsi.peaks.service.services.denovo.DenovoSummarizeArchiver;
import com.bsi.peaks.service.services.featuredetection.FeatureDetectionArchiver;
import com.bsi.peaks.service.services.labelfreequantification.LfqFeatureAlignmentArchiver;
import com.bsi.peaks.service.services.labelfreequantification.LfqRetentionTimeAlignmentArchiver;
import com.bsi.peaks.service.services.labelfreequantification.LfqSummarizationArchiver;
import com.bsi.peaks.service.services.labelfreequantification.LfqSummarizationFilterArchiver;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaFeatureExtractorArchiver;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaLfqFilterSummarizationArchiver;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaLfqSummarizeArchive;
import com.bsi.peaks.service.services.labelfreequantification.dia.DiaRententionTimeAlignmentArchiver;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.ReporterIonQSummarizeArchiver;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.RiqBatchCalcArchiver;
import com.bsi.peaks.service.services.labelquantification.reporterIonq.RiqNormalizationArchiver;
import com.bsi.peaks.service.services.preprocess.DataRefineArchiver;
import com.bsi.peaks.service.services.preprocess.dia.DiaDataRefineArchiver;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderArchiver;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderDeNovoOnlyTagSearchArchiver;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderFilterSummarizationArchiver;
import com.bsi.peaks.service.services.ptmfinder.PtmFinderSummarizeArchiver;
import com.bsi.peaks.service.services.silac.SilacFeatureAlignmentArchiver;
import com.bsi.peaks.service.services.silac.SilacFeatureVectorSearchArchiver;
import com.bsi.peaks.service.services.silac.SilacFilterSummarizationArchiver;
import com.bsi.peaks.service.services.silac.SilacPeptideSummarizationArchiver;
import com.bsi.peaks.service.services.silac.SilacRtAlignmentArchiver;
import com.bsi.peaks.service.services.spectralLibrary.SlFilterSummarizationArchiver;
import com.bsi.peaks.service.services.spectralLibrary.SlPeptideSummarizationArchiver;
import com.bsi.peaks.service.services.spectralLibrary.SlSearchArchiver;
import com.bsi.peaks.service.services.spectralLibrary.dda.DdaSlSearchArchiver;
import com.bsi.peaks.service.services.spider.SpiderBatchSearchArchiver;
import com.bsi.peaks.service.services.spider.SpiderDeNovoOnlyTagSearchArchiver;
import com.bsi.peaks.service.services.spider.SpiderFilterSummarizationArchiver;
import com.bsi.peaks.service.services.spider.SpiderSummarizeArchiver;
import com.bsi.peaks.utilities.UniversalFileAccessor;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;

public class ArchiverMinion extends AbstractActor {
    private static final String TASK_FOLDER = "tasks";
    private static final String PROJECT_FILE = "peaks.meta";
    private static final String TASKS_FILE = "peaks.tasks";
    private static final String EVENTS_FILE = "peaks.events";
    private static final String INFO_FILE = "project.txt";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final ActorRef archiver;
    private final ApplicationStorageFactory storageFactory;
    private final ProjectService projectService;
    private final ProjectAggregateCommunication projectCommunication;
    private final FastaDatabaseService databaseService;
    private final RemotePath remotePath;
    private final Materializer materializer;
    private final ServiceKiller killer;
    private final int parallelism;
    private final AtomicInteger ongoingArchivingTasks;

    private ArchiverMinion(ActorRef archiver, ApplicationStorageFactory storageFactory, ProjectService projectService,
                           FastaDatabaseService databaseService, ProjectAggregateCommunication projectCommunication,
                           RemotePath path) {
        this.archiver = archiver;
        this.storageFactory = storageFactory;
        this.projectService = projectService;
        this.projectCommunication = projectCommunication;
        this.databaseService = databaseService;
        this.remotePath = path;
        this.materializer = storageFactory.getMaterializer();
        this.killer = new ServiceKiller();
        this.parallelism = context().system().settings().config().getInt("peaks.server.archiver.task-parallelism");
        this.ongoingArchivingTasks = new AtomicInteger(0);
    }

    static Props props(ActorRef archiver, ApplicationStorageFactory storageFactory, ProjectService projectService,
                       FastaDatabaseService databaseService, ProjectAggregateCommunication projectCommunication,
                       RemotePath path) {
        return Props.create(ArchiverMinion.class, () ->
            new ArchiverMinion(archiver, storageFactory, projectService, databaseService, projectCommunication, path));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(ArchiveProject.class, this::startArchive)
            .match(ImportArchive.class, this::importArchive)
            .match(StopArchiverMinion.class, this::stopProcessing)
            .match(RestartArchiveProject.class, this::restartArchive)
            .match(RestartImportArchive.class, this::restartImport)
            .build();
    }

    private void stopProcessing(StopArchiverMinion msg) {
        killer.kill();
    }

    private ArchiverCommand createCommand(ProgressUpdate.Builder progressBuilder) {
        switch (progressBuilder.getState()) {
            case ARCHIVING:
            case ARCHIVING_FAILED:
            case ARCHIVING_DONE:
                return ArchiverCommand.newBuilder()
                    .setUpdateArchiveProgress(progressBuilder)
                    .build();
            case IMPORTING:
            case IMPORTING_FAILED:
            case IMPORTING_DONE:
                return ArchiverCommand.newBuilder()
                    .setUpdateImportProgress(progressBuilder)
                    .build();
            default:
                throw new IllegalStateException("Unsupported state: " + progressBuilder.getState());
        }
    }

    private void restartArchive(RestartArchiveProject restart) {
        startArchive(restart.getArchive(), restart.getState());
    }

    private void restartImport(RestartImportArchive restart) {
        importArchive(restart.getArchive(), restart.getState());
    }

    private void importArchive(ImportArchive job, ArchiveTaskState previousState) {
        String destination = job.getTargetFolder().getSubPath();
        UUID projectId = UUID.fromString(job.getProjectId());

        long startTime = System.currentTimeMillis();
        log.info("Started importing project " + projectId);

        ProgressUpdate.Builder progressBuilder = ProgressUpdate.newBuilder()
            .setProjectId(projectId.toString())
            .setState(Progress.State.IMPORTING);

        CompletableFuture.supplyAsync(() -> {
            List<ProjectTaskListing> doneTasks = parseTaskList(destination).stream()
                .filter(task -> task.getState() == Progress.State.DONE) // only import done tasks
                .collect(Collectors.toList());
            List<ProjectTaskListing> tasks = doneTasks.stream()
                .filter(task -> {
                    if (previousState == null) {
                        return true;
                    } else {
                        ArchiveTaskProgress progress = previousState.getTasksOrDefault(task.getTaskId(), null);
                        return progress == null || progress.getState() != Progress.State.IMPORTING_DONE;
                    }
                })
                .collect(Collectors.toList());

            progressBuilder.setTotalTasks(doneTasks.size()).setFinishedTasks(doneTasks.size() - tasks.size());

            // load project first but override all timestamp to now so we can put it into latest data keyspace
            // instead of the fallback system keyspace
            Instant now = CommonFactory.nowTimeStamp();
            Project project = loadProject(destination, job.getNewProjectName(), now);

            // Calculate keyspace according to modified project creation date.
            // This creation time was modified in loadProject(...)
            String keyspace = storageFactory.getKeyspace(project);

            // first unarchive tasks
            unarchiveTasks(destination, keyspace, tasks, progressBuilder);
            killer.assertKilled();
            // load project
            unarchiveProject(now, destination, project, keyspace, job.getNewProjectName(), job.getNewOwnerId());
            killer.assertKilled();
            return Done.getInstance();
        }).whenComplete((ignore, error) -> {
            if (error != null) {
                if (!(error instanceof ServiceCancellationException)) {
                    log.error(error, "Unable to import project " + projectId);
                    ArchiverCommand failed = createCommand(progressBuilder.setState(Progress.State.IMPORTING_FAILED));
                    archiver.tell(failed, self());
                } else {
                    log.warning("Importing project " + projectId + " was cancelled");
                }
            } else {
                ArchiverCommand done = createCommand(progressBuilder
                    .setFinishedTasks(progressBuilder.getTotalTasks())
                    .setState(Progress.State.IMPORTING_DONE)
                );
                archiver.tell(done, self());

                long endTime = System.currentTimeMillis();
                log.info("Finished importing project " + projectId
                    + ", time consumption in seconds: " + (endTime - startTime) / 1000.0);
            }

            context().stop(self());
        });
    }

    private void importArchive(ImportArchive job) {
        importArchive(job, null);
    }

    private void startArchive(ArchiveProject job) {
        startArchive(job, null);
    }

    private void startArchive(ArchiveProject job, ArchiveTaskState previousState) {
        // Since this is in a separate thread and only has one thing to do, we can do everything synchronously
        UUID projectId = UUID.fromString(job.getProjectId());
        String destination = job.getTargetFolder().getSubPath();

        long startTime = System.currentTimeMillis();
        log.info("Started archiving project " + projectId);

        ArchiveNotificationFactory archiveNotificationFactory = ProjectAggregateCommandFactory.project(projectId, WorkManager.ADMIN_USERID).archive();
        ProgressUpdate.Builder progressBuilder = ProgressUpdate.newBuilder()
            .setProjectId(projectId.toString())
            .setState(Progress.State.ARCHIVING);

        CompletableFuture.supplyAsync(() -> {
            Project project = projectService.project(projectId, User.admin())
                .toCompletableFuture().join()
                .toBuilder()
                .setVersion(Version.CORE_VERSION)
                .build(); // attach version number to project

            ProjectAggregateQueryResponse.ProjectQueryResponse response = projectCommunication.command(archiveNotificationFactory.start())
                .thenCompose(done -> projectCommunication.queryProject(ProjectAggregateQueryRequest.newBuilder()
                    .setProjectId(uuidToByteString(projectId))
                    .setUserId(uuidToByteString(WorkManager.ADMIN_USERID))
                    .setProjectQueryRequest(ProjectAggregateQueryRequest.ProjectQueryRequest.newBuilder()
                        .setTaskListing(ProjectAggregateQueryRequest.ProjectQueryRequest.TaskListing.getDefaultInstance())
                    )
                    .build())
                ).toCompletableFuture().join();

            List<ProjectTaskListing> doneTasks = response.getTaskListing().getTasksList().stream()
                .filter(task -> !task.getDeleted())
                .filter(task -> task.getState() == Progress.State.DONE) // only archive done tasks
                .collect(Collectors.toList());

            List<ProjectTaskListing> tasks = doneTasks.stream()
                .filter(task -> {
                    if (previousState == null) {
                        return true;
                    } else {
                        ArchiveTaskProgress progress = previousState.getTasksOrDefault(task.getTaskId(), null);
                        return progress == null || progress.getState() != Progress.State.ARCHIVING_DONE;
                    }
                })
                .collect(Collectors.toList());

            progressBuilder.setTotalTasks(doneTasks.size());
            progressBuilder.setFinishedTasks(doneTasks.size() - tasks.size());
            killer.assertKilled();

            summarizeProject(destination, project);
            killer.assertKilled();

            archiveTasks(destination, project.getKeyspace(), tasks, progressBuilder, job.getDiscardScanPeaks());
            killer.assertKilled();

            // then archive task list
            final File taskFile = new File(destination, TASKS_FILE);
            try (OutputStream out = UniversalFileAccessor.createFile(remotePath, taskFile.getPath())) {
                Source.from(doneTasks).runForeach(task -> task.writeDelimitedTo(out), materializer).toCompletableFuture().join();
            } catch (IOException e) {
                throw new RuntimeException("Unable to archive task list");
            }

            // then archive project
            archiveProject(destination, project);
            killer.assertKilled();

            return Done.getInstance();
        }).whenComplete((ignore, error) -> {
            if (error != null) {
                if (!(ExceptionUtils.getRootCause(error) instanceof ServiceCancellationException)) {
                    log.error(error, "Unable to archive project " + projectId);
                    ArchiverCommand failed = createCommand(progressBuilder.setState(Progress.State.ARCHIVING_FAILED));
                    archiver.tell(failed, self());
                } else {
                    log.warning("Archiving project " + projectId + " was cancelled");
                    // wait for on going archiving tasks to finish first, but only for a given amount of time (2 mins)
                    int countdown = 240;
                    while (ongoingArchivingTasks.get() > 0 && countdown > 0) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Unable to sleep while waiting for archiving tasks to complete");
                        }
                        countdown--;
                    }

                    // delete folder
                    try {
                        // try our best to delete the folder
                        log.info("Trying to delete folder " + destination + " after cancelled archiving");
                        UniversalFileAccessor.deleteDir(remotePath, destination);
                    } catch (Exception e) {
                        // try to delete and give out a warning, but don't cause any trouble to outside
                        log.warning("Caught error {}, unable to cleanup directory " + destination, e);
                    }
                }
            } else {
                projectCommunication.command(archiveNotificationFactory.done());
                ArchiverCommand done = createCommand(progressBuilder
                    .setFinishedTasks(progressBuilder.getTotalTasks())
                    .setState(Progress.State.ARCHIVING_DONE)
                );
                archiver.tell(done, self());
            }

            long endTime = System.currentTimeMillis();
            log.info("Finished archiving project " + projectId
                + ", time consumption in seconds: " + (endTime - startTime) / 1000.0);

            context().stop(self());
        });
    }

    private void unarchiveTasks(String folder, String keyspace, List<ProjectTaskListing> tasks, ProgressUpdate.Builder progressBuilder) {
        File taskFolder = new File(folder, TASK_FOLDER);
        boolean hasTaskFolder = UniversalFileAccessor.dirExists(remotePath, taskFolder.getPath());
        Source.from(tasks)
            .via(killer.flow())
            .mapAsyncUnordered(parallelism, task -> unarchiveTask(keyspace, task, hasTaskFolder ? taskFolder.getPath() : folder))
            .runForeach(task -> {
                ArchiverCommand command = createCommand(progressBuilder
                    .setFinishedTasks(progressBuilder.getFinishedTasks() + 1)
                    .clearChangedTasks()
                    .addChangedTasks(ArchiveTaskProgress.newBuilder()
                        .setProjectId(progressBuilder.getProjectId())
                        .setTaskId(task.getTaskId())
                        .setState(Progress.State.IMPORTING_DONE)
                        .setType(task.getType())
                        .build()
                    )
                );
                archiver.tell(command, self());
            }, materializer)
            .toCompletableFuture().join();
        log.info("Finished unarchiving all tasks");
    }

    private CompletionStage<ProjectTaskListing> unarchiveTask(String keyspace, ProjectTaskListing task, String folder) {
        return CompletableFuture.supplyAsync(() -> {
            log.info("Started unarchiving task {}: {} for project {}", task.getType(), task.getTaskId(), task.getProjectId());
            TaskArchiver archiver = findArchiver(keyspace, task);
            File archiveFile = new File(folder, task.getTaskId() + ".task");
            CompletableFuture<Done> done = new CompletableFuture<>();
            try (InputStream in = UniversalFileAccessor.readFile(remotePath, archiveFile.getPath(), done)) {
                archiver.unarchive(task, in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                done.complete(Done.getInstance());
            }
            return task;
        }).whenComplete((integer, throwable) -> {
            if (throwable == null) {
                log.info("Finished unarchiving task {}: {} for project {}", task.getType(), task.getTaskId(), task.getProjectId());
            } else {
                log.error("Error unarchiving task {}: {} for project {}", task.getType(), task.getTaskId(), task.getProjectId());
            }
        });
    }

    private void archiveTasks(String folder, String keyspace, List<ProjectTaskListing> tasks,
                              ProgressUpdate.Builder progressBuilder, boolean discardPeaks) {
        File taskFolder = new File(folder, TASK_FOLDER);
        if (!UniversalFileAccessor.dirExists(remotePath, taskFolder.getPath())) {
            UniversalFileAccessor.mkdir(remotePath, taskFolder.getPath());
        }
        // first archive each individual task
        Source.from(tasks)
            .via(killer.flow())
            .zipWithIndex()
            .mapAsyncUnordered(parallelism, pair -> archiveTask(pair, keyspace, taskFolder.getPath(), discardPeaks))
            .runForeach(index -> {
                final ProjectTaskListing task = tasks.get(index);
                ArchiverCommand command = createCommand(progressBuilder
                    .setFinishedTasks(progressBuilder.getFinishedTasks() + 1)
                    .clearChangedTasks()
                    .addChangedTasks(ArchiveTaskProgress.newBuilder()
                        .setProjectId(progressBuilder.getProjectId())
                        .setTaskId(task.getTaskId())
                        .setState(Progress.State.ARCHIVING_DONE)
                        .setType(task.getType())
                        .build()
                    )
                );
                archiver.tell(command, self());
            }, materializer)
            .toCompletableFuture().join();
        log.info("Finished archiving all tasks");
    }

    private CompletionStage<Integer> archiveTask(Pair<ProjectTaskListing, Long> pair, String keyspace, String folder, boolean discardPeaks) {
        ongoingArchivingTasks.incrementAndGet();
        return CompletableFuture.supplyAsync(() -> {
            ProjectTaskListing task = pair.first();
            log.info("Started archiving task {}: {} for project {}", task.getType(), task.getTaskId(), task.getProjectId());
            TaskArchiver archiver = findArchiver(keyspace, task, discardPeaks);
            File archiveFile = new File(folder, task.getTaskId() + ".task");
            try (OutputStream out = UniversalFileAccessor.createFile(remotePath, archiveFile.getPath())) {
                archiver.archive(task, out);
            } catch (Throwable e) {
                log.error(e,"Error archiving task {}: {} for project {}", task.getType(), task.getTaskId(), task.getProjectId());
                throw new RuntimeException("Unable to archive task " + task.getType() + ", id " + task.getTaskId());
            }
            log.info("Finished archiving task {}: {} for project {}", task.getType(), task.getTaskId(), task.getProjectId());
            ongoingArchivingTasks.decrementAndGet();
            return pair.second().intValue();
        });
    }

    private TaskArchiver findArchiver(String keyspace, ProjectTaskListing task) {
        return findArchiver(keyspace, task, false);
    }

    private TaskArchiver findArchiver(String keyspace, ProjectTaskListing task, boolean discardPeaks) {
        ApplicationStorage storage = storageFactory.getStorage(keyspace);
        switch (task.getType()) {
            case DATA_LOADING:
                return new DataLoadingArchiver(storage, discardPeaks);
            case DIA_DATA_REFINE:
            case DIA_DB_DATA_REFINE:
                return new DiaDataRefineArchiver(storage, task.getType());
            case DATA_REFINE:
                return new DataRefineArchiver(storage);
            case FEATURE_DETECTION:
                return new FeatureDetectionArchiver(storage);
            case DENOVO:
                return new DenovoArchiver(storage);
            case DENOVO_FILTER:
                return new DenovoSummarizeArchiver(storage);
            case DB_TAG_SEARCH:
                return new TagSearchArchiver(storage);
            case DB_BATCH_SEARCH:
                return new DbBatchSearchArchiver(storage);
            case DB_TAG_SUMMARIZE:
                return new TagSearchSummarizeArchiver(storage);
            case FDB_PROTEIN_CANDIDATE_FROM_FASTA:
                return new FDBProteinCandidateFromFastaArchiver(storage);
            case DB_PRE_SEARCH:
                return new PreSearchArchiver(storage);
            case DB_SUMMARIZE:
                return new DbSearchSummarizationArchiver(storage);
            case DB_FILTER_SUMMARIZATION:
                return new DbFilterSummarizationArchiver(storage);
            case PTM_FINDER_BATCH_SEARCH:
                return new PtmFinderArchiver(storage);
            case PTM_FINDER_SUMMARIZE:
                return new PtmFinderSummarizeArchiver(storage);
            case PTM_FINDER_FILTER_SUMMARIZATION:
                return new PtmFinderFilterSummarizationArchiver(storage);
            case SPIDER_BATCH_SEARCH:
                return new SpiderBatchSearchArchiver(storage);
            case SPIDER_SUMMARIZE:
                return new SpiderSummarizeArchiver(storage);
            case SPIDER_FILTER_SUMMARIZATION:
                return new SpiderFilterSummarizationArchiver(storage);
            case LFQ_RETENTION_TIME_ALIGNMENT:
                return new LfqRetentionTimeAlignmentArchiver(storage);
            case LFQ_FEATURE_ALIGNMENT:
                return new LfqFeatureAlignmentArchiver(storage);
            case LFQ_SUMMARIZE:
                return new LfqSummarizationArchiver(storage);
            case LFQ_FILTER_SUMMARIZATION:
                return new LfqSummarizationFilterArchiver(storage);
            case REPORTER_ION_Q_BATCH_CALCULATION:
                return new RiqBatchCalcArchiver(storage);
            case REPORTER_ION_Q_NORMALIZATION:
                return new RiqNormalizationArchiver(storage);
            case REPORTER_ION_Q_FILTER_SUMMARIZATION:
                return new ReporterIonQSummarizeArchiver(storage);
            case DB_DENOVO_ONLY_TAG_SEARCH:
                return new DbDeNovoOnlyTagSearchArchiver(storage);
            case PTM_FINDER_DENOVO_ONLY_TAG_SEARCH:
                return new PtmFinderDeNovoOnlyTagSearchArchiver(storage);
            case SPIDER_DENOVO_ONLY_TAG_SEARCH:
                return new SpiderDeNovoOnlyTagSearchArchiver(storage);
            case SL_PEPTIDE_SUMMARIZATION:
                return new SlPeptideSummarizationArchiver(storage);
            case DIA_DB_PEPTIDE_SUMMARIZE:
                return new DiaDbPeptideSummarizationArchiver(storage);
            case SL_SEARCH:
                return new SlSearchArchiver(storage);
            case DIA_DB_PRE_SEARCH:
                return new DiaDbPreSearchArchiver(storage);
            case DIA_DB_SEARCH:
                return new DiaDbSearchArchiver(storage);
            case DDA_SL_SEARCH:
                return new DdaSlSearchArchiver(storage);
            case SL_FILTER_SUMMARIZATION:
                return new SlFilterSummarizationArchiver(storage);
            case DIA_DB_FILTER_SUMMARIZE:
                return new DiaDbFilterSummarizationFilterArchiver(storage);
            case SILAC_FEATURE_VECTOR_SEARCH:
                return new SilacFeatureVectorSearchArchiver(storage);
            case SILAC_PEPTIDE_SUMMARIZE:
                return new SilacPeptideSummarizationArchiver(storage);
            case SILAC_FILTER_SUMMARIZE:
                return new SilacFilterSummarizationArchiver(storage);
            case SILAC_FEATURE_ALIGNMENT:
                return new SilacFeatureAlignmentArchiver(storage);
            case SILAC_RT_ALIGNMENT:
                return new SilacRtAlignmentArchiver(storage);
            case DIA_LFQ_RETENTION_TIME_ALIGNMENT:
                return new DiaRententionTimeAlignmentArchiver(storage);
            case DIA_LFQ_FEATURE_EXTRACTION:
                return new DiaFeatureExtractorArchiver(storage);
            case DIA_LFQ_SUMMARIZE:
                return new DiaLfqSummarizeArchive(storage);
            case DIA_LFQ_FILTER_SUMMARIZATION:
                return new DiaLfqFilterSummarizationArchiver(storage);
            default:
                throw new IllegalStateException("Unsupported archiving task " + task.getType());
        }
    }

    private List<ProjectTaskListing> parseTaskList(String folder) {
        final File taskFile = new File(folder, TASKS_FILE);
        CompletableFuture<Done> done = new CompletableFuture<>();
        try(InputStream in = UniversalFileAccessor.readFile(remotePath, taskFile.getPath(), done)) {
            return ArchiveHelpers.createProtoSource(in, ProjectTaskListing::parseDelimitedFrom)
                .runWith(Sink.seq(), materializer)
                .toCompletableFuture().join();
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse task list", e);
        } finally {
            done.complete(Done.getInstance());
        }
    }

    private Project loadProject(String folder, String newProjectName, Instant creationTimeOverride) {
        final File projectFile = new File(folder, PROJECT_FILE);
        final Project project;
        CompletableFuture<Done> done = new CompletableFuture<>();
        try (InputStream in = UniversalFileAccessor.readFile(remotePath, projectFile.getPath(), done)) {
            project = Project.parseDelimitedFrom(in);
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse project information", e);
        } finally {
            done.complete(Done.getInstance());
        }

        if (project.getVersion() != Version.CORE_VERSION) {
            throw new IllegalStateException("Project " + newProjectName + "(" + project.getId()
                + ") has a version number " + project.getVersion() + " that doesn't match system version " + Version.CORE_VERSION);
        }
        return project.toBuilder().setCreatedTimestamp(CommonFactory.convert(creationTimeOverride).getEpochSecond()).build();
    }

    private void unarchiveProject(Instant now, String folder, Project project, String keyspace, String newProjectName, String newOwnerId) {
        final File eventFile = new File(folder, EVENTS_FILE);

        UUID projectId = UUID.fromString(project.getId());
        UUID ownerId = UUID.fromString(newOwnerId);
        CompletableFuture<Done> done = new CompletableFuture<>();
        try (InputStream in = UniversalFileAccessor.readFile(remotePath, eventFile.getPath(), done)) {
            ProjectEventToRestore.Builder messageBuilder = ProjectEventToRestore.newBuilder();
            // override timestamp of events to put imported project into latest data keyspace
            List<ProjectAggregateEvent> events = ArchiveHelpers.createProtoSource(in, ProjectAggregateEvent::parseDelimitedFrom)
                .zipWithIndex()
                .map(eventIndex -> {
                    ProjectAggregateEvent event = eventIndex.first();
                    int timeAcc = eventIndex.second().intValue();
                    ProjectAggregateEvent.Builder builder = event.toBuilder();
                    if (event.hasProject() && event.getProject().hasProjectCreated()) {
                        // intercept the project creation event to override keyspace
                        builder.getProjectBuilder().getProjectCreatedBuilder().setStorageKeyspace(keyspace);
                    }
                    // Artificially increment each timestamp with a counter, need to do this so
                    // unarchived project is in current keyspace instead of system keyspace
                    final Instant orderedInstant = now.toBuilder().setSecond(now.getSecond())
                        .setNano(now.getNano() + timeAcc).build();
                    return builder.setTimeStamp(orderedInstant)
                        .build();
                })
                .runWith(Sink.seq(), materializer).toCompletableFuture().join();
            messageBuilder.addAllEvents(events);

            // In case we want to reset the project name, we just append an extra event
            if (newProjectName != null && !newProjectName.isEmpty()) {
                ProjectInformation infoOverride = ProjectInformation.newBuilder()
                    .setName(newProjectName)
                    .setDescription(project.getDescription())
                    .build();
                ProjectAggregateEvent renameEvent = ProjectAggregateEventFactory
                    .project(projectId, ownerId)
                    .projectInformation(infoOverride);
                messageBuilder.addEvents(renameEvent);
            }

            // We also need to reset the project owner to the one imported the project
            ProjectPermission permission = ProjectPermission.newBuilder()
                .setOwnerId(ByteString.copyFrom(ModelConversion.uuidToBytes(ownerId)))
                .setShared(false)
                .build();
            ProjectAggregateEvent resetOwnerEvent = ProjectAggregateEventFactory
                .project(projectId, ownerId)
                .projectPermission(permission);
            messageBuilder.addEvents(resetOwnerEvent);

            projectCommunication.restoreProject(messageBuilder.build()).toCompletableFuture().join();
        } catch (IOException e) {
            throw new RuntimeException("Unable to unarchive project", e);
        } finally {
            done.complete(Done.getInstance());
        }
    }

    private void summarizeProject(String folder, Project project) {
        final ProjectSummarizer summarizer = new ProjectSummarizer(projectService, databaseService, project, materializer);
        final File infoFile = new File(folder, INFO_FILE);
        try (OutputStream out = UniversalFileAccessor.createFile(remotePath, infoFile.getPath())) {
            summarizer.summarize(out);
        } catch (IOException e) {
            throw new RuntimeException("Unable to summarize project", e);
        }
    }

    private void archiveProject(String folder, Project project) {
        final PersistenceQuery persistenceQuery = PersistenceQuery.get(context().system());
        final CurrentEventsByPersistenceIdQuery journal = Launcher.useCassandraJournal ? persistenceQuery.getReadJournalFor(
            CassandraReadJournal.class,
            CassandraReadJournal.Identifier()
        ) : persistenceQuery.getReadJournalFor(
            LeveldbReadJournal.class,
            LeveldbReadJournal.Identifier()
        );
        final String persistenceId = ProjectAggregateEntity.persistenceId(project.getId());
        final File eventFile = new File(folder, EVENTS_FILE);
        final File projectFile = new File(folder, PROJECT_FILE);

        try (OutputStream out = UniversalFileAccessor.createFile(remotePath, eventFile.getPath())) {
            journal.currentEventsByPersistenceId(persistenceId,0, Long.MAX_VALUE)
                .map(EventEnvelope::event)
                .map(GeneratedMessageV3.class::cast)
                .map(msg -> ProjectAggregateEvent.newBuilder().mergeFrom(msg).build())
                .runForeach(event -> event.writeDelimitedTo(out), materializer)
                .toCompletableFuture().join();
            log.info("Project event written for project {}", project.getId());
        } catch (IOException e) {
            throw new RuntimeException("Unable to archive project events");
        }

        try (OutputStream out = UniversalFileAccessor.createFile(remotePath, projectFile.getPath())) {
            project.writeDelimitedTo(out);
        } catch (IOException e) {
            throw new RuntimeException("Unable to archive project information");
        }
    }
}
