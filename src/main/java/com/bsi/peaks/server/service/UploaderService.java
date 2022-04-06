package com.bsi.peaks.server.service;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.BackoffSupervisor;
import akka.pattern.Patterns;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.UploaderEventFactory;
import com.bsi.peaks.event.projectAggregate.RemoteFraction;
import com.bsi.peaks.event.projectAggregate.RemoteSample;
import com.bsi.peaks.event.uploader.FileToUpload;
import com.bsi.peaks.event.uploader.FileUploaded;
import com.bsi.peaks.event.uploader.GetFiles;
import com.bsi.peaks.event.uploader.GetPaths;
import com.bsi.peaks.event.uploader.PathsResponse;
import com.bsi.peaks.event.uploader.UploadFiles;
import com.bsi.peaks.event.uploader.UploaderError;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.dto.FileNode;
import com.bsi.peaks.model.dto.FilePath;
import com.bsi.peaks.model.dto.RemotePath;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.es.WorkManager;
import com.bsi.peaks.uploader.UploaderConfig;
import com.bsi.peaks.uploader.services.Uploader;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.vertx.core.impl.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author span
 * Created by span on 2018-12-10
 */
@Singleton
public class UploaderService {
    private static final Logger LOG = LoggerFactory.getLogger(UploaderService.class);
    private final ScheduledExecutorService scheduler;
    private final Duration uploadAskTimeout;
    private final ActorRef uploader;
    private final ProjectService projectService;
    @Deprecated
    private final AtomicInteger quota;
    private final FairerUploaderQueue queue;
    private final ConcurrentHashSet<String> copiedFiles;
    private final int maxQuota;
    private final Duration askTimeout;
    private final Duration askTimeoutFileSystem;

    @Inject
    public UploaderService(
        final SystemConfig systemConfig,
        final ActorSystem system,
        final UploaderConfig uploaderConfig,
        final ProjectService projectService
    ) {
        final Config config = system.settings().config();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.uploader = system.actorOf(
            BackoffSupervisor.props(
                Uploader.props(uploaderConfig),
                Uploader.NAME + "Instance",
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                0.2
            ), Uploader.NAME);
        this.projectService = projectService;
        this.uploadAskTimeout = Duration.ofNanos(systemConfig.getDataLoaderTimeout().toNanos());
        this.maxQuota = systemConfig.getUploaderQuota();
        this.quota = new AtomicInteger(systemConfig.getUploaderQuota());
        this.queue = new FairerUploaderQueue();
        this.copiedFiles = new ConcurrentHashSet<>();
        this.askTimeout = config.getDuration("peaks.server.ask-timeout");
        this.askTimeoutFileSystem = config.getDuration("peaks.server.ask-timeout-filesystem");
    }

    private Done ack(Object response) {
        if (response instanceof UploaderError) {
            throw new RuntimeException(((UploaderError) response).getMessage());
        } else {
            return Done.getInstance();
        }
    }

    public CompletionStage<PathsResponse> retrievePaths() {
        GetPaths command = GetPaths.newBuilder().build();
        return Patterns.ask(uploader, command, askTimeout).toCompletableFuture().thenApply(response -> (PathsResponse) response);
    }

    public CompletionStage<Done> addPath(RemotePath path) {
        return Patterns.ask(uploader, UploaderEventFactory.addPath(path), askTimeoutFileSystem)
            .toCompletableFuture()
            .thenApply(this::ack);
    }

    public CompletionStage<Done> deletePath(String pathName) {
        return Patterns.ask(uploader, UploaderEventFactory.deletePath(pathName), askTimeout)
            .toCompletableFuture()
            .thenApply(this::ack);
    }

    public CompletionStage<Done> updatePath(String oldPathName, RemotePath newPath) {
        return Patterns.ask(uploader, UploaderEventFactory.updatePath(oldPathName, newPath), askTimeoutFileSystem)
            .toCompletableFuture()
            .thenApply(this::ack);
    }

    public CompletionStage<FileNode> getAllFiles(String pathName, String subPath) {
        FilePath path = FilePath.newBuilder().setRemotePathName(pathName).setSubPath(subPath).build();
        GetFiles command = GetFiles.newBuilder().setDir(path).build();
        return Patterns.ask(uploader, command, askTimeoutFileSystem).toCompletableFuture().thenApply(response -> {
            if (response instanceof UploaderError) {
                UploaderError error = (UploaderError) response;
                throw new RuntimeException(error.getMessage());
            }
            return (FileNode) response;
        });
    }

    private CompletionStage<String> uploadFile(FileToUpload file) {
        final UUID projectId = UUID.fromString(file.getProjectId());
        final UUID sampleId = UUID.fromString(file.getSampleId());
        final UUID fractionId = UUID.fromString(file.getFractionId());
        final UUID uploadId = UUID.fromString(file.getUploadTaskId());
        final UUID adminUserId = WorkManager.ADMIN_USERID;
        final ProjectAggregateCommandFactory.UploadNotificationFactory uploadNotificationFactory = ProjectAggregateCommandFactory
            .project(projectId, adminUserId)
            .sample(sampleId)
            .uploadFraction(fractionId, uploadId);

        return CompletableFuture.supplyAsync(() -> {
            FilePath filePath = file.getFile();
            LOG.info("Start uploading " + file.getFractionId() + " from remote " + filePath.getRemotePathName() + " path " + filePath.getSubPath());

            // notify project upload has started
            projectService.uploadSampleNotification(projectId, adminUserId, uploadNotificationFactory.start()).toCompletableFuture().join();

            FileUploaded uploaded = Patterns.ask(uploader, file, uploadAskTimeout)
                .thenApply(response -> {
                    if (response instanceof UploaderError) {
                        UploaderError error = (UploaderError) response;
                        throw new RuntimeException("Unable to upload " + fractionId + ": " + error.getMessage());
                    } else if (response instanceof FileUploaded) {
                        return (FileUploaded) response;
                    } else {
                        throw new IllegalStateException("Received unexpected response from uploader: " + response + ", this should never happen");
                    }
                })
                .toCompletableFuture().join();
            LOG.info("Finished uploading " + uploaded.getFractionId() + " into " + uploaded.getLocalPath());
            copiedFiles.add(uploaded.getLocalPath());
            projectService.uploadSampleNotification(projectId, adminUserId, uploadNotificationFactory.complete(uploaded.getLocalPath()))
                .toCompletableFuture().join();
            return uploaded.getLocalPath();
        }).whenComplete((localPath, error) -> {
            if (error != null) {
                // if something was wrong in any of the steps, just tell project it's a failure
                // better fail it instead of letting it stuck in some random state
                LOG.error("Something went wrong during uploading remote file for fraction " + fractionId, error);
                projectService.uploadSampleNotification(projectId, adminUserId, uploadNotificationFactory.fail(error.getMessage()));
                releaseQuota(null);
            } else {
                // now the uploaded file was forwarded to project aggregate entity and pending data loading,
                // ideally the quota should be released after the data is loaded into cassandra.
                // However there is always a possibility that data loading task may get lost somewhere in the loop:
                // project aggregate entity, work manager, or data loader.
                // As a last resort, we try to release the quota after 2 * data loading timeout
                // Note: this should always work if there is no browser uploaded file as the quota = # loaders + 1
                // but even if there are browser uploaded files, worst case we just go over the initial quota
                // gradually and occupy more disk space, and that's not a big problem.
                scheduler.schedule(() -> {
                    LOG.debug("Checking the whether file {} was already released", localPath);
                    if (releaseQuota(localPath)) {
                        LOG.warn("Uploaded file {} was never released from uploader quota, now force releasing it", localPath);
                    }
                }, 2 * uploadAskTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }
        });
    }

    public CompletionStage<Done> upload(UploadFiles command) {
         CompletableFuture.supplyAsync(() -> {
             // do the heavy lifting in a separate thread, make it non-blocking
             Map<UUID, List<FileToUpload>> uploads = new HashMap<>();
             Map<UUID, Map<UUID, RemoteSample.Builder>> remoteSamples = new HashMap<>();
             for (FileToUpload file : command.getFilesList()) {
                 final UUID projectId = UUID.fromString(file.getProjectId());
                 final UUID sampleId = UUID.fromString(file.getSampleId());
                 final UUID fractionId = UUID.fromString(file.getFractionId());
                 final UUID uploadId = UUID.fromString(file.getUploadTaskId());
                 uploads.computeIfAbsent(projectId, ignore -> new ArrayList<>());
                 remoteSamples.computeIfAbsent(projectId, ignore -> new HashMap<>());

                 uploads.get(projectId).add(file);
                 remoteSamples.get(projectId).computeIfAbsent(sampleId, ignore -> RemoteSample.newBuilder())
                     .setSampleId(ModelConversion.uuidsToByteString(sampleId))
                     .addFractions(RemoteFraction.newBuilder()
                         .setFractionId(ModelConversion.uuidsToByteString(fractionId))
                         .setUploadId(ModelConversion.uuidsToByteString(uploadId))
                         .setRemotePathName(file.getFile().getRemotePathName())
                         .setSubPath(file.getFile().getSubPath())
                         .build()
                     );
             }

             for (UUID projectId : uploads.keySet()) {
                 List<FileToUpload> projectFiles = uploads.get(projectId);
                 List<RemoteSample> projectSamples = remoteSamples.get(projectId).values().stream()
                     .map(RemoteSample.Builder::build)
                     .collect(Collectors.toList());
                 projectService.queueRemoteUploads(projectId, WorkManager.ADMIN_USERID, projectSamples).whenComplete((done, error) -> {
                     if (error != null) {
                         LOG.error("Can't add remote uploads to queue as we were unable to queue samples to project " + projectId, error);
                     } else {
                         queue.addAllForProject(projectId, projectFiles);
                         checkQueue(0);
                     }
                 });
             }
             return Done.getInstance();
        });

        return CompletableFuture.completedFuture(Done.getInstance()); // just ack now, do not block vertx or front-end
    }

    public void clearQuota() {
        LOG.info("Forced quota clearing, quota now set back to " + maxQuota);
        copiedFiles.clear();
        quota.set(maxQuota);
        checkQueue(0);
    }

    public synchronized boolean releaseQuota(String fileName) {
        if (fileName == null || copiedFiles.contains(fileName)) {
            LOG.info("Releasing quota for {}", fileName == null ? "nothing" : fileName);
            if (fileName != null ) copiedFiles.remove(fileName);
            // can upload one since quota is released
            FileToUpload file = queue.poll();
            if (file != null) uploadFile(file);
            // check whether have extra quota
            checkQueue(file == null ? 1 : 0);
            return true;
        } else {
            return false;
        }
    }

    private synchronized void checkQueue(int extraQuota) {
        // hold quota first to handle concurrency
        int availableQuota = Math.min(quota.getAndSet(0) + extraQuota, maxQuota);
        List<String> files = new ArrayList<>();
        try {
            while (!queue.isEmpty() && availableQuota > 0) {
                FileToUpload file = queue.poll();
                uploadFile(file);
                files.add(file.getFractionId() + ":" + file.getFile().getSubPath());
                availableQuota--;
            }
        } finally {
            quota.set(availableQuota);
            LOG.info("Upload quota remaining " + availableQuota);
            LOG.info("{} quota used in batch for: {}", files.size(), String.join(",", files));
        }
    }
}
