package com.bsi.peaks.server.service;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.BackoffSupervisor;
import akka.pattern.PatternsCS;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.ArchiverCommandFactory;
import com.bsi.peaks.event.common.AckStatus;
import com.bsi.peaks.event.project.ArchiveProject;
import com.bsi.peaks.event.project.ArchiverCommand;
import com.bsi.peaks.event.project.ArchiverResponse;
import com.bsi.peaks.event.project.ImportArchive;
import com.bsi.peaks.event.uploader.PathsResponse;
import com.bsi.peaks.model.dto.ArchiveTask;
import com.bsi.peaks.model.dto.FileNode;
import com.bsi.peaks.model.dto.ImportingTask;
import com.bsi.peaks.model.dto.RemotePath;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.archiver.Archiver;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@Singleton
public class ArchivingService {
    private final ActorRef archiver;
    private final Duration askTimeout;
    private final Duration askTimeoutFileSystem;

    @Inject
    public ArchivingService(
        final ActorSystem system,
        final ProjectService projectService,
        final FastaDatabaseService databaseService,
        final ApplicationStorageFactory storageFactory,
        final ProjectAggregateCommunication projectCommunication,
        final CassandraMonitorService cassandraMonitorService
    ) {
        final Config config = system.settings().config();
        this.archiver = system.actorOf(
            BackoffSupervisor.props(
                Archiver.props(storageFactory, projectService, databaseService, projectCommunication, cassandraMonitorService),
                Archiver.NAME + "Instance",
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                0.2
            ), Archiver.NAME
        );
        this.askTimeout = config.getDuration("peaks.server.ask-timeout");
        this.askTimeoutFileSystem = config.getDuration("peaks.server.ask-timeout-filesystem");
    }

    public CompletionStage<List<ArchiveTask>> getArchiveTasks(User user) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).getArchiveTasks();
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> (List<ArchiveTask>) res);
    }

    public CompletionStage<List<ImportingTask>> getImportingTasks(User user) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).getImportingTasks();
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> (List<ImportingTask>) res);
    }

    public CompletionStage<Done> startArchive(User user, ArchiveProject archiveProject) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).startArchive(archiveProject);
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> startImport(User user, ImportArchive importArchive) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).startImport(importArchive);
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<PathsResponse> retrievePaths(User user) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).retrievePaths();
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, msg -> ((ArchiverResponse)msg).getPaths()));
    }

    public CompletionStage<FileNode> retrieveFiles(User user, String pathName, String subPath) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).retrieveFiles(pathName, subPath);
        return PatternsCS.ask(archiver, command, askTimeoutFileSystem).thenApply(res -> tryExtractContent(res, ignore -> (FileNode)res));
    }

    public CompletionStage<Done> addPath(User user, RemotePath path) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).addPath(path);
        return PatternsCS.ask(archiver, command, askTimeoutFileSystem).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> updatePath(User user, String oldPathName, RemotePath newPath) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).updatePath(oldPathName, newPath);
        return PatternsCS.ask(archiver, command, askTimeoutFileSystem).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> createFolder(User user, String pathName, String subPath) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).createFolder(pathName, subPath);
        return PatternsCS.ask(archiver, command, askTimeoutFileSystem).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> deletePath(User user, String pathName) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).deletePath(pathName);
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> cancelArchive(User user, UUID projectId) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).cancelArchive(projectId);
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> batchCancelImport(User user, List<UUID> projectIds) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).batchCancelImports(projectIds);
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> cancelImport(User user, UUID projectId) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).cancelImport(projectId);
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> restartArchive(User user, UUID projectId) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).restartArchive(projectId);
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    public CompletionStage<Done> restartImport(User user, UUID projectId) {
        ArchiverCommand command = ArchiverCommandFactory.factory(user).restartImport(projectId);
        return PatternsCS.ask(archiver, command, askTimeout).thenApply(res -> tryExtractContent(res, ignore -> Done.getInstance()));
    }

    private <T> T tryExtractContent(Object msg, Function<Object, T> func) {
        if (msg instanceof ArchiverResponse) {
            ArchiverResponse res = (ArchiverResponse) msg;
            if (res.getError().isEmpty()) {
                if (res.getAck() == AckStatus.REJECT) {
                    throw new IllegalStateException("Command rejected by archiver");
                }
                return func.apply(res);
            } else {
                throw new IllegalStateException("Received error from archiver: " + res.getError());
            }
        } else {
            return func.apply(msg);
        }
    }
}
