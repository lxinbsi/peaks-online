package com.bsi.peaks.server.archiver;


import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.project.*;
import com.bsi.peaks.model.dto.*;
import com.bsi.peaks.utilities.PathValidator;
import com.bsi.peaks.utilities.UniversalFileAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("NotUsed")
class ArchiverState implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ArchiverState.class);
    private static final long serialVersionUID = 1L;
    private LinkedHashMap<String, RemotePath> paths;
    private LinkedHashMap<UUID, Pair<ArchiveTask, ArchiveTaskState>> archiveTasks;
    private LinkedHashMap<UUID, Pair<ImportingTask, ArchiveTaskState>> importingTasks;
    private LinkedList<ArchiverEvent> queuedArchiveEvents;
    private LinkedList<ArchiverEvent> queuedImportEvents;

    ArchiverState() {
        this(new LinkedHashMap<>(), new LinkedHashMap<>(), new LinkedHashMap<>(), new LinkedList<>(), new LinkedList<>());
    }

    private ArchiverState(LinkedHashMap<String, RemotePath> paths,
                          LinkedHashMap<UUID, Pair<ArchiveTask, ArchiveTaskState>> archiveTasks,
                          LinkedHashMap<UUID, Pair<ImportingTask, ArchiveTaskState>> importingTasks,
                          LinkedList<ArchiverEvent> queuedArchiveEvents,
                          LinkedList<ArchiverEvent> queuedImportEvents) {
        this.paths = paths;
        this.archiveTasks = archiveTasks;
        this.importingTasks = importingTasks;
        this.queuedArchiveEvents = queuedArchiveEvents;
        this.queuedImportEvents = queuedImportEvents;
    }

    void recoverFrom(ArchiverState state) {
        // want to work on this same instance
        paths = state.paths;
        archiveTasks = state.archiveTasks;
        importingTasks = state.importingTasks;
        queuedArchiveEvents = state.queuedArchiveEvents;
        queuedImportEvents = state.queuedImportEvents;
    }

    Pair<ArchiveTask, ArchiveTaskState> retrieveArchiveTask(UUID projectId) {
        return archiveTasks.getOrDefault(projectId, null);
    }

    Pair<ImportingTask, ArchiveTaskState> retrieveImportTask(UUID projectId) {
        return importingTasks.getOrDefault(projectId, null);
    }

    Source<ArchiveTask, NotUsed> archiveTaskSource() {
        return Source.from(archiveTasks.values()).map(Pair::first);
    }

    Source<ImportingTask, NotUsed> importingTaskSource() {
        return Source.from(importingTasks.values()).map(Pair::first);
    }

    List<RemotePath> getPaths() {
        return new ArrayList<>(paths.values());
    }

    RemotePath getPathByName(String name) {
        return paths.getOrDefault(name, null);
    }

    boolean hasQueuedArchiveEvents() {
        return !queuedArchiveEvents.isEmpty();
    }

    boolean hasQueuedImportEvents() {
        return !queuedImportEvents.isEmpty();
    }

    private void checkTask(ArchiverCommand command, UUID projectId, boolean isArchive) {
        if (isArchive) {
            if (!archiveTasks.containsKey(projectId)) {
                throw new IllegalStateException("Project " + projectId + " has no associated archive task");
            } else {
                ArchiveTask task = archiveTasks.get(projectId).first();
                if (!command.getIsAdmin() && !task.getCreatorId().equals(command.getUserId())) {
                    throw new IllegalStateException("Project " + projectId + " archive task doesn't belong to user");
                }
            }
        } else {
            if (!importingTasks.containsKey(projectId)) {
                throw new IllegalStateException("Project " + projectId + " has no associated import task");
            } else {
                ImportingTask task = importingTasks.get(projectId).first();
                if (!command.getIsAdmin() && !task.getCreatorId().equals(command.getUserId())) {
                    throw new IllegalStateException("Project " + projectId + " import task doesn't belong to user");
                }
            }
        }
    }

    ArchiverEvent validateRestartArchive(ArchiverCommand command) {
        RestartArchive restart = command.getRestartArchive();
        UUID projectId = UUID.fromString(restart.getProjectId());
        checkTask(command, projectId, true);
        ArchiveTask task = archiveTasks.get(projectId).first();
        if (task.getState() != ArchivingState.ARCHIVING_FAILED) {
            throw new IllegalStateException("Only failed archive task can be restarted, current task is " + task.getState());
        }
        RemotePath remotePath = getPathByName(task.getAssociatedRequest().getArchivePath().getRemotePathName());
        if (remotePath == null) {
            throw new IllegalArgumentException("No matching remote repository is found for "
                + task.getAssociatedRequest().getArchivePath().getRemotePathName());
        } else {
            return createEvent(command)
                .setArchiveRestarted(ArchiveRestarted.newBuilder()
                    .setProjectId(projectId.toString())
                    .setRemotePath(remotePath)
                    .build()
                )
                .build();
        }
    }

    ArchiverEvent validateRestartImport(ArchiverCommand command) {
        RestartImport restart = command.getRestartImport();
        UUID projectId = UUID.fromString(restart.getProjectId());
        checkTask(command, projectId, false);
        ImportingTask task = importingTasks.get(projectId).first();
        if (task.getState() != ArchivingState.IMPORTING_FAILED) {
            throw new IllegalStateException("Only failed import task can be restarted, current task is " + task.getState());
        }
        RemotePath remotePath = getPathByName(task.getAssociatedRequest().getArchivePath().getRemotePathName());
        if (remotePath == null) {
            throw new IllegalArgumentException("No matching remote repository is found for "
                + task.getAssociatedRequest().getArchivePath().getRemotePathName());
        } else {
            return createEvent(command)
                .setImportRestarted(ImportRestarted.newBuilder()
                    .setProjectId(projectId.toString())
                    .setRemotePath(remotePath)
                    .build()
                )
                .build();
        }
    }

    ArchiverEvent validateCancelArchive(ArchiverCommand command) {
        CancelArchive cancellation = command.getCancelArchive();
        UUID projectId = UUID.fromString(cancellation.getProjectId());
        checkTask(command, projectId, true);
        ArchiveTask task = archiveTasks.get(projectId).first();
        return createEvent(command)
            .setArchiveCancelled(ArchiveCancelled.newBuilder()
                .setProjectId(projectId.toString())
                .setPath(task.getAssociatedRequest().getArchivePath())
                .build()
            ).build();
    }

    ArchiverEvent validateCancelImport(ArchiverCommand command) {
        CancelImport cancellation = command.getCancelImport();
        UUID projectId = UUID.fromString(cancellation.getProjectId());
        checkTask(command, projectId, false);
        return createEvent(command)
            .setImportCancelled(ImportCancelled.newBuilder().setProjectId(projectId.toString()).build()).build();
    }

    ArchiverEvent validateBatchCancelImports(ArchiverCommand command) {
        BatchCancelImports cancelImports = command.getBatchCancelImports();
        for (String projectId : cancelImports.getProjectIdsList()) {
            checkTask(command, UUID.fromString(projectId), false);
        }
        return createEvent(command).setImportsBatchCancelled(
            ImportsBatchCancelled.newBuilder().addAllProjectIds(cancelImports.getProjectIdsList()).build()
        ).build();
    }

    ArchiverEvent validateUpdateArchiveProgress(ArchiverCommand command) {
        UUID projectId = UUID.fromString(command.getUpdateArchiveProgress().getProjectId());
        if (!archiveTasks.containsKey(projectId)) {
            throw new IllegalArgumentException("Project id " + projectId + " doesn't have corresponding archive task");
        }
        return createEvent(command).setArchiveProgressUpdated(command.getUpdateArchiveProgress()).build();
    }

    ArchiverEvent validateUpdateImportProgress(ArchiverCommand command) {
        UUID projectId = UUID.fromString(command.getUpdateImportProgress().getProjectId());
        if (!importingTasks.containsKey(projectId)) {
            throw new IllegalArgumentException("Project id " + projectId + " doesn't have corresponding importing task");
        }
        return createEvent(command).setImportProgressUpdated(command.getUpdateImportProgress()).build();
    }

    ArchiverEvent validateStartImport(ArchiverCommand command) {
        ImportArchive importArchive = command.getImportArchive();
        RemotePath remotePath = getPathByName(importArchive.getTargetFolder().getRemotePathName());
        if (remotePath == null) {
            throw new IllegalArgumentException("No matching remote repository is found for "
                + importArchive.getTargetFolder().getRemotePathName());
        } else {
            UUID projectId = UUID.fromString(importArchive.getProjectId());
            if (importingTasks.containsKey(projectId) && importingTasks.get(projectId).first().getState() != ArchivingState.IMPORTING_DONE) {
                throw new IllegalArgumentException("Project " + projectId + " is currently being imported, " +
                    "need to remove the old import task to re-import it");
            }
            return createEvent(command).setImportStarted(
                ImportStarted.newBuilder().setArchiveToImport(importArchive).setRemotePath(remotePath).build()
            ).build();
        }
    }

    ArchiverEvent validateStartArchive(ArchiverCommand command) {
        ArchiveProject archiveProject = command.getArchiveProject();
        RemotePath remotePath = getPathByName(archiveProject.getTargetFolder().getRemotePathName());
        if (remotePath == null) {
            throw new IllegalArgumentException("No matching remote repository is found for "
                + archiveProject.getTargetFolder().getRemotePathName());
        } else {
            UUID projectId = UUID.fromString(archiveProject.getProjectId());
            if (archiveTasks.containsKey(projectId) && archiveTasks.get(projectId).first().getState() != ArchivingState.ARCHIVING_DONE) {
                throw new IllegalArgumentException("Project " + projectId + " is currently being archived, " +
                    "need to remove the old archive task to re-archive it");
            }
            String destination = archiveProject.getTargetFolder().getSubPath();
            UniversalFileAccessor.mkdir(remotePath, destination);
            return createEvent(command).setArchiveStarted(
                ArchiveStarted.newBuilder().setRemotePath(remotePath).setProjectToArchive(archiveProject).build()
            ).build();
        }
    }

    ArchiverEvent validateAddPath(ArchiverCommand command) {
        PathValidator.validateAddPath(paths, command.getAddPath(), null);
        return createEvent(command)
        .setPathAdded(
            PathAdded.newBuilder()
                .setPath(command.getAddPath().getRemotePath())
                .build()
        ).build();
    }

    ArchiverEvent validateUpdatePath(ArchiverCommand command) {
        PathValidator.validateUpdatePath(paths, command.getUpdatePath());
        return createEvent(command).setPathUpdated(
            PathUpdated.newBuilder()
                .setOldPathName(command.getUpdatePath().getOldPathName())
                .setNewPath(command.getUpdatePath().getNewPath())
                .build()
        ).build();
    }

    ArchiverEvent validateDeletePath(ArchiverCommand command) {
        PathValidator.validateDeletePath(paths, command.getDeletePath());
        RemotePath path = paths.get(command.getDeletePath().getRemotePathName());
        return createEvent(command).setPathDeleted(
            PathDeleted.newBuilder()
                .setPath(path)
                .build()
        ).build();
    }

    ArchiverEvent update(ArchiverEvent event) {
        try {
            if (event.hasPathAdded()) {
                addPath(event.getPathAdded().getPath());
            } else if (event.hasPathDeleted()) {
                deletePath(event.getPathDeleted().getPath());
            } else if (event.hasPathUpdated()) {
                updatePath(event.getPathUpdated().getOldPathName(), event.getPathUpdated().getNewPath());
            } else if (event.hasArchiveStarted()) {
                addArchiveTask(event.getArchiveStarted());
                queuedArchiveEvents.add(event);
            } else if (event.hasImportStarted()) {
                addImportTask(event.getImportStarted());
                queuedImportEvents.add(event);
            } else if (event.hasArchiveProgressUpdated()) {
                updateArchiveProgress(event.getArchiveProgressUpdated());
            } else if (event.hasImportProgressUpdated()) {
                updateImportProgress(event.getImportProgressUpdated());
            } else if (event.hasArchiveCancelled()) {
                cancelArchive(event.getArchiveCancelled());
            } else if (event.hasImportCancelled()) {
                cancelImport(event.getImportCancelled());
            } else if (event.hasArchiveRestarted()) {
                restartArchive(event.getArchiveRestarted());
                queuedArchiveEvents.add(event);
            } else if (event.hasImportRestarted()) {
                restartImport(event.getImportRestarted());
                queuedImportEvents.add(event);
            } else if (event.hasArchiveEventDequeued()) {
                return popQueuedArchiveEvents();
            } else if (event.hasImportEventDequeued()) {
                return popQueuedImportEvents();
            } else if (event.hasImportsBatchCancelled()) {
                batchCancelImports(event.getImportsBatchCancelled());
            }
        } catch (Exception e) {
            LOG.error("Unable to update archiver state, event {}", event, e);
        }

        return null;
    }

    private ArchiverEvent popQueuedArchiveEvents() {
        if (!queuedArchiveEvents.isEmpty()) {
            return queuedArchiveEvents.pop();
        } else {
            return null;
        }
    }

    private ArchiverEvent popQueuedImportEvents() {
        if (!queuedImportEvents.isEmpty()) {
            return queuedImportEvents.pop();
        } else {
            return null;
        }
    }

    private void restartArchive(ArchiveRestarted restarted) {
        UUID projectId = UUID.fromString(restarted.getProjectId());
        Pair<ArchiveTask, ArchiveTaskState> oldPair = archiveTasks.get(projectId);
        ArchiveTask updatedTask = oldPair.first().toBuilder()
            .setState(ArchivingState.ARCHIVING)
            .build();
        List<ArchiveTaskProgress> finished = oldPair.second().getTasksMap().values().stream()
            .filter(task -> task.getState() == Progress.State.ARCHIVING_DONE)
            .collect(Collectors.toList());
        ArchiveTaskState.Builder updatedStateBuilder = oldPair.second().toBuilder().clearTasks();
        finished.forEach(task -> updatedStateBuilder.putTasks(task.getTaskId(), task));
        archiveTasks.put(projectId, Pair.create(updatedTask, updatedStateBuilder.build()));
    }

    private void restartImport(ImportRestarted restarted) {
        UUID projectId = UUID.fromString(restarted.getProjectId());
        Pair<ImportingTask, ArchiveTaskState> oldPair = importingTasks.get(projectId);
        ImportingTask updatedTask = oldPair.first().toBuilder()
            .setState(ArchivingState.IMPORTING)
            .build();
        List<ArchiveTaskProgress> finished = oldPair.second().getTasksMap().values().stream()
            .filter(task -> task.getState() == Progress.State.IMPORTING_DONE)
            .collect(Collectors.toList());
        ArchiveTaskState.Builder updatedStateBuilder = oldPair.second().toBuilder().clearTasks();
        finished.forEach(task -> updatedStateBuilder.putTasks(task.getTaskId(), task));
        importingTasks.put(projectId, Pair.create(updatedTask, updatedStateBuilder.build()));
    }

    private void cancelArchive(ArchiveCancelled cancelled) {
        UUID projectId = UUID.fromString(cancelled.getProjectId());
        archiveTasks.remove(projectId);
        // also clean up the queue
        queuedArchiveEvents.removeIf(event -> {
            if (event.hasArchiveStarted()) {
                return event.getArchiveStarted().getProjectToArchive().getProjectId().equals(projectId.toString());
            } else if (event.hasArchiveRestarted()) {
                return event.getArchiveRestarted().getProjectId().equals(projectId.toString());
            }
            return false;
        });
    }

    private void batchCancelImports(ImportsBatchCancelled cancelled) {
        for (String pid : cancelled.getProjectIdsList()) {
            UUID projectId = UUID.fromString(pid);
            importingTasks.remove(projectId);
        }
        queuedImportEvents.removeIf(event -> {
            if (event.hasImportStarted()) {
                String projectId = event.getImportStarted().getArchiveToImport().getProjectId();
                return cancelled.getProjectIdsList().contains(projectId);
            } else if (event.hasImportRestarted()) {
                String projectId = event.getImportRestarted().getProjectId();
                return cancelled.getProjectIdsList().contains(projectId);
            }
            return false;
        });
    }

    private void cancelImport(ImportCancelled cancelled) {
        UUID projectId = UUID.fromString(cancelled.getProjectId());
        importingTasks.remove(projectId);
        // also clean up the queue
        queuedImportEvents.removeIf(event -> {
            if (event.hasImportStarted()) {
                return event.getImportStarted().getArchiveToImport().getProjectId().equals(projectId.toString());
            } else if (event.hasImportRestarted()) {
                return event.getImportRestarted().getProjectId().equals(projectId.toString());
            }
            return false;
        });
    }

    private ArchivingState convertState(Progress.State state) {
        switch (state) {
            case ARCHIVING:
                return ArchivingState.ARCHIVING;
            case ARCHIVING_DONE:
                return ArchivingState.ARCHIVING_DONE;
            case ARCHIVING_FAILED:
                return ArchivingState.ARCHIVING_FAILED;
            case IMPORTING:
                return ArchivingState.IMPORTING;
            case IMPORTING_DONE:
                return ArchivingState.IMPORTING_DONE;
            case IMPORTING_FAILED:
                return ArchivingState.IMPORTING_FAILED;
            default:
                throw new IllegalStateException("Unsupported state: " + state);
        }
    }

    private void updateArchiveProgress(ProgressUpdate update) {
        UUID projectId = UUID.fromString(update.getProjectId());
        Pair<ArchiveTask, ArchiveTaskState> oldPair = archiveTasks.get(projectId);
        ArchiveTask updatedTask = oldPair.first().toBuilder()
            .setTotalTasks(update.getTotalTasks())
            .setFinishedTasks(update.getFinishedTasks())
            .setState(convertState(update.getState()))
            .build();
        ArchiveTaskState.Builder updatedStateBuilder = oldPair.second().toBuilder();
        update.getChangedTasksList().forEach(task -> updatedStateBuilder.putTasks(task.getTaskId(), task));
        archiveTasks.put(projectId, Pair.create(updatedTask, updatedStateBuilder.build()));
    }

    private void updateImportProgress(ProgressUpdate update) {
        UUID projectId = UUID.fromString(update.getProjectId());
        Pair<ImportingTask, ArchiveTaskState> oldPair = importingTasks.get(projectId);
        ImportingTask updatedTask = oldPair.first().toBuilder()
            .setTotalTasks(update.getTotalTasks())
            .setFinishedTasks(update.getFinishedTasks())
            .setState(convertState(update.getState()))
            .build();
        ArchiveTaskState.Builder updatedStateBuilder = oldPair.second().toBuilder();
        update.getChangedTasksList().forEach(task -> updatedStateBuilder.putTasks(task.getTaskId(), task));
        importingTasks.put(projectId, Pair.create(updatedTask, updatedStateBuilder.build()));
    }

    private long getEpochTimestamp() {
        return CommonFactory.convert(CommonFactory.nowTimeStamp()).getEpochSecond();
    }

    private void addArchiveTask(ArchiveStarted started) {
        UUID projectId = UUID.fromString(started.getProjectToArchive().getProjectId());
        ArchiveTask initialTask = ArchiveTask.newBuilder()
            .setProjectId(projectId.toString())
            .setCreatorId(started.getProjectToArchive().getCreatorId())
            .setProjectName(started.getProjectToArchive().getOldProjectName())
            .setState(ArchivingState.ARCHIVING)
            .setAssociatedRequest(ArchiveRequest.newBuilder()
                .setArchivePath(started.getProjectToArchive().getTargetFolder())
                .setProjectId(projectId.toString())
                .setDiscardScanPeaks(started.getProjectToArchive().getDiscardScanPeaks())
                .build()
            )
            .setCreatedAt(getEpochTimestamp())
            .build();
        archiveTasks.put(projectId, Pair.create(initialTask, ArchiveTaskState.getDefaultInstance()));
    }

    private void addImportTask(ImportStarted started) {
        UUID projectId = UUID.fromString(started.getArchiveToImport().getProjectId());
        ImportingTask initialTask = ImportingTask.newBuilder()
            .setProjectId(projectId.toString())
            .setCreatorId(started.getArchiveToImport().getNewOwnerId())
            .setProjectName(started.getArchiveToImport().getNewProjectName())
            .setState(ArchivingState.IMPORTING)
            .setAssociatedRequest(ImportRequest.newBuilder()
                .setArchivePath(started.getArchiveToImport().getTargetFolder())
                .setNewProjectName(started.getArchiveToImport().getNewProjectName())
                .build()
            )
            .setCreatedAt(getEpochTimestamp())
            .build();
        importingTasks.put(projectId, Pair.create(initialTask, ArchiveTaskState.getDefaultInstance()));
    }

    private void addPath(RemotePath path) {
        paths.put(path.getName(), path);
    }

    private void deletePath(RemotePath path) {
        paths.remove(path.getName());
    }

    private void updatePath(String oldPathName, RemotePath newPath) {
        paths.remove(oldPathName);
        paths.put(newPath.getName(), newPath);
    }

    private ArchiverEvent.Builder createEvent(ArchiverCommand command) {
        return ArchiverEvent.newBuilder().setDeliveryId(command.getDeliveryId()).setTimeStamp(
            CommonFactory.nowTimeStamp()
        );
    }
}
