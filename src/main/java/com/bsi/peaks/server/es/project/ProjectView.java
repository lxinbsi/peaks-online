package com.bsi.peaks.server.es.project;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent.ProjectEvent;
import com.bsi.peaks.event.projectAggregate.ProjectPermission;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.server.es.ProjectAggregateEntity;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.View;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;

@Singleton
public class ProjectView  {

    private PMap<UUID, Project> projects = HashTreePMap.empty();

    private static final Logger LOG = LoggerFactory.getLogger(ProjectView.class);

    @Inject
    public ProjectView(ActorSystem system) {
        View
            .events(
                system,
                ProjectAggregateEntity.eventTag(ProjectEvent.EventCase.PROJECTCREATED),
                ProjectAggregateEntity.eventTag(ProjectEvent.EventCase.PROJECTDELETED),
                ProjectAggregateEntity.eventTag(ProjectEvent.EventCase.PROJECTPERMISSION)
            )
            .runForeach(event -> {
                Object eventObject = event.event();
                if (eventObject instanceof ProjectAggregateEvent) {
                    processEvent((ProjectAggregateEvent) eventObject, event.sequenceNr());
                }
            }, ActorMaterializer.create(system));
    }

    private void processEvent(ProjectAggregateEvent event, long seqNr) {
        try {
            final UUID projectId = uuidFrom(event.getProjectId());
            final Project project = createOrGetProject(projectId);
            switch (event.getEventCase()) {
                case PROJECT:
                    final ProjectEvent projectEvent = event.getProject();
                    switch (projectEvent.getEventCase()) {
                        case PROJECTCREATED:
                            project.createdSequenceNumber = seqNr;
                            break;
                        case PROJECTPERMISSION:
                            project.projectPermission = projectEvent.getProjectPermission();
                            break;
                        case PROJECTDELETED:
                            project.deletedSequenceNumber = seqNr;
                            break;
                    }
            }
        } catch (Exception e) {
            LOG.error("Unable to handle event in project view", e);
        }
    }

    public Set<UUID> currentProjectIds() {
        return projects.entrySet().stream()
            .filter(entry -> !entry.getValue().isDeleted())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    public Set<UUID> currentProjectIds(UUID userId) {
        return projects.entrySet().stream()
            .filter(entry -> !entry.getValue().isDeleted())
            .filter(entry -> readPermission(userId, entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    public List<Pair<UUID, UUID>> currentProjectOwnerIds(UUID userId) {
        return projects.entrySet().stream()
            .filter(entry -> readPermission(userId, entry.getValue()))
            .map(entry -> Pair.create(entry.getKey(), entry.getValue().getOwnerId()))
            .collect(Collectors.toList());
    }

    public boolean readPermission(UUID userId, UUID projectId) {
        return getProject(projectId).map(project -> readPermission(userId, project)).orElse(false);
    }

    public boolean writePermission(UUID userId, UUID projectId) {
        return getProject(projectId)
            .map(project -> UserManager.ADMIN_USERID.equals(userId.toString()) ||
                project.getOwnerId().equals(userId)
            ).orElse(false);
    }

    private Optional<Project> getProject(UUID projectId) {
        return Optional.ofNullable(projects.get(projectId)).filter(project -> project.createdSequenceNumber > project.deletedSequenceNumber);
    }

    private Project createOrGetProject(UUID projectId) {
        Project project = projects.get(projectId);
        if (project == null) {
            project = new Project();
            projects = projects.plus(projectId, project);
        }
        return project;
    }

    private boolean readPermission(UUID userId, Project project) {
        return UserManager.ADMIN_USERID.equals(userId.toString()) ||
            project.projectPermission.getShared() ||
            project.getOwnerId().equals(userId);
    }

    private static class Project {
        ProjectPermission projectPermission;
        long createdSequenceNumber = -1;
        long deletedSequenceNumber = -1;
        public UUID getOwnerId() {
            return Optional.ofNullable(projectPermission.getOwnerId())
                .map(ModelConversion::uuidFrom)
                .orElse(null);
        }
        public boolean isDeleted() {
            return createdSequenceNumber < deletedSequenceNumber;
        }
    }

}
