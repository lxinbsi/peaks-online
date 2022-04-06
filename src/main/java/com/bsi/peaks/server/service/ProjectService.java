package com.bsi.peaks.server.service;

import akka.Done;
import akka.NotUsed;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.analysis.dto.Analysis;
import com.bsi.peaks.analysis.analysis.dto.AnalysisParameters;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.graph.GraphTaskIds;
import com.bsi.peaks.event.projectAggregate.*;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest.AnalysisQueryRequest;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.dto.ProjectAnalysisInfos;
import com.bsi.peaks.model.dto.Sample;
import com.bsi.peaks.model.dto.SampleIdList;
import com.bsi.peaks.model.dto.SampleSubmission;
import com.bsi.peaks.model.filter.FilterFunction;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.system.User;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface ProjectService {

    // COMMANDS
    CompletionStage<Done> restartFailedUploads(UUID projectId, User user);
    CompletionStage<UUID> createProject(Project project, User creator);

    default CompletionStage<Done> deleteProject(UUID projectId, User user) {
        return deleteProject(projectId, user, false);
    }
    CompletionStage<Done> deleteProject(UUID projectId, User user, boolean forceDeleteWithoutKeyspace);
    CompletionStage<Done> deleteProjectsInKeyspace(User user, String keyspace);
    CompletionStage<Done> updateProject(UUID projectId, User user, ProjectInformation projectInformation);
    CompletionStage<Done> updateProject(UUID projectId, User user, ProjectPermission projectPermission);
    CompletionStage<SampleIdList> createSamples(UUID projectId, SampleSubmission sample, User creator);
    CompletionStage<Done> createDaemonFractionsInfo(UUID projectId, List<UUID> fractionIds, int timeOut, User creator);
    CompletionStage<Done> updateSample(UUID projectId, UUID sampleId, User user, SampleInformation sampleInformation);
    CompletionStage<Done> updateSampleEnzyme(UUID projectId, UUID sampleId, User user, String enyzme);
    CompletionStage<Done> updateSampleActivationMethod(UUID projectId, UUID sampleId, User user, String activationMethod);
    CompletionStage<Done> updatedFractionName(UUID projectId, UUID sampleId, UUID fractionId, User user, String newName);
    CompletionStage<Done> deleteSample(UUID projectId, UUID sampleId, User user);
    CompletionStage<Done> deleteSamplesByStatus(UUID projectId, ProjectAggregateState.SampleState.SampleStatus status, User user);
    CompletionStage<Done> uploadSampleNotification(UUID projectId, UUID user, ProjectAggregateCommand uploadNotification);
    CompletionStage<Done> queueRemoteUploads(UUID projectId, UUID userId, List<RemoteSample> remoteSamples);
    CompletionStage<UUID> createAnalysis(UUID projectId, Analysis analysis, User creator);
    CompletionStage<Done> updateAnalysis(UUID projectId, UUID analysisId, User user, AnalysisInformation analysisInformation);
    CompletionStage<Done> deleteAnalysis(UUID projectId, UUID analysisId, User user);
    CompletionStage<Done> cancelAnalysis(UUID projectId, UUID analysisId, User user);
    CompletionStage<Done> setFilter(UUID projectId, UUID analysisId, WorkflowStepFilter filter, User user);

    // QUERIES

    Source<Project, NotUsed> projects(User user);
    default Source<Project, NotUsed> projectsInKeyspace(User user, String keyspace) {
        return projects(user).filter(project -> project.getKeyspace().equals(keyspace));
    }

    Source<ProjectAnalysisInfos, NotUsed> projectAnalysisInfos(User user);
    CompletionStage<Sample> sample(UUID projectId, UUID sampleId, User user);
    CompletionStage<Analysis> analysis(UUID projectId, UUID analysisId, User user);
    CompletionStage<AnalysisParameters> analysisParameters(UUID projectId, UUID analysisId, User user);

    Source<Progress, NotUsed> currentProjectProgress(UUID projectId, User user);
    Source<Progress, NotUsed> currentAllAnalysisProgress(User user);
    Source<Progress, NotUsed> analysisProgress(UUID projectId, UUID analysisId, User user);
    Source<Progress, NotUsed> sampleProgress(UUID projectId, UUID sampleId, User user);
    Source<Progress, NotUsed> daemonSamplesCopyProgress(UUID projectId, User user);

    CompletionStage<Boolean> projectExist(UUID projectId);
    CompletionStage<Project> project(UUID projectId, User user);
    Source<Analysis, NotUsed> projectAnalysis(UUID projectId, User user);

    Source<Sample, NotUsed> projectSamples(UUID projectId, User user);

    CompletionStage<GraphTaskIds> sampleGraphTaskIds(UUID projectId, UUID sampleId, User user);

    //TODO maybe replace all of these?
    CompletionStage<GraphTaskIds> analysisGraphTaskIds(UUID projectId, UUID analysis, WorkFlowType type, User user);
    CompletionStage<GraphTaskIds> analysisFractionGraphTaskIds(UUID projectId, UUID analysis, UUID sampleId, UUID fractionId, WorkFlowType type, User user);
    CompletionStage<GraphTaskIds> analysisGraphTaskIdsLastWorkflowType(UUID projectId, UUID analysisId, User user);
    CompletionStage<GraphTaskIds> analysisGraphTaskIds(UUID projectId, UUID analysisId, User user, AnalysisQueryRequest.GraphTaskIds request);

    CompletionStage<FilterFunction[]> analysisFiltersByWorkflowType(UUID projectId, UUID analysisId, User user, WorkFlowType workFlowType);
    CompletionStage<Query> analysisQuery(UUID projectId, UUID analysisId, User user, Optional<WorkFlowType> workFlowType);
    CompletionStage<Map<WorkFlowType, Query>> analysisQueriesAll(UUID projectId, UUID analysisId, User user);

    default CompletionStage<List<UUID>> samplesInAnalysis(UUID projectId, UUID analysisId, User user) {
        return analysis(projectId, analysisId, user)
            .thenApply(analysis -> Lists.transform(analysis.getSampleIdsList(), UUID::fromString));
    }
}
