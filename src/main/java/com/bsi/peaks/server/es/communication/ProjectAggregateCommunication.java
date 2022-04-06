package com.bsi.peaks.server.es.communication;

import akka.Done;
import akka.actor.ActorSystem;
import com.bsi.peaks.event.project.ProjectEventToRestore;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateCommand;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse.AnalysisQueryResponse;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse.ProjectQueryResponse;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse.SampleQueryResponse;
import com.bsi.peaks.server.es.ProjectAggregateEntity;
import com.bsi.peaks.server.handlers.HandlerException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;

@Singleton
public class ProjectAggregateCommunication extends AbstractCommunication<ProjectAggregateCommand, ProjectAggregateQueryRequest, ProjectAggregateQueryResponse> {

    @Inject
    public ProjectAggregateCommunication(
        final ActorSystem system
    ) {
        super(system, ProjectAggregateQueryResponse.class, ProjectAggregateEntity::instance);
    }

    @Override
    protected String tagFromCommand(ProjectAggregateCommand command) {
        return "ProjectCommand  " + command.getCommandCase().name();
    }

    public CompletionStage<Done> restoreProject(ProjectEventToRestore events) {
        // supply a longer timeout 3 mins to restore project
        return ask(events, Duration.ofSeconds(180))
            .thenApply(resposne -> {
                if (resposne instanceof Done) {
                    return Done.getInstance();
                } else if (resposne instanceof RuntimeException) {
                    throw (RuntimeException) resposne;
                } else if (resposne instanceof Throwable) {
                    throw new RuntimeException((Throwable) resposne);
                } else {
                    throw new IllegalStateException("Unexpected response: " + resposne.toString());
                }
            });
    }

    public CompletionStage<ProjectQueryResponse> queryProject(ProjectAggregateQueryRequest query) {
        if (!query.hasProjectQueryRequest()) {
            throw new IllegalArgumentException("Analysis query required");
        }
        return query(query)
            .thenApply(response -> {
                switch (response.getResponseCase()) {
                    case PROJECTQUERYRESPONSE:
                        return response.getProjectQueryResponse();
                    case FAILURE:
                        throw handlerException(errorInfo(query), response.getFailure());
                    default:
                        throw new HandlerException(500, errorInfo(query) + ": Unexpected query response " + response.getResponseCase());
                }
            });
    }

    public CompletionStage<Optional<ProjectQueryResponse>> queryProjectOptional(ProjectAggregateQueryRequest query) {
        if (!query.hasProjectQueryRequest()) {
            throw new IllegalArgumentException("Analysis query required");
        }
        return query(query)
            .thenApply(response -> {
                switch (response.getResponseCase()) {
                    case PROJECTQUERYRESPONSE:
                        return Optional.of(response.getProjectQueryResponse());
                    case FAILURE:
                        final ProjectAggregateQueryResponse.QueryFailure failure = response.getFailure();
                        switch (failure.getReason()) {
                            case NOT_FOUND:
                            case NOT_EXIST:
                            case ACCESS_DENIED:
                                return Optional.empty();
                            case ERROR:
                                throw new HandlerException(500, errorInfo(query) + ": " + failure.getDescription());
                            default:
                                throw new HandlerException(500, errorInfo(query) + ": Unexpected");
                        }
                    default:
                        throw new HandlerException(500, "Unexpected query response " + response.getResponseCase());
                }
            });
    }

    private String errorInfo(ProjectAggregateQueryRequest query) {
        final UUID projectId = uuidFrom(query.getProjectId());
        switch (query.getRequestCase()) {
            case PROJECTQUERYREQUEST:
                return "Project " + projectId + " " + query.getProjectQueryRequest().getQueryCase();
            case SAMPLEQUERYREQUEST:
                final ProjectAggregateQueryRequest.SampleQueryRequest sampleQueryRequest = query.getSampleQueryRequest();
                return "Project " + projectId + " Sample " + uuidFrom(sampleQueryRequest.getSampleId()) + " " + sampleQueryRequest.getQueryCase();
            case ANALYSISQUERYREQUEST:
                final ProjectAggregateQueryRequest.AnalysisQueryRequest analysisQueryRequest = query.getAnalysisQueryRequest();
                return "Project " + projectId + " Sample " + uuidFrom(analysisQueryRequest.getAnalysisId()) + " " + analysisQueryRequest.getQueryCase();
            default:
                return "Project " + projectId + " " + query.getProjectQueryRequest().getQueryCase();
        }
    }

    public CompletionStage<SampleQueryResponse> querySample(ProjectAggregateQueryRequest query) {
        if (!query.hasSampleQueryRequest()) {
            throw new IllegalArgumentException("Analysis query required");
        }
        return query(query)
            .thenApply(response -> {
                switch (response.getResponseCase()) {
                    case SAMPLEQUERYRESPONSE:
                        return response.getSampleQueryResponse();
                    case FAILURE:
                        throw handlerException(errorInfo(query), response.getFailure());
                    default:
                        throw new HandlerException(500, "Unexpected query response " + response.getResponseCase());
                }
            });
    }

    public CompletionStage<AnalysisQueryResponse> queryAnalysis(ProjectAggregateQueryRequest query) {
        if (!query.hasAnalysisQueryRequest()) {
            throw new IllegalArgumentException("Analysis query required");
        }
        return query(query)
            .thenApply(response -> {
                switch (response.getResponseCase()) {
                    case ANALYSISQUERYRESPONSE:
                        return response.getAnalysisQueryResponse();
                    case FAILURE:
                        throw handlerException(errorInfo(query), response.getFailure());
                    default:
                        throw new HandlerException(500, "Unexpected query response " + response.getResponseCase());
                }
            });
    }

    @NotNull
    private HandlerException handlerException(String query, ProjectAggregateQueryResponse.QueryFailure failure) {
        switch (failure.getReason()) {
            case NOT_FOUND:
                return new HandlerException(404, query + ": " + failure.getDescription());
            case ACCESS_DENIED:
                return new HandlerException(403, query + ": " + failure.getDescription());
            case ERROR:
                return new HandlerException(500, query + ": " + failure.getDescription());
            case NOT_EXIST:
                return new HandlerException(404, query + ": " + failure.getReason().name());
            default:
                return new HandlerException(500, query + ": Unexpected");
        }
    }

}
