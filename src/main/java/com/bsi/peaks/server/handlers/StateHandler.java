package com.bsi.peaks.server.handlers;

import com.bsi.peaks.event.PeptideDatabaseQueryFactory;
import com.bsi.peaks.event.modification.PeptideDatabaseQueryResponse;
import com.bsi.peaks.event.modification.PeptideDatabaseReady;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest.ProjectQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.work.WorkQueryRequest;
import com.bsi.peaks.event.work.WorkQueryResponse;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.es.PeptideDatabaseManager;
import com.bsi.peaks.server.es.ProjectAggregateEntity;
import com.bsi.peaks.server.es.WorkManager;
import com.bsi.peaks.server.es.communication.PeptideDatabaseCommunication;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.bsi.peaks.server.es.communication.WorkManagerCommunication;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;
import static java.util.Collections.singletonList;

public class StateHandler extends ApiHandler {

    private static final Logger LOG = LoggerFactory.getLogger(StateHandler.class);

    private final ProjectAggregateCommunication projectCommunication;
    private final WorkManagerCommunication workManagerCommunication;
    private final PeptideDatabaseCommunication peptideDatabaseCommunication;

    @Inject
    public StateHandler(
        final Vertx vertx,
        final ProjectAggregateCommunication projectCommunication,
        WorkManagerCommunication workManagerCommunication, final PeptideDatabaseCommunication peptideDatabaseCommunication
    ) {
        super(vertx);
        this.projectCommunication = projectCommunication;
        this.workManagerCommunication = workManagerCommunication;
        this.peptideDatabaseCommunication = peptideDatabaseCommunication;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);

        subRouter.get("/project/:pid").handler(this::projectHandler);
        subRouter.get("/workmanager").handler(this::workManagerHandler);
        subRouter.get("/peptidedatabase").handler(this::peptidedatabase);

        return subRouter;
    }

    private void peptidedatabase(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final HttpServerResponse response = context.response();
            response.exceptionHandler(e -> {
                LOG.error("Response exception", e);
                response.close();
            });

            final CompletionStage<PeptideDatabaseReady> query = peptideDatabaseCommunication
                .query(PeptideDatabaseQueryFactory.databaseReady())
                .thenApply(PeptideDatabaseQueryResponse::getDatabaseReady);
            handle(context, response, query, PeptideDatabaseManager.PERSISTENCE_ID);
        });
    }


    private void projectHandler(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final HttpServerRequest request = context.request();
            final HttpServerResponse response = context.response();
            response.exceptionHandler(e -> {
                LOG.error("Response exception", e);
                response.close();
            });

            final UUID projectId;
            try {
                projectId = UUID.fromString(request.getParam("pid"));
            } catch (Exception e) {
                LOG.warn("Unable to parse ids", e);
                badRequest(context, "Unable to parse ids");
                return;
            }

            final String persistenceId = ProjectAggregateEntity.persistenceId(projectId);
            CompletionStage<String> projectState = projectCommunication
                .queryProject(ProjectAggregateQueryRequest.newBuilder()
                    .setProjectId(uuidToByteString(projectId))
                    .setUserId(uuidToByteString(WorkManager.ADMIN_USERID))
                    .setProjectQueryRequest(ProjectQueryRequest.newBuilder()
                        .setState(ProjectQueryRequest.State.getDefaultInstance())
                    )
                    .build()
                )
                .thenApply(ProjectAggregateQueryResponse.ProjectQueryResponse::getStateJson);
            handleJsonString(context, response, projectState, persistenceId);
        });
    }

    private void workManagerHandler(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final HttpServerResponse response = context.response();
            response.exceptionHandler(e -> {
                LOG.error("Response exception", e);
                response.close();
            });

            final CompletionStage<String> workManagerState = workManagerCommunication.query(WorkQueryRequest.newBuilder()
                .setUserId(ModelConversion.uuidToByteString(User.admin().userId()))
                .setState(WorkQueryRequest.QueryState.getDefaultInstance())
                .build()
            ).thenApply(WorkQueryResponse::getStateJson);
            handleJsonString(context, response, workManagerState, WorkManager.PERSISTENCE_ID);
        });
    }

    private void handle(RoutingContext context, HttpServerResponse response, CompletionStage<? extends Object> futureState, String persistenceId) {
        final CompletionStage<String> futureJson = futureState.thenApply(state -> {
            try {
                return OM.writeValueAsString(state);
            } catch (JsonProcessingException e) {
                throw Throwables.propagate(e);
            }
        });
        handleJsonString(context, response, futureJson, persistenceId);
    }

    private void handleJsonString(RoutingContext context, HttpServerResponse response, CompletionStage<String> futureState, String persistenceId) {
        futureState
            .thenAccept(jsonString -> {
                response.putHeader("Content-Disposition", "attachment; filename=\"" + persistenceId + ".json\"");
                response.putHeader("Content-Type", "application/json");
                response.end(jsonString);
            })
            .whenComplete((done, error) -> {
                if (error != null) {
                    if (error instanceof NoSuchElementException) {
                        notFound(context);
                    } else {
                        context.fail(error);
                    }
                } else {
                    response.end();
                }
            });
    }

}
