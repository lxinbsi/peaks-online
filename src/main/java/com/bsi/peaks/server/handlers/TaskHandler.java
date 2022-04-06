package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.WorkQueryFactory;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse.AnalysisQueryResponse;
import com.bsi.peaks.event.work.WorkQueryRequest;
import com.bsi.peaks.event.work.WorkQueryResponse;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.bsi.peaks.server.es.communication.WorkManagerCommunication;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskHandler extends ProtoHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskHandler.class);

    private final ProjectAggregateCommunication projectAggregateCommunication;
    private final WorkManagerCommunication workManagerCommunication;

    @Inject
    public TaskHandler(
        Vertx vertx,
        ActorSystem system,
        ProjectAggregateCommunication projectAggregateCommunication,
        WorkManagerCommunication workManagerCommunication) {
        super(vertx, system);
        this.projectAggregateCommunication = projectAggregateCommunication;
        this.workManagerCommunication = workManagerCommunication;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.get("/").handler(this::listTasks);
        subRouter.get("/pending").handler(this::listPendingTasks);
        subRouter.get("/:projectId/:analysisId").handler(this::getAnalysisTask);
        return subRouter;
    }

    private void getAnalysisTask(RoutingContext context) {
        auth(context, user -> {
            getId(context, "projectId", projectId -> {
                getId(context, "analysisId", analysisId -> {
                    final HttpServerRequest request = context.request();
                    final HttpServerResponse response = context.response();
                    final ProjectAggregateQueryRequest query = ProjectAggregateCommandFactory.project(projectId, user.userId()).analysis(analysisId).queryTaskListing();

                    projectAggregateCommunication.queryAnalysis(query)
                        .thenApply(AnalysisQueryResponse::getTaskListing)
                        .thenApply(ProjectAggregateQueryResponse.TaskListing::getTasksList)
                        .thenCompose(tasks -> serializationProcedure(request, response, Source.from(tasks), "analysisTaskList"))
                        .whenComplete((done, error) -> {
                            if (error != null) {
                                context.fail(error);
                            } else {
                                response.end();
                            }
                        });
                });
            });
        });
    }

    private void listTasks(RoutingContext context) {
        auth(context, user -> {
            final HttpServerRequest request = context.request();
            final HttpServerResponse response = context.response();
            final WorkQueryRequest workQueryRequest = WorkQueryFactory.actingAsUserId(user.userId()).listTasks();

            workManagerCommunication.query(workQueryRequest)
                .thenApply(WorkQueryResponse::getListTasks)
                .thenApply(WorkQueryResponse.QueryTasksResponse::getTasksList)
                .thenCompose(tasks -> serializationProcedure(request, response, Source.from(tasks), "workerTaskList"))
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        response.end();
                    }
                });
        });
    }

    private void listPendingTasks(RoutingContext context) {
        auth(context, user -> {
            final HttpServerRequest request = context.request();
            final HttpServerResponse response = context.response();
            final WorkQueryRequest workQueryRequest = WorkQueryFactory.actingAsUserId(user.userId()).listPendingTasks();

            workManagerCommunication.query(workQueryRequest)
                .thenApply(WorkQueryResponse::getListPendingTasks)
                .thenApply(WorkQueryResponse.QueryPendingTasksResponse::getTasksList)
                .thenCompose(tasks -> serializationProcedure(request, response, Source.from(tasks), "workerPendingTaskList"))
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        response.end();
                    }
                });
        });
    }
}
