package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.WorkflowCommandFactory;
import com.bsi.peaks.event.workflow.WorkflowResponse;
import com.bsi.peaks.model.dto.Workflow;
import com.bsi.peaks.server.auth.VertxUser;
import com.bsi.peaks.server.es.communication.WorkflowCommunicator;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.function.Consumer;

public class WorkflowHandler extends ProtoHandler {
    private static final Logger LOG = LoggerFactory.getLogger(WorkflowHandler.class);
    private final Materializer materializer;
    private final WorkflowCommunicator workflowCommunicator;

    @Inject
    public WorkflowHandler(
        final Vertx vertx,
        final ActorSystem system,
        WorkflowCommunicator workflowCommunicator
    ) {
        super(vertx, system);
        this.materializer = ActorMaterializer.create(system);
        this.workflowCommunicator = workflowCommunicator;
    }

    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.post("/").handler(BodyHandler.create()).handler(this::create);
        subRouter.route("/:id").handler(BodyHandler.create());
        subRouter.post("/:id/setOwner").handler(BodyHandler.create()).handler(this::setOwner);
        subRouter.put("/:id").consumes("*/json").handler(this::update);
        subRouter.delete("/:id").handler(this::delete);
        subRouter.get("/").handler(this::readAll);
        subRouter.get("/id").handler(this::readOne);
        return subRouter;
    }

    private void commandFactory(RoutingContext context, Consumer<WorkflowCommandFactory> userConsumer) {
        auth(context, user -> userConsumer.accept(WorkflowCommandFactory.actingAsUserId(user.userId().toString())));
    }

    protected void create(RoutingContext context) {
        commandFactory(context, commandFactory -> {
            final Workflow workflowWithoutId = getBody(context, Workflow.newBuilder());
            final UUID userId = ((VertxUser) context.user()).getUser().userId();
            final UUID workflowId = UUID.randomUUID();
            final Workflow workflow = workflowWithoutId.toBuilder()
                .setId(workflowId.toString())
                .setOwnerId(userId.toString())
                .build();

            workflowCommunicator
                .command(commandFactory.create(workflow))
                .thenCompose(done -> workflowCommunicator.query(commandFactory.queryById(workflowId)))
                .thenApply(WorkflowResponse::getEntries)
                .whenComplete((entries, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        respondWithProtobufSource(context, "Workflow", Source.from(entries.getEntriesList()));
                    }
                });
            }
        );
    }

    protected void setOwner(RoutingContext context) {
        auth(context, user -> {
            ok(context, Workflow.getDefaultInstance());
        });
    }

    protected void update(RoutingContext context) {
        commandFactory(context, commandFactory -> {
            final Workflow workflow = getBody(context, Workflow.newBuilder());
            final UUID workflowId = UUID.fromString(workflow.getId());
            workflowCommunicator
                .command(commandFactory.update(workflowId, workflow))
                .thenCompose(done -> workflowCommunicator.query(commandFactory.queryById(workflowId)))
                .thenApply(WorkflowResponse::getEntries)
                .whenComplete((entries, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        respondWithProtobufSource(context, "Workflow", Source.from(entries.getEntriesList()));
                    }
                });
            }
        );
    }

    protected void delete(RoutingContext context) {
        commandFactory(context, commandFactory -> workflowCommunicator
            .command(commandFactory.delete(UUID.fromString(context.pathParam("id"))))
            .whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    ok(context);
                }
            })
        );
    }

    protected void readAll(RoutingContext context) {
        commandFactory(context, commandFactory -> {
            workflowCommunicator
                .query(commandFactory.queryAll())
                .thenApply(WorkflowResponse::getEntries)
                .whenComplete((entries, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        respondWithProtobufSource(context, "Workflows", Source.from(entries.getEntriesList()));
                    }
                });
            }
        );
    }

    protected void readOne(RoutingContext context) {
        commandFactory(context, commandFactory -> getId(context, "id", workflowId -> {
                workflowCommunicator
                    .query(commandFactory.queryById(workflowId))
                    .thenApply(WorkflowResponse::getEntries)
                    .whenComplete((entries, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            respondWithProtobufSource(context, "Workflow", Source.from(entries.getEntriesList()));
                        }
                    });
            })
        );
    }
}
