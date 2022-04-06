package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.ReporterIonQMethodCommandFactory;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodResponse;
import com.bsi.peaks.model.dto.peptide.ReporterIonQMethod;
import com.bsi.peaks.server.es.communication.RiqMethodManagerCommunication;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.function.Consumer;

public class ReportIonQMethodHandler extends ProtoHandler {

    private final RiqMethodManagerCommunication reporterIonQMethodManagerCommunication;

    @Inject
    public ReportIonQMethodHandler(
        Vertx vertx,
        ActorSystem system,
        RiqMethodManagerCommunication reporterIonQMethodManagerCommunication
    ) {
        super(vertx, system);
        this.reporterIonQMethodManagerCommunication = reporterIonQMethodManagerCommunication;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.route("/").handler(BodyHandler.create());
        subRouter.route("/:id").handler(BodyHandler.create());
        subRouter.post("/").consumes("*/json").handler(this::create);
        subRouter.get("/").handler(this::readAll);
        subRouter.get("/:id").handler(this::readOne);
        subRouter.put("/:id").consumes("*/json").handler(this::update);
        subRouter.delete("/:id").handler(this::delete);
        subRouter.post("/:id/takeownership").handler(this::takeownership);
        return subRouter;
    }

    private void commandFactory(RoutingContext context, Consumer<ReporterIonQMethodCommandFactory> userConsumer) {
        auth(context, user -> userConsumer.accept(ReporterIonQMethodCommandFactory.actingAsUserId(user.userId().toString())));
    }

    private void readAll(RoutingContext context) {
        commandFactory(context, commandFactory -> {
            reporterIonQMethodManagerCommunication
                .query(commandFactory.queryAll())
                .thenApply(ReporterIonQMethodResponse::getEntries)
                .whenComplete((entries, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        respondWithProtobufSource(context, "PrecursorIonQMethods", Source.from(entries.getEntriesList()));
                    }
                });
            }
        );
    }

    private void readOne(RoutingContext context) {
        commandFactory(context, commandFactory -> {
            reporterIonQMethodManagerCommunication
                .query(commandFactory.queryByName(context.pathParam("id")))
                .thenApply(response -> response.getEntries().getEntries(0))
                .whenComplete((method, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context, method);
                    }
                });
            }
        );
    }

    private void create(RoutingContext context) {
        final ReporterIonQMethod reporterIonQMethod = getBody(context, ReporterIonQMethod.newBuilder());
        commandFactory(context, commandFactory -> reporterIonQMethodManagerCommunication
            .command(commandFactory.create(reporterIonQMethod))
            .whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    ok(context);
                }
            })
        );
    }


    private void update(RoutingContext context) {
        final ReporterIonQMethod reporterIonQMethod = getBody(context, ReporterIonQMethod.newBuilder());
        commandFactory(context, commandFactory -> reporterIonQMethodManagerCommunication
            .command(commandFactory.update(context.pathParam("id"), reporterIonQMethod))
            .whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    ok(context);
                }
            })
        );
    }

    private void delete(RoutingContext context) {
        commandFactory(context, commandFactory -> reporterIonQMethodManagerCommunication
            .command(commandFactory.delete(context.pathParam("id")))
            .whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    ok(context);
                }
            })
        );
    }

    private void takeownership(RoutingContext context) {
        commandFactory(context, commandFactory -> reporterIonQMethodManagerCommunication
            .command(commandFactory.takeOwnership(context.pathParam("id")))
            .whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    ok(context);
                }
            })
        );
    }

}
