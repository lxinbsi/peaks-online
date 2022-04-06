package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.SilacQMethodCommandFactory;
import com.bsi.peaks.event.silacqmethod.SilacQMethodResponse;
import com.bsi.peaks.model.dto.peptide.SilacQMethod;
import com.bsi.peaks.server.es.communication.SilacQMethodManagerCommunication;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.function.Consumer;

public class SilacQMethodHandler extends ProtoHandler {

    private final SilacQMethodManagerCommunication silacQMethodManagerCommunication;

    @Inject
    public SilacQMethodHandler(
        Vertx vertx,
        ActorSystem system,
        SilacQMethodManagerCommunication silacQMethodManagerCommunication
    ) {
        super(vertx, system);
        this.silacQMethodManagerCommunication = silacQMethodManagerCommunication;
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

    private void commandFactory(RoutingContext context, Consumer<SilacQMethodCommandFactory> userConsumer) {
        auth(context, user -> userConsumer.accept(SilacQMethodCommandFactory.actingAsUserId(user.userId().toString())));
    }

    private void readAll(RoutingContext context) {
        commandFactory(context, commandFactory -> {
            silacQMethodManagerCommunication
                .query(commandFactory.queryAll())
                .thenApply(SilacQMethodResponse::getEntries)
                .whenComplete((entries, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        respondWithProtobufSource(context, "SilacQMethods", Source.from(entries.getEntriesList()));
                    }
                });
            }
        );
    }

    private void readOne(RoutingContext context) {
        commandFactory(context, commandFactory -> {
            silacQMethodManagerCommunication
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
        final SilacQMethod silacQMethod = getBody(context, SilacQMethod.newBuilder());
        commandFactory(context, commandFactory -> silacQMethodManagerCommunication
            .command(commandFactory.create(silacQMethod))
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
        final SilacQMethod silacQMethod = getBody(context, SilacQMethod.newBuilder());
        commandFactory(context, commandFactory -> silacQMethodManagerCommunication
            .command(commandFactory.update(context.pathParam("id"), silacQMethod))
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
        commandFactory(context, commandFactory -> silacQMethodManagerCommunication
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
        commandFactory(context, commandFactory -> silacQMethodManagerCommunication
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
