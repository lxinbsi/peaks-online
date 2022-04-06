package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import com.bsi.peaks.model.dto.InstrumentDaemon;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.service.InstrumentDaemonService;
import com.bsi.peaks.server.service.LogService;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;

public class InstrumentDaemonHandler extends ProtoHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MonitorHandler.class);

    private final LogService logService;
    private final Config config;
    private final InstrumentDaemonService instrumentDaemonService;

    @Inject
    public InstrumentDaemonHandler(
        final Vertx vertx,
        final ActorSystem system,
        final LogService logService,
        final Config config,
        final InstrumentDaemonService instrumentDaemonService
    ) {
        super(vertx, system);
        this.logService = logService;
        this.config = config;
        this.instrumentDaemonService = instrumentDaemonService;
    }

    @Override
    public Router createSubRouter() {
        BodyHandler bodyHandler = BodyHandler.create();
        Router subRouter = Router.router(vertx);
        subRouter.get("/").handler(this::getAllDaemon);
        subRouter.put("/update/:name").consumes("*/json").handler(bodyHandler).handler(this::updateDaemon);
        subRouter.delete("/delete/:name").handler(this::deleteDaemon);
        return subRouter;
    }


    private void getAllDaemon(RoutingContext context) {
        auth(context, user -> {
            HttpServerResponse response = context.response();
            serializationProcedure(
                context.request(),
                response,
                instrumentDaemonService.queryAllDaemon(user.userId()),
                "instrumentDaemons"
            ).whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    response.end();
                }
            });
        });
    }

    private void updateDaemon(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            InstrumentDaemon instrumentDaemon = getBody(context, InstrumentDaemon.newBuilder());
            instrumentDaemonService.update(user.userId(), instrumentDaemon)
                .whenComplete((ok, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        });
    }

    private void deleteDaemon(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final String name = context.request().getParam("name");
            instrumentDaemonService.delete(user.userId(), name)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        });
    }
}
