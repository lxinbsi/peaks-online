package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import com.bsi.peaks.model.dto.SmtpServer;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.service.EmailService;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.Collections;

/**
 * @author span
 * Created by span on 2019-03-07
 */
public class EmailHandler extends ProtoHandler {
    private final EmailService service;

    @Inject
    public EmailHandler(final Vertx vertx, final ActorSystem system, final EmailService service) {
        super(vertx, system);
        this.service = service;
    }

    @Override
    public Router createSubRouter() {
        BodyHandler requestHandler = BodyHandler.create();
        Router subRouter = Router.router(vertx);

        subRouter.get("/config").handler(this::getServer);
        subRouter.put("/config").handler(requestHandler).handler(this::updateServer);

        return subRouter;
    }

    private void getServer(RoutingContext context) {
        auth(context, Collections.singletonList(User.Role.SYS_ADMIN), user ->
            service.getServer(user).whenComplete((server, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context, server);
                }
            })
        );
    }

    private void updateServer(RoutingContext context) {
        auth(context, Collections.singletonList(User.Role.SYS_ADMIN), user -> {
            SmtpServer payload = getBody(context, SmtpServer.newBuilder());
            service.updateServer(user, payload).whenComplete((server, error ) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context, server);
                }
            });
        });
    }
}
