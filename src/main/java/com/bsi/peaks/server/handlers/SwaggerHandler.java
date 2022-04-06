package com.bsi.peaks.server.handlers;

import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;

/**
 * @author Shengying Pan Created by span on 2/9/17.
 */
public class SwaggerHandler extends ApiHandler {
    @Inject
    public SwaggerHandler(final Vertx vertx) {
        super(vertx);
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);

        StaticHandler handler = StaticHandler.create()
                .setWebRoot("swagger")
                .setIndexPage("index.html")
                .setCachingEnabled(false)
                .setFilesReadOnly(false);

        subRouter.route("/*").handler(handler);

        return subRouter;
    }
}
