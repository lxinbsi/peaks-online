package com.bsi.peaks.server.handlers;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import com.bsi.peaks.model.dto.FileUploadList;
import com.bsi.peaks.model.dto.RemotePath;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.service.UploaderService;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import static java.util.Collections.singletonList;

/**
 * @author span
 * Created by span on 2018-12-10
 */
public class RemoteUploadHandler extends ProtoHandler {
    private final UploaderService uploaderService;

    @Inject
    public RemoteUploadHandler(final Vertx vertx, final ActorSystem system, final UploaderService uploaderService) {
        super(vertx, system);
        this.uploaderService = uploaderService;
    }

    @Override
    public Router createSubRouter() {
        BodyHandler requestHandler = BodyHandler.create();
        Router subRouter = Router.router(vertx);

        subRouter.get("/paths").handler(this::getAllPaths);
        subRouter.get("/files/:name").handler(this::getAllFiles);
        subRouter.post("/paths").handler(requestHandler).handler(this::addPath);
        subRouter.delete("/paths/:name").handler(this::deletePath);
        subRouter.put("/paths/:name").handler(requestHandler).handler(this::updatePath);
        subRouter.post("/upload").handler(requestHandler).handler(this::upload);

        return subRouter;
    }

    private void getAllPaths(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        auth(context, user -> {
            Source<RemotePath, NotUsed> remotePathNotUsedSource = Source.fromCompletionStage(uploaderService.retrievePaths())
                .flatMapConcat(f -> Source.from(f.getPathsList()))
                .filter(p -> user.role() == User.Role.SYS_ADMIN ||
                    p.getUserIdCount() == 0 ||
                    p.getUserIdList().contains(user.userId().toString()));

            serializationProcedure(request, response, remotePathNotUsedSource, "remotePath")
                .whenComplete((done, error) -> {
                    if (error != null) {
                        handleError(context, error);
                    } else {
                        response.end();
                    }
                });
            }
        );
    }

    private void addPath(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            RemotePath payload = getBody(context, RemotePath.newBuilder());
            uploaderService.addPath(payload).whenComplete((ignore, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            });
        });
    }

    private void updatePath(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            String oldPathName = context.request().getParam("name");
            RemotePath payload = getBody(context, RemotePath.newBuilder());
            uploaderService.updatePath(oldPathName, payload).whenComplete((ignore, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            });
        });
    }

    private void deletePath(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            String pathName = context.request().getParam("name");
            uploaderService.deletePath(pathName).whenComplete((ignore, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            });
        });
    }

    private void upload(RoutingContext context) {
        auth(context, user -> {
            FileUploadList payload = getBody(context, FileUploadList.newBuilder());
            uploaderService.upload(DtoAdaptors.convertFileUploadList(payload)).whenComplete((ignore, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            });
        });
    }

    private void getAllFiles(RoutingContext context) {
        auth(context, user -> {
            String pathName = context.request().getParam("name");
            String subPath = context.request().getParam("subpath");
            if (subPath == null) {
                subPath = "";
            }
            // this might be slow
            uploaderService.getAllFiles(pathName, subPath).whenComplete((node, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context, node);
                }
            });
        });
    }
}
