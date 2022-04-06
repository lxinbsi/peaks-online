package com.bsi.peaks.server.handlers;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.event.project.ArchiveProject;
import com.bsi.peaks.event.project.ImportArchive;
import com.bsi.peaks.model.dto.*;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.service.ArchivingService;
import com.bsi.peaks.server.service.ProjectService;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.*;

import static java.util.Collections.singletonList;

public class ArchivingHandler extends ProtoHandler {
    private final ArchivingService service;
    private final ProjectService projectService;

    @Inject
    public ArchivingHandler(final Vertx vertx, final ActorSystem system,
                            final ArchivingService service, final ProjectService projectService) {
        super(vertx, system);
        this.service = service;
        this.projectService = projectService;
    }

    @Override
    public Router createSubRouter() {
        BodyHandler requestHandler = BodyHandler.create();
        Router subRouter = Router.router(vertx);

        // for management
        subRouter.get("/paths").handler(this::getPaths);
        subRouter.get("/files/:name").handler(this::getFiles);
        subRouter.post("/paths").handler(requestHandler).handler(this::addPath);
        subRouter.delete("/paths/:name").handler(this::deletePath);
        subRouter.put("/paths/:name").handler(requestHandler).handler(this::updatePath);
        subRouter.post("/paths/:name/create-folder").handler(requestHandler).handler(this::createFolder);

        // for archiving/importing process
        subRouter.post("/project/:pid/archive").handler(requestHandler).handler(this::startArchive);
        subRouter.put("/project/:pid/archive").handler(requestHandler).handler(this::retryArchive);
        subRouter.delete("/project/:pid/archive").handler(this::cancelArchive);

        subRouter.post("/project/:pid/import").handler(requestHandler).handler(this::startImport);
        subRouter.put("/project/:pid/import").handler(requestHandler).handler(this::retryImport);
        subRouter.delete("/project/:pid/import").handler(this::cancelImport);

        // for listing
        subRouter.get("/archive-tasks").handler(this::getArchiveTasks);
        subRouter.get("/import-tasks").handler(this::getImportTasks);
        subRouter.get("/archive-tasks/:pid").handler(this::getArchiveTask);
        subRouter.get("/import-tasks/:pid").handler(this::getImportTask);

        // batch cancel import
        subRouter.delete("/import-tasks").handler(this::batchCancelImport);

        return subRouter;
    }

    private void getArchiveTask(RoutingContext context) {
        auth(context, user -> {
            String projectId = context.request().getParam("pid");
            service.getArchiveTasks(user).whenComplete((list, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    try {
                        ArchiveTask match = list.stream()
                            .filter(task -> task.getProjectId().equals(projectId) &&
                                (user.role().equals(User.Role.SYS_ADMIN) || task.getCreatorId().equals(user.userId().toString()))
                            )
                            .findFirst().orElseThrow(NoSuchElementException::new);
                        ok(context, match);
                    } catch (Exception e) {
                        handleError(context, e);
                    }
                }
            });
        });
    }

    private void getImportTask(RoutingContext context) {
        auth(context, user -> {
            String projectId = context.request().getParam("pid");
            service.getImportingTasks(user).whenComplete((list, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    try {
                        ImportingTask match = list.stream()
                            .filter(task -> task.getProjectId().equals(projectId) &&
                                (user.role().equals(User.Role.SYS_ADMIN) || task.getCreatorId().equals(user.userId().toString()))
                            )
                            .findFirst().orElseThrow(NoSuchElementException::new);
                        ok(context, match);
                    } catch (Exception e) {
                        handleError(context, e);
                    }
                }
            });
        });
    }

    private void getArchiveTasks(RoutingContext context) {
        auth(context, user -> {
            service.getArchiveTasks(user).whenComplete((list, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    respondWithProtobufSource(context, "tasks", Source.from(list).filter(task ->
                        user.role().equals(User.Role.SYS_ADMIN) || task.getCreatorId().equals(user.userId().toString()
                        )));
                }
            });
        });
    }

    private void getImportTasks(RoutingContext context) {
        auth(context, user -> {
            service.getImportingTasks(user).whenComplete((list, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    respondWithProtobufSource(context, "tasks", Source.from(list).filter(task ->
                        user.role().equals(User.Role.SYS_ADMIN) || task.getCreatorId().equals(user.userId().toString()
                        )));
                }
            });
        });
    }

    private void addPath(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            RemotePath payload = getBody(context, RemotePath.newBuilder());
            service.addPath(user, payload).whenComplete((ignore, error) -> {
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
            service.deletePath(user, pathName).whenComplete((ignore, error) -> {
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
            service.updatePath(user, oldPathName, payload).whenComplete((ignore, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            });
        });
    }

    private void createFolder(RoutingContext context) {
        auth(context, user -> {
            FilePath filePath = getBody(context, FilePath.newBuilder());

            Source.fromCompletionStage(service.retrievePaths(user))
                .flatMapConcat(f -> Source.from(f.getPathsList()))
                .filter(p -> p.getName().equals(filePath.getRemotePathName()))
                .filter(p -> user.role() == User.Role.SYS_ADMIN ||
                    p.getUserIdCount() == 0 ||
                    p.getUserIdList().contains(user.userId().toString()))
                .runWith(Sink.head(), materializerWithErrorLogging(context.request()))
                .thenCompose(ignore -> service.createFolder(user, filePath.getRemotePathName(), filePath.getSubPath()))
                .whenComplete((match, error) -> {
                    if (error != null) {
                        handleError(context, error);
                    } else {
                        ok(context);
                    }
                });
        });
    }

    private void getPaths(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        auth(context, user -> {
                Source<RemotePath, NotUsed> remotePathNotUsedSource = Source.fromCompletionStage(service.retrievePaths(user))
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

    private void getFiles(RoutingContext context) {
        auth(context, user -> {
            String pathName = context.request().getParam("name");
            String subPath = context.request().getParam("subpath");
            if (subPath == null) {
                subPath = "";
            }
            // this might be slow
            service.retrieveFiles(user, pathName, subPath).whenComplete((node, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context, node);
                }
            });
        });
    }

    private void startArchive(RoutingContext context) {
        auth(context, user -> getId(context,"pid", projectId ->
            projectService.project(projectId, user).thenCompose(project -> {
                if (!project.getState().equals(Progress.State.DONE)) {
                    throw new IllegalStateException("Only project in done state can be archived");
                }
                ArchiveRequest request = getBody(context, ArchiveRequest.newBuilder());
                ArchiveProject archiveProject = ArchiveProject.newBuilder()
                    .setProjectId(projectId.toString())
                    .setTargetFolder(request.getArchivePath())
                    .setCreatorId(user.userId().toString())
                    .setOldProjectName(project.getName())
                    .setDiscardScanPeaks(request.getDiscardScanPeaks())
                    .build();
                return service.startArchive(user, archiveProject);
            }).whenComplete((done, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            })
        ));
    }

    private void retryArchive(RoutingContext context) {
        auth(context, user -> getId(context,"pid", projectId ->
            service.restartArchive(user, projectId).whenComplete((done, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            })
        ));
    }

    private void cancelArchive(RoutingContext context) {
        auth(context, user -> getId(context,"pid", projectId ->
            service.cancelArchive(user, projectId).whenComplete((done, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            })
        ));
    }

    private void startImport(RoutingContext context) {
        auth(context, user -> getId(context,"pid", projectId ->
            projectService.projectExist(projectId).thenCompose(existence -> {
                if (existence) {
                    throw new IllegalStateException("Archive collides with existing project, id: " + projectId);
                }
                ImportRequest request = getBody(context, ImportRequest.newBuilder());
                ImportArchive importArchive = ImportArchive.newBuilder()
                    .setTargetFolder(request.getArchivePath())
                    .setNewProjectName(request.getNewProjectName())
                    .setProjectId(projectId.toString())
                    .setNewOwnerId(user.userId().toString())
                    .build();
                return service.startImport(user, importArchive);
            }).whenComplete((done, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            })
        ));
    }

    private void retryImport(RoutingContext context) {
        auth(context, user -> getId(context,"pid", projectId ->
            service.restartImport(user, projectId).whenComplete((done, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            })
        ));
    }

    private void batchCancelImport(RoutingContext context) {
        auth(context, user -> {
            String ids = context.request().getParam("pids");
            if (ids == null || ids.trim().isEmpty()) {
                badRequest(context, "No import task to cancel");
            } else {
                String[] pids = ids.replace(" ", "").split(",");
                List<UUID> parsedIds = new ArrayList<>();
                for (String pid : pids) {
                    try {
                        parsedIds.add(UUID.fromString(pid));
                    } catch (Exception e) {
                        badRequest(context, "Unable to cancel " + pid + ", invalid id");
                        return;
                    }
                }
                service.batchCancelImport(user, parsedIds).whenComplete((done, error) -> {
                    if (error != null) {
                        handleError(context, error);
                    } else {
                        ok(context);
                    }
                });
            }
        });
    }

    private void cancelImport(RoutingContext context) {
        auth(context, user -> getId(context,"pid", projectId ->
            service.cancelImport(user, projectId).whenComplete((done, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    ok(context);
                }
            })
        ));
    }
}
