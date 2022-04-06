package com.bsi.peaks.server.handlers;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.analysis.dto.Analysis;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.projectAggregate.AnalysisInformation;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateState;
import com.bsi.peaks.event.projectAggregate.ProjectInformation;
import com.bsi.peaks.event.projectAggregate.SampleInformation;
import com.bsi.peaks.event.projectAggregate.WorkflowStepFilter;
import com.bsi.peaks.model.dto.FilePath;
import com.bsi.peaks.model.dto.FileStructureNode;
import com.bsi.peaks.model.dto.FilterList;
import com.bsi.peaks.model.dto.InstrumentDaemonSampleInfo;
import com.bsi.peaks.model.dto.Progress;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.dto.Sample;
import com.bsi.peaks.model.dto.SampleSubmission;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.cluster.Master;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.bsi.peaks.server.es.project.ProjectView;
import com.bsi.peaks.server.handlers.helper.InstrumentDaemonHelper;
import com.bsi.peaks.server.handlers.specifications.ProtoHandlerSpecification;
import com.bsi.peaks.server.parsers.ParserException;
import com.bsi.peaks.server.service.InstrumentDaemonService;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.server.service.UserService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.bsi.peaks.server.handlers.helper.InstrumentDaemonHelper.countFilesForDaemonTimstof;
import static com.bsi.peaks.server.handlers.helper.InstrumentDaemonHelper.createFoldersInstrumentDaemon;
import static com.bsi.peaks.server.handlers.helper.InstrumentDaemonHelper.writeDirectoryToDataRepo;
import static com.bsi.peaks.server.handlers.helper.InstrumentDaemonHelper.writeToDataRepo;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * @author Shengying Pan Created by span on 2/3/17.
 */
public class ProjectHandler extends ProtoHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ProjectHandler.class);
    private static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
        .registerModule(new ProtobufModule())
        .registerModule(new Jdk8Module());
    private final ActorRef masterProxy;
    private final ProjectAggregateCommunication projectCommunication;
    private final ProjectService projectService;
    private final InstrumentDaemonService instrumentDaemonService;
    private final UserService userService;
    private final ProjectView projectView;
    public static final String UPLOADS_DIR = "file-uploads";

    @Inject
    public ProjectHandler(
        final Vertx vertx,
        final ActorSystem system,
        @Named(Master.PROXY_NAME) final ActorRef masterProxy,
        final ProjectAggregateCommunication projectCommunication,
        final ProjectService projectService,
        final InstrumentDaemonService instrumentDaemonService,
        final UserService userService,
        final ProjectView projectView
    ) {
        super(vertx, system);
        this.masterProxy = masterProxy;
        this.projectCommunication = projectCommunication;
        this.projectService = projectService;
        this.userService = userService;
        this.projectView = projectView;
        this.instrumentDaemonService = instrumentDaemonService;
    }

    @NotNull
    private static boolean isMultipart(RoutingContext context) {
        return Optional.ofNullable(context.request().getHeader(HttpHeaders.CONTENT_TYPE))
            .map(String::toLowerCase)
            .map(contentType -> contentType.startsWith(HttpHeaderValues.MULTIPART_FORM_DATA.toString()))
            .orElse(false);
    }

    @Override
    public Router createSubRouter() {
        BodyHandler bodyHandler = BodyHandler.create();
        Router subRouter = Router.router(vertx);

        subRouter.route("/").handler(bodyHandler);
        subRouter.route("/:id").handler(bodyHandler);
        subRouter.get("/").handler(this::readAll);
        subRouter.get("/projectAnalysisInfos").handler(this::readAllProjectAnalysisInfos);
        subRouter.get("/:pid").handler(this::readOne);

        subRouter.post("/:pid/restart-uploads").handler(this::restartUploads);
        subRouter.put("/:id").consumes("*/json").handler(this::updateProject);
        subRouter.put("/:pid/samples/:sampleId").consumes("*/json").handler(bodyHandler).handler(this::updateSample);
        subRouter.delete("/:pid/samples/:sampleId/delete").handler(this::deleteSample);
        subRouter.delete("/:pid/samples/:status").handler(this::deleteSamplesByStatus);
        subRouter.put("/:pid/analysis/:aid/description").consumes("*/json").handler(bodyHandler).handler(this::updateAnalysisDescription);
        subRouter.put("/:pid/analysis/:aid").consumes("*/json").handler(bodyHandler).handler(this::updateAnalysis);
        subRouter.put("/:pid/analysis/:aid/priority").consumes("*/json").handler(bodyHandler).handler(this::updateAnalysisPriority);
        subRouter.delete("/:pid").handler(this::deleteProject);
        subRouter.get("/:pid/analysis").handler(this::readAllAnalysis);
        subRouter.get("/:pid/analysis/:aid").handler(this::readAnalysis);
        subRouter.get("/:pid/analysisParameters/:aid").handler(this::readAnalysisParameters);
        subRouter.get("/:pid/analysis/:aid/filter").handler(this::getFilter);
        subRouter.put("/:pid/analysis/:aid/filter").handler(this::updateFilter);
        subRouter.delete("/:pid/analysis/:aid").handler(this::deleteAnalysis);
        subRouter.delete("/:pid/analysis/:aid/cancel").handler(this::cancelAnalysis);
        subRouter.delete("/:pid/analysis/:aid/delete").handler(this::deleteAnalysis);

        subRouter.get("/:pid/samples/:sid/progress").handler(this::getSamplesProgress);
        subRouter.get("/:pid/daemonSamplesCopyProgress").handler(this::getDaemonSamplesCopyProgress);
        subRouter.get("/:pid/samples").handler(this::getSamples);
        subRouter.get("/:pid/samples/:sampleId").handler(this::getSample);
        subRouter.post("/:pid/samples").handler(bodyHandler).handler(this::createSamples);
        subRouter.post("/:pid/daemonSamples").handler(bodyHandler).handler(this::createDaemonSamples);
        subRouter.post("/project").handler(bodyHandler).handler(this::createProject);
        subRouter.get("/:pid/progress").handler(this::getProgress);
        subRouter.get("/progressForAnalysis").handler(this::getProgressForAnalysis);
        subRouter.get("/:pid/progressSingleAnalysis/:aid").handler(this::getProgressSingleAnalysis);

        subRouter.post("/:pid/analysis").handler(bodyHandler).handler(this::createAnalysis);
        subRouter.post("/:pid/upload/:sid/:fid").handler(this::upload);
        subRouter.post("/:pid/uploadDaemon/:sid/:fid").handler(this::uploadFromDaemon);
        subRouter.post("/:pid/share/:share").handler(this::share);

        subRouter.post("/:pid/instrumentDaemonSamples").handler(bodyHandler).handler(this::uploadInstrumentDaemonSample);

        return subRouter;
    }

    protected void share(RoutingContext context) {
        auth(context, user -> {
            getId(context, "pid", projectId -> {
                String shareStr = context.request().getParam("share").toLowerCase();

                //update project availability
                boolean share = Boolean.parseBoolean(shareStr);

                projectService.updateProject(projectId, user, ProjectAggregateCommandFactory.projectPermission(user.userId(), share))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            context.fail(error);
                        }
                    })
                    .thenCompose(done -> projectService.project(projectId, user))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            ok(context, result);
                        }
                    });
            });
        });
    }

    protected void restartUploads(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            projectService.restartFailedUploads(
                projectId, user)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        }));
    }

    protected void updateProject(RoutingContext context) {
        auth(context, user -> getId(context, "id", projectId -> {
            Project project = getBody(context, Project.newBuilder());
            ProjectInformation projectInformation = ProjectInformation.newBuilder()
                .setName(project.getName())
                .setDescription(project.getDescription())
                .build();
            projectService.updateProject(projectId, user, projectInformation)
                .thenCompose(done -> projectService.project(projectId, user))
                .whenComplete((result, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context, result);
                    }
                });
        }));
    }

    protected void updateSample(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            getId(context, "sampleId", sampleId -> {
                String sampleName = context.getBodyAsJson().getString("name");
                //update sample name
                SampleInformation sampleInformation = SampleInformation.newBuilder()
                    .setName(sampleName)
                    .build();
                projectService.updateSample(projectId, sampleId, user, sampleInformation)
                    .thenCompose(done -> projectService.sample(projectId, sampleId, user))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            ok(context, result);
                        }
                    });
            });
        }));
    }

    protected void deleteSamplesByStatus(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            ProjectAggregateState.SampleState.SampleStatus status = ProjectAggregateState.SampleState.SampleStatus.valueOf(
                context.request().getParam("status").trim().toUpperCase()
            );
            projectService.deleteSamplesByStatus(projectId, status, user).whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    ok(context);
                }
            });
        }));
    }

    protected void deleteSample(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            getId(context, "sampleId", sampleId -> {
                projectService.deleteSample(projectId, sampleId, user)
                    .whenComplete((done, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            ok(context);
                        }
                    });
            });
        }));
    }

    protected void updateAnalysisDescription(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            getId(context, "aid", analysisId -> {
                String description = context.getBodyAsJson().getString("description");
                projectService.updateAnalysis(projectId, analysisId, user, AnalysisInformation.newBuilder().setDescription(description).build())
                    .thenCompose(done -> projectService.analysis(projectId, analysisId, user))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            ok(context, result);
                        }
                    });
            });
        }));
    }

    protected void updateAnalysis(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            getId(context, "aid", analysisId -> {
                Analysis analysis = getBody(context, Analysis.newBuilder());
                AnalysisInformation analysisInformation = AnalysisInformation.newBuilder()
                    .setName(analysis.getName())
                    .setDescription(analysis.getDescription())
                    .build();
                projectService.updateAnalysis(projectId, analysisId, user, analysisInformation)
                    .thenCompose(done -> projectService.analysis(projectId, analysisId, user))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            ok(context, result);
                        }
                    });
            });
        }));
    }

    protected void updateAnalysisPriority(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            getId(context, "aid", analysisId -> {
                int priority = context.getBodyAsJson().getInteger("priority");
                final ProjectAggregateCommandFactory.AnalysisCommandFactory analysisCommand = ProjectAggregateCommandFactory.project(projectId, user.userId()).analysis(analysisId);
                projectCommunication.command(analysisCommand.setPriority(priority))
                    .thenCompose(done -> projectService.analysis(projectId, analysisId, user))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            ok(context, result);
                        }
                    });
            });
        }));
    }

    protected void readOne(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            projectService.project(projectId, user)
                .whenComplete((project, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context, project);
                    }
                });
        }));

    }

    protected void deleteProject(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            projectService.deleteProject(projectId, user)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        }));
    }

    protected void deleteAnalysis(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> getId(context, "aid", analysisId -> {
            projectService.deleteAnalysis(projectId, analysisId, user)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        })));
    }

    protected void cancelAnalysis(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> getId(context, "aid", analysisId -> {
            projectService.cancelAnalysis(projectId, analysisId, user)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        })));
    }

    protected void getSamplesProgress(RoutingContext context) {
        auth(context, user ->
            getId(context, "pid", projectId ->
                getId(context, "sid", sampleId ->
                    respondProgress(context, projectService.sampleProgress(projectId, sampleId, user)))));
    }

    protected void getDaemonSamplesCopyProgress(RoutingContext context) {
        auth(context, user ->
            getId(context, "pid", projectId ->
                    respondProgress(context, projectService.daemonSamplesCopyProgress(projectId, user))));
    }

    protected void getProgressSingleAnalysis(RoutingContext context) {
        auth(context, user ->
            getId(context, "pid", projectId ->
                getId(context, "aid", analysisId ->
                    respondProgress(context, projectService.analysisProgress(projectId, analysisId, user)))));
    }

    protected void getFilter(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> getId(context, "aid", analysisId -> {
            projectService.analysisQueriesAll(projectId, analysisId, user)
                .whenComplete((query, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context, DtoAdaptors.IDFilters(query));
                    }
                });
        })));
    }

    protected void updateFilter(RoutingContext context) {
        final HttpServerRequest request = context.request();
        auth(context, user -> getId(context, "pid", projectId -> getId(context, "aid", analysisId -> {
            try {
                final ImmutableMap<String, String> apiParameters = ImmutableMap.copyOf(request.params());
                final WorkFlowType workFlowType = ProtoHandlerSpecification.getAnySearchType(apiParameters).orElse(WorkFlowType.LFQ);

                final Optional<FilterList> filterList;
                switch (workFlowType) {
                    case LFQ:
                        filterList = ProtoHandlerSpecification.readLfqFilters(apiParameters);
                        break;
                    case REPORTER_ION_Q:
                        filterList = ProtoHandlerSpecification.readRiqFilters(apiParameters);
                        break;
                    case SPECTRAL_LIBRARY:
                    case DIA_DB_SEARCH:
                        filterList = ProtoHandlerSpecification.readSlFilter(apiParameters);
                        break;
                    case SILAC:
                        filterList = ProtoHandlerSpecification.readSilacFilter(apiParameters);
                        break;
                    case DENOVO:
                    case DB_SEARCH:
                    case PTM_FINDER:
                    case SPIDER:
                    default:
                        filterList = ProtoHandlerSpecification.readIdFilters(apiParameters);
                }

                WorkflowStepFilter filter = WorkflowStepFilter.newBuilder()
                    .setWorkFlowType(workFlowType)
                    .setFilter(filterList.orElseThrow(() -> new ParserException("Unable to set filter. Not filter was provided.")))
                    .build();


                projectService.setFilter(projectId, analysisId, filter, user)
                    .whenComplete((done, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            ok(context);
                        }
                    });
            } catch (Exception e) {
                context.fail(e);
            }
        })));
    }

    protected void getSamples(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            final HttpServerResponse response = context.response();
            final Source<Sample, NotUsed> source = projectService.projectSamples(projectId, user);
            serializationProcedure(
                context.request(),
                response,
                source,
                "samples"
            ).whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    response.end();
                }
            });
        }));
    }

    protected void getSample(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId ->
            getId(context, "sampleId", sampleId -> {
             projectService.sample(projectId, sampleId, user)
             .whenComplete((result, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    ok(context, result);
                }
            });
        })));
    }

    protected void getProgress(RoutingContext context) {
        auth(context, user -> getId(context, "pid", projectId -> {
            respondProgress(context, projectService.currentProjectProgress(projectId, user));
        }));
    }

    protected void getProgressForAnalysis(RoutingContext context) {
        auth(context, user -> respondProgress(context, projectService.currentAllAnalysisProgress(user)));
    }

    private void respondProgress(RoutingContext context, Source<Progress, NotUsed> progress) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        if ("websocket".equalsIgnoreCase(request.getHeader("Upgrade"))) {
            final ServerWebSocket webSocket;
            try {
                webSocket = request.upgrade();
            } catch (Exception e) {
                badRequest(context, e.getMessage());
                return;
            }
            respondWebSocket(webSocket, progress, materializerWithErrorLogging(request));
        } else {
            serializationProcedure(
                request,
                response,
                progress,
                "progress"
            ).whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    response.end();
                }
            });
        }
    }

    protected void readAll(RoutingContext context) {
        auth(context, user -> {
            HttpServerResponse response = context.response();
            Source<Project, NotUsed> projects;
            String keyspace = context.request().getParam("keyspace");
            if (user.role() == User.Role.SYS_ADMIN && keyspace != null && !keyspace.isEmpty()) {
                projects = projectService.projectsInKeyspace(user, keyspace);
            } else {
                projects = projectService.projects(user);
            }
            serializationProcedure(context.request(), response, projects, "projects").whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    response.end();
                }
            });
        });
    }

    protected void readAllProjectAnalysisInfos(RoutingContext context) {
        auth(context, user -> {
            HttpServerResponse response = context.response();

            serializationProcedure(
                context.request(),
                response,
                projectService.projectAnalysisInfos(user),
                "projectsAnalysisInfos"
            )
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        response.end();
                    }
                });
        });
    }

    private void getUploadedFile(final RoutingContext context, Consumer<FileUpload> fileUploadConsumer) {
        try {
            fileUploadConsumer.accept(Iterators.getOnlyElement(context.fileUploads().iterator()));
        } catch (Exception e) {
            badRequest(context, "Unable to access uploaded file");
        }
    }

    private void readAnalysisParameters(final RoutingContext context) {
        auth(context, user ->
            getId(context, "pid", projectId -> {
                getId(context, "aid", analysisId -> {
                    projectService.analysisParameters(projectId, analysisId, user)
                        .whenComplete((analysis, throwable) -> {
                            if (throwable != null) {
                                handleError(context, throwable);
                            } else  {
                                ok(context, analysis);
                            }
                        });
                });
            }));
    }

    private void readAnalysis(final RoutingContext context) {
        auth(context, user ->
            getId(context, "pid", projectId -> {
                getId(context, "aid", analysisId -> {
                    projectService.analysis(projectId, analysisId, user)
                        .whenComplete((analysis, throwable) -> {
                            if (throwable != null) {
                                handleError(context, throwable);
                            } else  {
                                ok(context, analysis);
                            }
                        });
                });
            }));
    }

    private void readAllAnalysis(final RoutingContext context) {
        auth(context, user ->
            getId(context, "pid", projectId -> {
                HttpServerResponse response = context.response();
                serializationProcedure(
                    context.request(),
                    response,
                    projectService.projectAnalysis(projectId, user),
                    "analysis"
                )
                    .whenComplete((done, error) -> {
                        if (error != null) {
                            context.fail(error);
                        } else {
                            response.end();
                        }
                    });
            }));
    }

    private CompletionStage<File> prepareUploadedFiles(final File uploadedDir) {
        return CompletableFuture.supplyAsync(() -> {
            File[] content = uploadedDir.listFiles();
            if (content == null || content.length == 0 || content.length > 2) {
                throw new IllegalStateException("Unexpected uploaded dir, illegal content at " + uploadedDir.getAbsolutePath());
            }
            if (content.length == 1) {
                // 1 single file
                File file = content[0];
                String fileName = file.getName();
                if (fileName.toLowerCase().endsWith(".zip")) {
                    // zipped folder, try to unzip first then delete the zip file itself
                    try {
                        ZipFile zipFile = new ZipFile(file);
                        String destination = new File(uploadedDir, fileName + "_deflated").getAbsolutePath();
                        zipFile.extractAll(destination);
                        File folder = new File(destination);
                        while (folder.isDirectory() && folder.listFiles() != null
                            && folder.listFiles().length == 1 && folder.listFiles()[0].isDirectory()) {
                            folder = folder.listFiles()[0];
                        }
                        return folder;
                    } catch (ZipException e) {
                        throw new RuntimeException("Unable to unzip data archive", e);
                    } finally {
                        try {
                            Files.deleteIfExists(file.toPath());
                        } catch (IOException e) {
                            LOG.warn("Unable to delete zip file after extraction: " + file.getAbsolutePath());
                        }
                    }
                } else {
                    return file;
                }
            } else {
                // 2 or more files return dir
                return uploadedDir;
            }
        });
    }

    private void cleanUploadedFiles(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            LOG.error("Unable to delete uploaded file {}", dir, e);
        }
    }

    private void upload(final RoutingContext context) {
        if (!isMultipart(context)) {
            context.fail(415);
        } else {
            auth(context, user ->
                getId(context, "pid", projectId ->
                    getId(context, "sid", sampleId ->
                        getId(context, "fid", fractionId ->
                            upload(context, projectId, fractionId, sampleId, user, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
                        )
                    )
                )
            );
        }
    }

    private void uploadFromDaemon(final RoutingContext context) {
        HttpServerRequest request = context.request();
        String name = request.getHeader("instrument-daemon-name");
        String apiKey = request.getHeader("instrument-daemon-api-key");
        String remotePathName = request.getHeader("remotePathName");
        String subPath = request.getHeader("subPath");
        String sampleName = request.getHeader("sampleName");
        String fractionName = request.getHeader("fractionName");
        String fileStructure = request.getHeader("fileStructure");
        String enzyme = request.getHeader("enzyme");
        String activationMethod = request.getHeader("activationMethod");
        Optional <FileStructureNode> fileStructureNode = Optional.empty();
        try {
            if (fileStructure != null && !fileStructure.isEmpty()) {
                fileStructureNode = Optional.of(OM.readValue(fileStructure, FileStructureNode.class));
            }
        } catch (IOException e) {
            LOG.error("Failed to parse fileStructure node json string for timstof daemon upload " + e.getMessage());
            e.printStackTrace();
        }
        Boolean hasPermission = instrumentDaemonService
            .checkUploadPermission(UUID.fromString(User.ADMIN_USERID), name, apiKey)
            .toCompletableFuture().join();
        if (hasPermission){
            UUID projectId = UUID.fromString(context.request().getParam("pid"));
            UUID sampleId = UUID.fromString(context.request().getParam("sid"));
            UUID fractionId = UUID.fromString(context.request().getParam("fid"));
            Optional<FilePath> filePathOptional;
            if (remotePathName.isEmpty() && subPath.isEmpty()) {
                filePathOptional = Optional.empty();
            } else {
                filePathOptional = Optional.of(FilePath.newBuilder()
                    .setRemotePathName(remotePathName)
                    .setSubPath(subPath)
                    .build());
                instrumentDaemonService.updateInstrumentDaemonFileCopyStatus(UUID.fromString(User.ADMIN_USERID),
                    projectId, sampleId, fractionId, Progress.State.PENDING).toCompletableFuture().join();
            }
            if (!enzyme.isEmpty()) {
                projectService.updateSampleEnzyme(projectId, sampleId, User.admin(), enzyme);
            }
            if (!activationMethod.isEmpty()) {
                projectService.updateSampleActivationMethod(projectId, sampleId, User.admin(), activationMethod);
            }
            upload(context, projectId, fractionId, sampleId, User.admin(), filePathOptional, Optional.of(sampleName), Optional.of(fractionName), fileStructureNode);
        }
    }

    private void upload(
        RoutingContext context,
        UUID projectId,
        UUID fractionId,
        UUID sampleId,
        User user,
        Optional<FilePath> filePath,
        Optional<String> sampleName,
        Optional<String> fractionName,
        Optional<FileStructureNode> fileStructureNode
    ) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        request.setExpectMultipart(true);

        final FileSystem fileSystem = context.vertx().fileSystem();
        final File uploadedDir = new File(UPLOADS_DIR, UUID.randomUUID().toString());
        if (!fileSystem.existsBlocking(UPLOADS_DIR)) {
            fileSystem.mkdirsBlocking(UPLOADS_DIR);
        }
        fileSystem.mkdirsBlocking(uploadedDir.getAbsolutePath());
        final Map<String, String> nameToAbsolutePath = new HashMap<>();
        if (fileStructureNode.isPresent()) {
            FileStructureNode structureNode = fileStructureNode.get();
            createFoldersInstrumentDaemon(structureNode, fileSystem, uploadedDir.getAbsolutePath(), nameToAbsolutePath);
        }

        final ProjectAggregateCommandFactory.UploadNotificationFactory uploadNotificationFactory = ProjectAggregateCommandFactory.project(projectId, user.userId())
            .sample(sampleId)
            .uploadFraction(fractionId, UUID.randomUUID());

        try {
            projectService.uploadSampleNotification(projectId, user.userId(), uploadNotificationFactory.start()).toCompletableFuture().join();
        } catch (Exception e) {
            // If error sending start event, then cancel.
            LOG.error("Upload start nacked for fraction " + fractionId, e);
            request.pause();
            response.setStatusCode(410).end();
            request.connection().close();
        }

        final AtomicInteger fileQuota = new AtomicInteger(0);
        final CompletableFuture<Done> uploadFuture = new CompletableFuture<>();

        request.exceptionHandler(e -> {
            projectService.uploadSampleNotification(projectId, user.userId(), uploadNotificationFactory.fail(e.toString()))
                .whenComplete((done, throwable2) -> {
                    if (throwable2 != null) {
                        LOG.error("Upload fail nacked for fraction " + fractionId, throwable2);
                    }
                });
            context.fail(e);
            cleanUploadedFiles(uploadedDir);
        });
        AtomicReference<String> uploadFileName = new AtomicReference<>();
        request.uploadHandler(upload -> {
            // we actually upload to a file with a generated filename
            File target;
            if (fileStructureNode.isPresent()) {
                target = new File(nameToAbsolutePath.get(upload.filename()), upload.filename());
            } else {
                target = new File(uploadedDir, upload.filename());
            }
            uploadFileName.set(upload.filename());
            upload.streamToFileSystem(target.getAbsolutePath());
            upload.exceptionHandler(uploadFuture::completeExceptionally);
            upload.endHandler(ignore -> {
                if (response.ended()) {
                    uploadFuture.completeExceptionally(new IllegalStateException("Response closed prematurely. Failing upload for fraction" + fractionId));
                } else if (fileStructureNode.isPresent()) {
                    LOG.info("File {} uploaded to Vertx into directory {}", upload.filename(), target.getAbsolutePath());
                    int expectedQuota = countFilesForDaemonTimstof(fileStructureNode.get());
                    if (fileQuota.incrementAndGet() == expectedQuota) {
                        uploadFuture.complete(Done.getInstance());
                    }
                } else {
                    LOG.info("File {} uploaded to Vertx into directory {}", upload.filename(), uploadedDir);
                    int expectedQuota = (upload.filename().toLowerCase().endsWith(".wiff") ||
                        upload.filename().toLowerCase().endsWith(".wiff.scan")) ? 2 : 1;
                    if (fileQuota.incrementAndGet() == expectedQuota) {
                        uploadFuture.complete(Done.getInstance());
                    }
                }
            });
        });

        uploadFuture.whenComplete((done, error) -> {
            ok(context); // but no matter what, will always ok the client as we don't want the CLI to fail
            fractionName.ifPresent(fName -> projectService.updatedFractionName(projectId, sampleId, fractionId, user, fName));
            sampleName.ifPresent(s -> projectService.updateSample(projectId, sampleId, user, SampleInformation.newBuilder().setName(s).build()));
            if (error != null) {
                projectService.uploadSampleNotification(projectId, user.userId(), uploadNotificationFactory.fail(error.getMessage()))
                    .thenCompose(ignore -> instrumentDaemonService.updateInstrumentDaemonFileCopyStatus(UUID.fromString(User.ADMIN_USERID),
                        projectId, sampleId, fractionId, Progress.State.FAILED))
                    .whenComplete((ignore, throwable2) -> {
                        if (throwable2 != null) {
                            LOG.error("Upload fail nacked for fraction " + fractionId, throwable2);
                        }
                    });
            } else {
                CompletionStage<Done> copyFileFuture = completedFuture(Done.getInstance());
                if (filePath.isPresent()) {
                    copyFileFuture = instrumentDaemonService.updateInstrumentDaemonFileCopyStatus(UUID.fromString(User.ADMIN_USERID),
                            projectId, sampleId, fractionId, Progress.State.PROGRESS)
                        .thenCompose(ignore -> {
                            if (fileStructureNode.isPresent()) {
                                for (File file : uploadedDir.listFiles()) {
                                    if (file.getName().equals(fileStructureNode.get().getName())) {
                                        writeDirectoryToDataRepo(filePath.get().getRemotePathName(),
                                            filePath.get().getSubPath(), file, fileStructureNode.get(),
                                            fractionId, projectId, LOG);
                                    }
                                }
                                return CompletableFuture.completedFuture(Done.getInstance());
                            }
                            else {
                                return writeToDataRepo(filePath.get(), uploadedDir, uploadFileName.get(), fractionId, projectId, LOG);
                            }
                        })
                        .thenCompose(ignore -> instrumentDaemonService.updateInstrumentDaemonFileCopyStatus(UUID.fromString(User.ADMIN_USERID),
                            projectId, sampleId, fractionId, Progress.State.DONE));
                }
                copyFileFuture
                    .thenCompose(ignore -> prepareUploadedFiles(uploadedDir))
                    .whenComplete((uploadedFile, preparationError) -> {
                        if (preparationError != null) {
                            final String msg = MessageFormat.format("Unable to prepare the uploaded folder {} for project {} fraction {}",
                                uploadedDir.getAbsolutePath(), projectId, fractionId);
                            LOG.error(msg);
                            cleanUploadedFiles(uploadedDir);
                            projectService.uploadSampleNotification(projectId, user.userId(), uploadNotificationFactory.fail(msg))
                                .whenComplete((ignore, throwable2) -> {
                                    if (throwable2 != null) {
                                        LOG.error("Upload fail nacked for fraction " + fractionId, throwable2);
                                    }
                                });

                        } else {
                            projectService.uploadSampleNotification(projectId, user.userId(), uploadNotificationFactory.complete(uploadedFile.getAbsolutePath()))
                                .whenComplete((ignore, throwable) -> {
                                    if (throwable != null) {
                                        String message = "Unable to update master about the uploaded file";
                                        LOG.error(message, throwable);
                                        cleanUploadedFiles(uploadedDir);
                                        LOG.error("Upload done nacked for fraction " + fractionId, throwable);
                                    } else {
                                        LOG.info("Updated master about the uploaded file for project {} fraction {}",
                                            projectId, fractionId);
                                        // already passed to data loader, no need to delete the file here
                                    }
                                });
                        }
                    });
            }
        });
    }

    private void createSamples(RoutingContext context) {
        auth(context, (User user) -> {
            getId(context, "pid", (UUID projectId) -> {
                //create samples for project
                try {
                    final SampleSubmission sample = getBody(context, SampleSubmission.newBuilder());
                    projectService.createSamples(projectId, sample, user)
                        .whenComplete((idList, throwable) -> {
                            if (throwable != null) {
                                handleError(context, throwable);
                            } else  {
                                ok(context, idList);
                            }
                        });
                } catch (Exception e) {
                    handleError(context, e);
                }
            });
        });
    }

    private void createDaemonSamples(RoutingContext context) {
        auth(context, (User user) -> {
            getId(context, "pid", (UUID projectId) -> {
                //create samples for project
                try {
                    final InstrumentDaemonSampleInfo sampleInfo =
                        getBody(context, InstrumentDaemonSampleInfo.newBuilder());
                    SampleSubmission sampleSubmission = InstrumentDaemonHelper.createSamplesFromInstrumentDaemonSampleInfo(sampleInfo);
                    List<UUID> fractionIds = new ArrayList<>();
                    projectService.createSamples(projectId, sampleSubmission, user)
                        .thenCompose(sampleIdList -> {
                            List<UUID> sampleIds = sampleIdList.getIdList().stream().map(UUID::fromString).collect(Collectors.toList());
                            sampleIds.forEach(sampleId -> {
                                Sample sample = projectService.sample(projectId, sampleId, user).toCompletableFuture().join();
                                fractionIds.addAll(sample.getFractionsList().stream().map(Sample.Fraction::getId).map(UUID::fromString).collect(Collectors.toList()));
                            });
                            return instrumentDaemonService.uploadDaemonSample(user.userId(), sampleInfo, projectId, sampleIds, fractionIds);
                        })
                        .thenCompose(done -> projectService.createDaemonFractionsInfo(projectId, fractionIds, sampleInfo.getParsingRule().getSampleTimeout(), user))
                        .thenCompose((ignore) ->
                            serializationProcedure(context.request(), context.response(),
                                projectService.projectSamples(projectId, user), "samples"))
                        .whenComplete((done, error) -> {
                            if (error != null) {
                                context.fail(error);
                            } else {
                                context.response().end();
                            }
                        });
                } catch (Exception e) {
                    handleError(context, e);
                }
            });
        });
    }

    private void createAnalysis(RoutingContext context) {
        auth(context, user -> {
            getId(context, "pid", projectId -> {
                //create analysis for project
                try {
                    final Analysis analysis = getBody(context, Analysis.newBuilder());
                    projectService.createAnalysis(projectId, analysis, user)
                        .whenComplete((id, throwable) -> {
                            if (throwable != null) {
                                handleError(context, throwable);
                            } else  {
                                ok(context, id);
                            }
                        });
                } catch (Exception e) {
                    handleError(context, e);
                }
            });
        });
    }

    private void createProject(RoutingContext context) {
        auth(context, user -> {
            try {
                final Project project = getBody(context, Project.newBuilder());
                projectService.createProject(project, user)
                    .whenComplete((id, throwable) -> {
                        if (throwable != null) {
                            handleError(context, throwable);
                        } else  {
                            ok(context, id);
                        }
                    });
            } catch (Exception e) {
                handleError(context, e);
            }
        });
    }

    private void uploadInstrumentDaemonSample(RoutingContext context) {
        auth(context, (User user) -> {
            getId(context, "pid", (UUID projectId) -> {
                try {
                    final List<UUID> fractionIds = new ArrayList<>();
                    final InstrumentDaemonSampleInfo sampleInfo =
                        getBody(context, InstrumentDaemonSampleInfo.newBuilder());
                    SampleSubmission sampleSubmission = InstrumentDaemonHelper.createSamplesFromInstrumentDaemonSampleInfo(sampleInfo);
                    projectService.createSamples(projectId, sampleSubmission, user)
                        .thenCompose(sampleIdList -> {
                            List<UUID> sampleIds = sampleIdList.getIdList().stream().map(UUID::fromString).collect(Collectors.toList());
                            sampleIds.stream()
                                .map(sampleId -> projectService.sample(projectId, sampleId, user).toCompletableFuture().join()) //TODO add query that grabs multiple samples by sampleIds
                                .forEach(sample -> sample.getFractionsList().forEach(fraction -> fractionIds.add(UUID.fromString(fraction.getId()))));
                            return instrumentDaemonService.uploadDaemonSample(user.userId(), sampleInfo, projectId, sampleIds, fractionIds);
                        })
                        .thenCompose(done -> projectService.createDaemonFractionsInfo(projectId, fractionIds, sampleInfo.getParsingRule().getSampleTimeout(), user))
                        .whenComplete((done, error) -> {
                            if (error != null) {
                                handleError(context, error);
                            } else {
                                ok(context);
                            }
                        });
                } catch (Exception e) {
                    handleError(context, e);
                }
            });
        });
    }
}
