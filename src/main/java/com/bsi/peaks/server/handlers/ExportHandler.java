package com.bsi.peaks.server.handlers;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.graph.DbApplicationGraph;
import com.bsi.peaks.data.storage.application.cache.FractionCacheEntry;
import com.bsi.peaks.model.filter.FilterFunction;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.handlers.helper.ApplicationGraphFactory;
import com.bsi.peaks.server.handlers.helper.VertxChunkedOutputStream;
import com.bsi.peaks.server.handlers.specifications.ExportSpecification;
import com.bsi.peaks.server.handlers.specifications.ExportSpecificationsCreator;
import com.bsi.peaks.server.service.ProjectService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Created by tandersen on 2017-02-28.
 */
public class ExportHandler extends ApiHandler implements ExportSpecificationsCreator {
    public static final BodyHandler BODY_HANDLER = BodyHandler.create();
    private static final Logger LOG = LoggerFactory.getLogger(ExportHandler.class);

    private final ActorSystem system;
    private final ProjectService projectService;
    private final ApplicationGraphFactory applicationGraphFactory;
    private final Materializer materializer;

    @Inject
    public ExportHandler(
        final Vertx vertx,
        final ActorSystem system,
        final ProjectService projectService,
        final ApplicationGraphFactory applicationGraphFactory
        ) {
        super(vertx);
        this.system = system;
        this.projectService = projectService;
        this.applicationGraphFactory = applicationGraphFactory;
        this.materializer = ActorMaterializer.create(system);
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);

        route(subRouter,"/mzidentml/:pid/:aid", dbMzIdentMLSpecifications());
        route(subRouter,"/pepxml/:pid/:aid", dbPepXmlSpecifications());
        route(subRouter,"/mzxml/:pid/:aid/:sid/:fid", dbMzxmlSpecifications());
        route(subRouter,"/feature/project/:pid/analysis/:aid/sample/:sid/fraction/:fid", featureSpecification());
        route(subRouter,"/protein-fasta/:pid/:aid", proteinFastaSpecification());
        route(subRouter,"/mgfRawMs1Ms2/project/:pid/sample/:sid/fraction/:fid", downloadMs1Ms2RawMGFSpecification());
        route(subRouter,"/mgfRawMs2/project/:pid/sample/:sid/fraction/:fid", downloadMs2RawMGFSpecification());
        route(subRouter,"/mgfRefined/project/:pid/analysis/:aid/sample/:sid/fraction/:fid", downloadMs2RefinedMGFSpecification());
        route(subRouter,"/peaks-proto/project/:pid/sample/:sid/fraction/:fid", downloadPeaksProtoSpecification());

        return subRouter;
    }

    private <Q extends Query, A> void route(Router router, String path, ExportSpecification<Q, A> spec) {
        router.get(path).handler(BODY_HANDLER).handler(get(spec));
        router.post(path).handler(BODY_HANDLER).handler(post(spec));
    }

    @Override
    public ObjectMapper om() {
        return ApiHandler.OM;
    }

    public <Q extends Query, A> Handler<RoutingContext> get(ExportSpecification<Q, A> exportSpecification) {
        return (RoutingContext context) -> auth(context, user -> {
            final UUID projectId;
            final UUID analysisId;
            final UUID sampleId;
            final UUID fractionId;

            try {
                final HttpServerRequest request = context.request();
                final String pid = request.getParam("pid");
                final String aid = request.getParam("aid");
                final String sid = request.getParam("sid");
                final String fid = request.getParam("fid");
                projectId = UUID.fromString(pid);
                analysisId = aid == null ? null : UUID.fromString(aid);
                sampleId = sid == null ? null : UUID.fromString(sid);
                fractionId = fid == null ? null : UUID.fromString(fid);
            } catch (Exception e) {
                LOG.warn("Unable to parse query", e);
                badRequest(context, "Unable to parse query");
                return;
            }

            if (analysisId != null && sampleId == null && fractionId == null) {
                handleAnalysis(context, exportSpecification, user, projectId, analysisId);
            } else if (analysisId == null && sampleId != null && fractionId != null) {
                handleFraction(context, exportSpecification, user, projectId, sampleId, fractionId);
            } else if (analysisId != null && sampleId != null &&  fractionId != null) {
                handleAnalysisFraction(context, exportSpecification, user, projectId, analysisId, sampleId, fractionId);
            } else {
                context.fail(new HandlerException(400, "Id not provided"));
            }
        });
    }

    public <Q extends Query, A> Handler<RoutingContext> post(ExportSpecification<Q, A> exportSpecification) {
        return (RoutingContext context) -> auth(context, user -> {
            final UUID projectId;
            final UUID analysisId;
            final UUID sampleId;
            final UUID fractionId;
            final JsonObject body;
            try {
                final HttpServerRequest request = context.request();
                final String pid = request.getParam("pid");
                final String aid = request.getParam("aid");
                final String sid = request.getParam("sid");
                final String fid = request.getParam("fid");
                projectId = UUID.fromString(pid);
                analysisId = aid == null ? null : UUID.fromString(aid);
                sampleId = sid == null ? null : UUID.fromString(sid);
                fractionId = fid == null ? null : UUID.fromString(fid);
                body = context.getBodyAsJson();
                if (body == null) {
                    badRequest(context, "POST with no body");
                    return;
                }
            } catch (Exception e) {
                LOG.warn("Unable to parse query", e);
                badRequest(context, "Unable to parse query");
                return;
            }

            if (analysisId != null && sampleId == null && fractionId == null) {
                handleAnalysis(context, exportSpecification, user, projectId, analysisId);
            } else if (analysisId == null && sampleId != null && fractionId != null) {
                handleFraction(context, exportSpecification, user, projectId, sampleId, fractionId);
            } else if (analysisId != null && sampleId != null &&  fractionId != null) {
                handleAnalysisFraction(context, exportSpecification, user, projectId, analysisId, sampleId, fractionId);
            } else {
                context.fail(new HandlerException(400, "Id not provided"));
            }
        });
    }

    private <Q extends Query, A> void handleFraction(
        RoutingContext context,
        ExportSpecification<Q, A> exportSpecification,
        User user,
        UUID projectId,
        UUID sampleId,
        UUID fractionId
    ) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final String host = request.host();
        response.exceptionHandler(e -> {
            LOG.error("Response exception", e);
            response.close();
        });
        final OutputStream output = new BufferedOutputStream(new VertxChunkedOutputStream(response));
        projectService.sampleGraphTaskIds(projectId, sampleId, user)
            .thenCompose(graphTaskIds -> {
                DbApplicationGraph applicationGraph = applicationGraphFactory.createFromSampleOnly(graphTaskIds);
                ImmutableMap<String, String> apiParameters = ImmutableMap.copyOf(request.params());
                final String fileName = applicationGraph.fractionSrcFile(fractionId);
                if (fileName == null) {
                    throw new IllegalArgumentException("Unable to find the sample and fraction info for " + fractionId);
                }

                FractionCacheEntry fraction = applicationGraph.fraction(fractionId).fractionCacheEntry()
                    .toCompletableFuture().join().orElse(null);
                if (fraction != null && fraction.hasIonMobility()) {
                    apiParameters = ImmutableMap.<String, String>builder().putAll(apiParameters).put("ionMobility", "true").build();
                }

                response.putHeader("Content-Disposition", MessageFormat.format("attachment; filename=\"{0}.{1}\"", fileName, exportSpecification.name(apiParameters)));
                response.putHeader("Content-Type", exportSpecification.mimeType());
                response.setChunked(true);

                LOG.info("Start export " + exportSpecification.exportLabel());
                try {
                    return exportSpecification.process(fileName, host, applicationGraph, output, materializer, apiParameters);
                } catch (Exception e) {
                    failChunkedResponse(response);
                    throw Throwables.propagate(e);
                }
            })
            .whenComplete(endResponseAndOutputStream(context, exportSpecification, response, output));
    }

    private <Q extends Query, A> void handleAnalysis(
        RoutingContext context,
        ExportSpecification<Q, A> exportSpecification,
        User user,
        UUID projectId,
        UUID analysisId
    ) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final String host = request.host();
        response.exceptionHandler(e -> {
            LOG.error("Response exception", e);
            response.close();
        });

        final OutputStream output = new BufferedOutputStream(new VertxChunkedOutputStream(response));
        projectService.project(projectId, user)
            .thenCompose(project -> {
                final ImmutableMap<String, String> apiParameters = ImmutableMap.copyOf(request.params());
                final WorkFlowType workFlowType = exportSpecification.filterType(apiParameters);
                final String fileName = project.getName() + "." + exportSpecification.name(workFlowType);

                return projectService.analysisGraphTaskIds(projectId, analysisId, workFlowType, user)
                    .thenCompose(graphTaskIds ->
                        exportSpecification.readFilters(apiParameters)
                            .map(optional -> completedFuture(optional))
                            .orElse(projectService.analysisFiltersByWorkflowType(projectId, analysisId, user, workFlowType).toCompletableFuture())
                            .thenApply(filters -> applicationGraphFactory.createFromIdentificationQuery(graphTaskIds, filters, apiParameters))
                    ).thenCompose(applicationGraph -> {
                        response.putHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
                        response.putHeader("Content-Type", exportSpecification.mimeType());
                        response.setChunked(true);

                        LOG.info("Start export " + exportSpecification.exportLabel());
                        try {
                            return exportSpecification.process(fileName, host, applicationGraph, output, materializer, apiParameters);
                        } catch (Exception e) {
                            failChunkedResponse(response);
                            Throwables.throwIfUnchecked(e);
                            throw new RuntimeException(e);
                        }
                    });
            })
            .whenComplete(endResponseAndOutputStream(context, exportSpecification, response, output));
    }

    private <Q extends Query, A> void handleAnalysisFraction(
        RoutingContext context,
        ExportSpecification<Q, A> exportSpecification,
        User user,
        UUID projectId,
        UUID analysisId,
        UUID sampleId,
        UUID fractionId
    ) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();
        final String host = request.host();
        response.exceptionHandler(e -> {
            LOG.error("Response exception", e);
            response.close();
        });

        final OutputStream output = new BufferedOutputStream(new VertxChunkedOutputStream(response));
        projectService.project(projectId, user).thenCompose(project -> {
            final ImmutableMap<String, String> apiParameters = ImmutableMap.copyOf(request.params());
            final WorkFlowType workFlowType = exportSpecification.filterType(apiParameters);
            return projectService.analysisFractionGraphTaskIds(projectId, analysisId, sampleId, fractionId, workFlowType, user)
                .thenCompose(graphTaskIds -> {
                    if (!workFlowType.equals(WorkFlowType.DATA_REFINEMENT)) {
                        return exportSpecification.readFilters(apiParameters)
                            .map(CompletableFuture::completedFuture)
                            .orElse(projectService.analysisFiltersByWorkflowType(projectId, analysisId, user, workFlowType).toCompletableFuture())
                            .thenApply(filters -> applicationGraphFactory.createFromIdentificationQuery(graphTaskIds, filters, apiParameters));
                    } else {
                        return CompletableFuture.supplyAsync(() ->
                            applicationGraphFactory.createFromIdentificationQuery(graphTaskIds, new FilterFunction[0], apiParameters));
                    }
                })
                .thenCompose(applicationGraph -> {
                    final String fileName = applicationGraph.fractionSrcFile(fractionId);
                    if (fileName == null) {
                        throw new IllegalArgumentException("Unable to find the sample and fraction info for " + fractionId);
                    }

                    response.putHeader("Content-Disposition", MessageFormat.format("attachment; filename=\"{0}.{1}\"", fileName, exportSpecification.name()));
                    response.putHeader("Content-Type", exportSpecification.mimeType());
                    response.setChunked(true);

                    LOG.info("Start export " + exportSpecification.exportLabel());
                    try {
                        return exportSpecification.process(fileName, host, applicationGraph, output, materializer, apiParameters);
                    } catch (Exception e) {
                        failChunkedResponse(response);
                        Throwables.throwIfUnchecked(e);
                        throw new RuntimeException(e);
                    }
                });
            })
            .whenComplete(endResponseAndOutputStream(context, exportSpecification, response, output));
    }

    @NotNull
    private BiConsumer<Done, Throwable> endResponseAndOutputStream(RoutingContext context, ExportSpecification exportSpecification, HttpServerResponse response, OutputStream output) {
        return (done, error) -> {
            closeOutputStream(output);
            if (error != null) {
                LOG.error("Unable to process export " + exportSpecification.exportLabel(), error);
                context.fail(error);
            } else {
                response.end();
            }
        };
    }

    public static void closeOutputStream(OutputStream output) {
        try {
            output.flush();
            output.close();
        } catch (IOException e) {
            LOG.error("Exception during close of output stream", e);
        }
    }

    @Override
    public ApplicationGraphFactory applicationGraphFactory() {
        return applicationGraphFactory;
    }
}
