package com.bsi.peaks.server.handlers;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.function.Function2;
import akka.pattern.BackoffSupervisor;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.graph.AnalysisInfoApplicationGraph;
import com.bsi.peaks.data.graph.GraphTaskIds;
import com.bsi.peaks.data.graph.LfqApplicationGraph;
import com.bsi.peaks.io.writer.dto.Csv;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.server.handlers.helper.AckingReceiver;
import com.bsi.peaks.server.handlers.helper.ApplicationGraphFactory;
import com.bsi.peaks.server.handlers.specifications.AnalysisSpecificationsCreator;
import com.bsi.peaks.server.handlers.specifications.CsvProtoHandlerSpecification;
import com.bsi.peaks.server.handlers.specifications.DtoSourceFactory;
import com.bsi.peaks.server.handlers.specifications.ProtoHandlerSpecification;
import com.bsi.peaks.server.service.ProjectService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.GeneratedMessageV3;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AnalysisProtoHandler extends ProtoHandler implements AnalysisSpecificationsCreator {
    public static final int WEB_SOCKET_GROUP_SIZE = 500;
    public static final BodyHandler BODY_HANDLER = BodyHandler.create();
    private static final Logger LOG = LoggerFactory.getLogger(AnalysisProtoHandler.class);
    private final ApplicationGraphFactory applicationGraphFactory;
    protected final ProjectService projectService;
    protected final DtoSourceFactory dtoSourceFactory;

    @Inject
    public AnalysisProtoHandler(
        final Vertx vertx,
        final ActorSystem system,
        final ProjectService projectService,
        final ApplicationGraphFactory applicationGraphFactory
    ) {
        this(
            vertx,
            system,
            projectService,
            applicationGraphFactory,
            new DtoSourceFactory()
        );
    }

    protected AnalysisProtoHandler(
        final Vertx vertx,
        final ActorSystem system,
        final ProjectService projectService,
        final ApplicationGraphFactory applicationGraphFactory,
        final DtoSourceFactory dtoSourceFactory
    ) {
        super(vertx, system);
        this.applicationGraphFactory = applicationGraphFactory;
        this.projectService = projectService;
        this.dtoSourceFactory = dtoSourceFactory;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);

        route(subRouter, "/protein/:pid/:aid", proteinHandler());
        route(subRouter, "/all-protein/:pid/:aid", allProteinHandler());
        route(subRouter, "/protein/:pid/:aid/:protein", proteinHandler());

        route(subRouter, "/peptide/:pid/:aid", peptideHandler());
        route(subRouter, "/peptide/:pid/:aid/:peptide", peptideHandler());
        route(subRouter, "/peptide/:pid/:aid/:peptide/menu", peptideMenuHandler());
        route(subRouter, "/db/ptmProfile/:pid/:aid", ptmProfileHandler());

        route(subRouter, "/protein-peptide/:pid/:aid", proteinPeptideHandler());
        route(subRouter, "/protein-peptide/:pid/:aid/:protein", proteinPeptideHandler());
        route(subRouter, "/sample-peptide-frequency/:pid/:aid/:protein", samplePeptideHandler());

        route(subRouter, "/peptide-ionMatch/:pid/:aid/:peptide", peptideIonMatchHandler());
        route(subRouter, "/psm-ionMatch/:pid/:aid/:fractionId/:psmId", psmIonMatchHandler());
        route(subRouter, "/denovo-ionMatch/:pid/:aid/:fractionId/:scannum", denovoIonMatchHandler());

        route(subRouter, "/denovo/:pid/:aid", denovoHandler());
        route(subRouter, "/denovo-all/:pid/:aid", denovoAllHandler());
        route(subRouter, "/denovo-summary/:pid/:aid", denovoSummaryHandler());
        route(subRouter, "/denovo-only/:pid/:aid", denovoOnlyHandler());
        route(subRouter, "/denovo-only-all/:pid/:aid", denovoOnlyAllHandler());
        route(subRouter, "/denovo-only/:pid/:aid/menu", denovoOnlyMenuHandler());
        route(subRouter, "/filtered-statistics/:pid/:aid", statisticsOfFilteredResult());

        route(subRouter, "/sampleScanStats/:pid/:aid", sampleScanStats());

        subRouter.get("/simple-protein/:pid/:aid").handler(this::simpleProteinsHandler);

        route(subRouter, "/lfq/feature/:pid/:aid", lfqFeatureHandler());
        route(subRouter, "/lfq/missingId/:pid/:aid", lfqMissingIds());
        route(subRouter, "/lfq/protein/:pid/:aid", lfqProteinHandler());
        route(subRouter, "/lfq/protein-peptide/:pid/:aid", lfqProteinPeptidesHandler());
        route(subRouter, "/lfq/protein-peptide/:pid/:aid/:protein", lfqProteinPeptidesHandler());
        route(subRouter, "/lfq/peptide/:pid/:aid", lfqPeptideHandler());
        route(subRouter, "/lfq/protein-fold-change/:pid/:aid", lfqProteinFoldChange());
        route(subRouter, "/lfq/filteredStatistics/:pid/:aid", lfqStatisticsOfFilteredResult());
        route(subRouter, "/lfq/psm-ionMatch/:pid/:aid/:fractionId/:psmId", lfqPsmIonMatchHandler());
        route(subRouter, "/lfq/peptide-feature/:pid/:aid/menu/:peptide", lfqFeatureVectorMenuHandler());
        route(subRouter, "/lfq/peptide-feature/:pid/:aid/:featureId", lfqPeptideFeatureHandler());
        route(subRouter, "/lfq/normalization-factors/:pid/:aid", lfqNormalizationFactorHandler());

        route(subRouter, "/riq/peptide/:pid/:aid", riqPeptideHandler());
        route(subRouter, "/riq/psm/:pid/:aid", riqPsmHandler());
        route(subRouter, "/riq/sample-peptide-frequency/:pid/:aid/:protein", riqSamplePeptideHandler());
        route(subRouter, "/riq/psm-ionMatch/:pid/:aid/:fractionId/:psmId", riqPsmIonMatchHandler());
        route(subRouter, "/riq/:pid/:aid/:peptide/menu", riqPeptideMenuHandler());
        route(subRouter, "/riq/protein/:pid/:aid", riqProteinHandler());
        route(subRouter, "/riq/protein-peptide/:pid/:aid", riqProteinPeptidesHandler());
        route(subRouter, "/riq/protein-peptide/:pid/:aid/:protein", riqProteinPeptidesHandler());
        route(subRouter, "/riq/protein-fold-change/:pid/:aid", riqProteinFoldChange());
        route(subRouter, "/riq/summary-statistics/:pid/:aid", riqSummaryStatistics());
        route(subRouter, "/riq/rawScan/:pid/:aid/:fid/:scannum", riqRawScanHandler());
        route(subRouter, "/riq/scan-ion-results/:pid/:aid/:fid/:scannum", riqScanReporterIonQResultsHandler());
        route(subRouter, "/riq/normalization-factors/:pid/:aid", riqNormalizationFactorHandler());
        route(subRouter, "/riq/filter/:pid/:aid", riqFilterHandler());

        route(subRouter, "/sl/psm/:pid/:aid", slPsmHandler());
        route(subRouter, "/sl/peptide/:pid/:aid", slPeptideHandler());
        route(subRouter, "/sl/protein/:pid/:aid", slProteinHandler());
        route(subRouter, "/sl/peptideDisplay/:pid/:aid/:peptide/:fractionId/:psmId", slPeptideDisplay());
        route(subRouter, "/sl/peptide/:pid/:aid/:peptide/menu", slPeptideMenu());
        route(subRouter, "/sl/summary/:pid/:aid", slSummary());
        route(subRouter, "/sl/sampleScanStats/:pid/:aid", slSampleScanStats());
        route(subRouter, "/sl/protein-peptide/:pid/:aid", slProteinPeptideHandler());
        route(subRouter, "/sl/protein-peptide/:pid/:aid/:protein", slProteinPeptideHandler());
        route(subRouter, "/sl/sample-peptide-frequency/:pid/:aid/:protein", slSamplePeptideHandler());
        route(subRouter, "/sl/on-the-fly-peptide/:pid/:aid", slDiaOnTheFlyHandler());
        route(subRouter, "/sl/timsScansManualPeptide/:pid/:aid/:ms2FrameId", timsCcsDiaPeptideManualInput());
        route(subRouter, "/sl/timsScanPeptideDisplay/:pid/:aid/:peptide/:fractionId/:psmId/:ms2FrameId", timsScanPeptideDisplayHandler());
        route(subRouter, "/lfq/sl/peptideDisplay/:pid/:aid/:featureId", lfqSlPeptideFeatureDisplayHandler());
        route(subRouter, "/lfq/sl/timsScanPeptideDisplay/:pid/:aid/:featureId/:ms2FrameId", lfqSlTimsScanPeptideFeatureDisplayHandler());
        route(subRouter, "/lfq/peptideFvDisplay/:pid/:aid/:featureVectorId", lfqSlPeptideFvDisplay());

        route(subRouter, "/silac/filteredStatistics/:pid/:aid", silacStatisticsOfFilteredResult());
        route(subRouter, "/silac/protein/:pid/:aid", silacProteinHandler());
        route(subRouter, "/silac/protein-peptide/:pid/:aid", silacProteinPeptidesHandler());
        route(subRouter, "/silac/protein-peptide/:pid/:aid/:protein", silacProteinPeptidesHandler());
        route(subRouter, "/silac/peptide/:pid/:aid", silacPeptideHandler());
        route(subRouter, "/silac/peptide-feature/:pid/:aid/:fractionId/:peptide/:featureVectorId", silacPeptideDisplay());
        route(subRouter, "/silac/peptide-feature/:pid/:aid/menu/:peptide", silacPeptideMenu());
        route(subRouter, "/silac/psm-ionMatch/:pid/:aid/:fractionId/:psmId", silacPsmIonMatchHandler());
        route(subRouter, "/silac/feature/:pid/:aid/:sample", silacFeatureVectorHandler());

        route(subRouter, "/diaDenovo/peptideDisplay/:pid/:aid/:fractionId/:scanNum/:candidateId/:featureId", diaDenovoPeptideDisplay());
        route(subRouter, "/diaDenovo/timsScanPeptideDisplay/:pid/:aid/:fractionId/:scanNum/:candidateId/:featureId/:ms2FrameId", diaDenovoTimsScanPeptideDisplay());
        return subRouter;
    }

    private void route(Router router, String path, ProtoHandlerSpecification spec) {
        router.get(path).handler(getHandler(spec));
        router.post(path).handler(BODY_HANDLER).handler(postHandler(spec));
    }

    @Override
    public ApplicationGraphFactory applicationGraphFactory() {
        return applicationGraphFactory;
    }

    @Override
    public DtoSourceFactory dtoSourceFactory() {
        return dtoSourceFactory;
    }

    @Override
    public ObjectMapper om() {
        return ApiHandler.OM;
    }

    private <T extends GeneratedMessageV3> void respond(
        RoutingContext context,
        String extensionName,
        CompletionStage<? extends Csv<T>> csvHandler,
        Function2<HttpServerRequest, LfqApplicationGraph, Source<T, NotUsed>> proc
    ) {
        auth(context, user -> {

            final HttpServerRequest request = context.request();
            final HttpServerResponse response = context.response();
            response.exceptionHandler(e -> {
                LOG.error("Response exception", e);
                response.close();
            });

            getId(context, "pid", projectId ->
                getId(context, "aid", analysisId ->
                    projectService.project(projectId, user).thenCombine(
                        projectService.analysisGraphTaskIdsLastWorkflowType(projectId, analysisId, user)
                            .thenApply(graphTaskIds -> applicationGraphFactory.lfqNoFilters(graphTaskIds)),
                        (project, applicationGraph) -> {
                            try {
                                final String name = project.getName() + "." + extensionName;
                                final Source<T, NotUsed> source = proc.apply(request, applicationGraph);
                                respond(context, name, csvHandler, source);
                            } catch (Exception e) {
                                throw Throwables.propagate(e);
                            }
                            return Done.getInstance();
                        }
                    )
                )
            );
        });
    }

    private <T extends GeneratedMessageV3> void respond(
        RoutingContext context,
        String name,
        CompletionStage<? extends Csv<T>> csvHandler,
        Source<T, NotUsed> source
    ) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        if ("websocket".equalsIgnoreCase(request.getHeader("Upgrade"))) {
            final ServerWebSocket webSocket;
            try {
                webSocket = request.upgrade();
            } catch (Exception e) {
                LOG.error("Error during WebSockect upgrade", e);
                badRequest(context, e.getMessage());
                return;
            }
            respondWebSocket(webSocket, source, materializerWithErrorLogging(request));
        } else {
            Sink<T, CompletionStage<Done>> sink = null;
            for (String accept : request.headers().getAll("Accept")) {
                if (accept.contains("protobuf")) {
                    sink = ProtoHandler.protobuf(name, response);
                    break;
                } else if (accept.contains("json")) {
                    sink = ProtoHandler.json(name, response);
                    break;
                } else if (csvHandler != null && accept.contains("csv")) {
                    sink = csvHandler
                        .thenApply(csv -> csv(name, csv, response))
                        .toCompletableFuture()
                        .join();
                    break;
                }
            }
            if (sink == null) {
                if (csvHandler == null) {
                    sink = ProtoHandler.protobuf(name, response);
                } else {
                    sink = csvHandler
                        .thenApply(csv -> csv(name, csv, response))
                        .toCompletableFuture()
                        .join();
                }
            }
            source
                .runWith(sink, materializerWithErrorLogging(request))
                .whenComplete((done, error) -> {
                    if (error != null) {
                        handleError(context, error);
                    } else {
                        response.end();
                    }
                });
        }
    }

    private void simpleProteinsHandler(RoutingContext context) {
        respond(
            context,
            ".simpleProteinList",
            null,
            (request, applicationGraph) -> dtoSourceFactory().lfq(applicationGraph).simpleProteinSource()
        );
    }

    private <T extends GeneratedMessageV3, Q extends Query, A extends AnalysisInfoApplicationGraph> Handler<RoutingContext> postHandler(
        ProtoHandlerSpecification<T, Q, A> spec
    ) {
        return (RoutingContext context) -> auth(context, user -> {

            final HttpServerRequest request = context.request();
            final HttpServerResponse response = context.response();
            response.exceptionHandler(e -> {
                LOG.error("Response exception", e);
                response.close();
            });

            final UUID projectId;
            final UUID analysisId;
            final Map<String, String> apiParameters;
            final Optional<WorkFlowType> workFlowType;

            final JsonObject body;
            try {
                projectId = UUID.fromString(request.getParam("pid"));
                analysisId = UUID.fromString(request.getParam("aid"));
                body = context.getBodyAsJson();
                if (body == null) {
                    badRequest(context, "POST with no body");
                    return;
                }

                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                request.params().forEach(builder::put);
                builder.put("filter", body.toString());
                apiParameters = builder.build();
                workFlowType = spec.getWorkFlowType(apiParameters);
            } catch (Exception e) {
                LOG.warn("Unable to parse query", e);
                badRequest(context, "Unable to parse query");
                return;
            }

            projectService.project(projectId, user)
                .thenCombine(projectService.analysisGraphTaskIds(projectId, analysisId, user, spec.graphTaskIdsQuery(apiParameters, workFlowType)), Pair::create)
                .thenCombine(projectService.analysisQuery(projectId, analysisId, user, workFlowType), Pair::create)
                .thenCompose(p -> {
                    final Project project = p.first().first();
                    GraphTaskIds graphTaskIds = p.first().second();
                    final Q queryParameters = (Q) p.second();

                    return serializationProcedure(spec, request, response, queryParameters, project, graphTaskIds);
                })
                .whenComplete((done, error) -> {
                    if (error != null) {
                        handleError(context, error);
                    } else {
                        response.end();
                    }
                });
        });
    }

    private <T extends GeneratedMessageV3, Q extends Query, A extends AnalysisInfoApplicationGraph> Handler<RoutingContext> getHandler(
        ProtoHandlerSpecification<T, Q, A> spec
    ) {
        return (RoutingContext context) -> auth(context, user -> {

            final HttpServerRequest request = context.request();
            final HttpServerResponse response = context.response();
            final UUID projectId;
            final UUID analysisId;
            try {
                projectId = UUID.fromString(request.getParam("pid"));
                analysisId = UUID.fromString(request.getParam("aid"));
            } catch (Exception e) {
                LOG.warn("Unable to parse UUIDs", e);
                badRequest(context, "Unable to parse UUID");
                return;
            }

            final Map<String, String> apiParameters;
            final Optional<WorkFlowType> workFlowType;
            try {
                apiParameters = ImmutableMap.copyOf(request.params());
                workFlowType = spec.getWorkFlowType(apiParameters);
            } catch (Exception e) {
                LOG.warn("Unable to generate parameters from request", e);
                badRequest(context, "Unable to generate parameters from request");
                return;
            }

            if ("websocket".equalsIgnoreCase(request.getHeader("Upgrade"))) {
                final ServerWebSocket webSocket;
                try {
                    webSocket = request.upgrade().pause();
                } catch (Exception e) {
                    LOG.error("Error during WebSockect upgrade", e);
                    badRequest(context, e.getMessage());
                    return;
                }

                final CompletableFuture<JsonObject> futureQuery = new CompletableFuture<>();
                final CompletableFuture<Done> futureDone = new CompletableFuture<>();
                final ActorRef receiver = system.actorOf(BackoffSupervisor.props(
                    AckingReceiver.props(webSocket, futureDone),
                    AckingReceiver.NAME,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(10),
                    0.2
                ));

                webSocket.frameHandler(event -> {
                    if (!event.isText()) {
                        futureQuery.completeExceptionally(WebSocketExcpetion.protocolError("Expected JSON message"));
                        return;
                    }

                    final String queryString = event.textData();
                    if (queryString.equals(AckingReceiver.CONTINUE)) {
                        receiver.tell(AckingReceiver.CONTINUE, null);
                    } else if (!futureQuery.isDone()) {
                        try {
                            JsonObject json = new JsonObject(event.textData());
                            futureQuery.complete(json);
                        } catch (Exception e) {
                            futureQuery.completeExceptionally(WebSocketExcpetion.protocolError("Expected JSON message", e));
                        }
                    } else {
                        LOG.warn("More than one message received, {}", queryString);
                    }
                });

                webSocket.resume();

                final CompletionStage<Source<T, NotUsed>> futureSource = projectService.analysisGraphTaskIds(projectId, analysisId, user, spec.graphTaskIdsQuery(apiParameters, workFlowType))
                    .thenCombine(projectService.analysisQuery(projectId, analysisId, user, workFlowType), Pair::create)
                    .thenApply(p -> {
                        GraphTaskIds graphTaskIds = p.first();
                        Q query = (Q) p.second();
                        final A applicationGraph = spec.applicationGraph(graphTaskIds, query, apiParameters);
                        return spec.source(applicationGraph, query, apiParameters);
                    });

                final Source<T, NotUsed> source = Source
                    .fromSourceCompletionStage(futureSource)
                    .mapMaterializedValue(ignore -> NotUsed.getInstance());

                respondWebSocketBackPressure(webSocket, source, materializerWithErrorLogging(request), system, receiver, futureDone);
            } else {
                response.exceptionHandler(e -> {
                    LOG.error("Response exception", e);
                    response.close();
                });

                projectService.project(projectId, user)
                    .thenCombine(projectService.analysisGraphTaskIds(projectId, analysisId, user, spec.graphTaskIdsQuery(apiParameters, workFlowType)), Pair::create)
                    .thenCombine(projectService.analysisQuery(projectId, analysisId, user, workFlowType), Pair::create)
                    .thenCompose(p -> {
                        Project project = p.first().first();
                        GraphTaskIds graphTaskIds = p.first().second();
                        Q query = (Q) p.second();

                        return serializationProcedure(spec, request, response, query, project, graphTaskIds);
                    })
                    .whenComplete((done, error) -> {
                        if (error != null) {
                            handleError(context, error);
                        } else {
                            response.end();
                        }
                    });
            }
        });
    }

    private <T extends GeneratedMessageV3, Q extends Query, A extends AnalysisInfoApplicationGraph> CompletionStage<Done> serializationProcedure(
        ProtoHandlerSpecification<T, Q, A> spec,
        HttpServerRequest request,
        HttpServerResponse response,
        Q queryParameters,
        Project project,
        GraphTaskIds graphTaskIds
    ) {
        ImmutableMap<String, String> apiParameters = ImmutableMap.copyOf(request.params());
        final A applicationGraph = spec.applicationGraph(graphTaskIds, queryParameters, apiParameters);
        final Source<T, NotUsed> source = spec.source(applicationGraph, queryParameters, apiParameters);
        final boolean supportCsv;
        final CompletionStage<? extends Csv<T>> csvHandler;
        if (spec instanceof CsvProtoHandlerSpecification) {
            csvHandler = ((CsvProtoHandlerSpecification) spec).csv(applicationGraph, queryParameters, apiParameters);
            supportCsv = true;
        } else {
            csvHandler = null;
            supportCsv = false;
        }

        return serializationProcedure(
            request,
            response,
            source,
            project,
            spec.extensionName(applicationGraph, queryParameters, apiParameters),
            supportCsv,
            csvHandler
        );
    }
}
