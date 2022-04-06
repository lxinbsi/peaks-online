package com.bsi.peaks.server.handlers;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.japi.Pair;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.dto.peptide.Spectrum;
import com.bsi.peaks.model.dto.peptide.TICPoint;
import com.bsi.peaks.server.handlers.helper.ApplicationGraphFactory;
import com.bsi.peaks.server.handlers.helper.SampleSpecifications;
import com.bsi.peaks.server.handlers.specifications.DtoSourceFactory;
import com.bsi.peaks.server.service.ProjectService;
import com.google.inject.Inject;
import com.google.protobuf.GeneratedMessageV3;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class SampleProtoHandler extends ProtoHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SampleProtoHandler.class);

    private final ApplicationGraphFactory applicationGraphFactory;
    protected final ProjectService projectService;
    protected final DtoSourceFactory dtoSourceFactory;

    @Inject
    public SampleProtoHandler(
        final Vertx vertx,
        final ActorSystem system,
        final ProjectService projectService,
        final ApplicationStorageFactory storageFactory
    ) {
        this(
            vertx,
            system,
            projectService,
            new ApplicationGraphFactory(storageFactory),
            new DtoSourceFactory()
        );
    }

    protected SampleProtoHandler(
        final Vertx vertx,
        final ActorSystem system,
        final ProjectService projectService,
        final ApplicationGraphFactory applicationGraphFactory,
        final DtoSourceFactory dtoSourceFactory
    ) {
        super(vertx, system);
        this.applicationGraphFactory = applicationGraphFactory;
        this.dtoSourceFactory = dtoSourceFactory;
        this.projectService = projectService;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);

        subRouter.get("/tic/:pid/:sid/:fid").handler(handler(this.ticCurve()));
        subRouter.get("/ms1/:pid/:sid/:fid/:frameId").handler(handler(this.ms1Spectrum()));
        subRouter.get("/ms2/:pid/:sid/:fid").handler(handler(this.ms2SpectrumByList()));

        return subRouter;
    }

    private <T extends GeneratedMessageV3> Handler<RoutingContext> handler(SampleSpecifications<T> sampleSpecifications) {
        return (RoutingContext context) -> auth(context, user -> {
            final HttpServerRequest request = context.request();
            final HttpServerResponse response = context.response();
            response.exceptionHandler(e -> {
                LOG.error("Response exception", e);
                response.close();
            });

            final UUID projectId;
            final UUID sampleId;
            try {
                projectId = UUID.fromString(request.getParam("pid"));
                sampleId = UUID.fromString(request.getParam("sid"));
            } catch (Exception e) {
                LOG.warn("Unable to parse ids", e);
                badRequest(context, "Unable to parse ids");
                return;
            }

            projectService.project(projectId, user)
                .thenCombine(projectService.sampleGraphTaskIds(projectId, sampleId, user), Pair::create)
                .thenApply(p -> Pair.create(p.first(), applicationGraphFactory.createFromSampleOnly(p.second())))
                .thenCompose(p -> {
                        Source<T, NotUsed> source = sampleSpecifications.getSource(p.second(), request);
                        return serializationProcedure(request, response, p.first(), source, sampleSpecifications.getExtensionName());
                    }
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

    protected <T extends GeneratedMessageV3> CompletionStage<Done> serializationProcedure(
        HttpServerRequest request,
        HttpServerResponse response,
        Project project,
        Source<T, NotUsed> source,
        String extensionName
    ){
        return serializationProcedure(request, response, source, project, extensionName, false, Futures.failedCompletionStage(new HandlerException(400, "CSV not supported")));
    }

    private SampleSpecifications<TICPoint> ticCurve() {
        return SampleSpecifications.create(
            (applicationGraph, request) -> {
                UUID sampleId = UUID.fromString(request.getParam("sid"));
                UUID fractionId = UUID.fromString(request.getParam("fid"));
                return dtoSourceFactory.db(applicationGraph).sampleTICCurve(sampleId, fractionId);
            },
            "ticCurve"
        );
    }

    private SampleSpecifications<Spectrum> ms1Spectrum() {
        return SampleSpecifications.create(
            (applicationGraph, request) -> {
                UUID sampleId = UUID.fromString(request.getParam("sid"));
                UUID fractionId = UUID.fromString(request.getParam("fid"));
                int frameId = Integer.parseInt(request.getParam("frameId"));
                return Source.fromCompletionStage(dtoSourceFactory.db(applicationGraph).ms1SpectrumForFrameId(sampleId, fractionId, frameId)
                    .thenApply(optional -> optional.orElseThrow(() -> new HandlerException(404, "FrameId does not exist")))
                );
            },
            "ms1Spectrum"
        );
    }

    private SampleSpecifications<Spectrum> ms2SpectrumByList() {
        return SampleSpecifications.create(
            (applicationGraph, request) -> {
                UUID sampleId = UUID.fromString(request.getParam("sid"));
                UUID fractionId = UUID.fromString(request.getParam("fid"));
                final String strMs2ScanNums = request.getParam("ms2ScanNums");

                if (strMs2ScanNums == null || strMs2ScanNums.isEmpty()) {
                    LOG.warn("Attempting to get ms2 spectrum without specifying any scan numbers.");
                    return Source.empty();
                }

                final List<Integer> ms2ScanNums;
                try {
                    ms2ScanNums = OM.readValue(strMs2ScanNums, OM.getTypeFactory().constructCollectionType(List.class, Integer.class));
                } catch (IOException e) {
                    throw new HandlerException(400, "Unable to parse scan numbers from query");
                }
                return dtoSourceFactory.db(applicationGraph).ms2SpectrumForScanNums(sampleId, fractionId, ms2ScanNums);
            },
            "ms2Spectrum"
        );
    }
}
