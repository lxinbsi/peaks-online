package com.bsi.peaks.server.handlers;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.tuple.Tuple4;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import com.bsi.peaks.analysis.analysis.dto.Analysis;
import com.bsi.peaks.analysis.exoports.dto.Export;
import com.bsi.peaks.analysis.exoports.dto.ExportType;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.common.iterable.Immutables;
import com.bsi.peaks.data.graph.AnalysisInfoApplicationGraph;
import com.bsi.peaks.data.graph.DbApplicationGraph;
import com.bsi.peaks.data.graph.GraphTaskIds;
import com.bsi.peaks.data.graph.SampleFractions;
import com.bsi.peaks.io.writer.csv.ExportableCsv;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.filter.DenovoCandidateFilter;
import com.bsi.peaks.model.filter.FilterFunction;
import com.bsi.peaks.model.query.IdentificationQuery;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.handlers.helper.ApplicationGraphFactory;
import com.bsi.peaks.server.handlers.helper.VertxChunkedOutputStream;
import com.bsi.peaks.server.handlers.specifications.AnalysisSpecificationsCreator;
import com.bsi.peaks.server.handlers.specifications.CsvProtoHandlerSpecification;
import com.bsi.peaks.server.handlers.specifications.DtoSourceFactory;
import com.bsi.peaks.server.handlers.specifications.ExportSpecification;
import com.bsi.peaks.server.handlers.specifications.ExportSpecificationsCreator;
import com.bsi.peaks.server.handlers.specifications.TxtSpecification;
import com.bsi.peaks.server.handlers.specifications.TxtSpecificationCreator;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.server.service.SpectralLibraryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.util.JsonFormat;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.bsi.peaks.server.handlers.helper.ApplicationGraphFactory.SAMPLE_ID_API_PARAMETERS;

public class ZipExportHandler extends ProtoHandler
    implements AnalysisSpecificationsCreator, ExportSpecificationsCreator, TxtSpecificationCreator
{
    public static final BodyHandler BODY_HANDLER = BodyHandler.create();
    private static final Logger LOG = LoggerFactory.getLogger(ZipExportHandler.class);
    private final ApplicationGraphFactory applicationGraphFactory;
    protected final ProjectService projectService;
    protected final FastaDatabaseService fastaDatabaseService;
    protected final SpectralLibraryService spectralLibraryService;
    protected final DtoSourceFactory dtoSourceFactory;

    @Inject
    public ZipExportHandler(
        final Vertx vertx,
        final ActorSystem system,
        final ProjectService projectService,
        final ApplicationGraphFactory applicationGraphFactory,
        final FastaDatabaseService fastaDatabaseService,
        final SpectralLibraryService spectralLibraryService
    ) {
        this(
            vertx,
            system,
            projectService,
            applicationGraphFactory,
            new DtoSourceFactory(),
            fastaDatabaseService,
            spectralLibraryService
        );

    }

    public ZipExportHandler(
        Vertx vertx,
        ActorSystem system,
        final ProjectService projectService,
        final ApplicationGraphFactory applicationGraphFactory,
        final DtoSourceFactory dtoSourceFactory,
        final FastaDatabaseService fastaDatabaseService,
        final SpectralLibraryService spectralLibraryService
    ) {
        super(vertx, system);
        this.applicationGraphFactory = applicationGraphFactory;
        this.projectService = projectService;
        this.fastaDatabaseService = fastaDatabaseService;
        this.spectralLibraryService = spectralLibraryService;
        this.dtoSourceFactory = dtoSourceFactory;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.post("/download/:pid/:aid").handler(BODY_HANDLER).handler(zipPostHandler(this::postExportExtractor));
        subRouter.get("/download/:pid/:aid").handler(BODY_HANDLER).handler(zipPostHandler(this::getExportExtractor));
        return subRouter;
    }

    private ExportSpecification exportSpecLookup(ExportType exportType) {
        switch (exportType) {
            case SL_PROTEIN_FASTA_CSV:
                return slProteinFastaSpecification();
            case PEP_XML:
                return dbPepXmlSpecifications();
            case MZ_IDENT_ML:
                return dbMzIdentMLSpecifications();
            case PROTEIN_FASTA:
                return proteinFastaSpecification();
            case REFINED_MGF:
                return downloadMs2RefinedMGFSpecification();
            case MZXML:
                return dbMzxmlSpecifications();
            case ALL_PARAMETERS_TXT:
            case DENOVO_CSV:
            case DENOVO_WITH_ALL_CANDIDATES_CSV:
            case DENOVO_ONLY_CSV:
            case DENOVO_ONLY_WITH_ALL_CANDIDATES_CSV:
            case PROTEIN_CSV:
            case PROTEIN_PEPTIDE_CSV:
            case PEPTIDE_CSV:
            case PSM_CSV:
            case PTM_PROFILE_CSV:
            case PROTEIN_FEATURE_VECTOR_CSV:
            case PROTEIN_PEPTIDE_FEATURE_VECTOR_CSV:
            case PEPTIDE_FEATURE_VECTOR_CSV:
            case PROTEIN_WITH_PEPTIDE_FEATURE_VECTOR_CSV:
            case FEATURE_VECTOR_CSV:
            case DENOVO_SUMMARY_CSV:
            case IDENTIFICATION_SUMMARY_CSV:
            case RIQ_PROTEIN_CSV:
            case RIQ_PROTEIN_PEPTIDE_CSV:
            case RIQ_PEPTIDE_CSV:
            case RIQ_PSM_CSV:
            case RIQ_PARAMETERS_CSV:
            case RIQ_NORMALIZATION_FACTOR_CSV:
            case SL_SUMMARY:
            case SL_PEPTIDE_CSV:
            case SL_PSM_CSV:
            case SL_PROTEIN_PEPTIDE_CSV:
            case SL_PROTEIN_CSV:
            case SILAC_SUMMARY:
            case SILAC_FEATURE_VECTOR_CSV:
            case SILAC_PEPTIDE_FEATURE_VECTOR_CSV:
            case SILAC_PROTEIN_FEATURE_VECTOR_CSV:
            case SILAC_PROTEIN_PEPTIDE_FEATURE_VECTOR_CSV:
                throw new IllegalArgumentException("No ExportSpecification for " + exportType);
            default:
                throw new IllegalArgumentException("Unexpected export type " + exportType);
        }
    }

    private CsvProtoHandlerSpecification protoSpecLookup(ExportType exportType) {
        switch (exportType) {
            case DENOVO_CSV:
                return denovoHandler();
            case DENOVO_WITH_ALL_CANDIDATES_CSV:
                return denovoAllHandler();
            case DENOVO_ONLY_CSV:
                return denovoOnlyHandler();
            case DENOVO_ONLY_WITH_ALL_CANDIDATES_CSV:
                return denovoOnlyAllHandler();
            case PROTEIN_CSV:
                return proteinHandler();
            case PROTEIN_PEPTIDE_CSV:
                return proteinPeptideHandler();
            case PEPTIDE_CSV:
            case PSM_CSV:
                return peptideHandler();
            case PTM_PROFILE_CSV:
                return ptmProfileHandler();
            case PROTEIN_FEATURE_VECTOR_CSV:
                return lfqProteinHandler();
            case PROTEIN_PEPTIDE_FEATURE_VECTOR_CSV:
                return lfqProteinPeptidesHandler();
            case PEPTIDE_FEATURE_VECTOR_CSV:
                return lfqPeptideHandler();
            case PROTEIN_WITH_PEPTIDE_FEATURE_VECTOR_CSV:
                return lfqProteinWithPeptidesHandler();
            case FEATURE_VECTOR_CSV:
                return lfqFeatureHandler();
            case DENOVO_SUMMARY_CSV:
                return denovoSummaryHandler();
            case IDENTIFICATION_SUMMARY_CSV:
                return statisticsOfFilteredResult();
            case RIQ_PROTEIN_CSV:
                return riqProteinHandler();
            case RIQ_PROTEIN_PEPTIDE_CSV:
                return riqProteinPeptidesHandler();
            case RIQ_PEPTIDE_CSV:
                return riqPeptideHandler();
            case RIQ_PSM_CSV:
                return riqPsmHandler();
            case RIQ_PARAMETERS_CSV:
                return riqFilterHandler();
            case RIQ_NORMALIZATION_FACTOR_CSV:
                return riqNormalizationFactorHandler();
            case SL_SUMMARY:
                return slSummary();
            case SL_PEPTIDE_CSV:
                return slPeptideHandler();
            case SL_PSM_CSV:
                return slPsmHandler();
            case SL_PROTEIN_PEPTIDE_CSV:
                return slProteinPeptideHandler();
            case SL_PROTEIN_CSV:
                return slProteinHandler();
            case SILAC_SUMMARY:
                return silacStatisticsOfFilteredResult();
            case SILAC_PROTEIN_FEATURE_VECTOR_CSV:
                return silacProteinHandler();
            case SILAC_PROTEIN_PEPTIDE_FEATURE_VECTOR_CSV:
                return silacProteinPeptidesHandler();
            case SILAC_PEPTIDE_FEATURE_VECTOR_CSV:
                return silacPeptideHandler();
            case SILAC_FEATURE_VECTOR_CSV:
                return silacFeatureVectorHandler();
            case SL_PROTEIN_FASTA_CSV:
            case ALL_PARAMETERS_TXT:
            case PEP_XML:
            case MZ_IDENT_ML:
            case PROTEIN_FASTA:
            case REFINED_MGF:
            case MZXML:
                throw new IllegalArgumentException("No ProtoHandlerSpecification for " + exportType);
            default:
                throw new IllegalArgumentException("Unexpected export type " + exportType);
        }
    }

    private List<Export> getExportExtractor(RoutingContext context, HttpServerRequest request) {
        final List<Export> exports = new ArrayList<>();
        try {
            String exportsParameters = request.getParam("exports");
            JsonArray objects = new JsonArray(exportsParameters);
            if (exportsParameters == null) {
                badRequest(context, "Get with out exports parameter");
                return Arrays.asList();
            }
            for(int i = 0; i < objects.size(); i++) {
                JsonObject jsonObject = objects.getJsonObject(i);
                Export.Builder builder = Export.newBuilder();
                JsonFormat.parser().merge(jsonObject.toString(), builder);
                exports.add(builder.build());
            }
        } catch (Exception e) {
            LOG.warn("Unable to parse query", e);
            badRequest(context, "Unable to parse query");
            return Arrays.asList();
        }
        return exports;
    }

    private List<Export> postExportExtractor(RoutingContext context, HttpServerRequest request) {
        final List<Export> exports = new ArrayList<>();
        try {
            final JsonArray body = context.getBodyAsJsonArray();
            if (body == null) {
                badRequest(context, "POST with no body");
                return Collections.emptyList();
            }

            for(int i = 0; i < body.size(); i++) {
                JsonObject jsonObject = body.getJsonObject(i);
                Export.Builder builder = Export.newBuilder();
                JsonFormat.parser().merge(jsonObject.toString(), builder);
                exports.add(builder.build());
            }
        } catch (Exception e) {
            LOG.warn("Unable to parse query", e);
            badRequest(context, "Unable to parse query");
            return Collections.emptyList();
        }
        return exports;
    }

    private Handler<RoutingContext> zipPostHandler(
        BiFunction<RoutingContext, HttpServerRequest, List<Export>> exportExtractor
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

            final List<Export> exports = exportExtractor.apply(context, request);
            try {
                projectId = UUID.fromString(request.getParam("pid"));
                analysisId = UUID.fromString(request.getParam("aid"));
            } catch (Exception e) {
                LOG.warn("Unable to parse query", e);
                badRequest(context, "Unable to parse query");
                return;
            }

            CompletionStage<Tuple4<Project, Analysis, Map<WorkFlowType, Query>, GraphTaskIds>> futureQueries = projectService.project(projectId, user)
                .thenCombine(projectService.analysis(projectId, analysisId, user), Pair::create)
                .thenCompose(projectAnalysis -> projectService.analysisQueriesAll(projectId, analysisId, user)
                    .thenCombine(projectService.analysisGraphTaskIdsLastWorkflowType(projectId, analysisId, user), Pair::create)
                    .thenApply(queryGraphTaskIds -> Tuple4.create(projectAnalysis.first(), projectAnalysis.second(), queryGraphTaskIds.first(), queryGraphTaskIds.second()))
                );

            final boolean seperateBySamples = Boolean.parseBoolean(request.params().get("seperateBySamples"));

            futureQueries.thenCompose(t -> {
                final Project project = t.t1();
                final Analysis analysis = t.t2();
                final Map<WorkFlowType, Query> workFlowTypeToQuery = t.t3();
                final GraphTaskIds genericTaskIds = t.t4();
                final ImmutableMap<String, String> params = ImmutableMap.copyOf(request.params());

                ImmutableList.Builder<BiFunction<ZipOutputStream, Materializer, CompletionStage<Done>>> serializeProcedures = ImmutableList.builder();
                for (Export export : exports) {
                    ExportType exportType = export.getExportType();
                    switch (exportType) {
                        case ALL_PARAMETERS_TXT: {
                            serializeProcedures.add(txtExportFactory(downloadAllParameters(), params, user));
                            continue;
                        }
                        case MZXML:
                        case REFINED_MGF: {
                            for (SampleFractions sample : genericTaskIds.samples()) {
                                UUID sampleId = sample.sampleId();
                                List<UUID> fractions = sample.fractionIds();
                                for (int i = 0; i < fractions.size(); i++) {
                                    UUID fractionId = fractions.get(i);
                                    final ExportSpecification spec = exportSpecLookup(exportType);
                                    ImmutableMap<String, String> map = ImmutableMap.<String, String>builder()
                                        .put("sampleId", sampleId.toString())
                                        .put("fractionId", fractionId.toString())
                                        .put("filename", sample.fractionSrcFile().get(i) + "." + spec.name())
                                        .build();
                                    ImmutableMap<String, String> updatedParmaters = Immutables.updateMap(params, map);
                                    CompletionStage<DbApplicationGraph> futureApplicationGraph =
                                        projectService.analysisFractionGraphTaskIds(projectId, analysisId, sampleId, fractionId, WorkFlowType.DATA_REFINEMENT, user)
                                            .thenApply(graphTaskIds -> applicationGraphFactory.createFromIdentificationQuery(graphTaskIds, new FilterFunction[0], updatedParmaters));
                                    FactoryInputs<DbApplicationGraph, IdentificationQuery> factoryInputs = new FactoryInputs<>(null, futureApplicationGraph, request, updatedParmaters, user);
                                    serializeProcedures.add(exportFactory(spec, factoryInputs));
                                }
                            }
                            continue;
                        }
                        case DENOVO_CSV:
                        case DENOVO_ONLY_CSV:
                        case DENOVO_WITH_ALL_CANDIDATES_CSV:
                        case DENOVO_ONLY_WITH_ALL_CANDIDATES_CSV: {
                            final CsvProtoHandlerSpecification spec = protoSpecLookup(exportType);
                            final boolean top = exportType == ExportType.DENOVO_CSV || exportType == ExportType.DENOVO_ONLY_CSV;
                            IdentificationQuery idQuery = (IdentificationQuery) workFlowTypeToQuery.get(WorkFlowType.DENOVO);
                            IdentificationQuery updatedIdQuery = Arrays.stream(idQuery.filters())
                                .filter(filter -> filter instanceof DenovoCandidateFilter)
                                .findFirst()
                                .map(filter -> idQuery.withDenovoCandidateFilter(((DenovoCandidateFilter) filter).withTop(top)))
                                .orElseThrow(IllegalStateException::new);

                            for (SampleFractions sample : genericTaskIds.samples()) {
                                ImmutableMap<String, String> extraParams = ImmutableMap.<String, String>builder()
                                    .put(SAMPLE_ID_API_PARAMETERS, sample.sampleId().toString())
                                    .put("search", export.getWorkFlowType().toString())
                                    .build();
                                Map<String, String> updatedParams = Immutables.updateMap(params, extraParams);
                                CompletionStage<Object> futureApplicationGraph =
                                    projectService.analysisGraphTaskIds(projectId, analysisId, user, spec.graphTaskIdsQuery(extraParams, Optional.of(export.getWorkFlowType())))
                                        .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, updatedIdQuery, updatedParams));
                                serializeProcedures.add(protoFactory(spec, new FactoryInputs(updatedIdQuery, futureApplicationGraph, request, updatedParams, user)));
                            }
                            continue;
                        }
                        case PROTEIN_PEPTIDE_CSV:
                        case PEPTIDE_CSV: {
                            ImmutableMap<String, String> extraParams = ImmutableMap.<String, String>builder() .put("topPsm", "1").put("search", workFlowTypeDisplay(export.getWorkFlowType())).build();
                            Map<String, String> updatedParams = Immutables.updateMap(params, extraParams);
                            final CsvProtoHandlerSpecification spec = protoSpecLookup(exportType);
                            final Query query = workflowToQuery(export.getWorkFlowType(), workFlowTypeToQuery);
                            if (seperateBySamples) {
                                for (SampleFractions sample : genericTaskIds.samples()) {
                                    UUID sampleId = sample.sampleId();
                                    ImmutableMap<String, String> map = ImmutableMap.<String, String>builder()
                                        .put(SAMPLE_ID_API_PARAMETERS, sampleId.toString())
                                        .build();
                                    ImmutableMap<String, String> updatedParmaters = Immutables.updateMap(updatedParams, map);
                                    CompletionStage<Object> futureApplicationGraph = projectService
                                        .analysisGraphTaskIds(projectId, analysisId, user, spec.graphTaskIdsQuery(updatedParmaters, Optional.of(export.getWorkFlowType())))
                                        .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, query, updatedParmaters));
                                    FactoryInputs factoryInputs = new FactoryInputs(query, futureApplicationGraph, request, updatedParmaters, user);
                                    serializeProcedures.add(protoFactory(spec, factoryInputs));
                                }
                            } else {
                                CompletionStage<Object> futureApplicationGraph = projectService
                                    .analysisGraphTaskIds(projectId, analysisId, export.getWorkFlowType(), user)
                                    .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, query, updatedParams));
                                serializeProcedures.add(protoFactory(spec, new FactoryInputs(query, futureApplicationGraph, request, updatedParams, user)));
                            }
                            continue;
                        }
                        case PSM_CSV:
                        case PROTEIN_CSV:
                        case SL_PEPTIDE_CSV:
                        case SL_PSM_CSV:
                        case SL_PROTEIN_PEPTIDE_CSV:
                        case SL_PROTEIN_CSV: {
                            Map<String, String> updatedParams = Immutables.updateMap(params,"search", workFlowTypeDisplay(export.getWorkFlowType()));
                            final CsvProtoHandlerSpecification spec = protoSpecLookup(exportType);
                            final Query query = workflowToQuery(export.getWorkFlowType(), workFlowTypeToQuery);
                            if (seperateBySamples) {
                                for (SampleFractions sample : genericTaskIds.samples()) {
                                    UUID sampleId = sample.sampleId();
                                    ImmutableMap<String, String> map = ImmutableMap.<String, String>builder()
                                        .put(SAMPLE_ID_API_PARAMETERS, sampleId.toString())
                                        .build();
                                    ImmutableMap<String, String> updatedParmaters = Immutables.updateMap(updatedParams, map);
                                    CompletionStage<Object> futureApplicationGraph = projectService
                                        .analysisGraphTaskIds(projectId, analysisId, user, spec.graphTaskIdsQuery(updatedParmaters, Optional.of(export.getWorkFlowType())))
                                        .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, query, updatedParmaters));
                                    FactoryInputs factoryInputs = new FactoryInputs(query, futureApplicationGraph, request, updatedParmaters, user);
                                    serializeProcedures.add(protoFactory(spec, factoryInputs));
                                }
                            } else {
                                CompletionStage<Object> futureApplicationGraph = projectService
                                    .analysisGraphTaskIds(projectId, analysisId, export.getWorkFlowType(), user)
                                    .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, query, updatedParams));
                                FactoryInputs factoryInputs = new FactoryInputs(query, futureApplicationGraph, request, updatedParams, user);
                                serializeProcedures.add(protoFactory(spec, factoryInputs));
                            }
                            continue;
                        }
                        case SILAC_FEATURE_VECTOR_CSV: {
                            final CsvProtoHandlerSpecification spec = protoSpecLookup(exportType);
                            final Query query = workflowToQuery(export.getWorkFlowType(), workFlowTypeToQuery);
                            Set<UUID> sampleIdsInSilac = analysis.getSilacStep().getSilacParameters().getGroupsList().stream()
                                .flatMap(group -> group.getSamplesList().stream())
                                .map(sample -> UUID.fromString(sample.getSampleId()))
                                .collect(Collectors.toSet());
                            for (SampleFractions sample : genericTaskIds.samples()) {
                                UUID sampleId = sample.sampleId();
                                if (sampleIdsInSilac.contains(sampleId)) {
                                    ImmutableMap<String, String> map = ImmutableMap.<String, String>builder()
                                        .put(SAMPLE_ID_API_PARAMETERS, sampleId.toString())
                                        .build();
                                    ImmutableMap<String, String> updatedParmaters = Immutables.updateMap(params, map);
                                    CompletionStage<Object> futureApplicationGraph = projectService
                                        .analysisGraphTaskIds(projectId, analysisId, export.getWorkFlowType(), user)
                                        .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, query, updatedParmaters));
                                    FactoryInputs factoryInputs = new FactoryInputs(query, futureApplicationGraph, request, updatedParmaters, user);
                                    serializeProcedures.add(protoFactory(spec, factoryInputs));
                                }
                            }
                            continue;
                        }
                        case SL_SUMMARY:{
                            Map<String, String> updatedParams = Immutables.updateMap(params,"search", workFlowTypeDisplay(export.getWorkFlowType()));
                            final CsvProtoHandlerSpecification spec = protoSpecLookup(exportType);
                            Query query = workflowToQuery(export.getWorkFlowType(), workFlowTypeToQuery);
                            CompletionStage<Object> futureApplicationGraph =
                                projectService.analysisGraphTaskIds(projectId, analysisId, user, spec.graphTaskIdsQuery(updatedParams, Optional.of(export.getWorkFlowType())))
                                    .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, query, updatedParams));
                            FactoryInputs factoryInputs = new FactoryInputs(query, futureApplicationGraph, request, updatedParams, user);
                            serializeProcedures.add(protoFactory(spec, factoryInputs));
                            continue;
                        }
                        case PROTEIN_FEATURE_VECTOR_CSV:
                        case PROTEIN_PEPTIDE_FEATURE_VECTOR_CSV:
                        case PEPTIDE_FEATURE_VECTOR_CSV:
                        case PROTEIN_WITH_PEPTIDE_FEATURE_VECTOR_CSV:
                        case FEATURE_VECTOR_CSV:
                        case DENOVO_SUMMARY_CSV:
                        case IDENTIFICATION_SUMMARY_CSV:
                        case PTM_PROFILE_CSV:
                        case RIQ_PROTEIN_CSV:
                        case RIQ_PROTEIN_PEPTIDE_CSV:
                        case RIQ_PEPTIDE_CSV:
                        case RIQ_PSM_CSV:
                        case RIQ_PARAMETERS_CSV:
                        case RIQ_NORMALIZATION_FACTOR_CSV:
                        case SILAC_SUMMARY:
                        case SILAC_PROTEIN_FEATURE_VECTOR_CSV:
                        case SILAC_PROTEIN_PEPTIDE_FEATURE_VECTOR_CSV:
                        case SILAC_PEPTIDE_FEATURE_VECTOR_CSV: {
                            final CsvProtoHandlerSpecification spec = protoSpecLookup(exportType);
                            Query query = workflowToQuery(export.getWorkFlowType(), workFlowTypeToQuery);
                            CompletionStage<Object> futureApplicationGraph =
                                projectService.analysisGraphTaskIds(projectId, analysisId, user, spec.graphTaskIdsQuery(params, Optional.of(export.getWorkFlowType())))
                                    .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, query, params));
                            FactoryInputs factoryInputs = new FactoryInputs(query, futureApplicationGraph, request, params, user);
                            serializeProcedures.add(protoFactory(spec, factoryInputs));
                            continue;
                        }
                        case SL_PROTEIN_FASTA_CSV:
                        case PEP_XML:
                        case MZ_IDENT_ML:
                        case PROTEIN_FASTA: {
                            ImmutableMap<String, String> map = ImmutableMap.<String, String>builder()
                                .put("search", workFlowTypeDisplay(export.getWorkFlowType()))
                                .build();
                            ImmutableMap<String, String> updatedParams = Immutables.updateMap(params, map);
                            final ExportSpecification spec = exportSpecLookup(exportType);
                            Query query = workflowToQuery(export.getWorkFlowType(), workFlowTypeToQuery);
                            CompletionStage<Object> futureApplicationGraph =
                                projectService.analysisGraphTaskIds(projectId, analysisId, export.getWorkFlowType(), user)
                                    .thenApply(graphTaskIds -> spec.applicationGraph(graphTaskIds, query, updatedParams));
                            FactoryInputs factoryInputs = new FactoryInputs(query, futureApplicationGraph, request, updatedParams, user);
                            serializeProcedures.add(exportFactory(spec, factoryInputs));
                            continue;
                        }
                        default:
                            throw new IllegalArgumentException("Unexpected " + exportType);
                    }
                }
                return serializeZipProcedure2(
                    request,
                    response,
                    serializeProcedures.build(),
                    project.getName().concat(".").concat(analysis.getName()).concat(".zip")
                );
            }).whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    LOG.info("Finished creating zip file.");
                    response.end();
                }
            });
        });
    }

    private static Query workflowToQuery(WorkFlowType workFlowType, Map<WorkFlowType, Query> query) {
        switch (workFlowType) {
            case SPIDER:
                return query.get(WorkFlowType.SPIDER);
            case PTM_FINDER:
                return query.get(WorkFlowType.PTM_FINDER);
            case DB_SEARCH:
                return query.get(WorkFlowType.DB_SEARCH);
            case DENOVO:
                return query.get(WorkFlowType.DENOVO);
            case LFQ_FRACTION_ID_TRANSFER:
            case LFQ:
                return query.get(WorkFlowType.LFQ);
            case REPORTER_ION_Q:
                return query.get(WorkFlowType.REPORTER_ION_Q);
            case SPECTRAL_LIBRARY:
                return query.get(WorkFlowType.SPECTRAL_LIBRARY);
            case SILAC_FRACTION_ID_TRANSFER:
            case SILAC:
                return query.get(WorkFlowType.SILAC);
            case DIA_DB_SEARCH:
                return query.get(WorkFlowType.DIA_DB_SEARCH);
            case UNRECOGNIZED:
            default:
                throw new IllegalStateException("Unrecognized work flow " + workFlowType + " cannot create query filters");
        }
    }

    protected CompletionStage<Done> serializeZipProcedure2(
        HttpServerRequest request,
        HttpServerResponse response,
        List<BiFunction<ZipOutputStream, Materializer, CompletionStage<Done>>> exportFutureMakers,
        String zipName
    ) {
        Materializer materializer = materializerWithErrorLogging(request);

        response.putHeader("Content-Disposition", "attachment; filename=\""+ zipName + "\"");
        response.putHeader("Content-Type", "application/zip");
        response.setChunked(true);

        ZipOutputStream output = new ZipOutputStream(new VertxChunkedOutputStream(response));

        return Source.from(exportFutureMakers)
            .runFoldAsync(Done.getInstance(), (done, f) -> f.apply(output, materializer), materializer)
            .whenComplete((done, throwable) -> {
                try {
                    output.close();
                } catch (IOException e) {
                    LOG.error("Error close zip export", e);
                }
                if (throwable != null) {
                    LOG.error("Error writing exports", throwable);
                } else {
                    LOG.info("Download complete.");
                }
            });
    }

    private <T extends GeneratedMessageV3, A extends AnalysisInfoApplicationGraph, Q extends Query> BiFunction<ZipOutputStream, Materializer, CompletionStage<Done>> exportFactory(
        ExportSpecification<Q, A> spec,
        FactoryInputs<A, Q> inputs
    ){
        final String name = inputs.apiParameters.getOrDefault("filename", spec.name(inputs.query, inputs.apiParameters));
        final String host = inputs.request.host();
        return (output, materializer) -> {
            try {
                output.putNextEntry(new ZipEntry(name));
                return inputs.applicationGraph.thenCompose(applicationGraph -> {
                    try {
                        return spec.process(name, host, applicationGraph, output, materializer, inputs.apiParameters);
                    } catch (Throwable e) {
                        Throwables.throwIfUnchecked(e);
                        throw new CompletionException(e);
                    }
                })
                .exceptionally((Throwable throwable) -> {
                    closeEntry(output);
                    if (throwable !=  null) {
                        try {
                            output.putNextEntry(new ZipEntry(name + ".error"));
                        } catch (IOException e) {
                            Throwables.throwIfUnchecked(e);
                            throw new RuntimeException(e);
                        }
                        PrintStream printStream = new PrintStream(output);
                        throwable.printStackTrace(printStream);
                        printStream.flush(); // Do not close, this will close the ZipOutputStream.
                    }
                    return Done.getInstance();
                })
                .thenApply(closeEntry(output));
            } catch (Exception e) {
                LOG.error("Error writing export" + name, e);
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        };
    }

    private BiFunction<ZipOutputStream, Materializer, CompletionStage<Done>> txtExportFactory(
        TxtSpecification spec,
        Map<String, String> apiParameters,
        User user
    ) {
        final String name = spec.getFileName();
        return (output, materialzer) -> {
            try {
                output.putNextEntry(new ZipEntry(name));
            } catch (IOException e) {
                LOG.error("Exception during file creation." + name, e);
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
            return spec.generateTxt(output, apiParameters, user)
                .exceptionally((Throwable throwable) -> {
                    closeEntry(output);
                    if (throwable != null) {
                        try {
                            output.putNextEntry(new ZipEntry(name + ".error"));
                        } catch (IOException e) {
                            Throwables.throwIfUnchecked(e);
                            throw new RuntimeException(e);
                        }
                        PrintStream printStream = new PrintStream(output);
                        throwable.printStackTrace(printStream);
                        printStream.flush(); // Do not close, this will close the ZipOutputStream.
                    }
                    return Done.getInstance();
                })
                .thenApply(closeEntry(output));
        };
    }

    private <T extends GeneratedMessageV3, A extends AnalysisInfoApplicationGraph> BiFunction<ZipOutputStream, Materializer, CompletionStage<Done>> protoFactory(
        CsvProtoHandlerSpecification<T, Query, A> spec,
        FactoryInputs<A, Query> inputs
    ){
        return (output, materializer) -> inputs.applicationGraph.thenCompose(applicationGraph -> {
            Source<T, NotUsed> source = spec.source(applicationGraph, inputs.query, inputs.apiParameters);
            String name = spec.extensionName(applicationGraph, inputs.query, inputs.apiParameters).concat(".csv");

            return spec.csv(applicationGraph, inputs.query, inputs.apiParameters)
                .thenCompose(csv -> {
                    try {
                        output.putNextEntry(new ZipEntry(name));
                        ExportableCsv<T> tExportableCsv = new ExportableCsv<>(csv, source);
                        return tExportableCsv.export("", output, materializer);
                    } catch (IOException e) {
                        LOG.error("Exception during file creation." + name, e);
                        throw new RuntimeException(e);
                    }
                })
                .exceptionally((Throwable throwable) -> {
                    closeEntry(output);
                    if (throwable !=  null) {
                        try {
                            output.putNextEntry(new ZipEntry(name + ".error"));
                        } catch (IOException e) {
                            Throwables.throwIfUnchecked(e);
                            throw new RuntimeException(e);
                        }
                        PrintStream printStream = new PrintStream(output);
                        throwable.printStackTrace(printStream);
                        printStream.flush(); // Do not close, this will close the ZipOutputStream.
                    }
                    return Done.getInstance();
                })
                .thenApply(closeEntry(output));
        });
    }

    public Function<Done, Done> closeEntry(ZipOutputStream output) {
        return done -> {
            try {
                output.closeEntry();
            } catch (IOException e) {
                LOG.error("Exception during file zip file entry closing.", e);
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }

            return Done.getInstance();
        };
    }

    private String workFlowTypeDisplay(WorkFlowType workFlowType) {
        switch (workFlowType) {
            case DENOVO: return "denovo";
            case DB_SEARCH: return "db";
            case PTM_FINDER: return "ptm";
            case SPIDER: return "spider";
            case LFQ: return "lfq";
            case DATA_REFINEMENT: return "data.refine";
            case REPORTER_ION_Q: return "riq";
            case SPECTRAL_LIBRARY: return "sl";
            case SILAC:
            case SILAC_FRACTION_ID_TRANSFER:
                return "silac";
            case DIA_DB_SEARCH: return "dia.db";
            default: return "workflow.undefined";
        }
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

    @Override
    public ProjectService projectService() {
        return projectService;
    }

    @Override
    public FastaDatabaseService databaseService() {
        return fastaDatabaseService;
    }

    @Override
    public SpectralLibraryService spectralLibraryService() {
        return spectralLibraryService;
    }

    @Override
    public Materializer createMaterializer() {
        return ActorMaterializer.create(system);
    }

    private static class FactoryInputs<A extends AnalysisInfoApplicationGraph, Q extends Query> {
        final Q query;
        final CompletionStage<A> applicationGraph;
        final HttpServerRequest request;
        final Map<String, String> apiParameters;
        final User user;
        FactoryInputs(
            Q query,
            CompletionStage<A> applicationGraph,
            HttpServerRequest request,
            Map<String, String> apiParameters,
            User user
        ) {
            this.query = query;
            this.applicationGraph = applicationGraph;
            this.request = request;
            this.apiParameters = apiParameters;
            this.user = user;
        }
    }
}
