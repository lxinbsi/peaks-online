package com.bsi.peaks.server.service;

import akka.Done;
import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import com.bsi.peaks.analysis.parameterModels.dto.AnalysisAcquisitionMethod;
import com.bsi.peaks.analysis.parameterModels.dto.MassUnit;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.graph.AnalysisInfoApplicationGraph;
import com.bsi.peaks.data.graph.AnalysisInfoApplicationGraphImplementation;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.SpectralLibraryRepository;
import com.bsi.peaks.event.ModificationCommandFactory;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.SpectralLibraryCommandFactory;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse;
import com.bsi.peaks.event.spectralLibrary.SLCommand;
import com.bsi.peaks.event.spectralLibrary.SLMetaData;
import com.bsi.peaks.event.spectralLibrary.SLProcessTaskIds;
import com.bsi.peaks.event.spectralLibrary.SLResponse;
import com.bsi.peaks.internal.task.SlSearchParameters;
import com.bsi.peaks.io.writer.csv.SpectralLibrary.SpectralLibraryColumns;
import com.bsi.peaks.io.writer.csv.SpectralLibrary.SpectralLibraryImporter;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.applicationgraph.GraphTaskIds;
import com.bsi.peaks.model.dto.*;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.model.proteomics.Modification;
import com.bsi.peaks.model.proto.AcquisitionMethod;
import com.bsi.peaks.model.query.DbSearchQuery;
import com.bsi.peaks.model.query.SlQuery;
import com.bsi.peaks.model.sl.SLSpectrum;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.es.communication.ModificationCommunication;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.bsi.peaks.server.es.communication.SpectralLibraryCommunication;
import com.bsi.peaks.server.es.tasks.Helper;
import com.bsi.peaks.server.handlers.HandlerException;
import com.bsi.peaks.service.services.libgenerator.DecoyGenerator;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.bsi.peaks.model.ModelConversion.uuidToByteString;
import static com.bsi.peaks.model.ModelConversion.uuidsToByteString;

public class SpectralLibraryService {
    private static final Logger LOG = LoggerFactory.getLogger(SpectralLibraryService.class);
    private final SpectralLibraryCommunication spectralLibraryCommunication;
    private final ProjectAggregateCommunication projectCommunication;
    private final int parallelism;
    private final Materializer materializer;
    private final SpectralLibraryRepository spectralLibraryRepository;
    private final ModificationCommunication modificationCommunication;
    private final ApplicationStorage storage;
    private final ApplicationStorageFactory storageFactory;

    @Inject
    public SpectralLibraryService(
        final SpectralLibraryCommunication spectralLibraryCommunication,
        final ModificationCommunication modificationCommunication,
        final ProjectAggregateCommunication projectCommunication,
        final ApplicationStorageFactory storageFactory
    ) {
        this.spectralLibraryCommunication = spectralLibraryCommunication;
        this.modificationCommunication = modificationCommunication;
        this.projectCommunication = projectCommunication;
        this.parallelism = storageFactory.getParallelism();
        storage = storageFactory.getSystemStorage();
        this.materializer = storage.getMaterializer();
        this.spectralLibraryRepository = storage.getSpectralLibraryRepository();
        this.storageFactory = storageFactory;
    }

    public Source<SpectralLibraryListItem, NotUsed> list(UUID actingAsUserId) {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        return Source.fromSourceCompletionStage(spectralLibraryCommunication.query(commandFactory.queryAll())
            .thenApply(response -> Source.from(response.getList().getSpectralLibraryList()))
        ).mapMaterializedValue(ignore -> NotUsed.getInstance());
    }

    public CompletionStage<SpectralLibraryListItem> get(UUID actingAsUserId, UUID slId) {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        return spectralLibraryCommunication.query(commandFactory.queryById(slId))
            .thenApply(SLResponse::getList)
            .thenApply(list -> {
                if (list.getSpectralLibraryCount() != 1) {
                    throw new HandlerException(404, "Invalid spectral library id given.");
                }
                return list.getSpectralLibrary(0);
            });
    }

    public CompletionStage<Optional<SpectralLibraryListItem>> safeGet(UUID actingAsUserId, UUID slId) {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        return spectralLibraryCommunication.query(commandFactory.queryById(slId))
            .thenApply(SLResponse::getList)
            .thenApply(list -> {
                if (list.getSpectralLibraryCount() != 1) {
                    return Optional.empty();
                }
                return Optional.of(list.getSpectralLibrary(0));
            });
    }

    public CompletionStage<Done> delete(UUID actingAsUserId, UUID slId) {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        return spectralLibraryCommunication.command(commandFactory.delete(slId));
    }

    public CompletionStage<Done> setOwner(UUID actingAsUserId, UUID slId, UUID newOwnerId) {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        return spectralLibraryCommunication.command(commandFactory.grantPermission(slId, newOwnerId));
    }

    public CompletionStage<Done> setName(UUID actingAsUserId, UUID slId, String name) {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        return spectralLibraryCommunication.command(commandFactory.setName(slId, name));
    }

    public CompletionStage<String> meta(UUID actingAsUserId, UUID slId) {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        return spectralLibraryCommunication.query(commandFactory.queryMetaById(slId))
            .thenApply(response -> response.getMeta());
    }

    public CompletionStage<UUID> createLibraryFromImport(UUID actingAsUserId, String slName, Path csvToImport, Optional<Path> metaPath) throws IOException {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        final Optional<String> metaContents;
        if (metaPath.isPresent()) {
            metaContents = Optional.of(new String(Files.readAllBytes(metaPath.get()), "UTF-8"));
        } else {
            metaContents = Optional.empty();
        }
        BufferedReader tsvReader = Files.newBufferedReader(csvToImport);
        String header = tsvReader.readLine();
        tsvReader.close();
//        SpectralLibraryColumns.order(headers)

        SLCommand createCommand = commandFactory.createImport(builder -> builder.mergeFrom(metaDataFromMetaFile(actingAsUserId, slName, header, metaContents)));
        final UUID slId = ModelConversion.uuidFrom(createCommand.getCreateImport().getSlId());
        return spectralLibraryCommunication.command(createCommand).thenApply(ignore -> slId);
    }

    public CompletionStage<Done> importLibraryCsv(UUID actingAsUserId, UUID slId, Path csvToImport) {
        SpectralLibraryCommandFactory commandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);
        return spectralLibraryCommunication.command(commandFactory.processNotification(slId, SLProgressState.IMPORTING))
            .thenCombine(
                modificationCommunication.query(ModificationCommandFactory.queryModificationById(actingAsUserId.toString())),
                (done, modificationResponse) -> modificationResponse
            )
            .thenCompose(modiRepsonse -> {
                Map<String, String> permissions = modiRepsonse.getPermissionMap();
                List<Modification> modiList = modiRepsonse.getModificationList().stream()
                    .map(m -> (Modification) DtoAdaptors.convertModificationVM(m, UUID.fromString(permissions.get(m.getName()))))
                    .collect(Collectors.toList());
                return FileIO.fromPath(csvToImport)
                    .async()
                    .via(SpectralLibraryImporter.tsvFlow(modiList))
                    .async()
                    .alsoToMat(spectralLibraryRepository.sinkTarget(slId), Keep.right())
                    .toMat(Sink.<TLongList, SLSpectrum>fold(new TLongArrayList(), (l, spectrum) -> {
                        l.add(spectrum.retentionTimeMs());
                        return l;
                    }), (done, futureTargetCount) -> done.thenCompose(ignore -> futureTargetCount))
                    .run(materializer);
            })
            .thenCompose(targetRetensionTimes -> {
                int targetCount = targetRetensionTimes.size();
                ArrayList<Long> targetRts = new ArrayList<>(targetRetensionTimes.size());
                targetRetensionTimes.forEach(targetRts::add);
                DecoyGenerator decoyGenerator = new DecoyGenerator(targetRts);
                return spectralLibraryRepository.sourceOrderedOnlyTarget(slId)
                    .mapConcat(decoyGenerator::generateDecoys)
                    .toMat(spectralLibraryRepository.sinkDecoy(slId), Keep.right())
                    .run(materializer)
                    .thenApply(done -> targetCount);
            })
            .handle((entryCount, throwable) -> {
                if (throwable == null) {
                    LOG.info("Done importing spectral library {}. ({} entries)", slId, entryCount);
                    spectralLibraryCommunication.command(commandFactory.setEntryCount(slId, entryCount));
                    spectralLibraryCommunication.command(commandFactory.processNotification(slId, SLProgressState.DONE));
                } else {
                    LOG.error("Failed importing spectral library " + slId, throwable);
                    spectralLibraryCommunication.command(commandFactory.processNotification(slId, SLProgressState.FAILED));
                }
                return Done.getInstance();
            });
    }

    public CompletionStage<SpectralLibraryListItem> create(UUID actingAsUserId, CreateSpectralLibrary spectralLibrary) {
        final UUID projectId = UUID.fromString(spectralLibrary.getProjectId());
        final UUID analysisId = UUID.fromString(spectralLibrary.getAnalysisId());

        final ProjectAggregateCommandFactory projectCommandFactory = ProjectAggregateCommandFactory.project(projectId, actingAsUserId);

        final ProjectAggregateQueryRequest query;
        final ProjectAggregateQueryRequest queryFilters;
        final AcquisitionMethod acquisitionMethod;
        if (spectralLibrary.getAcquisitionMethod().equals(AnalysisAcquisitionMethod.DIA)) {
            query = projectCommandFactory
                    .analysis(analysisId)
                    .queryGraphTaskIds(WorkFlowType.DIA_DB_SEARCH);
            queryFilters = projectCommandFactory
                    .analysis(analysisId)
                    .filters(ProjectAggregateQueryRequest.AnalysisQueryRequest.Filters.newBuilder()
                            .setWorkflowType(WorkFlowType.DIA_DB_SEARCH)
                            .build());
            acquisitionMethod = AcquisitionMethod.DIA;
        } else {
            query = projectCommandFactory
                    .analysis(analysisId)
                    .queryGraphTaskIds(WorkFlowType.DB_SEARCH);
            queryFilters = projectCommandFactory
                    .analysis(analysisId)
                    .filters(ProjectAggregateQueryRequest.AnalysisQueryRequest.Filters.newBuilder()
                            .setWorkflowType(WorkFlowType.DB_SEARCH)
                            .build());
            acquisitionMethod = AcquisitionMethod.DDA;
        }

        final SpectralLibraryCommandFactory slCommandFactory = SpectralLibraryCommandFactory.actingAsUserId(actingAsUserId);

        return projectCommunication.queryAnalysis(query)
            .thenCombine(projectCommunication.queryAnalysis(queryFilters), (response, responseFilter) -> {
                GraphTaskIds taskIds = response.getGraphTaskIds();
                List<String> fids = taskIds.getFractionIdsList();
                ProjectAggregateQueryResponse.AnalysisQueryResponse.FilterQueries filterQueries = responseFilter.getFilterQueries();
                SLProcessTaskIds.Builder process;
                com.bsi.peaks.data.graph.GraphTaskIds graphTaskIds;
                try {
                    graphTaskIds = com.bsi.peaks.data.graph.GraphTaskIds.from(taskIds);
                    if (acquisitionMethod.equals(AcquisitionMethod.DIA)) {
                        SlQuery slQuery = (SlQuery) Helper.convertQuery(filterQueries.getQueryJson(0), filterQueries.getWorkflowType(0));
                        process =  SLProcessTaskIds.newBuilder()
                                .setFractionIds(uuidsToByteString(Lists.transform(fids, UUID::fromString)))
                                .setDataLoadingTaskIds(uuidsToByteString(Lists.transform(fids, fid -> UUID.fromString(taskIds.getFractionIdToDataLoadingTaskIdOrThrow(fid)))))
                                .setDbSearchTaskIds(uuidsToByteString(Lists.transform(fids, fid -> UUID.fromString(taskIds.getFractionIdToSlPsmRepositoryTaskIdOrThrow(fid)))))
                                .setSlSearchParameters(taskIds.getSlSearchParameters())
                                .setKeyspace(taskIds.getKeyspace())
                                .setSlPsmFilter(slQuery.slPsmFilter().proto())
                                .setSlPeptideFilter(slQuery.slPeptideFilter().dto(SlFilter.PeptideFilter.newBuilder()))
                                .setPsmDecoyInfoTaskId(uuidToByteString(UUID.fromString(taskIds.getPeptideRepositoryTaskId())))
                                .setAcquisitionMethod(AcquisitionMethod.DIA);

                        ApplicationStorage dataStorage = storageFactory.getStorage(graphTaskIds.keyspace());
                        dataStorage.getSpectralLibraryRepository().sourceModifications(graphTaskIds.peptideRepositoryTaskId().get())
                                .map(p -> p.proto(com.bsi.peaks.model.proto.Modification.newBuilder()))
                                .runWith(StreamConverters.asJavaStream(), materializer)
                                .forEach(process::addModifications);
                    } else {
                        DbSearchQuery dbQuery = (DbSearchQuery) Helper.convertQuery(filterQueries.getQueryJson(0), filterQueries.getWorkflowType(0));
                        process = SLProcessTaskIds.newBuilder()
                                .setFractionIds(uuidsToByteString(Lists.transform(fids, UUID::fromString)))
                                .setDataLoadingTaskIds(uuidsToByteString(Lists.transform(fids, fid -> UUID.fromString(taskIds.getFractionIdToDataLoadingTaskIdOrThrow(fid)))))
                                .setDbSearchTaskIds(uuidsToByteString(Lists.transform(fids, fid -> UUID.fromString(taskIds.getFractionIdToPsmRepositoryTaskIdOrThrow(fid)))))
                                .setDbSearchParametersJSON(taskIds.getJsonSearchParameters())
                                .setKeyspace(taskIds.getKeyspace())
                                .setPsmFilter(dbQuery.psmFilter().toDto())
                                .setPeptideFilter(dbQuery.peptideFilter().dto(IDFilter.PeptideFilter.newBuilder()))
                                .setPsmDecoyInfoTaskId(uuidToByteString(UUID.fromString(taskIds.getPsmDecoyInfoTaskId())))
                                .setAcquisitionMethod(AcquisitionMethod.DDA);
                    }
                } catch (Throwable e) {
                    Throwables.throwIfUnchecked(e);
                    throw new RuntimeException(e);
                }

                AnalysisInfoApplicationGraph applicationGraph = new AnalysisInfoApplicationGraphImplementation(storage, graphTaskIds, Optional.empty());
                final SpectralLibraryType type;
                if (applicationGraph.hasIonMobility()) {
                    type = SpectralLibraryType.SL_TIMS;
                } else if (applicationGraph.hasFaims()) {
                    type = SpectralLibraryType.SL_FAIMS;
                } else {
                    type = SpectralLibraryType.SL_NONE;
                }

                return slCommandFactory.createProcess(
                    metaBuilder -> metaBuilder
                        .setName(spectralLibrary.getSlName())
                        .setProjectName(taskIds.getProjectName())
                        .setAnalysisName(taskIds.getAnalysisName())
                        .setJson(metaInfoFromGraphTaskIds(taskIds, spectralLibrary.getUseRtAsIRt()))
                        .addAllDatabase(databaseNames(taskIds))
                        .setUseRtAsIRt(spectralLibrary.getUseRtAsIRt())
                        .setType(type)
                        .setAcquisitionMethod(acquisitionMethod),
                    processBuilder -> processBuilder.mergeFrom(process.build()),
                    spectralLibrary.getUseRtAsIRt()
                );
            })
            .thenCompose(createCommand -> {
                UUID spectralLibraryId = ModelConversion.uuidFrom(createCommand.getCreateProcess().getSlId());
                return spectralLibraryCommunication.command(createCommand)
                    .thenCompose(done -> spectralLibraryCommunication.query(slCommandFactory.queryById(spectralLibraryId)))
                    .thenApply(response -> Iterables.getOnlyElement(response.getList().getSpectralLibraryList()));
                });
    }

    private List<String> databaseNames(GraphTaskIds taskIds) {
        return taskIds.getAnalysisDatabasesList()
            .stream()
            .map(db ->  {
                String name = db.getName();
                List<String> taxonomyNames = db.getTaxonomyNamesList();
                if (taxonomyNames.isEmpty()) {
                    return name;
                } else {
                    return name + " (" + String.join(",", taxonomyNames) + ")";
                }
            })
            .collect(Collectors.toList());
    }

    private String metaInfoFromGraphTaskIds(GraphTaskIds graphTaskIds, boolean useRTasIRT) {
        JsonObject meta = new JsonObject();
        meta.put(MetaKeys.PROJECT_NAME, graphTaskIds.getProjectName());
        meta.put(MetaKeys.ANALYSIS_NAME, graphTaskIds.getAnalysisName());

        //search parameters
        JsonObject searchParameters = new JsonObject();
        try {
            BiFunction<Double, Boolean, JsonObject> toleranceJson = (tol, isPpm) -> {
                JsonObject ret = new JsonObject();
                ret.put(MetaKeys.TOLERANCE, tol);
                ret.put(MetaKeys.TOLERANCE_UNIT, isPpm ? "PPM" : "Da");
                return ret;
            };
            double precursorTolerance;
            boolean precursorToleranceIsPpm;
            double fragmentTolerance;
            boolean fragmentToleranceIsPpm;
            if (graphTaskIds.hasSlSearchParameters()) {
                SlSearchParameters slSearchParameters = graphTaskIds.getSlSearchParameters();
                precursorTolerance = slSearchParameters.getSlSearchSharedParameters().getPrecursorTolerance().getParam();
                precursorToleranceIsPpm = slSearchParameters.getSlSearchSharedParameters().getPrecursorTolerance().getUnit() == MassUnit.PPM;
                fragmentTolerance = slSearchParameters.getSlSearchSharedParameters().getFragmentTolerance().getParam();
                fragmentToleranceIsPpm = slSearchParameters.getSlSearchSharedParameters().getFragmentTolerance().getUnit() == MassUnit.PPM;
            } else {
                DBSearchParameters dbSearchParameters = Helper.OM.readValue(graphTaskIds.getJsonSearchParameters(), DBSearchParameters.class);
                JsonArray fixedModifications = new JsonArray();
                dbSearchParameters.fixedModificationNames().forEach(fixedModifications::add);
                JsonArray variableModifications = new JsonArray();
                dbSearchParameters.variableModificationNames().forEach(variableModifications::add);
                searchParameters.put(MetaKeys.FIX_MODIFICATIONS, fixedModifications);
                searchParameters.put(MetaKeys.VARIABLE_MODIFICATIONS, variableModifications);
                searchParameters.put(MetaKeys.ENZYME_NAME, dbSearchParameters.enzyme().name());
                precursorTolerance = dbSearchParameters.precursorTol();
                precursorToleranceIsPpm = dbSearchParameters.isPPMForPrecursorTol();
                fragmentTolerance = dbSearchParameters.fragmentTol();
                fragmentToleranceIsPpm = dbSearchParameters.isPPMForFragmentTol();
            }
            searchParameters.put(MetaKeys.PRECURSOR_TOLERANCE, toleranceJson.apply(precursorTolerance, precursorToleranceIsPpm));
            searchParameters.put(MetaKeys.FRAGMENTATION_TOLERANCE, toleranceJson.apply(fragmentTolerance, fragmentToleranceIsPpm));
        } catch (IOException e) {
            LOG.error("Unable to calculate search paremeters for spectral library info file", e);
        }
        meta.put(MetaKeys.SEARCH_PARAMETERS, searchParameters);

        //db names
        JsonArray databaseNames = new JsonArray();
        databaseNames(graphTaskIds).forEach(db -> databaseNames.add(db));
        meta.put(MetaKeys.DATABASE_NAMES, databaseNames);

        //raw files
        JsonArray rawFiles = new JsonArray();
        graphTaskIds.getSamplesList().forEach(sample ->
            sample.getFractionSrcFileList().forEach(fraction -> {
                JsonObject rawFile = new JsonObject();
                rawFile.put(MetaKeys.FILE_NAME, fraction)
                    .put(MetaKeys.INSTRUMENT_NAME, sample.getInstrument().getName())
                    .put(MetaKeys.FRACTION_FRAGMENTATION, sample.getActivationMethod().name());
                rawFiles.add(rawFile);
            })
        );
        meta.put(MetaKeys.RAW_FILE, rawFiles);

        //fragment annotation
        meta.put(MetaKeys.FRAGMENTATION_ANNOTATION, "no loss, -NH3,-H2O etc");

        meta.put(MetaKeys.USE_RT_AS_IRT, useRTasIRT);

        //Acquisition Method
        meta.put(MetaKeys.ACQUISITION_METHOD, graphTaskIds.getAnalysisAcquisitionMethod() == AnalysisAcquisitionMethod.DIA ? "DIA" : "DDA");

        return meta.toString();
    }

    private SLMetaData metaDataFromMetaFile(UUID actingUserId, String slName, String header, Optional<String> metaFileContents) {
        final SLMetaData.Builder builder = SLMetaData.newBuilder();

        JsonObject metaFileJson;
        String projectName;
        String analysisName;
        boolean useRTasIRT;
        String acquisitionMethodString;
        if (metaFileContents.isPresent()) {
            metaFileJson = new JsonObject(metaFileContents.get());
            projectName = metaFileJson.getString(MetaKeys.PROJECT_NAME, "");
            analysisName = metaFileJson.getString(MetaKeys.ANALYSIS_NAME, "");
            useRTasIRT = metaFileJson.getBoolean(MetaKeys.USE_RT_AS_IRT, false);
            acquisitionMethodString = metaFileJson.getString(MetaKeys.ACQUISITION_METHOD, "");
        } else {
            metaFileJson = new JsonObject();
            projectName = "";
            analysisName = "";
            useRTasIRT = false;
            acquisitionMethodString = "";
        }
        JsonArray databaseNameJsonArray = metaFileJson.getJsonArray(MetaKeys.DATABASE_NAMES, new JsonArray());
        for (int i = 0; i < databaseNameJsonArray.size(); i++) {
            builder.addDatabase(databaseNameJsonArray.getString(i));
        }

        final SpectralLibraryType type;
        List<SpectralLibraryColumns> spectralLibraryColumns = Arrays.asList(SpectralLibraryImporter.splitHeader(header));
        boolean isTims = spectralLibraryColumns.contains(SpectralLibraryColumns.CCS);
        boolean isFaims = spectralLibraryColumns.contains(SpectralLibraryColumns.FAIMS_CV);
        if (isTims && isFaims) {
            type = SpectralLibraryType.SL_UNKNOWN;
        } else if (isTims) {
            type = SpectralLibraryType.SL_TIMS;
        } else if (isFaims) {
            type = SpectralLibraryType.SL_FAIMS;
        } else {
            type = SpectralLibraryType.SL_NONE;
        }


        return builder
            .setName(slName)
            .setOwnerId(ModelConversion.uuidsToByteString(actingUserId))
            .setProjectName(projectName)
            .setAnalysisName(analysisName)
            .setJson(metaFileJson.toString())
            .setType(type)
            .setUseRtAsIRt(useRTasIRT)
            .setAcquisitionMethod(acquisitionMethodString.equals("DIA") ? AcquisitionMethod.DIA : AcquisitionMethod.DDA)
            .build();
    }

    static class MetaKeys {
        private static final String PROJECT_NAME = "Project Name";
        private static final String ANALYSIS_NAME = "Search Result Name";
        private static final String DATABASE_NAMES = "Database names";
        private static final String SEARCH_PARAMETERS = "Search Parameters";
        private static final String PRECURSOR_TOLERANCE = "Precursor Tolerance";
        private static final String FRAGMENTATION_TOLERANCE = "Fragmentation Tolerance";
        private static final String TOLERANCE = "Tolerance";
        private static final String TOLERANCE_UNIT = "Unit";
        private static final String FIX_MODIFICATIONS = "Fixed Modifications";
        private static final String VARIABLE_MODIFICATIONS = "Variables Modifications";
        private static final String ENZYME_NAME = "Enzyme";
        private static final String RAW_FILE = "Raw File";
        private static final String FILE_NAME = "File Name";
        private static final String INSTRUMENT_NAME = "Instrument Name";
        private static final String FRACTION_FRAGMENTATION = "Fragmentation";
        private static final String FRAGMENTATION_ANNOTATION = "Fragmentation Annotation";
        private static final String USE_RT_AS_IRT = "Use RT as iRT";
        private static final String ACQUISITION_METHOD = "Acquisition Method";
    }
}
