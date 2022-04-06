package com.bsi.peaks.server.parsers;

import akka.japi.Pair;
import com.bsi.peaks.analysis.analysis.dto.Analysis;
import com.bsi.peaks.analysis.analysis.dto.DbSearchStep;
import com.bsi.peaks.analysis.analysis.dto.IdentificaitonFilterSummarization;
import com.bsi.peaks.analysis.analysis.dto.SlStep;
import com.bsi.peaks.analysis.parameterModels.dto.AnalysisAcquisitionMethod;
import com.bsi.peaks.analysis.parameterModels.dto.DbSearchParameters;
import com.bsi.peaks.analysis.parameterModels.dto.DeNovoParameters;
import com.bsi.peaks.analysis.parameterModels.dto.EnzymeSpecificity;
import com.bsi.peaks.analysis.parameterModels.dto.LfqParameters;
import com.bsi.peaks.analysis.parameterModels.dto.MassUnit;
import com.bsi.peaks.analysis.parameterModels.dto.PtmFinderModificationSearchType;
import com.bsi.peaks.analysis.parameterModels.dto.SilacNormalization;
import com.bsi.peaks.analysis.parameterModels.dto.SilacParameters;
import com.bsi.peaks.analysis.parameterModels.dto.SlProteinInferenceStepParameters;
import com.bsi.peaks.analysis.parameterModels.dto.SlSearchStepParameters;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.event.EnzymeCommandFactory;
import com.bsi.peaks.event.InstrumentCommandFactory;
import com.bsi.peaks.event.ReporterIonQMethodCommandFactory;
import com.bsi.peaks.event.SilacQMethodCommandFactory;
import com.bsi.peaks.event.SpectralLibraryCommandFactory;
import com.bsi.peaks.event.enzyme.EnzymeResponse;
import com.bsi.peaks.event.instrument.InstrumentResponse;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodResponse;
import com.bsi.peaks.event.silacqmethod.SilacQMethodResponse;
import com.bsi.peaks.internal.task.*;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.PeaksParameters;
import com.bsi.peaks.model.core.quantification.labelfree.ColourHandler;
import com.bsi.peaks.model.core.quantification.labelfree.LabelFreeGroup;
import com.bsi.peaks.model.core.quantification.labelfree.LabelFreeGroupBuilder;
import com.bsi.peaks.model.dto.DenovoOnlyTagSearchParameters;
import com.bsi.peaks.model.dto.FdrType;
import com.bsi.peaks.model.dto.Group;
import com.bsi.peaks.model.dto.IDFilter;
import com.bsi.peaks.model.dto.LfqFilter;
import com.bsi.peaks.model.dto.ReporterIonQExperimentSettings;
import com.bsi.peaks.model.dto.ReporterIonQFilterSummarization;
import com.bsi.peaks.model.dto.ReporterIonQParameters;
import com.bsi.peaks.model.dto.ReporterIonQSpectrumFilter;
import com.bsi.peaks.model.dto.SlFilter;
import com.bsi.peaks.model.dto.SlSearchSharedParameters;
import com.bsi.peaks.model.dto.SpectralLibraryListItem;
import com.bsi.peaks.model.dto.peptide.ReporterIonQMethod;
import com.bsi.peaks.model.dto.peptide.SilacQMethod;
import com.bsi.peaks.model.filter.DenovoCandidateFilter;
import com.bsi.peaks.model.parameters.DBSearchConservedParameters;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.model.parameters.DBSearchParametersBuilder;
import com.bsi.peaks.model.parameters.DataRefineParameters;
import com.bsi.peaks.model.parameters.DenovoConservedParameters;
import com.bsi.peaks.model.parameters.DenovoParameters;
import com.bsi.peaks.model.parameters.DenovoParametersBuilder;
import com.bsi.peaks.model.parameters.FeatureDetectionConservedParameters;
import com.bsi.peaks.model.parameters.FeatureDetectionParameters;
import com.bsi.peaks.model.parameters.FeatureDetectionParametersBuilder;
import com.bsi.peaks.model.parameters.IDBaseParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationConservedParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParametersBuilder;
import com.bsi.peaks.model.parameters.PtmFinderConservedParameters;
import com.bsi.peaks.model.parameters.PtmFinderParameters;
import com.bsi.peaks.model.parameters.PtmFinderParametersBuilder;
import com.bsi.peaks.model.parameters.RetentionTimeAlignmentParameters;
import com.bsi.peaks.model.parameters.RetentionTimeAlignmentParametersBuilder;
import com.bsi.peaks.model.parameters.SpiderConservedParameters;
import com.bsi.peaks.model.parameters.SpiderParameters;
import com.bsi.peaks.model.parameters.SpiderParametersBuilder;
import com.bsi.peaks.model.parameters.TagSearchConservedParameters;
import com.bsi.peaks.model.parameters.TagSearchParameters;
import com.bsi.peaks.model.parameters.TagSearchParametersBuilder;
import com.bsi.peaks.model.proteomics.Enzyme;
import com.bsi.peaks.model.proteomics.Instrument;
import com.bsi.peaks.model.proteomics.fasta.FastaSequenceDatabase;
import com.bsi.peaks.model.proto.SimplifiedFastaSequenceDatabaseInfo;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.es.communication.EnzymeCommunication;
import com.bsi.peaks.server.es.communication.InstrumentCommunication;
import com.bsi.peaks.server.es.communication.RiqMethodManagerCommunication;
import com.bsi.peaks.server.es.communication.SilacQMethodManagerCommunication;
import com.bsi.peaks.server.es.communication.SpectralLibraryCommunication;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.bsi.peaks.analysis.analysis.dto.IdentificaitonFilterSummarization.PsmFilterTypeCase.MAXFDR;
import static com.bsi.peaks.io.writer.dto.DtoConversion.enzymeSpecificity;
import static java.util.Collections.singletonList;

/**
 * @author Shengying Pan Created by span on 2/23/17.
 */

@Singleton
@SuppressWarnings("WeakerAccess")
public class ParametersParser {

    private final Config localConfig;
    private final FastaDatabaseService fastaDatabaseService;
    private final RiqMethodManagerCommunication reporterIonQMethodManagerCommunication;
    private final ParametersPopulater parametersPopulater;
    private final EnzymeCommunication enzymeCommunication;
    private final InstrumentCommunication instrumentCommunication;
    private final SpectralLibraryCommunication spectralLibraryCommunication;
    private final SilacQMethodManagerCommunication silacQMethodManagerCommunication;

    @Inject
    public ParametersParser(
        Config localConfig,
        ParametersPopulater parametersPopulater,
        FastaDatabaseService fastaDatabaseService,
        RiqMethodManagerCommunication reporterIonQMethodManagerCommunication,
        EnzymeCommunication enzymeCommunication,
        InstrumentCommunication instrumentCommunication,
        SpectralLibraryCommunication spectralLibraryCommunication,
        SilacQMethodManagerCommunication silacQMethodManagerCommunication
    ) {
        this.localConfig = localConfig;
        this.parametersPopulater = parametersPopulater;
        this.fastaDatabaseService = fastaDatabaseService;
        this.reporterIonQMethodManagerCommunication = reporterIonQMethodManagerCommunication;
        this.enzymeCommunication = enzymeCommunication;
        this.instrumentCommunication = instrumentCommunication;
        this.spectralLibraryCommunication = spectralLibraryCommunication;
        this.silacQMethodManagerCommunication = silacQMethodManagerCommunication;
    }

    public Enzyme fetchEnzyme(User user, String name) {
        if (name.equals(Enzyme.SAMPLE_ENZYME.name())) {
            return Enzyme.SAMPLE_ENZYME;
        }
        final String actingUserId = user.userId().toString();
        com.bsi.peaks.model.proto.Enzyme enzyme;
        try {
            enzyme = enzymeCommunication
                .query(EnzymeCommandFactory.queryEnzymeByName(actingUserId, singletonList(name)))
                .thenApply(EnzymeResponse::getEnzymeList)
                .toCompletableFuture().join().get(0);
        } catch (Exception e) {
            throw new ParserException("Unable to find enzyme with name " + name + " for user " + user.username());
        }
        return DtoAdaptors.convertEnzymeVM(enzyme, user.userId());
    }

    public Instrument fetchInstrument(User user, String name) {
        final String actingUserId = user.userId().toString();
        com.bsi.peaks.model.proto.Instrument instrument;
        try {
            instrument = instrumentCommunication
                .query(InstrumentCommandFactory.queryInstrumentByName(actingUserId, singletonList(name)))
                .thenApply(InstrumentResponse::getInstrumentList)
                .toCompletableFuture().join().get(0);
        } catch (Exception e) {
            throw new ParserException("Unable to find instrument with name " + name + " for user " + user.username());
        }

        return DtoAdaptors.convertInstrumentVM(instrument, user.userId());
    }

    public DiaDataRefineParameters diadataRefineParameters(Analysis analysis, WorkFlowType workFlowType) {
        if (!analysis.getAnalysisAcquisitionMethod().equals(AnalysisAcquisitionMethod.DIA)) {
            throw new IllegalStateException("Cannot create dia Dia Data Refine parameters for analysis with acquisition method " + analysis.getAnalysisAcquisitionMethod());
        }

        String conservedJSON = localConfig.getConfig("peaks.parameters.diaDataRefineConserved").root()
            .render(ConfigRenderOptions.concise());

        DiaDataRefineConservedParameters.Builder conservedBuilder = DiaDataRefineConservedParameters.newBuilder();
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(conservedJSON, conservedBuilder);
        } catch (IOException e) {
            throw new ParserException("Unable to load conserved parameters for dia data refinement");
        }
        conservedBuilder.build();

        final float ccsTolerance;
        switch (workFlowType) {
            case DIA_DB_SEARCH:
                ccsTolerance = analysis.getDbSearchStep().getDbSearchParameters().getCcsTolerance();
                break;
            case DENOVO:
                ccsTolerance = analysis.getDenovoStep().getDeNovoParameters().getCcsTolerance();
                break;
            default:
                throw new IllegalStateException("Cannot create dia data refiner for workflow type" + workFlowType);
        }

        return DiaDataRefineParameters.newBuilder()
            .setDiaDataRefineConservedParameters(conservedBuilder)
            .setCcsTolerance(ccsTolerance)
            .build();
    }

    public DenovoParameters denovoParameters(User user,  Analysis analysis) {
        DeNovoParameters deNovoParameters = analysis.getDenovoStep().getDeNovoParameters();
        DenovoConservedParameters conservedParameters = PeaksParameters.fromConfig(
            localConfig.getConfig(DenovoConservedParameters.ROOT + DenovoConservedParameters.SECTION), DenovoConservedParameters.class);

        final Enzyme enzyme;
        if (deNovoParameters.getEnzymeName() != null && !deNovoParameters.getEnzymeName().isEmpty()) {
            enzyme = fetchEnzyme(user, deNovoParameters.getEnzymeName());
        } else {
            enzyme = Enzyme.SAMPLE_ENZYME;
        }

        DenovoParameters denovoParameters = new DenovoParametersBuilder()
            .precursorTol(deNovoParameters.getPrecursorTol().getParam())
            .isPPMForPrecursorTol(deNovoParameters.getPrecursorTol().getUnit().equals(MassUnit.PPM))
            .fragmentTol(deNovoParameters.getFragmentTol().getParam())
            .isPPMForFragmentTol(deNovoParameters.getFragmentTol().getUnit().equals(MassUnit.PPM))
            .fixedModificationNames(deNovoParameters.getFixedModificationNamesList())
            .variableModificationNames(deNovoParameters.getVariableModificationNamesList())
            .maxVariablePtmPerPeptide(deNovoParameters.getMaxVariablePtmPerPeptide())
            .enzyme(enzyme)
            .reportTopNCandidate(5)
            .conservedParameter(conservedParameters)
            .build();

        IDBaseParameters idDbParams = parametersPopulater.populateDBParameters(denovoParameters);

        return new DenovoParametersBuilder()
            .from(denovoParameters)
            .fixedModification(idDbParams.fixedModification())
            .variableModification(idDbParams.variableModification())
            .build();
    }

    public DiaDbSearchParameters getDiaDbSearchParameters(User user, Analysis analysis, Optional<UUID> libId) {
        DbSearchParameters dbSearchParameters = analysis.getDbSearchStep().getDbSearchParameters();
        //create enyzme
        DiaDbSearchSharedParameters sharedParams = getDiaDbSharedParams(user, libId, dbSearchParameters);

        DiaDbSearchParameters.Builder builder = DiaDbSearchParameters.newBuilder()
            .setDiaDbSearchSharedParameters(sharedParams)
            .setCcsTolerance(dbSearchParameters.getCcsTolerance())
            .setPrecursorTol(dbSearchParameters.getPrecursorTol())
            .setFragmentTol(dbSearchParameters.getFragmentTol())
            .setEnzymeSpecificityName(dbSearchParameters.getEnzymeSpecificityName())
            .setEnableToleranceOptimization(dbSearchParameters.getEnableToleranceOptimization());
        return builder.build();
    }

    public DiaDbPreSearchParameters getDiaDbPreSearchParameters(User user, Analysis analysis, Optional<UUID> libId) {
        DbSearchParameters dbSearchParameters = analysis.getDbSearchStep().getDbSearchParameters();
        //create enyzme
        DiaDbSearchSharedParameters sharedParams = getDiaDbSharedParams(user, libId, dbSearchParameters);

        DiaDbPreSearchParameters.Builder builder = DiaDbPreSearchParameters.newBuilder()
            .setDiaDbSearchSharedParameters(sharedParams)
            .setIsEnzymeSpecificityNoDigest(dbSearchParameters.getEnzymeSpecificityName().equals(EnzymeSpecificity.NO_DIGESTION));
        return builder.build();
    }

    private DiaDbSearchSharedParameters getDiaDbSharedParams(User user, Optional<UUID> libId, DbSearchParameters dbSearchParameters) {
        final Enzyme enzyme;
        dbSearchParameters.getEnzymeName();
        if (!dbSearchParameters.getEnzymeName().isEmpty()) {
            enzyme = fetchEnzyme(user, dbSearchParameters.getEnzymeName());
        } else {
            enzyme = Enzyme.SAMPLE_ENZYME;
        }

        //create parameters
        DiaDbSearchSharedParameters.Builder sharedBuilder = DiaDbSearchSharedParameters.newBuilder()
            .addAllFixedModificationNames(dbSearchParameters.getFixedModificationNamesList())
            .addAllVariableModificationNames(dbSearchParameters.getVariableModificationNamesList())
            .setEnzyme(enzyme.proto())
            .setMaxVariablePtmPerPeptide(dbSearchParameters.getMaxVariablePtmPerPeptide())
            .setMissCleavage(dbSearchParameters.getMissCleavage())
            .setPeptideLenMin(dbSearchParameters.getPeptideLenMin())
            .setPeptideLenMax(dbSearchParameters.getPeptideLenMax())
            .setPrecursorChargeMin(dbSearchParameters.getPrecursorChargeMin())
            .setPrecursorChargeMax(dbSearchParameters.getPrecursorChargeMax())
            .setPrecursorMzMin(dbSearchParameters.getPrecursorMzMin())
            .setPrecursorMzMax(dbSearchParameters.getPrecursorMzMax())
            .setFragmentIonMzMin(dbSearchParameters.getFragmentIonMzMin())
            .setFragmentIonMzMax(dbSearchParameters.getFragmentIonMzMax());
        libId.ifPresent(id -> sharedBuilder.setSpectraLibraryId(ModelConversion.uuidsToByteString(id)));
        return sharedBuilder.build();
    }

    public DBSearchParameters getDbSearchParameters(User user, Analysis analysis) {
        DBSearchConservedParameters dbConservedParameters = PeaksParameters.fromConfig(
            localConfig.getConfig(DBSearchConservedParameters.ROOT + DBSearchConservedParameters.SECTION),
            DBSearchConservedParameters.class);
        DbSearchParameters dbSearchParameters = analysis.getDbSearchStep().getDbSearchParameters();

        final Enzyme enzyme;
        if (dbSearchParameters.getEnzymeName() != null && !dbSearchParameters.getEnzymeName().isEmpty()) {
            enzyme = fetchEnzyme(user, dbSearchParameters.getEnzymeName());
        } else {
            enzyme = Enzyme.SAMPLE_ENZYME;
        }

        Pair<List<String>, List<String>> fixedAndVariableToAdd = riqFixedAndVariableModifications(user, analysis);
        List<String> silacVariableModifications = silacVariableModifications(user, analysis);
        List<String> fixed = mergeModis(dbSearchParameters.getFixedModificationNamesList(), fixedAndVariableToAdd.first());
        List<String> variable = mergeModis(dbSearchParameters.getVariableModificationNamesList(), fixedAndVariableToAdd.second(), silacVariableModifications);

        if (dbSearchParameters.getPeptideLenMax() < 1 || dbSearchParameters.getPeptideLenMin() < 0
            || dbSearchParameters.getPeptideLenMax() < dbSearchParameters.getPeptideLenMin()
        ) {
            throw new ParserException("Invalid peptide lengths. Max must be > 0 and > min. Min must be a positive.");
        }

        DBSearchParameters dbParams = new DBSearchParametersBuilder()
            .precursorTol(dbSearchParameters.getPrecursorTol().getParam())
            .isPPMForPrecursorTol(dbSearchParameters.getPrecursorTol().getUnit().equals(MassUnit.PPM))
            .fragmentTol(dbSearchParameters.getFragmentTol().getParam())
            .isPPMForFragmentTol(dbSearchParameters.getFragmentTol().getUnit().equals(MassUnit.PPM))
            .fixedModificationNames(fixed)
            .variableModificationNames(variable)
            .maxVariablePtmPerPeptide(dbSearchParameters.getMaxVariablePtmPerPeptide())
            .enzyme(enzyme)
            .missCleavage(dbSearchParameters.getMissCleavage())
            .enzymeSpecificity(enzymeSpecificity(dbSearchParameters.getEnzymeSpecificityName()))
            .dbSearchConservedParameter(dbConservedParameters)
            .peptideLenMin(dbSearchParameters.getPeptideLenMin())
            .peptideLenMax(dbSearchParameters.getPeptideLenMax())
            .build();

        IDBaseParameters idDbParams = parametersPopulater.populateDBParameters(dbParams);

        return new DBSearchParametersBuilder()
            .from(dbParams)
            .fixedModification(idDbParams.fixedModification())
            .variableModification(idDbParams.variableModification())
            .build();
    }

    public List<SimplifiedFastaSequenceDatabaseInfo> getDatabases(User user, DbSearchParameters dbSearchParameters) {
        final List<FastaSequenceDatabase> dbs;
        try {
            dbs = fastaDatabaseService.databasesAccessibleByUser(user.userId()).toCompletableFuture().get();
        } catch (Exception e) {
            throw new ParserException("Not able to fetch fasta databases.", e);
        }

        return Stream.concat(
            dbSearchParameters.getDatabasesList().stream().map(d -> DtoAdaptors.simplifiedFastaSequenceDatabaseInfo(dbs, d, false)),
            dbSearchParameters.getContaminantDatabasesList().stream().map(d -> DtoAdaptors.simplifiedFastaSequenceDatabaseInfo(dbs, d, true))
        ).collect(Collectors.toList());
    }

    public TagSearchParameters getTagSearchParameters(User user, Analysis analysis) {
        TagSearchConservedParameters tagConservedParameters = PeaksParameters.fromConfig(
            localConfig.getConfig(TagSearchConservedParameters.ROOT + TagSearchConservedParameters.SECTION),
            TagSearchConservedParameters.class);
        DBSearchParameters dbSearchParameters = getDbSearchParameters(user, analysis);

        return new TagSearchParametersBuilder()
            .from(dbSearchParameters)
            .missCleavage(dbSearchParameters.missCleavage())
            .enzymeSpecificity(dbSearchParameters.enzymeSpecificity())
            .conservedParameter(tagConservedParameters)
            .build();
    }

    public PtmFinderParameters getPtmFinderParameters(User user, Analysis analysis) {
        PtmFinderConservedParameters ptmFinderConservedParameters = PeaksParameters.fromConfig(
            localConfig.getConfig(PtmFinderConservedParameters.ROOT + PtmFinderConservedParameters.SECTION),
            PtmFinderConservedParameters.class);
        DBSearchConservedParameters dbConservedParameters = PeaksParameters.fromConfig(
            localConfig.getConfig(DBSearchConservedParameters.ROOT + DBSearchConservedParameters.SECTION),
            DBSearchConservedParameters.class);
        com.bsi.peaks.analysis.parameterModels.dto.PtmFinderParameters ptmFinderParameters = analysis.getPtmFinderStep().getPtmFinderParameters();
        DBSearchParameters dbSearchParameters = getDbSearchParameters(user, analysis);

        List<String> fixedModifications = new ArrayList<>(dbSearchParameters.fixedModificationNames());
        List<String> variableModifications = new ArrayList<>(dbSearchParameters.variableModificationNames());

        if (ptmFinderParameters.getModificationSearchType().equals(PtmFinderModificationSearchType.PREFERRED)) {
            fixedModifications = mergeModis(dbSearchParameters.fixedModificationNames(), ptmFinderParameters.getFixedModificationNamesList());
            variableModifications = mergeModis(dbSearchParameters.variableModificationNames(), ptmFinderParameters.getVariableModificationNamesList());
        }

        Pair<List<String>, List<String>> fixedAndVariableToAdd = riqFixedAndVariableModifications(user, analysis);
        List<String> silacVariableModifications = silacVariableModifications(user, analysis);
        fixedModifications = mergeModis(fixedModifications, fixedAndVariableToAdd.first());
        variableModifications = mergeModis(variableModifications, fixedAndVariableToAdd.second(), silacVariableModifications);

        return new PtmFinderParametersBuilder()
            .from(dbSearchParameters)
            .fixedModificationNames(fixedModifications)
            .variableModificationNames(variableModifications)
            .dbSearchConservedParameter(dbConservedParameters)
            .denovoScoreThreshold(ptmFinderParameters.getDeNovoScoreThreshold())
            .ptmFinderConservedParameter(ptmFinderConservedParameters)
            .allPTM(ptmFinderParameters.getModificationSearchType().equals(PtmFinderModificationSearchType.ALL_BUILT_IN))
            .build();
    }

    public SpiderParameters getSpiderParameters(User user, Analysis analysis) {
        SpiderConservedParameters spiderConservedParameters = PeaksParameters.fromConfig(
            localConfig.getConfig(SpiderConservedParameters.ROOT + SpiderConservedParameters.SECTION),
            SpiderConservedParameters.class);

        /**
         * Unlike PTM finder, that allows additional modifications. SPIDER only uses modifications specified in DB step.
         */
        DBSearchParameters dbSearchParameters = getDbSearchParameters(user, analysis);
        return new SpiderParametersBuilder()
            .from(dbSearchParameters)
            .denovoScoreThreshold(0.15)
            .spiderConservedParameter(spiderConservedParameters)
            .isQEK(true)
            .build();
    }

    private LabelFreeGroup createDefaultGroup(int i, UUID sample) {
        return new LabelFreeGroupBuilder()
            .groupName("Group ".concat(String.valueOf(i)))
            .addSampleIds(sample)
            .build();
    }

    public List<SimplifiedFastaSequenceDatabaseInfo> getDatabases(User user, SlProteinInferenceStepParameters slInferenceParameters) {
        final List<FastaSequenceDatabase> dbs = fastaDatabaseService.databasesAccessibleByUser(user.userId()).toCompletableFuture().join();
        int databasesCount = slInferenceParameters.getDatabasesCount();

        if (!slInferenceParameters.getIncluded()) {
            return Collections.emptyList();
        }

        if (databasesCount == 0) {
            throw new ParserException("Inference included, but no database provided.");
        } else if (databasesCount > 1) {
            throw new IllegalArgumentException("Currently we only support a single target database");
        }

        ImmutableList.Builder<SimplifiedFastaSequenceDatabaseInfo> builder = ImmutableList.builder();
        builder.add(DtoAdaptors.simplifiedFastaSequenceDatabaseInfo(dbs, slInferenceParameters.getDatabases(0), false));

        int contaminantDatabasesCount = slInferenceParameters.getContaminantDatabasesCount();
        if (contaminantDatabasesCount == 1) {
            builder.add(DtoAdaptors.simplifiedFastaSequenceDatabaseInfo(dbs, slInferenceParameters.getContaminantDatabases(0), true));
        } else if (contaminantDatabasesCount > 0) {
            throw new IllegalArgumentException("Currently we only support a single contaminant database");
        }
        return builder.build();
    }

    public SlPeptideSummarizationParameters getSlPeptideSummarizationParameters(User user, Analysis analysis, Optional<SlSearchParameters> optSlSearchParameters) {
        if (optSlSearchParameters.isPresent()) {
            SlSearchParameters slSearchParameters = optSlSearchParameters.get();
            final SlSearchSharedParameters slSearchSharedParameters = slSearchParameters.getSlSearchSharedParameters();
            SlProteinInferenceStepParameters slInferenceParameters = analysis.getSlStep().getSlProteinInferenceStepParameters();
            if (hasQ(analysis) && !slInferenceParameters.getIncluded()) {
                throw new InvalidParameterException("Cannot do Qunatification without protein inference");
            }

            return SlPeptideSummarizationParameters.newBuilder()
                .setSpectralLibraryId(slSearchParameters.getSpectraLibraryId())
                .setProteinHitMaxCapacity(slSearchParameters.getSlConservedParameters().getProteinHitMaxCapacity())
                .setMinPeptideLength(slSearchSharedParameters.getMinPeptideLength())
                .setMaxPeptideLength(slSearchSharedParameters.getMaxPeptideLength())
                .build();
        } else {
            String conservedJSON = localConfig.getConfig("peaks.parameters.SlConservedParameters").root()
                .render(ConfigRenderOptions.concise());

            SlConservedParameters.Builder slConservedBuilder = SlConservedParameters.newBuilder();
            try {
                JsonFormat.parser().ignoringUnknownFields().merge(conservedJSON, slConservedBuilder);
            } catch (IOException e) {
                throw new ParserException("Unable to load conserved parameters for spectral library");
            }
            slConservedBuilder.build();

            DbSearchParameters dbSearchParameters = analysis.getDbSearchStep().getDbSearchParameters();
            return SlPeptideSummarizationParameters.newBuilder()
                .setProteinHitMaxCapacity(slConservedBuilder.getProteinHitMaxCapacity())
                .setMinPeptideLength(dbSearchParameters.getPeptideLenMin())
                .setMaxPeptideLength(dbSearchParameters.getPeptideLenMax())
                .build();
        }
    }

    private Pair<UUID, Integer> getLibraryIdAndEntryCount(User user, String nameOrId) {
        UUID slId;
        int slEntryCount;
        try {
            slId = UUID.fromString(nameOrId);
            List<SpectralLibraryListItem> librariesWithName = spectralLibraryCommunication.query(SpectralLibraryCommandFactory.actingAsUserId(user.userId()).queryById(slId))
                .thenApply(response -> response.getList().getSpectralLibraryList())
                .toCompletableFuture()
                .join();
            if (librariesWithName.size() > 1)
                throw new ParserException("Found more then one Spectral Library with id " + slId);
            if (librariesWithName.size() == 0)
                throw new ParserException("Did not find any Spectral Library with id " + slId);
            slEntryCount = librariesWithName.get(0).getEntryCount();
        } catch (IllegalArgumentException ignore) {
            List<SpectralLibraryListItem> librariesWithName = spectralLibraryCommunication
                .query(SpectralLibraryCommandFactory.actingAsUserId(user.userId()).queryByName(nameOrId))
                .thenApply(response -> response.getList().getSpectralLibraryList())
                .toCompletableFuture()
                .join();

            if (librariesWithName.size() > 1)
                throw new ParserException("Found more then one Spectral Library with name " + nameOrId);
            if (librariesWithName.size() == 0)
                throw new ParserException("Did not find any Spectral Library with name " + nameOrId);

            slId = UUID.fromString(librariesWithName.get(0).getSlId());
            slEntryCount = librariesWithName.get(0).getEntryCount();
        }
        return Pair.create(slId, slEntryCount);
    }

    public Optional<Pair<UUID, Integer>> getLibraryIdAndEntryCountFromAnalysis(User user, Analysis analysis) {
        if (!analysis.getWorkFlowsEnabledList().contains(WorkFlowType.SPECTRAL_LIBRARY)) { //user remove sl after adding library
            return Optional.empty();
        }
        SlSearchStepParameters slSearchStepParameters = analysis.getSlStep().getSlSearchStepParameters();
        String nameOrId = slSearchStepParameters.getSlIdOrName();
        if (nameOrId.equals("")) {
            return Optional.empty();
        }
        return Optional.of(getLibraryIdAndEntryCount(user, nameOrId));
    }

    public SlSearchParameters getSlSearchParameters(User user, Analysis analysis) {
        SlSearchStepParameters slSearchStepParameters = analysis.getSlStep().getSlSearchStepParameters();

        String nameOrId = slSearchStepParameters.getSlIdOrName();
        Pair<UUID, Integer> libraryIdAndEntryCount = getLibraryIdAndEntryCount(user, nameOrId);
        UUID slId = libraryIdAndEntryCount.first();
        int slEntryCount = libraryIdAndEntryCount.second();

        String conservedJSON = localConfig.getConfig("peaks.parameters.SlConservedParameters").root()
                .render(ConfigRenderOptions.concise());

        SlConservedParameters.Builder builder = SlConservedParameters.newBuilder();
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(conservedJSON, builder);
        } catch (IOException e) {
            throw new ParserException("Unable to load conserved parameters for spectral library");
        }

        SlSearchParameters parameters = SlSearchParameters.newBuilder()
            .setSpectralLibraryEntryCount(slEntryCount)
            .setSlSearchSharedParameters(slSearchStepParameters.getSlSearchSharedParameters())
            .setSpectraLibraryId(ModelConversion.uuidsToByteString(slId))
            .setSlConservedParameters(builder.build())
            .build();
        return parameters;
    }

    public SlFilterSummarizationParameters getSlFilterSummarizationParameters(User user, SlStep slStep) {
        IdentificaitonFilterSummarization filters = slStep.getIdentificaitonFilterSummarization();
        IdentificaitonFilter.Builder filterBuilder = IdentificaitonFilter.newBuilder()
            .setFdrType(filters.getFdrType());
        final SlFilter.PeptideFilter.Builder slPeptideFilterBuilder = SlFilter.PeptideFilter.newBuilder();
        switch (filters.getPsmFilterTypeCase()) {
            case MINPVALUE:
                if (filters.getFdrType() == FdrType.PEPTIDE) {
                    slPeptideFilterBuilder.setUseFdr(false)
                        .setIncludeDecoy(false)
                        .setMinPValue(filters.getMinPValue());
                }

                filterBuilder.setMinPValue((float) filters.getMinPValue());
                break;
            case MAXFDR:
                if (filters.getFdrType() == FdrType.PEPTIDE) {
                    slPeptideFilterBuilder.setUseFdr(true)
                        .setIncludeDecoy(false)
                        .setMaxFDR(filters.getMaxFdr());
                }
                filterBuilder.setMaxFdr((float) filters.getMaxFdr());
                break;
        }

        if (filters.getFdrType() == FdrType.PEPTIDE) {
            filterBuilder.setSlPeptideFilter(slPeptideFilterBuilder.build());
        }

        return SlFilterSummarizationParameters.newBuilder()
            .setFilters(filterBuilder)
            .build();
    }

    public SlFilterSummarizationParameters getSlFilterSummarizationParameters(User user, DbSearchStep dbSearchStep) {
        IdentificaitonFilterSummarization filters = dbSearchStep.getFilterSummarization();
        IdentificaitonFilter.Builder filterBuilder = IdentificaitonFilter.newBuilder()
            .setFdrType(filters.getFdrType());
        final SlFilter.PeptideFilter.Builder slPeptideFilterBuilder = SlFilter.PeptideFilter.newBuilder();
        switch (filters.getPsmFilterTypeCase()) {
            case MINPVALUE:
                if (filters.getFdrType() == FdrType.PEPTIDE) {
                    slPeptideFilterBuilder.setUseFdr(false)
                        .setIncludeDecoy(false)
                        .setMinPValue(filters.getMinPValue());
                }

                filterBuilder.setMinPValue((float) filters.getMinPValue());
                break;
            case MAXFDR:
                if (filters.getFdrType() == FdrType.PEPTIDE) {
                    slPeptideFilterBuilder.setUseFdr(true)
                        .setIncludeDecoy(false)
                        .setMaxFDR(filters.getMaxFdr());
                }
                filterBuilder.setMaxFdr((float) filters.getMaxFdr());
                break;
        }

        if (filters.getFdrType() == FdrType.PEPTIDE) {
            filterBuilder.setSlPeptideFilter(slPeptideFilterBuilder.build());
        }

        return SlFilterSummarizationParameters.newBuilder()
            .setFilters(filterBuilder)
            .build();
    }

    public SilacParameters getSilacParameters(User user, Analysis analysis, WorkFlowType type) {
        SilacParameters silacParameters = analysis.getSilacStep().getSilacParameters();
        final SilacParameters correctSilacParameters;
        SilacQMethod silacQMethod = silacParameters.getSilacQMethod();
        switch (type) {
            case SILAC_FRACTION_ID_TRANSFER:
                if (silacParameters.getTransferOnlyGroup().getSamplesCount() < 1) {
                    throw new IllegalStateException("SILAC Fraction Id Transfer must contain samples for transfer.");
                }
                if (!silacParameters.getEnableMatchBetweenRuns()) {
                    throw new IllegalStateException("SILAC Fraction Id Transfer must include match between runs.");
                }
            default:
                //do nothing
        }
        if (silacQMethod.getLabelsList().size() == 0) {
            SilacQMethod fixedMethod = querySilacQMethod(user, analysis)
                .orElseThrow(() -> new IllegalStateException("Cannot find Silac Q Method with name " + silacQMethod.getMethodName()));
            correctSilacParameters = silacParameters.toBuilder()
                .setSilacQMethod(fixedMethod)
                .build();
        } else {
            correctSilacParameters = silacParameters;
        }
        //validate silac parameters
        final int labelCount = correctSilacParameters.getSilacQMethod().getLabelOrderCount();
        final SilacNormalization correctedNormalizations = correctSilacParameters.getNormalization();
        final List<SilacParameters.Group> correctedSilacGroups = correctSilacParameters.getGroupsList();
        //validate conditions
        if (correctSilacParameters.getConditionsCount() != labelCount) {
            throw new IllegalStateException("Silac parameters must have " + labelCount + " conditions, but only found " + silacParameters.getConditionsCount());
        }

        //validate expected ratios are correct size
        for(SilacNormalization.ExpectedRatios expectedRatios : correctedNormalizations.getExpectedRatiosList()) {
            if (expectedRatios.getExpectedRatioCount() != labelCount) {
                throw new ParserException("Silac normalization for sample " + expectedRatios.getSampleName()
                    + " must have " + labelCount + " expected ratios, but only found " + expectedRatios.getExpectedRatioCount());
            }
        }

        //validate all samples in groups have correct condition count
        for (SilacParameters.Group group : correctedSilacGroups) {
            for (SilacParameters.Sample sample : group.getSamplesList()) {
                if (sample.getLabelToConditionCount() != labelCount) {
                    throw new ParserException("Silac sample " + sample.getSampleName() + " must have " + labelCount + " conditions, but only found " + sample.getLabelToConditionCount());
                }
            }
        }

        //validate all samples in groups are found in normalization
        Set<String> sampleIds = correctedSilacGroups.stream()
            .flatMap(group -> group.getSamplesList().stream())
            .map(sample -> sample.getSampleId())
            .collect(Collectors.toSet());
        if (sampleIds.size() != correctedNormalizations.getExpectedRatiosCount()) {
            throw new ParserException("SilacParameters are expected to have the same samples as silac normalization");
        }
        for(SilacNormalization.ExpectedRatios expectedRatios : correctedNormalizations.getExpectedRatiosList()) {
            if (!sampleIds.contains(expectedRatios.getSampleId())) {
                throw new ParserException("Missing sampleId " + expectedRatios.getSampleId() + " in normalization");
            }
        }

        return correctSilacParameters;
    }

    public ReporterIonQBatchCalculationParameters getReporterIonQParameters(User user, Analysis analysis) {
        ReporterIonQParameters parameters = analysis.getReporterIonQStep().getReporterIonQParameters();
        String conservedJSON = localConfig.getConfig("peaks.parameters.LabeledQQuantificationConserved").root()
            .render(ConfigRenderOptions.concise());
        ReporterIonQConservedParameters.Builder builder = ReporterIonQConservedParameters.newBuilder();

        final ReporterIonQMethodCommandFactory reporterIonQMethodCommandFactory = ReporterIonQMethodCommandFactory.actingAsUserId(user.userId().toString());
        final ReporterIonQMethodResponse.Entries join = reporterIonQMethodManagerCommunication
            .query(reporterIonQMethodCommandFactory.queryByName(parameters.getMethod().getMethodName()))
            .thenApply(ReporterIonQMethodResponse::getEntries)
            .toCompletableFuture().join();

        float quantError = parameters.getQuantError();
        if (Float.compare(quantError, ((int) (quantError * 1000f) / 1000f)) != 0) {
            throw new ParserException("Invalid reporter ion q mass error tolerance. Precision can only be up to 3 decimals.");
        }

        if (join.getEntriesCount() != 1) {
            throw new ParserException("Invalid reporter ion q method name.");
        }

        final ReporterIonQMethod data = join.getEntries(0).getData();

        try {
            JsonFormat.parser().ignoringUnknownFields().merge(conservedJSON, builder);
        } catch (IOException e) {
            throw new ParserException("Unable to load conserved parameters for reporter ion Q");
        }
        ReporterIonQConservedParameters conservedParameters = builder.build();
        return ReporterIonQBatchCalculationParameters.newBuilder()
            .setParameters(ReporterIonQParameters.newBuilder()
                    .mergeFrom(parameters)
                    .setMethod(data)
                    .build())
            .setConservedParameters(conservedParameters)
            .build();
    }

    public ReporterIonQFilterSummarization getReporterIonQFilterSummarization(Analysis dtoAnalysis) {
        ReporterIonQFilterSummarization filterSummarization = dtoAnalysis.getReporterIonQStep().getFilterSummarization();
        ReporterIonQSpectrumFilter.Builder spectrumFilterBuilder = filterSummarization.getSpectrumFilter().toBuilder();
        ReporterIonQExperimentSettings experimentSettings = filterSummarization.getExperimentSettings();
        ReporterIonQExperimentSettings.ReporterIonQNormalization normalizationParameters = experimentSettings.getReporterQNormalization();
        if (experimentSettings.getGroupsCount() <= 1) {
            throw new ParserException("Invalid TMT/iTraq Groups must have 2 or more groups.");
        }

        final int numChannels = experimentSettings.getAliasesCount() / dtoAnalysis.getSampleIdsCount();
        final int expectedRatioCount = experimentSettings.getAllExperiments() ? experimentSettings.getAliasesCount() : numChannels;
        switch (normalizationParameters.getNormalizationMethod()) {
            case SPIKE_NORMALIZATION: {
                int ratiosCount = normalizationParameters.getSpikedExpectedRatiosCount();
                if (ratiosCount != expectedRatioCount)
                    throw new ParserException("Invalid number of expected ratios provided " + ratiosCount + " expected " + expectedRatioCount);
                break;
            }
            case MANUAL_NORMALIZATION: {
                int ratiosCount = normalizationParameters.getManualExpectedRatiosCount();
                if (ratiosCount != expectedRatioCount)
                    throw new ParserException("Invalid number of expected ratios provided " + ratiosCount + " expected " + expectedRatioCount);
                break;
            }
            case NO_NORMALIZATION:
            case AUTO_NORMALIZATION:
            default:
                //do nothing
        }

        if (experimentSettings.getAllExperiments()) {
            //intelligent error messages telling user how to correctly run their analysis
            Set<String> exprSamples = experimentSettings.getGroupsList().stream()
                .flatMap(group -> group.getSampleIndexesList().stream())
                .map(aliasIdx -> experimentSettings.getAliases(aliasIdx).getSampleId())
                .collect(Collectors.toSet());
            dtoAnalysis.getSampleIdsList().forEach(sample -> {
                    if (!exprSamples.contains(sample))
                        throw new ParserException("Invalid TMT/iTRAQ experiment settings. Run the analysis without the unselected samples.");
                }
            );
        }

        if (dtoAnalysis.getDbSearchStep().getFilterSummarization().getPsmFilterTypeCase() == MAXFDR) {
            spectrumFilterBuilder.setMaxFDR((float) dtoAnalysis.getDbSearchStep().getFilterSummarization().getMaxFdr());
        } else {
            spectrumFilterBuilder.setMinPValue((float) dtoAnalysis.getDbSearchStep().getFilterSummarization().getMinPValue());
        }

        ReporterIonQSpectrumFilter spectrumFilter = spectrumFilterBuilder.build();
        if (spectrumFilter.getFilterTypeCase().equals(ReporterIonQSpectrumFilter.FilterTypeCase.MAXFDR)
            && spectrumFilter.getMaxFDR() > 1 || spectrumFilter.getMaxFDR() < 0) {
            throw new ParserException("Invalid spectrum filter max fdr must be between 0 and 100%");
        }

        return filterSummarization.toBuilder().setSpectrumFilter(spectrumFilter).build();
    }

    public DiaFeatureExtractorParameters getDiaFeatureExtractorParameters(Analysis analysis) {
        final LfqParameters lfqParameters = analysis.getLfqStep().getLfqParameters();
        return DiaFeatureExtractorParameters.newBuilder()
            .setRtShiftTolerance((float)lfqParameters.getRtShift())
            .setAutoDetectRtTolerance(lfqParameters.getAutoDetectTolerance())
            .setCcsTolerance((float)lfqParameters.getK0Tolerance())
            .setFeatureIntensity((float)lfqParameters.getFeatureIntensityThreshold())
            .build();
    }

    public LabelFreeQuantificationParameters getLfqParameters(Analysis analysis) {
        final LabelFreeQuantificationConservedParameters labelFreeQuantificationConservedParameters = PeaksParameters.fromConfig(
            localConfig.getConfig(LabelFreeQuantificationConservedParameters.ROOT + LabelFreeQuantificationConservedParameters.SECTION),
            LabelFreeQuantificationConservedParameters.class);
        final LfqParameters lfqParameters = analysis.getLfqStep().getLfqParameters();
        final List<UUID> sampleIds = analysis.getSampleIdsList().stream().map(UUID::fromString).collect(Collectors.toList());

        if (lfqParameters.getFdrThreshold() < 0 || lfqParameters.getFdrThreshold() > 1) {
            throw new ParserException("Invalid lfq fdr threshold " + lfqParameters.getFdrThreshold());
        }

        final boolean attachIdentification = analysis.getWorkFlowsEnabledList().indexOf(WorkFlowType.DB_SEARCH) > 0
            || analysis.getWorkFlowsEnabledList().indexOf(WorkFlowType.SPECTRAL_LIBRARY) >= 0;
        return new LabelFreeQuantificationParametersBuilder()
            .labelFreeQuantificationConservedParameters(labelFreeQuantificationConservedParameters)
            .massErrorTolerance((float) lfqParameters.getMassErrorTolerance().getParam())
            .isMassToleranceInPpm(lfqParameters.getMassErrorTolerance().getUnit().equals(MassUnit.PPM))
            .rtShift((float)lfqParameters.getRtShift())
            .k0Tolerance((float)lfqParameters.getK0Tolerance())
            .fdrThreshold((float)lfqParameters.getFdrThreshold())
            .attachIdentification(attachIdentification)
            .groups(getLabelFreeGroups(lfqParameters.getGroupsList(), sampleIds))
            .minIntensity((float)lfqParameters.getFeatureIntensityThreshold())
            .fdrType(lfqParameters.getFdrType())
            .autoDetectTolerance(lfqParameters.getAutoDetectTolerance())
            .build();
    }

    @NotNull
    private Iterable<LabelFreeGroup> getLabelFreeGroups(List<Group> groupList, List<UUID> sampleIds) {
        final Iterable<LabelFreeGroup> groups;
        if (groupList.size() == 0) {
            groups = IntStream.range(0, sampleIds.size()).mapToObj(i -> createDefaultGroup(i, sampleIds.get(i)))::iterator;
        } else {
            groups = Iterables.transform(groupList, g -> DtoAdaptors.group(g, sampleIds));
        }

        return groups;
    }

    public RetentionTimeAlignmentParameters getRtAlignmentParameters(Analysis analysis) {
        LfqParameters lfqParameters = analysis.getLfqStep().getLfqParameters();
        final List<UUID> sampleIds = analysis.getSampleIdsList().stream().map(UUID::fromString).collect(Collectors.toList());

        List<UUID> sampleIdsForIdTransfer = lfqParameters.getSamplesForFractionIdTransfer().getSampleIndexesList().stream()
            .map(sampleIndex -> sampleIds.get(sampleIndex))
            .collect(Collectors.toList());

        return new RetentionTimeAlignmentParametersBuilder()
            .massTolerance((float) lfqParameters.getMassErrorTolerance().getParam())
            .isMassToleranceInPpm(lfqParameters.getMassErrorTolerance().getUnit().equals(MassUnit.PPM))
            .fdrThreshold((float)lfqParameters.getFdrThreshold())
            .groups(getLabelFreeGroups(lfqParameters.getGroupsList(), sampleIds))
            .minIntensity((float)lfqParameters.getFeatureIntensityThreshold())
            .fdrType(lfqParameters.getFdrType())
            .addAllSampleIdsForIdTransfer(sampleIdsForIdTransfer)
            .build();
    }

    public FeatureDetectionParameters getFeatureDetectionParameters() {
        Config config = ConfigFactory.empty()
            .withFallback(localConfig);

        FeatureDetectionConservedParameters featureDetectionConservedParameters = PeaksParameters.fromConfig(
            config.getConfig(FeatureDetectionConservedParameters.ROOT + FeatureDetectionConservedParameters.SECTION),
            FeatureDetectionConservedParameters.class);

        FeatureDetectionParameters feaDetParam = PeaksParameters.fromConfig(
            config.getConfig(FeatureDetectionParameters.ROOT + FeatureDetectionParameters.SECTION),
            FeatureDetectionParameters.class);

        return new FeatureDetectionParametersBuilder()
            .conservedParameters(featureDetectionConservedParameters)
            .isRemoveNoise(feaDetParam.isRemoveNoise())
            .noiseRemoveThreshold(feaDetParam.noiseRemoveThreshold())
            .maxIsotopeNumForArea(feaDetParam.maxIsotopeNumForArea())
            .build();
    }

    public DataRefineParameters getDataRefineParameters() {
        Config config = ConfigFactory.empty()
            .withFallback(localConfig);
        return PeaksParameters.fromConfig(config.getConfig(DataRefineParameters.ROOT + DataRefineParameters.SECTION),
            DataRefineParameters.class);
    }

    public DenovoFilter getDenovoFilterSummarizationParameters(Analysis dtoAnalysis) {
        if (dtoAnalysis.getDenovoStep().hasFilterSummarization()) {
            return DenovoFilter.newBuilder()
                .setMinDenovoAlc((float) dtoAnalysis.getDenovoStep().getFilterSummarization().getMinAlc())
                .build();
        }
        return DenovoFilter.newBuilder()
            .setMinDenovoAlc(DenovoCandidateFilter.DEFAULT.minAlc())
            .build();
    }

    public IdentificaitonFilter getIdentificationFilterSummarizationParameters(IdentificaitonFilterSummarization dtoFilterSummarization) {
        IdentificaitonFilter.Builder builder = IdentificaitonFilter.newBuilder();
        builder
            .setMinMutationIonIntensity((float)dtoFilterSummarization.getMinIonMutationIntensity())
            .setMinDenovoAlc((float)dtoFilterSummarization.getMinDenovoAlc())
            .setFdrType(dtoFilterSummarization.getFdrType());

        switch (dtoFilterSummarization.getFdrType()) {
            case PSM:
                break; //do nothing
            case PEPTIDE:
                builder.setIdPeptideFilter(IDFilter.PeptideFilter.newBuilder()
                    .setMinPValue(dtoFilterSummarization.getMinPValue())
                    .setMaxFDR(dtoFilterSummarization.getMaxFdr())
                    .setUseFdr(dtoFilterSummarization.getPsmFilterTypeCase() == MAXFDR)
                    .build());
        }

        switch (dtoFilterSummarization.getPsmFilterTypeCase()) {
            case MAXFDR:
                return builder.setMaxFdr((float)dtoFilterSummarization.getMaxFdr()).build();
            case MINPVALUE:
                return builder.setMinPValue((float)dtoFilterSummarization.getMinPValue()).build();
            default:
                throw new IllegalStateException("Must set PSM filter to be either FDR (%) or P-Value");
        }
    }

    public IdentificaitonFilter getDbFilterSummarizationParameters(Analysis dtoAnalysis) {
        return getIdentificationFilterSummarizationParameters(dtoAnalysis.getDbSearchStep().getFilterSummarization())
            .toBuilder()
            .setMinMutationIonIntensity(0f)
            .build();
    }

    public IdentificaitonFilter getPtmFilterSummarizationParameters(Analysis dtoAnalysis) {
        return getIdentificationFilterSummarizationParameters(dtoAnalysis.getPtmFinderStep().getFilterSummarization())
            .toBuilder()
            .setMinMutationIonIntensity(0f)
            .build();
    }

    public IdentificaitonFilter getSpiderFilterSummarizationParameters(Analysis dtoAnalysis) {
        return getIdentificationFilterSummarizationParameters(dtoAnalysis.getSpiderStep().getFilterSummarization())
            .toBuilder()
            .build();
    }

    public DenovoOnlyTagSearchParameters getDbDenovoOnlyTagSearchParameters(Analysis dtoAnalysis) {
        return dtoAnalysis.getDbSearchStep().getFilterSummarization().getDenovoOnlyTagSearch().toBuilder()
            .setMinAlc((float) dtoAnalysis.getDbSearchStep().getFilterSummarization().getMinDenovoAlc())
            .build();
    }

    public DenovoOnlyTagSearchParameters getPtmFinderDenovoOnlyTagSearchParameters(Analysis dtoAnalysis) {
        return dtoAnalysis.getPtmFinderStep().getFilterSummarization().getDenovoOnlyTagSearch().toBuilder()
            .setMinAlc((float) dtoAnalysis.getPtmFinderStep().getFilterSummarization().getMinDenovoAlc())
            .build();
    }

    public DenovoOnlyTagSearchParameters getSpiderDenovoOnlyTagSearchParameters(Analysis dtoAnalysis) {
        return dtoAnalysis.getSpiderStep().getFilterSummarization().getDenovoOnlyTagSearch().toBuilder()
            .setMinAlc((float) dtoAnalysis.getSpiderStep().getFilterSummarization().getMinDenovoAlc())
            .build();
    }

    public LfqFilter getLfqFilter(Analysis dtoAnalysis) {
        LfqFilter lfqFilter = dtoAnalysis.getLfqStep().getLfqFilter();
        LfqFilter.LabelFreeQueryParameters lfqParameters = lfqFilter.getLabelFreeQueryParameters();

        int numSamples = dtoAnalysis.getSampleIdsCount();
        switch (lfqParameters.getNormalizationMethod()) {
            case SPIKE_NORMALIZATION: {
                int ratiosCount = lfqParameters.getSpikedExpectedRatiosCount();
                if (ratiosCount != numSamples)
                    throw new ParserException("Invalid number of expected ratios provided " + ratiosCount +" expected " + numSamples);
                break;
            }
            case MANUAL_NORMALIZATION: {
                int ratiosCount = lfqParameters.getManualExpectedRatiosCount();
                if (ratiosCount != numSamples)
                    throw new ParserException("Invalid number of expected ratios provided " + ratiosCount +" expected " + numSamples);
                break;
            }
            case NO_NORMALIZATION:
            case AUTO_NORMALIZATION:
            default:
                //do nothing
        }

        if (lfqParameters.getGroupsCount() > 0) return lfqFilter;

        List<Group> groups = IntStream.range(0, numSamples)
            .mapToObj(i -> Group.newBuilder()
                .setColour(ColourHandler.getNextColour())
                .setGroupName("Group " + (i + 1)) //add 1 so group counts start at 1
                .addSampleIndexes(i)
                .build())
            .collect(Collectors.toList());

        return LfqFilter.newBuilder()
            .mergeFrom(lfqFilter)
            .setLabelFreeQueryParameters(LfqFilter.LabelFreeQueryParameters.newBuilder()
                .mergeFrom(lfqParameters)
                .addAllGroups(groups)
                .build())
            .build();
    }

    private Optional<SilacQMethod> querySilacQMethod(User user, Analysis analysis) {
        if (!analysis.getWorkFlowsEnabledList().contains(WorkFlowType.SILAC) &&
            !analysis.getWorkFlowsEnabledList().contains(WorkFlowType.SILAC_FRACTION_ID_TRANSFER)) {
            return Optional.empty();
        }
        String methodName = analysis.getSilacStep().getSilacParameters().getSilacQMethod().getMethodName();
        SilacQMethodCommandFactory silacQMethodCommandFactory = SilacQMethodCommandFactory.actingAsUserId(user.userId().toString());
        final SilacQMethodResponse.Entries join = silacQMethodManagerCommunication
            .query(silacQMethodCommandFactory.queryByName(methodName))
            .thenApply(SilacQMethodResponse::getEntries)
            .toCompletableFuture().join();

        if (join.getEntriesCount() != 1) {
            throw new ParserException("Invalid reporter ion q method name.");
        }

        return Optional.of(join.getEntries(0).getData());
    }

    private List<String> silacVariableModifications(User user, Analysis analysis) {
        return querySilacQMethod(user, analysis)
            .map(method -> method.getLabelsList().stream()
                .map(label -> label.getModification().getName())
                .filter(modi -> !modi.isEmpty())
                .collect(Collectors.toList())
            )
            .orElse(Collections.emptyList());
    }

    private Pair<List<String>, List<String>> riqFixedAndVariableModifications(User user, Analysis analysis) {
        if (analysis.getWorkFlowsEnabledList().indexOf(WorkFlowType.REPORTER_ION_Q) < 0) {
            return Pair.create(Collections.emptyList(), Collections.emptyList());
        }
        String methodName = analysis.getReporterIonQStep().getReporterIonQParameters().getMethod().getMethodName();
        final ReporterIonQMethodCommandFactory reporterIonQMethodCommandFactory = ReporterIonQMethodCommandFactory.actingAsUserId(user.userId().toString());
        final ReporterIonQMethodResponse.Entries join = reporterIonQMethodManagerCommunication
            .query(reporterIonQMethodCommandFactory.queryByName(methodName))
            .thenApply(ReporterIonQMethodResponse::getEntries)
            .toCompletableFuture().join();

        if (join.getEntriesCount() != 1) {
            throw new ParserException("Invalid reporter ion q method name.");
        }

        final ReporterIonQMethod data = join.getEntries(0).getData();
        return Pair.create(data.getFixedModificationNamesList(), data.getVariableModificationNamesList());
    }

    private List<String> mergeModis(List<String> a, List<String> b) {
         return Stream.concat(a.stream(), b.stream().filter(m -> !a.contains(m)))
             .sorted()
             .collect(Collectors.toList());
    }

    private List<String> mergeModis(List<String> ... lists) {
        Stream<String> modis = Stream.empty();
        for(int i = 0; i < lists.length; i++) {
            modis = Stream.concat(modis, lists[i].stream());
        }
        return modis
            .sorted()
            .distinct()
            .collect(Collectors.toList());
    }

    private boolean hasQ(Analysis analysis){
        return analysis.getWorkFlowsEnabledList().contains(WorkFlowType.LFQ)
            || analysis.getWorkFlowsEnabledList().contains(WorkFlowType.REPORTER_ION_Q);
    }
}
