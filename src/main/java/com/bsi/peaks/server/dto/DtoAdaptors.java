package com.bsi.peaks.server.dto;

import com.bsi.peaks.analysis.analysis.dto.DataRefinementStep;
import com.bsi.peaks.analysis.analysis.dto.IdentificaitonFilterSummarization;
import com.bsi.peaks.analysis.parameterModels.dto.Database;
import com.bsi.peaks.analysis.parameterModels.dto.DbSearchParameters;
import com.bsi.peaks.analysis.parameterModels.dto.DeNovoParameters;
import com.bsi.peaks.analysis.parameterModels.dto.EnzymeSpecificity;
import com.bsi.peaks.analysis.parameterModels.dto.FeatureDetectionConservedParameters;
import com.bsi.peaks.analysis.parameterModels.dto.LfqParameters;
import com.bsi.peaks.analysis.parameterModels.dto.MassUnit;
import com.bsi.peaks.analysis.parameterModels.dto.PtmFinderParameters;
import com.bsi.peaks.analysis.parameterModels.dto.SlProteinInferenceStepParameters;
import com.bsi.peaks.analysis.parameterModels.dto.SlSearchStepParameters;
import com.bsi.peaks.analysis.parameterModels.dto.SpiderParameters;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.model.vm.EnzymeVM;
import com.bsi.peaks.data.model.vm.EnzymeVMBuilder;
import com.bsi.peaks.data.model.vm.InstrumentVM;
import com.bsi.peaks.data.model.vm.InstrumentVMBuilder;
import com.bsi.peaks.data.model.vm.ModificationVM;
import com.bsi.peaks.data.model.vm.ModificationVMBuilder;
import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.event.uploader.FileToUpload;
import com.bsi.peaks.event.uploader.UploadFiles;
import com.bsi.peaks.internal.task.DiaDbSearchParameters;
import com.bsi.peaks.internal.task.DiaDbSearchSharedParameters;
import com.bsi.peaks.internal.task.DiaFeatureExtractorParameters;
import com.bsi.peaks.internal.task.IdentificaitonFilter;
import com.bsi.peaks.internal.task.ReporterIonQBatchCalculationParameters;
import com.bsi.peaks.internal.task.SlSearchParameters;
import com.bsi.peaks.io.writer.dto.DtoConversion;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.core.IonSource;
import com.bsi.peaks.model.core.ms.MassAnalyzer;
import com.bsi.peaks.model.core.ms.MassAnalyzerBuilder;
import com.bsi.peaks.model.core.ms.MassAnalyzerType;
import com.bsi.peaks.model.core.quantification.labelfree.LabelFreeGroup;
import com.bsi.peaks.model.core.quantification.labelfree.LabelFreeGroupBuilder;
import com.bsi.peaks.model.dto.FileUpload;
import com.bsi.peaks.model.dto.FilterList;
import com.bsi.peaks.model.dto.Group;
import com.bsi.peaks.model.dto.IDFilter;
import com.bsi.peaks.model.dto.IdResultInfo;
import com.bsi.peaks.model.dto.IonMobilityType;
import com.bsi.peaks.model.dto.LfqFilter;
import com.bsi.peaks.model.dto.LfqProteinFeatureVectorFilter;
import com.bsi.peaks.model.dto.ReporterIonQFilterSummarization;
import com.bsi.peaks.model.dto.ReporterIonQParameters;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.filter.DenovoCandidateFilter;
import com.bsi.peaks.model.filter.ProteinFeatureVectorFilter;
import com.bsi.peaks.model.filter.PsmFilter;
import com.bsi.peaks.model.filter.RiqProteinVectorFilter;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.model.parameters.DenovoParameters;
import com.bsi.peaks.model.parameters.FeatureDetectionParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationConservedParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationConservedParametersBuilder;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParameters;
import com.bsi.peaks.model.parameters.LabelFreeQuantificationParametersBuilder;
import com.bsi.peaks.model.parameters.RetentionTimeAlignmentParameters;
import com.bsi.peaks.model.proteomics.EnzymePatternBuilder;
import com.bsi.peaks.model.proteomics.ModificationBuilder;
import com.bsi.peaks.model.proteomics.fasta.FastaSequenceDatabase;
import com.bsi.peaks.model.proteomics.fasta.ProteinHash;
import com.bsi.peaks.model.proteomics.fasta.TaxonomyMapper;
import com.bsi.peaks.model.proto.EnzymePattern;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.model.proto.SimplifiedFastaSequenceDatabaseInfo;
import com.bsi.peaks.model.query.DbSearchQuery;
import com.bsi.peaks.model.query.DenovoQuery;
import com.bsi.peaks.model.query.IdentificationType;
import com.bsi.peaks.model.query.LabelFreeQuery;
import com.bsi.peaks.model.query.PtmFinderQuery;
import com.bsi.peaks.model.query.Query;
import com.bsi.peaks.model.query.ReporterIonQMethodQuery;
import com.bsi.peaks.model.query.SilacQuery;
import com.bsi.peaks.model.query.SlQuery;
import com.bsi.peaks.model.query.SpiderQuery;
import com.bsi.peaks.model.system.Progress;
import com.bsi.peaks.model.system.levels.MemoryOptimizedFractionLevelTask;
import com.bsi.peaks.model.system.steps.WorkflowStepStep;
import com.bsi.peaks.server.es.state.ProjectAggregateState;
import com.bsi.peaks.server.es.state.ProjectTaskPlanState;
import com.bsi.peaks.server.es.state.ProjectTaskState;
import com.bsi.peaks.server.es.tasks.Helper;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bsi.peaks.io.writer.dto.DtoConversion.areaCalculationMethod;
import static com.bsi.peaks.io.writer.dto.DtoConversion.isSearchTypeSearchAll;
import static com.bsi.peaks.io.writer.dto.DtoConversion.massParameter;
import static com.bsi.peaks.io.writer.dto.DtoConversion.massUnitIsPpm;

public class DtoAdaptors {
    private final static Logger LOG = LoggerFactory.getLogger(DtoAdaptors.class);

    public static SimplifiedFastaSequenceDatabaseInfo simplifiedFastaSequenceDatabaseInfo(List<FastaSequenceDatabase> dbs, Database database, boolean isContaminant) {
        try {
            String idOrName = database.getId();
            FastaSequenceDatabase db = dbs.stream()
                .filter(item -> item.state() == FastaSequenceDatabase.State.READY &&
                    (item.id().toString().equals(idOrName) || item.name().equals(idOrName)))
                .findFirst()
                .orElseThrow(NoSuchElementException::new);

            SimplifiedFastaSequenceDatabaseInfo.Builder builder = SimplifiedFastaSequenceDatabaseInfo.newBuilder();

            Set<Integer> taxonomyIds = new HashSet<>();
            if (database.getTaxonmyNameList().size() == 0) {
                // if nothing is provided, use all species
                taxonomyIds.addAll(TaxonomyMapper.mapName("all species"));
                builder.addTaxonomyNames("all species");
            } else {
                for (String taxName : database.getTaxonmyNameList()) {
                    final String trimmedTaxName = taxName.trim();
                    taxonomyIds.addAll(TaxonomyMapper.mapName(trimmedTaxName));
                    builder.addTaxonomyNames(trimmedTaxName);
                }
            }

            final Map<Integer, Integer> totalSequenceByTaxonomyId = db.totalSequenceByTaxonomyId();
            final boolean taxonomyFastaSequencesExist = taxonomyIds.stream()
                .mapToInt(taxId -> totalSequenceByTaxonomyId.getOrDefault(taxId, 0))
                .anyMatch(i -> i > 0);
            if (!taxonomyFastaSequencesExist) {
                throw new IllegalArgumentException("Taxonomy ids reference no data");
            }

            Map<Integer, ProteinHash> hashByTaxonomyId = db.proteinHashByTaxonomyId();
            ProteinHash.Builder hashBuilder = new ProteinHash.Builder();
            taxonomyIds.stream()
                .sorted() // When comparing tasks, we need the ids in identical order.
                .forEach(taxId -> {
                    ProteinHash hash = hashByTaxonomyId.getOrDefault(taxId, ProteinHash.NONE);
                    hashBuilder.xor(hash);
                    builder.addTaxonomyIds(taxId);
                    builder.addTaxonomyHash(ByteString.copyFrom(hash.toBytes()));
                });

            return builder
                .setId(db.id().toString())
                .setIdPattern(db.idPatternString())
                .setDescriptionPattern(db.descriptionPatternString())
                .setIsContaminant(isContaminant)
                .setName(db.name())
                .setHash(ByteString.copyFrom(hashBuilder.build().toBytes()))
                .build();
        } catch (Exception e) {
            throw new IllegalStateException("Not able to find fasta database with name/id " + database.getId(), e);
        }
    }

    public static Group group(LabelFreeGroup group, List<UUID> orderSamples) {
        return Group.newBuilder()
            .setGroupName(group.groupName())
            .addAllSampleIndexes(Lists.transform(group.sampleIds(), orderSamples::indexOf))
            .setColour(group.hexColour())
            .build();
    }

    public static LabelFreeGroup group(Group group, List<UUID> orderSamples) {
        return new LabelFreeGroupBuilder()
            .addAllSampleIds(Lists.transform(group.getSampleIndexesList(), orderSamples::get))
            .groupName(group.getGroupName())
            .hexColour(group.getColour())
            .build();
    }

    public static DeNovoParameters denovoParameters(WorkflowStep step) {
        final DenovoParameters parameters = (DenovoParameters) Helper.workflowStepParameters(step);
        MassUnit precursorUnit = massUnitIsPpm(parameters.isPPMForPrecursorTol());
        MassUnit fragUnit = massUnitIsPpm(parameters.isPPMForFragmentTol());

        return DeNovoParameters.newBuilder()
            .setPrecursorTol(massParameter(parameters.precursorTol(), precursorUnit))
            .setFragmentTol(massParameter(parameters.fragmentTol(), fragUnit))
            .setMaxVariablePtmPerPeptide(parameters.maxVariablePtmPerPeptide())
            .addAllFixedModificationNames(parameters.fixedModificationNames())
            .addAllVariableModificationNames(parameters.variableModificationNames())
            .setEnzymeName(parameters.enzyme().name())
            .build();
    }

    public static IdentificaitonFilterSummarization filterSummarizationWithDenovoTagParameters(WorkflowStep parameters) {
        IdentificaitonFilterSummarization.Builder builder = IdentificaitonFilterSummarization.newBuilder();

        final IdentificaitonFilter identificaitonFilter;
        switch (parameters.getParametersCase()) {
            case DBFILTERSUMMARIZATIONPARAMETERS:
                identificaitonFilter = parameters.getDbFilterSummarizationParameters();
                break;
            case PTMFILTERSUMMARIZATIONPARAMETERS:
                identificaitonFilter = parameters.getPtmFilterSummarizationParameters();
                break;
            case SPIDERFILTERSUMMARIZATIONPARAMETERS:
                identificaitonFilter = parameters.getSpiderFilterSummarizationParameters();
                builder.setMinIonMutationIntensity(identificaitonFilter.getMinMutationIonIntensity());
                break;
            case SLFILTERSUMMARIZATIONPARAMETERS:
                identificaitonFilter = parameters.getSlFilterSummarizationParameters().getFilters();
                break;
            case DENOVOFILTERPARAMETERS:
            default:
                throw new IllegalStateException("Unable to convert filter summaization.");
        }

        switch (identificaitonFilter.getPsmFilterTypeCase()) {
            case MINPVALUE:
                builder.setMinPValue(Double.parseDouble(identificaitonFilter.getMinPValue() + ""));
                break;
            case MAXFDR:
                builder.setMaxFdr(Double.parseDouble(identificaitonFilter.getMaxFdr() + ""));
                break;
        }

        return builder
            .setMinDenovoAlc(identificaitonFilter.getMinDenovoAlc())
            .setFdrType(identificaitonFilter.getFdrType())
            .build();
    }

    public static DbSearchParameters dbSearchParametersFromDiaDb(
        DiaDbSearchParameters diaDbSearchParameters,
        List<SimplifiedFastaSequenceDatabaseInfo> dbs
    ) {
        DbSearchParameters.Builder builder = DbSearchParameters.newBuilder();
        for (SimplifiedFastaSequenceDatabaseInfo database : dbs) {
            if(database.getIsContaminant()) {
                builder.addContaminantDatabases(database(database));
            } else {
                builder.addDatabases(database(database));
            }
        }
        DiaDbSearchSharedParameters sharedParameters = diaDbSearchParameters.getDiaDbSearchSharedParameters();
        return builder
            .setPrecursorTol(diaDbSearchParameters.getPrecursorTol())
            .setFragmentTol(diaDbSearchParameters.getFragmentTol())
            .setMaxVariablePtmPerPeptide(sharedParameters.getMaxVariablePtmPerPeptide())
            .addAllFixedModificationNames(sharedParameters.getFixedModificationNamesList())
            .addAllVariableModificationNames(sharedParameters.getVariableModificationNamesList())
            .setEnzymeName(sharedParameters.getEnzyme().getName())
            .setMissCleavage(sharedParameters.getMissCleavage())
            .setEnzymeSpecificityName(diaDbSearchParameters.getEnzymeSpecificityName())
            .setDatabaseName(dbs.get(0).getName())
            .setPeptideLenMin(sharedParameters.getPeptideLenMin())
            .setPeptideLenMax(sharedParameters.getPeptideLenMax())
            .setPredictSpectrumIRT(true)
            .setCcsTolerance(diaDbSearchParameters.getCcsTolerance())
            .setPrecursorChargeMin(sharedParameters.getPrecursorChargeMin())
            .setPrecursorChargeMax(sharedParameters.getPrecursorChargeMax())
            .setPrecursorMzMin(sharedParameters.getPrecursorMzMin())
            .setPrecursorMzMax(sharedParameters.getPrecursorMzMax())
            .setFragmentIonMzMin(sharedParameters.getFragmentIonMzMin())
            .setFragmentIonMzMax(sharedParameters.getFragmentIonMzMax())
            .setEnableToleranceOptimization(diaDbSearchParameters.getEnableToleranceOptimization())
            .build();
    }

    public static Database.Builder database(SimplifiedFastaSequenceDatabaseInfo db) {
        return Database.newBuilder()
            .setId(db.getId())
            .addAllTaxonmyName(db.getTaxonomyNamesList())
            .addAllTaxonomyId(db.getTaxonomyIdsList());
    }

    public static DbSearchParameters dbSearchParameters(
        DBSearchParameters parameters,
        List<SimplifiedFastaSequenceDatabaseInfo> databases
    ) {
        MassUnit precursorUnit = massUnitIsPpm(parameters.isPPMForPrecursorTol());
        MassUnit fragUnit = massUnitIsPpm(parameters.isPPMForFragmentTol());
        EnzymeSpecificity enzymeSpecificity = DtoConversion.enzymeSpecificity(parameters.enzymeSpecificity());

        DbSearchParameters.Builder builder = DbSearchParameters.newBuilder();

        if (databases.size() > 2) {
            throw new IllegalArgumentException("Unexpected number of databases " + databases.size());
        }

        for(SimplifiedFastaSequenceDatabaseInfo dbInfo : databases) {
            if (dbInfo.getIsContaminant()) {
                builder.addContaminantDatabasesBuilder()
                    .setId(dbInfo.getId())
                    .addAllTaxonomyId(dbInfo.getTaxonomyIdsList())
                    .addAllTaxonmyName(dbInfo.getTaxonomyNamesList());
            } else {
                builder.addDatabasesBuilder()
                    .setId(dbInfo.getId())
                    .addAllTaxonomyId(dbInfo.getTaxonomyIdsList())
                    .addAllTaxonmyName(dbInfo.getTaxonomyNamesList());
            }
        }

        return builder
            .setPrecursorTol(massParameter(parameters.precursorTol(), precursorUnit))
            .setFragmentTol(massParameter(parameters.fragmentTol(), fragUnit))
            .addAllFixedModificationNames(parameters.fixedModificationNames())
            .addAllVariableModificationNames(parameters.variableModificationNames())
            .setMaxVariablePtmPerPeptide(parameters.maxVariablePtmPerPeptide())
            .setEnzymeName(parameters.enzyme().name())
            .setMissCleavage(parameters.missCleavage())
            .setEnzymeSpecificityName(enzymeSpecificity)
            .setPeptideLenMin(parameters.peptideLenMin())
            .setPeptideLenMax(parameters.peptideLenMax())
            .build();
    }

    public static PtmFinderParameters ptmFinderParameters(WorkflowStep step) {
        final com.bsi.peaks.model.parameters.PtmFinderParameters parameters = (com.bsi.peaks.model.parameters.PtmFinderParameters) Helper.workflowStepParameters(step);
        return PtmFinderParameters.newBuilder()
            .setDeNovoScoreThreshold(parameters.denovoScoreThreshold())
            .addAllFixedModificationNames(parameters.fixedModificationNames())
            .addAllVariableModificationNames(parameters.variableModificationNames())
            .setModificationSearchType(isSearchTypeSearchAll(parameters.allPTM()))
            .build();
    }

    public static SpiderParameters spiderParameters(WorkflowStep step) {
        return SpiderParameters.newBuilder().build();
    }

    public static com.bsi.peaks.analysis.analysis.dto.DataRefinementStep dataRefinementStep(ProjectAggregateState data, WorkflowStep step) {
        DataRefinementStep.Builder builder = DataRefinementStep.newBuilder().setId(step.getId());

        for (ProjectTaskState task : data.taskIdToTask().values()) {
            ProjectTaskPlanState taskPlan = data.taskPlanIdToTaskPlan().get(task.taskPlanId());
            if (taskPlan.stepType().equals(StepType.DATA_REFINE)) {
                if (task.progress().equals(Progress.DONE)) {
                    final MemoryOptimizedFractionLevelTask fractionLevelTask = (MemoryOptimizedFractionLevelTask) taskPlan.level();
                    builder.addFinishedFractionIds(fractionLevelTask.getFractionId().toString());
                }
            }
        }

        return builder.build();
    }

    public static com.bsi.peaks.analysis.analysis.dto.FeatureDetectionStep featureDetectionStep(WorkflowStep step) {
        final FeatureDetectionParameters parameters = (FeatureDetectionParameters) Helper.workflowStepParameters(step);
        return com.bsi.peaks.analysis.analysis.dto.FeatureDetectionStep.newBuilder()
            .setConserveredParameters(FeatureDetectionConservedParameters.newBuilder()
                .setAreaCalculationMethod(areaCalculationMethod(parameters.conservedParameters().useIntensityForArea()))
                .build())
            .setId(step.getId())
            .build();
    }

    public static LabelFreeQuantificationParameters fromDiaLfq(
        DiaFeatureExtractorParameters extractorParameters,
        LfqFilter lfqFilter,
        List<UUID> orderedSampleIds
    ) {
        List<LabelFreeGroup> labelFreeGroups = lfqFilter.getLabelFreeQueryParameters().getGroupsList().stream()
            .map(group -> group(group, orderedSampleIds))
            .collect(Collectors.toList());
        return new LabelFreeQuantificationParametersBuilder()
            .massErrorTolerance(0) //not used for dia
            .isMassToleranceInPpm(true)
            .rtShift(extractorParameters.getRtShiftTolerance())
            .fdrThreshold(0f)
            .k0Tolerance(extractorParameters.getCcsTolerance())
            .minIntensity(extractorParameters.getFeatureIntensity())
            .attachIdentification(true)
            .autoDetectTolerance(extractorParameters.getAutoDetectRtTolerance())
            .addAllGroups(labelFreeGroups)
            .labelFreeQuantificationConservedParameters(new LabelFreeQuantificationConservedParametersBuilder()
                .proteinMaxPeptideFeatureVectorsToUse(0)
                .maxRatio(64)
                .build())
            .build();
    }

    public static LfqParameters lfqParameters(WorkflowStep summarizeStep, RetentionTimeAlignmentParameters rtParameters, List<UUID> orderedSamples) {
        final LabelFreeQuantificationParameters parameters = (LabelFreeQuantificationParameters) Helper.workflowStepParameters(summarizeStep);
        MassUnit massErrorUnit = massUnitIsPpm(parameters.isMassToleranceInPpm());

        final Group idTransferGroup = Group.newBuilder()
            .setGroupName("ID Transfer Group")
            .addAllSampleIndexes(Lists.transform(rtParameters.sampleIdsForIdTransfer(), orderedSamples::indexOf))
            .build();

        return LfqParameters.newBuilder()
            .setMassErrorTolerance(massParameter(parameters.massErrorTolerance(), massErrorUnit))
            .setRtShift(parameters.rtShift())
            .setFdrThreshold(parameters.fdrThreshold())
            .setK0Tolerance(parameters.k0Tolerance())
            .addAllGroups(parameters.groups().stream()
                .map(g -> DtoAdaptors.group(g, orderedSamples))
                .collect(Collectors.toList()))
            .setFeatureIntensityThreshold(parameters.minIntensity())
            .setAutoDetectTolerance(parameters.autoDetectTolerance())
            .setSamplesForFractionIdTransfer(idTransferGroup)
            .build();
    }

    public static SlSearchStepParameters slSearchStepParameters(SlSearchParameters slSearchParameters) {
        return SlSearchStepParameters.newBuilder()
            .setSlSearchSharedParameters(slSearchParameters.getSlSearchSharedParameters())
            .setSlIdOrName(ModelConversion.uuidFrom(slSearchParameters.getSpectraLibraryId()).toString())
            .build();
    }

    public static SlProteinInferenceStepParameters slProteinInferenceStepParameters(
        List<SimplifiedFastaSequenceDatabaseInfo> databases
    ) {
        SlProteinInferenceStepParameters.Builder builder = SlProteinInferenceStepParameters.newBuilder();
        if (databases.size() > 2) {
            throw new IllegalArgumentException("Unexpected number of databases " + databases.size());
        }
        for(SimplifiedFastaSequenceDatabaseInfo dbInfo : databases) {
            if (dbInfo.getIsContaminant()) {
                builder.addContaminantDatabasesBuilder()
                    .setId(dbInfo.getId())
                    .addAllTaxonomyId(dbInfo.getTaxonomyIdsList())
                    .addAllTaxonmyName(dbInfo.getTaxonomyNamesList());
            } else {
                builder.addDatabasesBuilder()
                    .setId(dbInfo.getId())
                    .addAllTaxonomyId(dbInfo.getTaxonomyIdsList())
                    .addAllTaxonmyName(dbInfo.getTaxonomyNamesList());
            }
        }
        if (databases.size() == 0) {
            builder.setIncluded(false);
        } else {
            builder.setIncluded(true);
        }
        return builder.build();
    }

    public static IdResultInfo idResultInfo(Map<Double, Double> fdrs, Map<Integer, Double> fdrMapping) {
        IdResultInfo.Builder idResultInfo = IdResultInfo.newBuilder();
        Map<Double, Double> sortedFDRs = fdrs.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
        for (int x : fdrMapping.keySet()) {
            idResultInfo.addFdrMappingX(x);
            idResultInfo.addFdrMappingY(fdrMapping.get(x));
        }
        return idResultInfo.build();
    }

    public static FilterList IDFilters(Map<WorkFlowType, Query> queries) {
        FilterList.Builder filterList =  FilterList.newBuilder();

        Set<WorkFlowType> workFlowTypes = queries.keySet();
        if (workFlowTypes.contains(WorkFlowType.DENOVO)) {
            IDFilter.Builder denovoFilter = IDFilter.newBuilder();
            DenovoQuery denovoQuery = (DenovoQuery) queries.get(WorkFlowType.DENOVO);
            IDFilter.DenovoCandidateFilter denovoCandidateFilter =
                    convertDenovoCandidateFilter(denovoQuery.denovoCandidateFilter());
            denovoFilter.setIdentificationType(getIdentificationType(denovoQuery.identificationType()));
            denovoFilter.setDenovoCandidateFilter(denovoCandidateFilter);
            filterList.addIdFilters(denovoFilter.build());
        }

        if (workFlowTypes.contains(WorkFlowType.DB_SEARCH)) {
            IDFilter.Builder dbFilter = IDFilter.newBuilder();
            DbSearchQuery dbSearchQuery = (DbSearchQuery) queries.get(WorkFlowType.DB_SEARCH);

            IDFilter.DenovoCandidateFilter denovoCandidateFilter =
                    convertDenovoCandidateFilter(dbSearchQuery.denovoCandidateFilter());
            IDFilter.ProteinHitFilter proteinHitFilter = dbSearchQuery.proteinHitFilter().toDto();
            IDFilter.PsmFilter psmFilter = convertPsmFilter(dbSearchQuery.psmFilter());

            dbFilter.setIdentificationType(getIdentificationType(dbSearchQuery.identificationType()));
            dbFilter.setDenovoCandidateFilter(denovoCandidateFilter);
            dbFilter.setProteinHitFilter(proteinHitFilter);
            dbFilter.setPsmFilter(psmFilter);
            filterList.addIdFilters(dbFilter.build());
        }

        if (workFlowTypes.contains(WorkFlowType.PTM_FINDER)) {
            IDFilter.Builder ptmFilter = IDFilter.newBuilder();
            PtmFinderQuery ptmFinderQuery = (PtmFinderQuery) queries.get(WorkFlowType.PTM_FINDER);

            IDFilter.DenovoCandidateFilter denovoCandidateFilter =
                    convertDenovoCandidateFilter(ptmFinderQuery.denovoCandidateFilter());
            IDFilter.ProteinHitFilter proteinHitFilter = ptmFinderQuery.proteinHitFilter().toDto();
            IDFilter.PsmFilter psmFilter = convertPsmFilter(ptmFinderQuery.psmFilter());

            ptmFilter.setIdentificationType(getIdentificationType(ptmFinderQuery.identificationType()));
            ptmFilter.setDenovoCandidateFilter(denovoCandidateFilter);
            ptmFilter.setProteinHitFilter(proteinHitFilter);
            ptmFilter.setPsmFilter(psmFilter);
            filterList.addIdFilters(ptmFilter.build());
        }

        if (workFlowTypes.contains(WorkFlowType.SPIDER)) {
            IDFilter.Builder spiderFilter = IDFilter.newBuilder();
            SpiderQuery spiderQuery = (SpiderQuery) queries.get(WorkFlowType.SPIDER);

            IDFilter.DenovoCandidateFilter denovoCandidateFilter =
                    convertDenovoCandidateFilter(spiderQuery.denovoCandidateFilter());
            IDFilter.ProteinHitFilter proteinHitFilter = spiderQuery.proteinHitFilter().toDto();
            IDFilter.PsmFilter psmFilter = convertPsmFilter(spiderQuery.psmFilter());

            spiderFilter.setIdentificationType(getIdentificationType(spiderQuery.identificationType()));
            spiderFilter.setDenovoCandidateFilter(denovoCandidateFilter);
            spiderFilter.setProteinHitFilter(proteinHitFilter);
            spiderFilter.setPsmFilter(psmFilter);
            filterList.addIdFilters(spiderFilter.build());
        }

        if (workFlowTypes.contains(WorkFlowType.LFQ)) {
            LabelFreeQuery labelFreeQuery = (LabelFreeQuery) queries.get(WorkFlowType.LFQ);
            LfqProteinFeatureVectorFilter proteinFilter = ProteinFeatureVectorFilter.toDto(labelFreeQuery.proteinFeatureVectorFilter());
            filterList.setLfqProteinFeatureVectorFilter(proteinFilter);
        }

        if (workFlowTypes.contains(WorkFlowType.REPORTER_ION_Q)) {
            ReporterIonQMethodQuery riqQuery = (ReporterIonQMethodQuery) queries.get(WorkFlowType.REPORTER_ION_Q);
            RiqProteinVectorFilter riqProteinVectorFilter = riqQuery.proteinVectorFilter();
            filterList.setReporterIonIonQProteinFilter(RiqProteinVectorFilter.toDto(riqProteinVectorFilter));
        }

        if (workFlowTypes.contains(WorkFlowType.SPECTRAL_LIBRARY)) {
            SlQuery slQuery = (SlQuery) queries.get(WorkFlowType.SPECTRAL_LIBRARY);
            filterList.setSlFilter(slQuery.proto());
        }

        if (workFlowTypes.contains(WorkFlowType.DIA_DB_SEARCH)) {
            SlQuery slQuery = (SlQuery) queries.get(WorkFlowType.DIA_DB_SEARCH);
            filterList.setDiaDbFilter(slQuery.proto());
        }

        if (workFlowTypes.contains(WorkFlowType.SILAC)) {
            SilacQuery silacQuery = (SilacQuery) queries.get(WorkFlowType.SILAC);
            silacQuery.proteinFeatureVectorFilter().proto(filterList.getSilacProteinFilterBuilder());
        }

        return filterList.build();
    }


    public static IDFilter.DenovoCandidateFilter convertDenovoCandidateFilter(DenovoCandidateFilter denovoCandidateFilter) {
        IDFilter.DenovoCandidateFilter.Builder convertedFilter = IDFilter.DenovoCandidateFilter.newBuilder();
        convertedFilter.setMinAlc(denovoCandidateFilter.minAlc());
        return convertedFilter.build();
    }

    private static IDFilter.PsmFilter convertPsmFilter(PsmFilter psmFilter) {
        IDFilter.PsmFilter.Builder convertedFilter = IDFilter.PsmFilter.newBuilder();
        convertedFilter.setIncludeDecoy(psmFilter.includeDecoy());
        convertedFilter.setMinIntensity(psmFilter.minIntensity());
        convertedFilter.setMinMutationIonIntensity(psmFilter.minMutationIonIntensity());
        convertedFilter.setMinPValue(psmFilter.minPValue());
        convertedFilter.setMinFDR(psmFilter.minFDR());
        return convertedFilter.build();
    }

    public static WorkFlowType identificationTypeToWorkFlowType(IdentificationType type) {
        switch (type) {
            case DB:
                return WorkFlowType.DB_SEARCH;
            case PTM_FINDER:
                return WorkFlowType.PTM_FINDER;
            case SPIDER:
                return WorkFlowType.SPIDER;
            default:
                throw new IllegalStateException("Unable to convert identification type " + type.name() + " to workflow type");
        }
    }

    private static IDFilter.IdentificationType getIdentificationType(IdentificationType identificationType) {
        switch (identificationType) {
            case NONE:
                return IDFilter.IdentificationType.NONE;
            case DB:
                return IDFilter.IdentificationType.DB;
            case PTM_FINDER:
                return IDFilter.IdentificationType.PTM_FINDER;
            case SPIDER:
                return IDFilter.IdentificationType.SPIDER;
            default:
                throw new IllegalStateException("Invalid IdentificationType not support by dtos");
        }
    }

    public static ModificationVM convertModificationVM(Modification modification, UUID creatorId) {
        return new ModificationVMBuilder()
                .from(convertModificationIM(modification))
                .creatorId(creatorId)
                .builtin(modification.getBuiltin())
                .build();
    }
    public static com.bsi.peaks.model.proteomics.Modification convertModificationIM(Modification modification) {
        return new ModificationBuilder()
                .from(com.bsi.peaks.model.proteomics.Modification.from(modification))
                .build();

    }
    public static Modification convertModificationProto(ModificationVM modificationVM) {
        return modificationVM.proto(Modification.newBuilder());
    }

    public static com.bsi.peaks.model.proto.Enzyme convertEnzymeProto(EnzymeVM immutableEnzyme) {
        return com.bsi.peaks.model.proto.Enzyme.newBuilder()
            .setBuiltIn(immutableEnzyme.builtin())
            .setName(immutableEnzyme.name())
            .addAllPatterns(immutableEnzyme.patterns().stream().map(p -> EnzymePattern.newBuilder()
                .setCutSide(p.cutSide())
                .setCutSet(p.cutSet())
                .setRestrictSet(p.restrictSet())
                .build())::iterator)
            .build();
    }

    public static EnzymeVM convertEnzymeVM(com.bsi.peaks.model.proto.Enzyme enzyme, UUID creatorId) {
        return new EnzymeVMBuilder()
            .builtin(enzyme.getBuiltIn())
            .name(enzyme.getName())
            .creatorId(creatorId)
            .addAllPatterns(enzyme.getPatternsList().stream()
                .map(e -> new EnzymePatternBuilder()
                    .cutSet(e.getCutSet())
                    .cutSide(e.getCutSide())
                    .restrictSet(e.getRestrictSet())
                    .build())
                ::iterator
            )
            .build();
    }

    public static com.bsi.peaks.model.dto.MassAnalyzer convertMassAnalyzerProto(MassAnalyzer immutableMassAnalyzer) {
        return com.bsi.peaks.model.dto.MassAnalyzer.newBuilder()
            .setResolution(immutableMassAnalyzer.resolution())
            .setErrorTolerance(immutableMassAnalyzer.errorTolerance())
            .setPpm(immutableMassAnalyzer.ppm())
            .setCentroided(immutableMassAnalyzer.centroided())
            .setType(MassAnalyzerType.proto(immutableMassAnalyzer.type()))
            .build();
    }

    public static MassAnalyzer convertMassAnalyzerImmutable(com.bsi.peaks.model.dto.MassAnalyzer immutableMassAnalyzer) {
        return new MassAnalyzerBuilder()
            .resolution(immutableMassAnalyzer.getResolution())
            .errorTolerance(immutableMassAnalyzer.getErrorTolerance())
            .ppm(immutableMassAnalyzer.getPpm())
            .centroided(immutableMassAnalyzer.getCentroided())
            .type(MassAnalyzerType.from(immutableMassAnalyzer.getType().getDisplayName()))
            .build();
    }

    public static IonMobilityType convertIonMobilityTypeDto(com.bsi.peaks.model.dto.IonMobilityType ionMobilityType) {
        switch (ionMobilityType) {
            case NONE:
                return IonMobilityType.NONE;
            case TIMS:
                return IonMobilityType.TIMS;
            case FAIMS:
                return IonMobilityType.FAIMS;
            case HDMSe:
                return IonMobilityType.HDMSe;
            default:
                throw new IllegalStateException("Ion mobility type not specified");
        }
    }

    public static com.bsi.peaks.model.dto.IonMobilityType convertIonMobilityTypeProto(IonMobilityType ionMobilityType) {
        switch (ionMobilityType) {
            case NONE:
                return com.bsi.peaks.model.dto.IonMobilityType.NONE;
            case TIMS:
                return com.bsi.peaks.model.dto.IonMobilityType.TIMS;
            case FAIMS:
                return com.bsi.peaks.model.dto.IonMobilityType.FAIMS;
            case HDMSe:
                return com.bsi.peaks.model.dto.IonMobilityType.HDMSe;
            default:
                throw new IllegalStateException("Ion mobility type not specified");
        }
    }

    public static com.bsi.peaks.model.proto.Instrument convertInstrumentProto(InstrumentVM immutableInstrument) {
        return com.bsi.peaks.model.proto.Instrument.newBuilder()
            .setBuiltIn(immutableInstrument.builtin())
            .setName(immutableInstrument.name())
            .setManufacturer(immutableInstrument.manufacturer())
            .setModel(immutableInstrument.model())
            .setMsMassAnalyzer(convertMassAnalyzerProto(immutableInstrument.massAnalyzers().get(1).get(0)))
            .addAllMsmsMassAnalyzer(immutableInstrument.massAnalyzers().get(2).stream().map(DtoAdaptors::convertMassAnalyzerProto)::iterator)
            .setIonsource(immutableInstrument.ionSource().proto())
            .setLockMass(immutableInstrument.lockMass())
            .setIonMobillityType(convertIonMobilityTypeProto(immutableInstrument.ionMobilityType()))
            .build();
    }

    public static InstrumentVM convertInstrumentVM(com.bsi.peaks.model.proto.Instrument proto, UUID creatorId) {
        com.bsi.peaks.model.dto.IonSource protoIonSource = proto.getIonsource();
        final IonSource source;
        if (protoIonSource.getDisplayName().equals(IonSource.UNKNOWN.displayName())) {
            source = IonSource.UNKNOWN;
        } else {
            source = IonSource.from(protoIonSource.getNickNames(0));
        }
        return new InstrumentVMBuilder()
            .creatorId(creatorId)
            .builtin(proto.getBuiltIn())
            .name(proto.getName())
            .manufacturer(proto.getManufacturer())
            .model(proto.getModel())
            .putMassAnalyzers(1, Collections.singletonList(convertMassAnalyzerImmutable(proto.getMsMassAnalyzer())))
            .putMassAnalyzers(2, proto.getMsmsMassAnalyzerList().stream().map(DtoAdaptors::convertMassAnalyzerImmutable).collect(Collectors.toList()))
            .ionSource(source)
            .lockMass(proto.getLockMass())
            .ionMobilityType(convertIonMobilityTypeDto(proto.getIonMobillityType()))
            .build();
    }

    public static UploadFiles convertFileUploadList(com.bsi.peaks.model.dto.FileUploadList dto) {
        UploadFiles.Builder internal = UploadFiles.newBuilder();

        for(FileUpload uploadDto : dto.getFilesList()){
            internal.addFiles(FileToUpload.newBuilder()
                .setFile(uploadDto.getFile())
                .setProjectId(uploadDto.getProjectId())
                .setSampleId(uploadDto.getSampleId())
                .setFractionId(uploadDto.getFractionId())
                .setUploadTaskId(UUID.randomUUID().toString())

                // Doesn't seem to be required. Can omitting this cause problems?
                // Uploader doesn't use this value. It rather just overwrites it. Can this be less messy?
                .setFullRemotePath(uploadDto.getFullRemotePath())

                .build());
        }


        return internal.build();
    }


    public static ReporterIonQParameters reporterIonQParameters(WorkflowStep workflowStep) {
        ReporterIonQBatchCalculationParameters calculationParameters = workflowStep.getReporterIonQBatchCalculationParameters();
        if (calculationParameters == null) {
            return ReporterIonQParameters.getDefaultInstance();
        }
        return calculationParameters.getParameters();
    }

    private static ReporterIonQFilterSummarization filterIonQParameters(WorkflowStepStep workflowStepStep) {
        ReporterIonQFilterSummarization summarizationParameters = workflowStepStep.parameters().getReporterIonQFilterSummarizationParameters();
        return summarizationParameters;
    }

}
