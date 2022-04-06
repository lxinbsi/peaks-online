package com.bsi.peaks.server.archiver;

import akka.stream.Materializer;
import com.bsi.peaks.analysis.analysis.dto.IdentificaitonFilterSummarization;
import com.bsi.peaks.analysis.parameterModels.dto.*;
import com.bsi.peaks.event.project.AnalysisArchiveInfo;
import com.bsi.peaks.event.project.FractionArchiveInfo;
import com.bsi.peaks.event.project.ProjectArchiveInfo;
import com.bsi.peaks.event.project.SampleArchiveInfo;
import com.bsi.peaks.model.dto.*;
import com.bsi.peaks.model.proteomics.fasta.fastasequencedatabase.FastaSequenceDatabaseAttributes;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.server.service.ProjectService;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

class ProjectSummarizer {
    private static final String DIVIDER = StringUtils.repeat("=", 100);
    private final ProjectService projectService;
    private final FastaDatabaseService databaseService;
    private final Project project;
    private final Materializer materializer;

    ProjectSummarizer(ProjectService projectService, FastaDatabaseService databaseService, Project project, Materializer materializer) {
        this.projectService = projectService;
        this.databaseService = databaseService;
        this.project = project;
        this.materializer = materializer;
    }

    void summarize(OutputStream output) throws IOException {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final ProjectArchiveInfo info = aggregateInfo();
        try (PrintWriter writer = new PrintWriter(output)) {
            writer.println(DIVIDER);
            writer.println("Project:");
            writer.println("* Name: " + info.getName());
            writer.println("* Description: " + info.getDescription());
            writer.println("* Owner: " + info.getOwnerName());
            writer.println("* Created At: " + format.format(new Date(info.getCreatedAt() * 1000)));

            writer.println(DIVIDER);
            writer.println("Analyses:");
            info.getAnalysesList().forEach(analysis -> {
                writer.println("+ " + analysis.getName());
                writer.println("  Description: " + analysis.getDescription());
                writer.println("  Created At: " + format.format(new Date(analysis.getCreatedAt() * 1000)));

                if (analysis.hasDenovoParameters()) {
                    DeNovoParameters parameters = analysis.getDenovoParameters();
                    writer.println("  De Novo Parameters:");
                    writer.println("    Precursor Mass Error Tolerance: " + parameters.getPrecursorTol().getParam()
                        + parameters.getPrecursorTol().getUnit().name());
                    writer.println("    Fragment Mass Error Tolerance: " + parameters.getFragmentTol().getParam()
                        + parameters.getFragmentTol().getUnit().name());
                    writer.println("    Enzyme: " + parameters.getEnzymeName());
                    writer.println("    Fixed Modifications: ");
                    parameters.getFixedModificationNamesList().forEach(modi -> writer.println("    - " + modi));
                    writer.println("    Variable Modifications: ");
                    parameters.getVariableModificationNamesList().forEach(modi -> writer.println("    - " + modi));
                }

                if (analysis.hasDbParameters()) {
                    DbSearchParameters parameters = analysis.getDbParameters();
                    Database targetDatabase = parameters.getDatabasesList().get(0);
                    String[] taxonomyNames = targetDatabase.getTaxonmyNameList().toArray(new String[0]);
                    Database contaminantDatabase = parameters.getContaminantDatabasesCount() > 0 ?
                        parameters.getContaminantDatabasesList().get(0) : null;
                    writer.println("  Database Search Parameters:");
                    writer.println("    Precursor Mass Error Tolerance: " + parameters.getPrecursorTol().getParam()
                        + parameters.getPrecursorTol().getUnit().name());
                    writer.println("    Fragment Mass Error Tolerance: " + parameters.getFragmentTol().getParam()
                        + parameters.getFragmentTol().getUnit().name());
                    writer.println("    Enzyme: " + parameters.getEnzymeName());
                    writer.println("    Digest Mode: " + parameters.getEnzymeSpecificityName());
                    writer.println("    Missed Cleavage: " + parameters.getMissCleavage());
                    writer.println("    Target Database: " + getDatabaseName(targetDatabase.getId()));
                    writer.println("    Taxonomy: " + String.join(", ", taxonomyNames));
                    if (contaminantDatabase != null) {
                        writer.println("    Contaminant Database: " + getDatabaseName(contaminantDatabase.getId()));
                    }
                    writer.println("    Fixed Modifications: ");
                    parameters.getFixedModificationNamesList().forEach(modi -> writer.println("    - " + modi));
                    writer.println("    Variable Modifications: ");
                    parameters.getVariableModificationNamesList().forEach(modi -> writer.println("    - " + modi));
                }

                if (analysis.hasPtmParameters()) {
                    PtmFinderParameters parameters = analysis.getPtmParameters();
                    writer.println("  PEAKS PTM Parameters:");
                    writer.println("    De Novo ALC(%) Threshold: " + parameters.getDeNovoScoreThreshold() * 100f);
                    if (parameters.getModificationSearchType() == PtmFinderModificationSearchType.ALL_BUILT_IN) {
                        writer.println("    Modifications: search with all built-in modifications");
                    } else {
                        writer.println("    Additional Fixed Modifications: ");
                        parameters.getFixedModificationNamesList().forEach(modi -> writer.println("    - " + modi));
                        writer.println("    Additional Variable Modifications: ");
                        parameters.getVariableModificationNamesList().forEach(modi -> writer.println("    - " + modi));
                    }
                }

                if (analysis.hasSpiderParameters()) {
                    writer.println("  Find Mutations and Sequence Variants with SPIDER");
                }

                if (analysis.hasDbParameters()) {
                    IdentificaitonFilterSummarization psmFilter = analysis.getIdFilterSummarization();
                    writer.println("  PSM Filter:");
                    if (psmFilter.getPsmFilterTypeCase() == IdentificaitonFilterSummarization.PsmFilterTypeCase.MINPVALUE) {
                        writer.println("    PSM -10lgP>=: " + psmFilter.getMinPValue());
                    } else {
                        writer.println("    PSM FDR(%): " + psmFilter.getMaxFdr() * 100f);
                    }
                    writer.println("    De Novo ALC(%)>=: " + psmFilter.getMinDenovoAlc() * 100f);
                    if (analysis.hasSpiderParameters()) {
                        writer.println("    Mutation Ion Intensity(%)>=: " + psmFilter.getMinIonMutationIntensity() * 100f);
                    }
                }

                if (analysis.hasLfqParameters()) {
                    LfqParameters parameters = analysis.getLfqParameters();
                    LfqFilter filter = analysis.getLfqFilter();
                    writer.println("  Label Free Quantification Parameters:");
                    writer.println("    Mass Error Tolerance: " + parameters.getMassErrorTolerance().getParam()
                        + parameters.getMassErrorTolerance().getUnit().name());
                    writer.println("    Retention Time Shift Tolerance (MIN): " + parameters.getRtShift());
                    writer.println("    Feature Intensity>=: " + parameters.getFeatureIntensityThreshold());
                    writer.println("    FDR Threshold(%): " + parameters.getFdrThreshold() * 100f);
                    writer.println("    RT Range: " + filter.getFeatureVectorFilter().getMinRt() + " <= RT <= " + filter.getFeatureVectorFilter().getMaxRt());
                    writer.println("    Peptide Feature: "
                        + String.format("Avg. intensity>=%f, %d<=charge<=%d, " +
                        "peptide ID count >= %d per a group, and Detected in at least %d samples per a group",
                        filter.getFeatureVectorFilter().getMinAvgArea(), filter.getFeatureVectorFilter().getMinCharge(),
                        filter.getFeatureVectorFilter().getMaxCharge(), filter.getLabelFreeQueryParameters().getMinPeptideIdCountPerGroup(),
                        filter.getLabelFreeQueryParameters().getMinConfidentSamplesPerGroup()));
                    writer.println("    Protein: " + String.format("Significance Method %s,%s%s Use Top %d peptides",
                        "ANOVA",
                        filter.getLabelFreeQueryParameters().getUseModifiedFormExclusion() ? " Modified Form Exclusion," : "",
                        filter.getLabelFreeQueryParameters().getRemoveOutlier() ? " Remove Outlier," : "",
                        filter.getLabelFreeQueryParameters().getNumberTopPeptideToUse()
                    ));
                    writer.println("    Normalization: " + filter.getLabelFreeQueryParameters().getNormalizationMethod().name());
                    writer.println("    Groups:");
                    parameters.getGroupsList().forEach(group -> {
                        String[] sampleIndexes = group.getSampleIndexesList().stream().map(Object::toString).toArray(String[]::new);
                        writer.println("      Group Name: " + group.getGroupName());
                        writer.println("      Sample Indexes: " + String.join(", ", sampleIndexes));
                    });
                }

                if (analysis.hasRiqParameters()) {
                    ReporterIonQParameters parameters = analysis.getRiqParameters();
                    ReporterIonQFilterSummarization filter = analysis.getRiqFilterSummarization();
                    writer.println("  TMT/iTRAQ Quantification Parameters:");
                    writer.println("    Method: " + parameters.getMethod().getMethodName());
                    writer.println("    Mass Error Tolerance: " + parameters.getQuantError()
                        + (parameters.getErrorIsPPM() ? "PPM" : "DA"));
                    writer.println("    Reporter Ion Type: " + (parameters.getIsMS3() ? "MS3" : "MS2"));
                    if (parameters.hasPurityCorrectionFacotrs()) {
                        writer.println("    Perform Purity Correction");
                    }
                    writer.println("    Protein: Significance Method ANOVA" +
                        (parameters.getUseModifiedFormExclusion() ? ", Modified Form Exclusion" : ""));
                    String spectrumFilterType;
                    if (filter.getSpectrumFilter().getFilterTypeCase() == ReporterIonQSpectrumFilter.FilterTypeCase.MINPVALUE) {
                        spectrumFilterType = "-10lgP>=" + filter.getSpectrumFilter().getMinPValue();
                    } else {
                        spectrumFilterType = "FDR(%)<=" + (filter.getSpectrumFilter().getMaxFDR() * 100f);
                    }
                    writer.println("    Spectrum Filter: " + String.format("%s, Quality: %d, Reporter Ion Intensity>=%d, Number of Channels Present>=%d%s",
                        spectrumFilterType, filter.getSpectrumFilter().getQuality(),
                        filter.getSpectrumFilter().getMinReporterIonIntensity(),
                        filter.getSpectrumFilter().getMinPresentChannelCount(),
                        filter.getSpectrumFilter().getPresentReferenceChannel() ? ", Reference Channel Present" : ""));
                    writer.println("    Experiment Setting:");
                    if (filter.getExperimentSettings().getAllExperiments()) {
                        writer.println("      All Experiments");
                    }
                    if (filter.getExperimentSettings().getEnableInterExperimentNormalization()) {
                        writer.println("      Enable Inter Experiment Normalization");
                        writer.println("      Reference Sample: " + filter.getExperimentSettings().getReferenceSample().getName());
                    }
                    if (filter.getExperimentSettings().getExcludeSpikedChannelForSignificance()) {
                        writer.println("      Exclude Spiked Channel For Significance");
                    }
                    writer.println("      Intra Sample Normalization: "
                        + filter.getExperimentSettings().getReporterQNormalization().getNormalizationMethod().name());
                    writer.println("      Experiment Groups:");
                    filter.getExperimentSettings().getGroupsList().forEach(group -> {
                        String[] sampleIndexes = group.getSampleIndexesList().stream().map(Object::toString).toArray(String[]::new);
                        writer.println("      Group Name: " + group.getGroupName());
                        writer.println("      Sample Indexes: " + String.join(", ", sampleIndexes));
                    });
                    writer.println("      Reference Label: " + filter.getExperimentSettings().getReferenceLabel().getChannelAlias());
                    writer.println();
                }

                writer.println(); // attach an empty line to separate analyses
            });

            writer.println(DIVIDER);
            writer.println(info.getSamplesCount() + " Samples:");
            info.getSamplesList().forEach(sample -> {
                String sampleInfo = String.format("%s: Enzyme: %s, Activation Method: %s, Instrument: %s, %d Fractions",
                    sample.getName(), sample.getEnzyme(), sample.getActivationMethod(), sample.getInstrument(), sample.getFractionsCount());
                writer.println("+ " + sampleInfo);
                sample.getFractionsList().forEach(fraction -> {
                    String fractionInfo;
                    if (sample.getHasIonMobility()) {
                        fractionInfo = String.format("%s: MS1 Frames: %d, MS2 Frames: %d, Elution Time: %.2f MINS",
                            fraction.getName(), fraction.getMs1ScanCount(), fraction.getMs2ScanCount(), fraction.getMaxRetentionTime() / 60d);
                    } else {
                        fractionInfo = String.format("%s: MS1 Count: %d, MS2 Count: %d, Elution Time: %.2f MINS",
                            fraction.getName(), fraction.getMs1ScanCount(), fraction.getMs2ScanCount(), fraction.getMaxRetentionTime() / 60d);
                    }
                    writer.println(("-- " + fractionInfo));
                });
            });

            writer.println(DIVIDER);
            output.flush();
        }
    }

    private ProjectArchiveInfo aggregateInfo() {
        UUID projectId = UUID.fromString(project.getId());
        ProjectArchiveInfo.Builder builder = ProjectArchiveInfo.newBuilder()
            .setName(project.getName())
            .setDescription(project.getDescription())
            .setCreatedAt(project.getCreatedTimestamp())
            .setOwnerName(project.getOwnerUsername());

        // attach analysis info
        projectService.projectAnalysis(projectId, User.admin())
            .runForeach(analysis -> {
                AnalysisArchiveInfo.Builder analysisBuilder = AnalysisArchiveInfo.newBuilder()
                    .setName(analysis.getName())
                    .setDescription(analysis.getDescription())
                    .setCreatedAt(analysis.getCreatedTimestamp());

                if (analysis.hasDenovoStep()) {
                    analysisBuilder.setDenovoParameters(analysis.getDenovoStep().getDeNovoParameters());
                }
                if (analysis.hasDbSearchStep()) {
                    analysisBuilder.setDbParameters(analysis.getDbSearchStep().getDbSearchParameters());
                    analysisBuilder.setIdFilterSummarization(analysis.getDbSearchStep().getFilterSummarization());
                }
                if (analysis.hasPtmFinderStep()) {
                    analysisBuilder.setPtmParameters(analysis.getPtmFinderStep().getPtmFinderParameters());
                }
                if (analysis.hasSpiderStep()) {
                    analysisBuilder.setSpiderParameters(analysis.getSpiderStep().getSpiderParameters());
                }
                if (analysis.hasLfqStep()) {
                    analysisBuilder.setLfqParameters(analysis.getLfqStep().getLfqParameters());
                    analysisBuilder.setLfqFilter(analysis.getLfqStep().getLfqFilter());
                }
                if (analysis.hasReporterIonQStep()) {
                    analysisBuilder.setRiqParameters(analysis.getReporterIonQStep().getReporterIonQParameters());
                    analysisBuilder.setRiqFilterSummarization(analysis.getReporterIonQStep().getFilterSummarization());
                }

                builder.addAnalyses(analysisBuilder.build());
            }, materializer)
            .toCompletableFuture().join();

        // attach sample info
        projectService.projectSamples(projectId, User.admin())
            .runForeach(sample -> {
                // attach sample information
                SampleArchiveInfo.Builder sampleBuilder = SampleArchiveInfo.newBuilder()
                    .setName(sample.getName())
                    .setEnzyme(sample.getEnzyme())
                    .setActivationMethod(sample.getActivationMethod().name())
                    .setInstrument(sample.getInstrument())
                    .setHasIonMobility(sample.getHasIonMobility());
                sample.getFractionsList().forEach(fraction -> sampleBuilder.addFractions(
                    FractionArchiveInfo.newBuilder()
                        .setName(fraction.getSourceFile())
                        .setMs1ScanCount(fraction.getMs1ScanCount())
                        .setMs2ScanCount(fraction.getMs2ScanCount())
                        .setMaxRetentionTime(fraction.getMaxRetentionTime())
                        .build()
                ));
                builder.addSamples(sampleBuilder.build());
            }, materializer)
            .toCompletableFuture().join();

        return builder.build();
    }

    private String getDatabaseName(String databaseId) {
        return databaseService.findDatabaseById(UUID.fromString(databaseId))
            .thenApply(FastaSequenceDatabaseAttributes::name)
            .exceptionally(e -> databaseId)
            .toCompletableFuture().join();
    }
}
