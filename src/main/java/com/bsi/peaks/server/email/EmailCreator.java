package com.bsi.peaks.server.email;

import akka.japi.Pair;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import com.bsi.peaks.analysis.analysis.dto.Analysis;
import com.bsi.peaks.analysis.parameterModels.dto.WorkFlowType;
import com.bsi.peaks.data.graph.DbApplicationGraph;
import com.bsi.peaks.data.graph.DbApplicationGraphImplementation;
import com.bsi.peaks.data.graph.GraphTaskIds;
import com.bsi.peaks.data.graph.LfqApplicationGraph;
import com.bsi.peaks.data.graph.LfqApplicationGraphImplementation;
import com.bsi.peaks.data.graph.RiqApplicationGraph;
import com.bsi.peaks.data.graph.RiqApplicationGraphImplementation;
import com.bsi.peaks.data.graph.SilacApplicationGraph;
import com.bsi.peaks.data.graph.SilacApplicationGraphImplementation;
import com.bsi.peaks.data.graph.SlApplicationGraph;
import com.bsi.peaks.data.graph.SlApplicationGraphImplementation;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.ProjectAggregateCommandFactory;
import com.bsi.peaks.event.email.Email;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryRequest;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateQueryResponse.AnalysisQueryResponse;
import com.bsi.peaks.io.writer.dto.DbDtoSources;
import com.bsi.peaks.io.writer.dto.DbDtoSourcesViaDbGraph;
import com.bsi.peaks.io.writer.dto.LfqDtoSources;
import com.bsi.peaks.io.writer.dto.LfqDtoSourcesViaLfqGraph;
import com.bsi.peaks.io.writer.dto.RiqDtoSources;
import com.bsi.peaks.io.writer.dto.RiqDtoSourcesViaRiqGraph;
import com.bsi.peaks.io.writer.dto.SlDtoSources;
import com.bsi.peaks.io.writer.dto.SlDtoSourcesViaSlGraph;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.dto.Sample;
import com.bsi.peaks.model.dto.peptide.DenovoSampleSummary;
import com.bsi.peaks.model.dto.peptide.LfqStatisticsOfFilteredResult;
import com.bsi.peaks.model.dto.peptide.PeptideStatistics;
import com.bsi.peaks.model.dto.peptide.ReporterIonQSummaryStatistics;
import com.bsi.peaks.model.dto.peptide.SamplePeptideStatistics;
import com.bsi.peaks.model.dto.peptide.SampleProteinStatistics;
import com.bsi.peaks.model.dto.peptide.StatisticsOfFilteredResult;
import com.bsi.peaks.model.filter.FilterFunction;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.email.template.AnalysisDoneTemplate;
import com.bsi.peaks.server.email.template.DenovoSummaryRow;
import com.bsi.peaks.server.email.template.IDSummaryRow;
import com.bsi.peaks.server.email.template.LfqSummary;
import com.bsi.peaks.server.email.template.RiqSummary;
import com.bsi.peaks.server.email.template.SilacSummaryTemplate;
import com.bsi.peaks.server.email.template.SlSummaryRow;
import com.bsi.peaks.server.es.communication.ProjectAggregateCommunication;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.server.service.UserService;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * @author span
 * Created by span on 2019-03-08
 */
public class EmailCreator {
    private final UserService userService;
    private final SystemConfig systemConfig;
    private final ApplicationStorageFactory storageFactory;
    private final ProjectAggregateCommunication projectCommunication;
    private final ProjectService projectService;
    private static final String SUBJECT_ANALYSIS_DONE = "Your peaks online analysis is finished";
    private static final String HBS_ANALYSIS_DONE = "/email/analysis_done_template";
    private final Materializer materializer;

    @Inject
    EmailCreator(
        UserService userService,
        SystemConfig systemConfig,
        ApplicationStorageFactory storageFactory,
        ProjectService projectService,
        ProjectAggregateCommunication projectCommunication
    ) {
        this.userService = userService;
        this.systemConfig = systemConfig;
        this.storageFactory = storageFactory;
        this.projectCommunication = projectCommunication;
        this.projectService = projectService;
        this.materializer = storageFactory.getSystemStorage().getMaterializer();
    }

    CompletionStage<Email> generateEmailForAnalysis(UUID projectId, UUID analysisId, String cc) {
        // fetch project and analysis first
        AnalysisDoneTemplate template = new AnalysisDoneTemplate();
        template.setProjectId(projectId);
        template.setAnalysisId(analysisId);

        return updateProject(template)
            .thenCompose(this::updateUser)
            .thenApply(t -> {
                t.setResultURL(EmailUtilities.generateResultURL(systemConfig, t.getProjectId(), t.getAnalysisId()));
                Email.Builder builder = Email.newBuilder()
                    .setSubject(SUBJECT_ANALYSIS_DONE);
                try {
                    builder.setHtml(generateHtml(t));
                } catch (IOException e) {
                    throw new CompletionException("Unable to generate HTML", e);
                }
                if (!t.getUserEmail().isEmpty()) builder.addTo(t.getUserEmail());
                if (!cc.isEmpty()) Arrays.stream(cc.split(",")).map(String::trim).forEach(builder::addCc);
                return builder.build();
            });
    }

    private CompletionStage<AnalysisDoneTemplate> updateProject(AnalysisDoneTemplate template) {
        final ProjectAggregateCommandFactory projectCommandFactory = ProjectAggregateCommandFactory
            .project(template.getProjectId(), EmailUtilities.ADMIN.userId());
        final ProjectAggregateCommandFactory.AnalysisCommandFactory analysisCommandFactory = ProjectAggregateCommandFactory
            .project(template.getProjectId(), EmailUtilities.ADMIN.userId())
            .analysis(template.getAnalysisId());

        return projectCommunication.queryProject(projectCommandFactory.queryProject(false, false))
            .thenCombine(projectCommunication.queryAnalysis(analysisCommandFactory.queryAnalysis(true)), Pair::create)
            .thenCompose(pair -> {
                final AnalysisQueryResponse.AnalysisDto analysisDto = pair.second().getAnalysisDto();
                final Project project = pair.first().getProjectDto().getProject();
                final Analysis analysis = analysisDto.getAnalysis();
                final ApplicationStorage storage = storageFactory.getStorage(project.getKeyspace());
                template.setUserId(UUID.fromString(project.getOwnerUserId()));
                template.setProjectName(project.getName());
                template.setAnalysisName(analysis.getName());
                template.setUserId(UUID.fromString(project.getOwnerUserId()));

                Map<String, String> names = new HashMap<>();
                for (Sample sample : analysisDto.getSampleList()) {
                    names.put(sample.getId(), sample.getName());
                }

                CompletionStage<AnalysisDoneTemplate> futureTemplate = completedFuture(template);
                // attach summary result
                if (analysis.hasDenovoStep()) {
                    CompletionStage<Pair<com.bsi.peaks.model.applicationgraph.GraphTaskIds, FilterFunction[]>> graphTaskIdsAndFilters = projectCommunication
                        .queryAnalysis(analysisCommandFactory.queryGraphTaskIds(WorkFlowType.DENOVO))
                        .thenApply(AnalysisQueryResponse::getGraphTaskIds)
                        .thenCombine(projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.DENOVO), Pair::create);
                    futureTemplate = futureTemplate.thenCompose(t -> graphTaskIdsAndFilters.thenCompose(graphFilters -> attachDenovo(storage, graphFilters.first(), graphFilters.second(), names, t)));
                }
                if (analysis.hasDbSearchStep()) {
                    FilterFunction[] sighs = projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.DB_SEARCH)
                        .toCompletableFuture().join();
                    CompletionStage<Pair<com.bsi.peaks.model.applicationgraph.GraphTaskIds, FilterFunction[]>> graphTaskIdsAndFilters = projectCommunication
                        .queryAnalysis(analysisCommandFactory.queryGraphTaskIds(WorkFlowType.DB_SEARCH))
                        .thenApply(AnalysisQueryResponse::getGraphTaskIds)
                        .thenCombine(projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.DB_SEARCH), Pair::create);
                    futureTemplate = futureTemplate.thenCompose(t -> graphTaskIdsAndFilters.thenCompose(g -> attachDB(storage, g.first(), g.second(), names, t)));
                }
                if (analysis.hasPtmFinderStep()) {
                    CompletionStage<Pair<com.bsi.peaks.model.applicationgraph.GraphTaskIds, FilterFunction[]>> graphTaskIdsAndFilters = projectCommunication
                        .queryAnalysis(analysisCommandFactory.queryGraphTaskIds(WorkFlowType.PTM_FINDER))
                        .thenApply(AnalysisQueryResponse::getGraphTaskIds)
                        .thenCombine(projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.PTM_FINDER), Pair::create);
                    futureTemplate = futureTemplate.thenCompose(t -> graphTaskIdsAndFilters.thenCompose(g -> attachPtmFinder(storage, g.first(), g.second(), names, t)));
                }
                if (analysis.hasSpiderStep()) {
                    final ProjectAggregateQueryRequest query = analysisCommandFactory.queryGraphTaskIds(WorkFlowType.SPIDER);
                    CompletionStage<Pair<com.bsi.peaks.model.applicationgraph.GraphTaskIds, FilterFunction[]>> graphTaskIdsAndFilters = projectCommunication.queryAnalysis(query)
                        .thenApply(AnalysisQueryResponse::getGraphTaskIds)
                        .thenCombine(projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.SPIDER), Pair::create);
                    futureTemplate = futureTemplate.thenCompose(t -> graphTaskIdsAndFilters.thenCompose(g -> attachSpider(storage, g.first(), g.second(), names, t)));
                }
                if (analysis.hasSlStep()) {
                    CompletionStage<Pair<com.bsi.peaks.model.applicationgraph.GraphTaskIds, FilterFunction[]>> graphTaskIdsAndFilters = projectCommunication
                        .queryAnalysis(analysisCommandFactory.queryGraphTaskIds(WorkFlowType.SPECTRAL_LIBRARY))
                        .thenApply(AnalysisQueryResponse::getGraphTaskIds)
                        .thenCombine(projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.SPECTRAL_LIBRARY), Pair::create);
                    futureTemplate = futureTemplate.thenCompose(t -> graphTaskIdsAndFilters.thenCompose(g -> attachSl(storage, g.first(), g.second(), names, t)));
                }
                if (analysis.hasSilacStep()) {
                    final ProjectAggregateQueryRequest query = analysisCommandFactory.queryGraphTaskIds(WorkFlowType.SILAC);
                    CompletionStage<Pair<com.bsi.peaks.model.applicationgraph.GraphTaskIds, FilterFunction[]>> graphTaskIdsAndFilters = projectCommunication.queryAnalysis(query)
                        .thenApply(AnalysisQueryResponse::getGraphTaskIds)
                    .thenCombine(projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.SILAC), Pair::create);
                    futureTemplate = futureTemplate.thenCompose(t -> graphTaskIdsAndFilters.thenCompose(g -> attachSilac(storage, g.first(), g.second(), t)));
                }
                if (analysis.hasReporterIonQStep()) {
                    CompletionStage<Pair<com.bsi.peaks.model.applicationgraph.GraphTaskIds, FilterFunction[]>> graphTaskIdsAndFilters = projectCommunication
                        .queryAnalysis(analysisCommandFactory.queryGraphTaskIds(WorkFlowType.REPORTER_ION_Q))
                        .thenApply(AnalysisQueryResponse::getGraphTaskIds)
                        .thenCombine(projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.REPORTER_ION_Q), Pair::create);
                    futureTemplate = futureTemplate.thenCompose(t -> graphTaskIdsAndFilters.thenCompose(g -> attachRiq(storage, g.first(), g.second(), t)));
                }
                if (analysis.hasLfqStep()) {
                    CompletionStage<Pair<com.bsi.peaks.model.applicationgraph.GraphTaskIds, FilterFunction[]>> graphTaskIdsAndFilters = projectCommunication
                        .queryAnalysis(analysisCommandFactory.queryGraphTaskIds(WorkFlowType.LFQ))
                        .thenApply(AnalysisQueryResponse::getGraphTaskIds)
                        .thenCombine(projectService.analysisFiltersByWorkflowType(template.getProjectId(), template.getAnalysisId(), EmailUtilities.ADMIN, WorkFlowType.LFQ), Pair::create);
                    futureTemplate = futureTemplate.thenCompose(t -> graphTaskIdsAndFilters.thenCompose(g -> attachLfq(storage, g.first(), g.second(), names, t)));
                }
                return futureTemplate;
            });
    }

    private CompletionStage<AnalysisDoneTemplate> updateUser(AnalysisDoneTemplate template) {
        return userService.getUserByUserId(template.getUserId()).thenApply(us -> {
            template.setUserFullName(us.getUserInformation().getFullName());
            template.setUsername(us.getUserInformation().getUserName());
            template.setUserEmail(us.getUserInformation().getReceiveEmail() ? us.getUserInformation().getEmail().trim() : "");
            return template;
        });
    }

    private CompletionStage<AnalysisDoneTemplate> attachDenovo(
        ApplicationStorage storage,
        com.bsi.peaks.model.applicationgraph.GraphTaskIds protoGraphTaskIds,
        FilterFunction[] filters,
        Map<String, String> names,
        AnalysisDoneTemplate template
    ) {
        final GraphTaskIds graphTaskIds;
        try {
            graphTaskIds = GraphTaskIds.from(protoGraphTaskIds);
        } catch (Throwable throwable) {
            throw propagate(throwable);
        }
        DbApplicationGraph applicationGraph = new DbApplicationGraphImplementation(storage, graphTaskIds, Optional.empty(), filters);
        DbDtoSources dtoSources = new DbDtoSourcesViaDbGraph(applicationGraph);

        return dtoSources.denovoSummaries().runWith(Sink.seq(), materializer).thenApply(summary -> {
            List<DenovoSummaryRow> rows = new ArrayList<>();
            rows.add(new DenovoSummaryRow()); // summary row
            int allMs2 = 0;
            int allPsm = 0;
            int allAlc30 = 0;
            int allAlc50 = 0;
            int allAlc70 = 0;

            for (DenovoSampleSummary sample : summary) {
                int alc70Count = sample.getData().getPsmBracketsOrDefault("70.0", 0)
                    + sample.getData().getPsmBracketsOrDefault("80.0", 0)
                    + sample.getData().getPsmBracketsOrDefault("90.0", 0);
                int alc50Count = alc70Count
                    + sample.getData().getPsmBracketsOrDefault("50.0", 0)
                    + sample.getData().getPsmBracketsOrDefault("60.0", 0);
                int alc30Count = alc50Count
                    + sample.getData().getPsmBracketsOrDefault("30.0", 0)
                    + sample.getData().getPsmBracketsOrDefault("40.0", 0);
                int ms2 = sample.getData().getScans();
                int psm = sample.getData().getPsmTotal();

                DenovoSummaryRow row = new DenovoSummaryRow();
                row.setSampleName(names.get(sample.getSampleId()));
                row.setMs2Count(ms2);
                row.setPsmCount(psm);
                row.setPsm30Count(alc30Count);
                row.setPsm50Count(alc50Count);
                row.setPsm70Count(alc70Count);
                row.setNotes("");
                rows.add(row);

                allMs2 += ms2;
                allPsm += psm;
                allAlc30 += alc30Count;
                allAlc50 += alc50Count;
                allAlc70 += alc70Count;
            }

            float percent = (100f * allPsm) / allMs2;
            DenovoSummaryRow firstRow = rows.get(0);
            firstRow.setSampleName("All");
            firstRow.setMs2Count(allMs2);
            firstRow.setPsmCount(allPsm);
            firstRow.setPsm30Count(allAlc30);
            firstRow.setPsm50Count(allAlc50);
            firstRow.setPsm70Count(allAlc70);
            firstRow.setNotes(String.format("#PSM / #MS2 = %.0f%%", percent));
            template.setDenovoSummary(rows);
            return template;
        });
    }


    private CompletionStage<List<IDSummaryRow>> generateIDRows(DbApplicationGraph graph, Map<String, String> names) {
        DbDtoSources dtoSources = new DbDtoSourcesViaDbGraph(graph);
        return dtoSources.statisticsOfFilteredResult().runWith(Sink.headOption(), materializer).thenApply(opt -> {
            StatisticsOfFilteredResult stats = opt.orElseThrow(NoSuchElementException::new);
            List<IDSummaryRow> rows = new ArrayList<>();
            rows.add(new IDSummaryRow());

            for (UUID sampleId : graph.allSampleIds()) {
                String sampleIdStr = sampleId.toString();
                SampleProteinStatistics proteinStats = stats.getSamplesOrThrow(sampleIdStr);
                SamplePeptideStatistics peptideStats = stats.getPeptideStats().getSamplesOrThrow(sampleIdStr);

                IDSummaryRow row = new IDSummaryRow();
                row.setSampleName(names.get(sampleIdStr));
                row.setMs2Count(peptideStats.getMs2Scans());
                row.setPsmCount(peptideStats.getPsmTotal());
                row.setPeptideCount(peptideStats.getPeptideTotal());
                row.setSequenceCount(peptideStats.getPeptideBackboneTotal());
                row.setProteinCount(proteinStats.getProteinTotal());
                row.setProteinGroupCount(proteinStats.getProteinGroupTotal());
                row.setNotes("");
                rows.add(row);
            }

            PeptideStatistics allPeptideStats = stats.getPeptideStats();
            float percent = (100f * allPeptideStats.getPeptideTargetSpecturmCount()) / allPeptideStats.getMs2Count();
            IDSummaryRow firstRow = rows.get(0);
            firstRow.setSampleName("All");
            firstRow.setMs2Count(allPeptideStats.getMs2Count());
            firstRow.setPsmCount(allPeptideStats.getPeptideTargetSpecturmCount());
            firstRow.setPeptideCount(allPeptideStats.getPeptideTargetSequencesCount());
            firstRow.setSequenceCount(allPeptideStats.getPeptideTargetBackboneSequencesCount());
            firstRow.setProteinCount(stats.getProteinTargetCount());
            firstRow.setProteinGroupCount(stats.getProteinGroupsCount());
            firstRow.setNotes(String.format("#PSM / #MS2 = %.0f%%", percent));

            return rows;
        });
    }

    private CompletionStage<List<SlSummaryRow>> generateSlRows(SlApplicationGraph graph, Map<String, String> names) {
        SlDtoSources dtoSources = new SlDtoSourcesViaSlGraph(graph);
        return dtoSources.slSummary().runWith(Sink.headOption(), materializer).thenApply(opt -> {
            StatisticsOfFilteredResult stats = opt.orElseThrow(NoSuchElementException::new);
            List<SlSummaryRow> rows = new ArrayList<>();

            { //all
                PeptideStatistics allPeptideStats = stats.getPeptideStats();
                long fractionCount = graph.sampleFractions().stream()
                    .flatMap(sample -> sample.fractionIds().stream())
                    .count();
                float percent = (100f * allPeptideStats.getPeptideTargetSpecturmCount()) / allPeptideStats.getMs2Count();
                SlSummaryRow firstRow = new SlSummaryRow()
                    .setSampleName("All")
                    .setMsRun((int) fractionCount)
                    .setMs1Count(allPeptideStats.getMs1Count())
                    .setMs2Count(allPeptideStats.getMs2Count())
                    .setPsmCount(allPeptideStats.getPeptideTargetSpecturmCount())
                    .setPeptideCount(allPeptideStats.getPeptideTargetSequencesCount())
                    .setSequenceCount(allPeptideStats.getPeptideTargetBackboneSequencesCount())
                    .setProteinCount(stats.getProteinTargetCount())
                    .setProteinGroupCount(stats.getProteinGroupsCount())
                    .setPsmCountMs2Count(String.format("%.0f%%", percent));
                rows.add(firstRow);
            }

            for (UUID sampleId : graph.allSampleIds()) {
                String sampleIdStr = sampleId.toString();
                SampleProteinStatistics proteinStats = stats.getSamplesOrThrow(sampleIdStr);
                SamplePeptideStatistics peptideStats = stats.getPeptideStats().getSamplesOrThrow(sampleIdStr);

                long fractionCount = graph.sampleFraction(sampleId).fractionIds().stream().count();

                float percent = (100f * peptideStats.getPsmTotal()) / peptideStats.getMs2Scans();
                SlSummaryRow row = new SlSummaryRow()
                    .setSampleName(names.get(sampleIdStr))
                    .setMsRun((int) fractionCount)
                    .setMs1Count(peptideStats.getMs1Scans())
                    .setMs2Count(peptideStats.getMs2Scans())
                    .setPsmCount(peptideStats.getPsmTotal())
                    .setPeptideCount(peptideStats.getPeptideTotal())
                    .setSequenceCount(peptideStats.getPeptideBackboneTotal())
                    .setProteinCount(proteinStats.getProteinTotal())
                    .setProteinGroupCount(proteinStats.getProteinGroupTotal())
                    .setPsmCountMs2Count(String.format("%.0f%%", percent));
                rows.add(row);
            }

            return rows;
        });
    }

    private CompletionStage<LfqSummary> generatelfq(LfqApplicationGraph graph) {
        LfqDtoSources dtoSources = new LfqDtoSourcesViaLfqGraph(graph);
        return dtoSources.lfqStatisticsOfFilteredResult().runWith(Sink.headOption(), materializer).thenApply(opt -> {
            LfqStatisticsOfFilteredResult lfqSummary = opt.orElseThrow(NoSuchElementException::new);
            return new LfqSummary()
                .setFeatureCount(lfqSummary.getFeaturesCount())
                .setFeatureIdCount(lfqSummary.getFeaturesWithIdCount())
                .setFeatureVectorsIdCount(lfqSummary.getFeatureVectorsWithIdCount())
                .setProteinGroupCount(lfqSummary.getProteinGroupsCount());
        });
    }

    private CompletionStage<AnalysisDoneTemplate> attachDB(
        ApplicationStorage storage,
        com.bsi.peaks.model.applicationgraph.GraphTaskIds protoGraphTaskIds,
        FilterFunction[] filters,
        Map<String, String> names,
        AnalysisDoneTemplate template
    ) {
        final GraphTaskIds graphTaskIds;
        try {
            graphTaskIds = GraphTaskIds.from(protoGraphTaskIds);
        } catch (Throwable throwable) {
            throw propagate(throwable);
        }
        DbApplicationGraph applicationGraph = new DbApplicationGraphImplementation(storage, graphTaskIds, Optional.empty(), filters);
        return generateIDRows(applicationGraph, names).thenApply(rows -> {
            template.setDbSummary(rows);
            return template;
        });
    }

    private CompletionStage<AnalysisDoneTemplate> attachPtmFinder(
        ApplicationStorage storage,
        com.bsi.peaks.model.applicationgraph.GraphTaskIds protoGraphTaskIds,
        FilterFunction[] filters,
        Map<String, String> names,
        AnalysisDoneTemplate template
    ) {
        final GraphTaskIds graphTaskIds;
        try {
            graphTaskIds = GraphTaskIds.from(protoGraphTaskIds);
        } catch (Throwable throwable) {
            throw propagate(throwable);
        }
        DbApplicationGraph applicationGraph = new DbApplicationGraphImplementation(storage, graphTaskIds, Optional.empty(), filters);
        return generateIDRows(applicationGraph, names).thenApply(rows -> {
            template.setPtmFinderSummary(rows);
            return template;
        });
    }

    private CompletionStage<AnalysisDoneTemplate> attachSpider(
        ApplicationStorage storage,
        com.bsi.peaks.model.applicationgraph.GraphTaskIds protoGraphTaskIds,
        FilterFunction[] filters,
        Map<String, String> names,
        AnalysisDoneTemplate template
    ) {
        final GraphTaskIds graphTaskIds;
        try {
            graphTaskIds = GraphTaskIds.from(protoGraphTaskIds);
        } catch (Throwable throwable) {
            throw propagate(throwable);
        }
        DbApplicationGraph applicationGraph = new DbApplicationGraphImplementation(storage, graphTaskIds, Optional.empty(), filters);
        return generateIDRows(applicationGraph, names).thenApply(rows -> {
            template.setSpiderSummary(rows);
            return template;
        });
    }

    private CompletionStage<AnalysisDoneTemplate> attachSl(
        ApplicationStorage storage,
        com.bsi.peaks.model.applicationgraph.GraphTaskIds protoGraphTaskIds,
        FilterFunction[] filters,
        Map<String, String> names,
        AnalysisDoneTemplate template
    ) {
        final GraphTaskIds graphTaskIds;
        try {
            graphTaskIds = GraphTaskIds.from(protoGraphTaskIds);
        } catch (Throwable throwable) {
            throw propagate(throwable);
        }
        SlApplicationGraph applicationGraph = new SlApplicationGraphImplementation(storage, graphTaskIds, Optional.empty(), filters);
        return generateSlRows(applicationGraph, names).thenApply(rows -> {
            template.setSlSummary(rows);
            return template;
        });
    }

    private CompletionStage<AnalysisDoneTemplate> attachLfq(
        ApplicationStorage storage,
        com.bsi.peaks.model.applicationgraph.GraphTaskIds protoGraphTaskIds,
        FilterFunction[] filters,
        Map<String, String> names,
        AnalysisDoneTemplate template
    ) {
        final GraphTaskIds graphTaskIds;
        try {
            graphTaskIds = GraphTaskIds.from(protoGraphTaskIds);
        } catch (Throwable throwable) {
            throw propagate(throwable);
        }
        LfqApplicationGraph applicationGraph = new LfqApplicationGraphImplementation(storage, graphTaskIds, Optional.empty(), filters);
        return generatelfq(applicationGraph).thenApply(summary -> {
            template.setLfqSummary(summary);
            return template;
        });
    }

    private CompletionStage<AnalysisDoneTemplate> attachRiq(
        ApplicationStorage storage,
        com.bsi.peaks.model.applicationgraph.GraphTaskIds protoGraphTaskIds,
        FilterFunction[] filters,
        AnalysisDoneTemplate template
    ) {
        final GraphTaskIds graphTaskIds;
        try {
            graphTaskIds = GraphTaskIds.from(protoGraphTaskIds);
        } catch (Throwable throwable) {
            throw propagate(throwable);
        }
        RiqApplicationGraph applicationGraph = new RiqApplicationGraphImplementation(storage, graphTaskIds, Optional.empty(), filters);
        RiqDtoSources dtoSources = new RiqDtoSourcesViaRiqGraph(applicationGraph);
        return dtoSources.riqSummaryStatistics().runWith(Sink.headOption(), materializer).thenApply(opt -> {
            ReporterIonQSummaryStatistics riqstats = opt.orElseThrow(NoSuchElementException::new);
            RiqSummary riqSummary = new RiqSummary()
                .setPsmCount(riqstats.getPsmMatches())
                .setProteinGroupCount(riqstats.getProteinGroupsCount())
                .setPeptideSequenceCount(riqstats.getPeptideSequences());
            template.setRiqSummary(riqSummary);
            return template;
        });
    }

    private CompletionStage<AnalysisDoneTemplate> attachSilac(
        ApplicationStorage storage,
        com.bsi.peaks.model.applicationgraph.GraphTaskIds protoGraphTaskIds,
        FilterFunction[] filters,
        AnalysisDoneTemplate template
    ) {
        final GraphTaskIds graphTaskIds;
        try {
            graphTaskIds = GraphTaskIds.from(protoGraphTaskIds);
        } catch (Throwable throwable) {
            throw propagate(throwable);
        }
        SilacApplicationGraph applicationGraph = new SilacApplicationGraphImplementation(storage, graphTaskIds, Optional.empty(), filters);
        return applicationGraph.summary()
            .thenApply(summary -> {
                SilacSummaryTemplate summaryTemplate = new SilacSummaryTemplate();
                summaryTemplate.setFeaturesCount(summary.getFeaturesCount());
                summaryTemplate.setFeatureVectorsCount(summary.getFeatureVectorsCount());
                summaryTemplate.setFeatureVectorGroupsCount(summary.getFeatureVectorGroupsCount());
                summaryTemplate.setPeptideSequencesCount(summary.getPeptideSequencesCount());
                template.setSilacSummary(summaryTemplate);
                return template;
            });
    }

    @NotNull
    public static String generateHtml(AnalysisDoneTemplate template) throws IOException {
        Handlebars handlebars = new Handlebars();
        Template hbs = handlebars.compile(HBS_ANALYSIS_DONE);
        String html = hbs.apply(template);
        html = html.replaceAll("\\s+", " ");
        return html;
    }
}
