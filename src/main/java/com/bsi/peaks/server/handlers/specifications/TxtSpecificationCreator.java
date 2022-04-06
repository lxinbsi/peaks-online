package com.bsi.peaks.server.handlers.specifications;

import akka.japi.Pair;
import akka.japi.tuple.Tuple3;
import akka.japi.tuple.Tuple4;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import com.bsi.peaks.analysis.analysis.dto.Analysis;
import com.bsi.peaks.analysis.analysis.dto.DbSearchStep;
import com.bsi.peaks.analysis.analysis.dto.SlStep;
import com.bsi.peaks.common.future.FutureHelper;
import com.bsi.peaks.io.writer.txt.AnalysisParametersTxt;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.model.dto.Sample;
import com.bsi.peaks.model.dto.SpectralLibraryListItem;
import com.bsi.peaks.model.proteomics.fasta.fastasequencedatabase.FastaSequenceDatabaseAttributes;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.server.service.SpectralLibraryService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface TxtSpecificationCreator {
    ProjectService projectService();
    FastaDatabaseService databaseService();
    SpectralLibraryService spectralLibraryService();

    Materializer createMaterializer();

    default TxtSpecification downloadAllParameters() {
        return new TxtSpecification(
            "parameters.txt",
            ((request, user) -> {
                UUID projectId = UUID.fromString(request.get("pid"));
                UUID analysisId = UUID.fromString(request.get("aid"));
                return projectService().analysis(projectId, analysisId, user)
                    .thenCompose(analysis -> {
                        List<CompletionStage<Pair<UUID, String>>> collect = new java.util.ArrayList<>(Collections.emptyList());
                        if (analysis.hasDbSearchStep()) {
                            DbSearchStep dbSearchStep = analysis.getDbSearchStep();
                            collect.addAll(Stream.concat(
                                dbSearchStep.getDbSearchParameters().getDatabasesList().stream(),
                                dbSearchStep.getDbSearchParameters().getContaminantDatabasesList().stream()
                            )
                                .map(database -> {
                                    final UUID dbId = UUID.fromString(database.getId());
                                    return databaseService().safeFindDatabaseById(dbId)
                                        .thenApply(db -> Pair.create(dbId, db.map(FastaSequenceDatabaseAttributes::name).orElse("Does not exist")));
                                })
                                .collect(Collectors.toList()));
                        }
                        if (analysis.hasSlStep()) {
                            SlStep slStep = analysis.getSlStep();
                            collect.addAll(Stream.concat(
                                slStep.getSlProteinInferenceStepParameters().getDatabasesList().stream(),
                                slStep.getSlProteinInferenceStepParameters().getContaminantDatabasesList().stream()
                            )
                                .map(db -> databaseService().findDatabaseById(UUID.fromString(db.getId())))
                                .map(future -> future.thenApply(db -> Pair.create(db.id(), db.name())))
                                .collect(Collectors.toList()));

                            UUID slId = UUID.fromString(slStep.getSlSearchStepParameters().getSlIdOrName());
                            collect.add(spectralLibraryService().safeGet(user.userId(), slId)
                                .thenApply(sl -> Pair.create(slId, sl.map(SpectralLibraryListItem::getSlName).orElse("Does not exist")))
                            );
                        }

                        return FutureHelper.combineToList(collect, false)
                            .thenCombine(projectService().projectSamples(projectId, user)
                            .filter(sample -> analysis.getSampleIdsList().stream().anyMatch(sample.getId()::equals))
                            .runWith(Sink.seq(), createMaterializer()), Pair::create)
                            .thenApply(p -> Tuple3.create(analysis, p.first(), p.second()));
                    })
                    .thenCombine(projectService().project(projectId, user), (tuple3, project) -> Tuple4.create(tuple3.t1(), tuple3.t2(), tuple3.t3(), project))
                    .thenCombine(projectService().analysisQueriesAll(projectId, analysisId, user), (tuple4, workFlowTypeQueryMap) -> {
                        Analysis analysis = tuple4.t1();
                        Map<UUID, String> dbSpectralLibraryMap = new HashMap<>();
                        tuple4.t2().forEach(pair -> dbSpectralLibraryMap.put(pair.first(), pair.second()));
                        final List<Sample> sampleList = tuple4.t3();
                        Project project = tuple4.t4();
                        return new AnalysisParametersTxt(project, analysis, sampleList, dbSpectralLibraryMap, workFlowTypeQueryMap);
                    });
            })
        );
    }
}
