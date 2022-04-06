package com.bsi.peaks.server.parsers;

import akka.japi.Pair;
import com.bsi.peaks.event.ModificationCommandFactory;
import com.bsi.peaks.event.ReporterIonQMethodCommandFactory;
import com.bsi.peaks.event.modification.ModificationResponse;
import com.bsi.peaks.event.reportionqmethod.ReporterIonQMethodResponse;
import com.bsi.peaks.internal.task.*;
import com.bsi.peaks.model.dto.ReporterIonQParameters;
import com.bsi.peaks.model.dto.peptide.ReporterIonQMethod;
import com.bsi.peaks.model.parameters.DenovoParameters;
import com.bsi.peaks.model.parameters.DenovoParametersBuilder;
import com.bsi.peaks.model.parameters.IDBaseParameters;
import com.bsi.peaks.model.parameters.IDBaseParametersBuilder;
import com.bsi.peaks.model.parameters.PtmFinderParameters;
import com.bsi.peaks.model.parameters.PtmFinderParametersBuilder;
import com.bsi.peaks.model.parameters.SpiderParameters;
import com.bsi.peaks.model.parameters.SpiderParametersBuilder;
import com.bsi.peaks.model.proteomics.Modification;
import com.bsi.peaks.model.proto.ModificationCategory;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.communication.ModificationCommunication;
import com.bsi.peaks.server.es.communication.RiqMethodManagerCommunication;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ProtocolStringList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

@Singleton
public class ParametersPopulater {
    private static final int FIX_PTM_WEIGHT = 0;
    private static final int VAR_PTM_WEIGHT = 1;

    private final ModificationCommunication modificationCommunication;
    private final RiqMethodManagerCommunication riqMethodManagerCommunication;

    @Inject
    public ParametersPopulater(ModificationCommunication modificationCommunication, RiqMethodManagerCommunication riqMethodManagerCommunication) {
        this.modificationCommunication = modificationCommunication;
        this.riqMethodManagerCommunication = riqMethodManagerCommunication;
    }

    public ReporterIonQBatchCalculationParameters populateRepoterIonQParameters(ReporterIonQBatchCalculationParameters parameters) {
        String methodName = parameters.getParameters().getMethod().getMethodName();
        ReporterIonQMethodResponse.Entries methods = riqMethodManagerCommunication
            .query(ReporterIonQMethodCommandFactory.actingAsUserId(UserManager.ADMIN_USERID).queryByName(methodName))
            .thenApply(ReporterIonQMethodResponse::getEntries)
            .toCompletableFuture().join();
        if (methods.getEntriesCount() != 1) {
            throw new IllegalArgumentException("Unable to fetch reporter ion q method");
        }
        ReporterIonQMethod method = methods.getEntries(0).getData();

        return ReporterIonQBatchCalculationParameters.newBuilder()
            .mergeFrom(parameters)
            .setParameters(ReporterIonQParameters.newBuilder()
                .mergeFrom(parameters.getParameters())
                .setMethod(method)
                .build())
            .build();
    }

    public DenovoParameters populateDeNovoParameters(DenovoParameters parameters) {
        final List<String> fixedNames = parameters.fixedModificationNames();
        final List<String> variableNames = parameters.variableModificationNames();
        return getModificationsByName(fixedNames)
            .thenCombine(
                getModificationsByName(variableNames),
                (fixed, variable) -> new DenovoParametersBuilder().from(parameters)
                    .fixedModification(Iterables.transform(fixed, Modification::from))
                    .variableModification(Iterables.transform(variable, Modification::from))
                    .build()
            ).toCompletableFuture().join();
    }

    private CompletionStage<List<com.bsi.peaks.model.proto.Modification>> getModificationsByName(List<String> names) {
        return modificationCommunication
            .query(ModificationCommandFactory.queryModificationByName(UserManager.ADMIN_USERID, names))
            .thenApply(ModificationResponse::getModificationList);
    }

    public IDBaseParameters populateDBParameters(IDBaseParameters parameters) {
        final List<Modification> fixed = new ArrayList<>();
        final List<Modification> variable = new ArrayList<>();

        getModificationsByName(parameters.fixedModificationNames())
            .toCompletableFuture().join()
            .forEach(m -> add(fixed, variable, m, FIX_PTM_WEIGHT));

        getModificationsByName(parameters.variableModificationNames())
            .toCompletableFuture().join()
            .forEach(m -> add(variable, fixed, m, m.getWeight()));

        return new IDBaseParametersBuilder().from(parameters)
            .fixedModification(fixed)
            .variableModification(variable)
            .build();
    }
    public DiaDbSearchTaskParameters.Builder populateDiaDbSearchTaskParameters(DiaDbSearchTaskParameters.Builder builder, DiaDbSearchParameters parameters) {
        Pair<List<com.bsi.peaks.model.proto.Modification>, List<com.bsi.peaks.model.proto.Modification>> modsPair =
                getFixedVariableModsFromStringList(parameters.getDiaDbSearchSharedParameters().getFixedModificationNamesList(),
                    parameters.getDiaDbSearchSharedParameters().getVariableModificationNamesList());
        builder.addAllFixedModifications(modsPair.first())
                .addAllVariableModifications(modsPair.second());
        return builder;
    }

    public DiaPreSearchTaskParameters.Builder populateDiaDbPreSearchTaskParameters(DiaPreSearchTaskParameters.Builder builder, DiaDbPreSearchParameters parameters) {
        Pair<List<com.bsi.peaks.model.proto.Modification>, List<com.bsi.peaks.model.proto.Modification>> modsPair =
                getFixedVariableModsFromStringList(parameters.getDiaDbSearchSharedParameters().getFixedModificationNamesList(),
                    parameters.getDiaDbSearchSharedParameters().getVariableModificationNamesList());
        builder.addAllFixedModifications(modsPair.first())
                .addAllVariableModifications(modsPair.second());
        return builder;
    }

    private Pair<List<com.bsi.peaks.model.proto.Modification>, List<com.bsi.peaks.model.proto.Modification>>
    getFixedVariableModsFromStringList(
        ProtocolStringList fixedList,
        ProtocolStringList variableList
    ) {
        final List<com.bsi.peaks.model.proto.Modification> fixed = new ArrayList<>();
        final List<com.bsi.peaks.model.proto.Modification> variable = new ArrayList<>();

        getModificationsByName(fixedList)
            .toCompletableFuture().join()
            .forEach(m -> addProto(fixed, variable, m, FIX_PTM_WEIGHT));

        getModificationsByName(variableList)
            .toCompletableFuture().join()
            .forEach(m -> addProto(variable, fixed, m, m.getWeight()));

        return Pair.create(fixed, variable);
    }

    private static void addProto(
        List<com.bsi.peaks.model.proto.Modification> one,
        List<com.bsi.peaks.model.proto.Modification> other,
        com.bsi.peaks.model.proto.Modification m, int weight
    ) {
        String name = m.getName();
        if (one.stream().anyMatch(mod -> mod.getName().equals(name))) {
            return;
        }
        if (other.stream().anyMatch(mod -> mod.getName().equals(name))) {
            return;
        }
        one.add(m.toBuilder().setWeight(Math.min(m.getWeight(), weight)).build());
    }
    public PtmFinderParameters populatePTMParameters(PtmFinderParameters parameters) {
        final List<Modification> fixed = new ArrayList<>();
        final List<Modification> variable = new ArrayList<>();

        getModificationsByName(parameters.fixedModificationNames())
            .toCompletableFuture().join()
            .forEach(m -> add(fixed, variable, m, FIX_PTM_WEIGHT));

        getModificationsByName(parameters.variableModificationNames())
            .toCompletableFuture().join()
            .forEach(m -> add(variable, fixed, m, VAR_PTM_WEIGHT));

        if (parameters.allPTM()) {
            modificationCommunication
                .query(ModificationCommandFactory.queryAllBuiltInModification(UserManager.ADMIN_USERID))
                .thenApply(ModificationResponse::getModificationList)
                .toCompletableFuture().join()
                .stream()
                .filter(modification -> modification.getCategory() == ModificationCategory.COMMON || modification.getCategory() == ModificationCategory.UNCOMMON)
                .forEach(m -> {
                    int weight = m.getWeight();
                    if (weight == 0) {
                        add(fixed, variable, m, weight);
                    } else {
                        add(variable, fixed, m, weight);
                    }
                });
        }

        boolean isCamC = false;
        for (Modification m : fixed) {
            if (m.name().equals("Carbamidomethylation")) {
                isCamC = true;
                break;
            }
        }
        if (isCamC) {
            for (int i = 0; i < variable.size(); i++) {
                if (variable.get(i).name().equals("Carbamidomethylation (DHKE, X@N-term)")) {
                    variable.set(i, variable.get(i).withWeight(VAR_PTM_WEIGHT));
                    break;
                }
            }
        }
        return new PtmFinderParametersBuilder().from(parameters)
            .fixedModification(fixed)
            .variableModification(variable)
            .build();
    }

    public SpiderParameters populateSpiderParameters(SpiderParameters parameters) {
        final List<Modification> fixed = new ArrayList<>();
        final List<Modification> variable = new ArrayList<>();

        getModificationsByName(parameters.fixedModificationNames())
            .toCompletableFuture().join()
            .forEach(m -> add(fixed, variable, m, FIX_PTM_WEIGHT));

        getModificationsByName(parameters.variableModificationNames())
            .toCompletableFuture().join()
            .forEach(m -> add(variable, fixed, m, VAR_PTM_WEIGHT));

        return new SpiderParametersBuilder().from(parameters)
            .fixedModification(fixed)
            .variableModification(variable)
            .build();
    }

    private static void add(List<Modification> one, List<Modification> other, com.bsi.peaks.model.proto.Modification m, int weight) {
        String name = m.getName();
        if (one.stream().map(Modification::name).anyMatch(name::equals)) {
            return;
        }
        if (other.stream().map(Modification::name).anyMatch(name::equals)) {
            return;
        }
        one.add(Modification.from(m.toBuilder().setWeight(Math.min(m.getWeight(), weight)).build()));
    }
}
