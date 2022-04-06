package com.bsi.peaks.server.es.state;

import com.bsi.peaks.event.tasks.WorkflowStep;
import com.bsi.peaks.model.proteomics.fasta.FastaSequenceDatabaseInfo;
import com.bsi.peaks.model.proto.SimplifiedFastaSequenceDatabaseInfo;
import com.bsi.peaks.server.es.tasks.Helper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

public class BackwardCompatibility {

    private static final Logger LOG = LoggerFactory.getLogger(BackwardCompatibility.class);

    @NotNull
    public static List<SimplifiedFastaSequenceDatabaseInfo> getSimplifiedFastaSequenceDatabaseInfos(List<WorkflowStep> stepsList) {
        for (WorkflowStep step : stepsList) {
            switch (step.getStepType()) {

                case DB_TAG_SEARCH:
                    try {
                        ObjectNode parameters = (ObjectNode) Helper.OM.readTree(step.getStepParametersJson()).get("parameters");
                        if (parameters.hasNonNull("dbInfo")) {
                            List<FastaSequenceDatabaseInfo> dbInfos = Helper.OM.readValue(
                                Helper.OM.treeAsTokens(parameters.get("dbInfo")),
                                Helper.OM.getTypeFactory().constructCollectionType(List.class, FastaSequenceDatabaseInfo.class)
                            );
                            return Lists.transform(dbInfos, FastaSequenceDatabaseInfo::simplifiedFastaSequenceDatabaseInfo);
                        } else {
                            // If it doesn't exist here, it won't exist anywhere else.
                            return Collections.emptyList();
                        }
                    } catch (Exception e) {
                        LOG.warn("Difficulty parsing DB_TAG_SEARCH", e);
                        continue;
                    }

                // Did FDB ever slip into production? I think we can remove this since it only affects internal projects never released into the wild.
                case FDB_PROTEIN_CANDIDATE_FROM_FASTA:
                    try {
                        UnknownFieldSet unknownFields = step.getFastDBProteinCandidateParameters().getUnknownFields();
                        if (unknownFields.hasField(1)) { // Proto index of dbInfos.
                            ImmutableList.Builder<SimplifiedFastaSequenceDatabaseInfo> listBuilder = ImmutableList.builder();
                            for (ByteString item : unknownFields.getField(1).getLengthDelimitedList()) {
                                listBuilder.add(SimplifiedFastaSequenceDatabaseInfo.parseFrom(item));
                            }
                            return listBuilder.build();
                        }
                    } catch (Exception e) {
                        LOG.warn("Difficulty parsing FDB_PROTEIN_CANDIDATE_FROM_FASTA", e);
                        continue;
                    }
            }
        }
        return Collections.emptyList();
    }
}
