package com.bsi.peaks.server.handlers.helper;

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
import com.bsi.peaks.model.filter.DenovoCandidateFilter;
import com.bsi.peaks.model.filter.DenovoCandidateFilterBuilder;
import com.bsi.peaks.model.filter.FilterFunction;
import com.bsi.peaks.model.filter.ProteinFilter;
import com.bsi.peaks.model.filter.ProteinFilterBuilder;
import com.bsi.peaks.model.filter.ProteinHitFilter;
import com.bsi.peaks.model.filter.ProteinHitFilterBuilder;
import com.bsi.peaks.model.filter.PsmFilter;
import com.bsi.peaks.model.filter.PsmFilterBuilder;
import com.bsi.peaks.model.query.IdentificationQuery;
import com.bsi.peaks.model.query.LabelFreeQuery;
import com.bsi.peaks.model.query.ReporterIonQMethodQuery;
import com.bsi.peaks.model.query.SilacQuery;
import com.bsi.peaks.model.query.SlQuery;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Singleton
public class ApplicationGraphFactory {
    public static final String SAMPLE_ID_API_PARAMETERS = "sample";
    public static final String INCLUDE_DECOY_API_PARAMETERS = "includeDecoy";
    private final ApplicationStorageFactory storageFactory;

    @Inject
    public ApplicationGraphFactory(ApplicationStorageFactory storageFactory) {
        this.storageFactory = storageFactory;
    }

    public DbApplicationGraph createFromSampleOnly(GraphTaskIds graphTaskIds) {
        ApplicationStorage applicationStorage = storageFactory.getStorage(graphTaskIds.keyspace());
        return new DbApplicationGraphImplementation(applicationStorage, graphTaskIds, Optional.empty());
    }

    public DbApplicationGraph createFromIdentificationQuery(
            GraphTaskIds graphTaskIds,
            IdentificationQuery identificationQuery,
            Map<String, String> apiParameters
    ) {
        return createFromIdentificationQuery(graphTaskIds, identificationQuery.filters(), apiParameters);
    }

    public DbApplicationGraph createFromIdentificationQueryAllDenovo(
        GraphTaskIds graphTaskIds,
        IdentificationQuery identificationQuery,
        Map<String, String> apiParameters
    ) {
        final FilterFunction[] filters = identificationQuery.filters();
        for (int i = 0; i < filters.length; i++) {
            if (filters[i] instanceof DenovoCandidateFilter) {
                filters[i] = new DenovoCandidateFilterBuilder()
                    .from((DenovoCandidateFilter) filters[i])
                    .top(false)
                    .build();
            }
        }
        return createFromIdentificationQuery(graphTaskIds, identificationQuery.filters(), apiParameters);
    }

    public DbApplicationGraph createFromIdentificationQueryTopDenovo(
        GraphTaskIds graphTaskIds,
        IdentificationQuery identificationQuery,
        Map<String, String> apiParameters
    ) {
        final FilterFunction[] filters = identificationQuery.filters();
        for (int i = 0; i < filters.length; i++) {
            if (filters[i] instanceof DenovoCandidateFilter) {
                filters[i] = new DenovoCandidateFilterBuilder()
                    .from((DenovoCandidateFilter) filters[i])
                    .top(true)
                    .build();
            }
        }
        return createFromIdentificationQuery(graphTaskIds, identificationQuery.filters(), apiParameters);
    }

    public DbApplicationGraph createFromIdentificationQuery(
        GraphTaskIds graphTaskIds,
        FilterFunction[] filters,
        Map<String, String> apiParameters
    ) {
        final String sample = apiParameters.get(SAMPLE_ID_API_PARAMETERS);
        Optional<UUID> sampleId = Strings.isNullOrEmpty(sample) ? Optional.empty() : Optional.of(UUID.fromString(sample));

        if (apiParameters.containsKey(INCLUDE_DECOY_API_PARAMETERS)) {
            for (int i = 0; i < filters.length; i++) {
                filters[i] = filters[i].withIncludeDecoy(true);
            }
        }
        ApplicationStorage applicationStorage = storageFactory.getStorage(graphTaskIds.keyspace());
        return new DbApplicationGraphImplementation(applicationStorage, graphTaskIds, sampleId, filters);
    }

    public SlApplicationGraph createFromSlQueryNoDecoy(
        GraphTaskIds graphTaskIds,
        SlQuery query,
        Map<String, String> apiParameters
    ) {
        final String sample = apiParameters.get(SAMPLE_ID_API_PARAMETERS);
        Optional<UUID> sampleId = Strings.isNullOrEmpty(sample) ? Optional.empty() : Optional.of(UUID.fromString(sample));

        ApplicationStorage applicationStorage = storageFactory.getStorage(graphTaskIds.keyspace());
        final FilterFunction[] filters = query.filters();
        if (apiParameters.containsKey(INCLUDE_DECOY_API_PARAMETERS)) {
            for (int i = 0; i < filters.length; i++) {
                filters[i] = filters[i].withIncludeDecoy(true);
            }
        }
        return new SlApplicationGraphImplementation(applicationStorage, graphTaskIds, sampleId, filters);
    }

    public DbApplicationGraph createFromIdentificationQueryIncludeDecoy(
            GraphTaskIds graphTaskIds,
            IdentificationQuery identificationQuery,
            Map<String, String> apiParameters
    ) {
        final String sample = apiParameters.get(SAMPLE_ID_API_PARAMETERS);
        Optional<UUID> sampleId = Strings.isNullOrEmpty(sample) ? Optional.empty() : Optional.of(UUID.fromString(sample));

        // Do not modify original - make a copy.
        final FilterFunction[] originalFilters = identificationQuery.filters();
        final FilterFunction[] filters = Arrays.copyOf(originalFilters, originalFilters.length);
        for (int i = 0; i < filters.length; i++) {
            if (filters[i] instanceof PsmFilter) {
                filters[i] = new PsmFilterBuilder()
                        .from((PsmFilter) filters[i])
                        .includeDecoy(true)
                        .build();
            } else if (filters[i] instanceof ProteinHitFilter) {
                filters[i] = new ProteinHitFilterBuilder()
                        .from((ProteinHitFilter) filters[i])
                        .includeDecoy(true)
                        .build();
            } else if (filters[i] instanceof ProteinFilter) {
                filters[i] = new ProteinFilterBuilder()
                    .from((ProteinFilter) filters[i])
                    .includeDecoy(true)
                    .build();
            }
        }
        ApplicationStorage applicationStorage = storageFactory.getStorage(graphTaskIds.keyspace());
        return new DbApplicationGraphImplementation(applicationStorage, graphTaskIds, sampleId, filters);
    }

    public SilacApplicationGraph createFromSilacQuery(
        GraphTaskIds graphTaskIds,
        SilacQuery silacQuery,
        Map<String, String> apiParameters
    ) {
        Optional<UUID> sampleId = Optional.empty(); // SILAC never does any export or query specified by sample.

        ApplicationStorage applicationStorage = storageFactory.getStorage(graphTaskIds.keyspace());
        return new SilacApplicationGraphImplementation(applicationStorage, graphTaskIds, sampleId, silacQuery.filters());
    }

    public LfqApplicationGraph createFromLabelFreeQuery(
            GraphTaskIds graphTaskIds,
            LabelFreeQuery labelFreeQuery,
            Map<String, String> apiParameters
    ) {
        Optional<UUID> sampleId = Optional.empty(); // LFQ never does any export or query specified by sample.

        ApplicationStorage applicationStorage = storageFactory.getStorage(graphTaskIds.keyspace());
        return new LfqApplicationGraphImplementation(applicationStorage, graphTaskIds, sampleId, labelFreeQuery.filters());
    }

    public RiqApplicationGraph createFromReporterIonQMethodQuery(
        GraphTaskIds graphTaskIds,
        ReporterIonQMethodQuery query,
        Map<String, String> apiParameters
    ) {
        final String sample = apiParameters.get(SAMPLE_ID_API_PARAMETERS);
        Optional<UUID> sampleId = Strings.isNullOrEmpty(sample) ? Optional.empty() : Optional.of(UUID.fromString(sample));

        ApplicationStorage applicationStorage = storageFactory.getStorage(graphTaskIds.keyspace());
        return new RiqApplicationGraphImplementation(applicationStorage, graphTaskIds, sampleId, query.filters());
    }

    public LfqApplicationGraph lfqNoFilters(
        GraphTaskIds graphTaskIds
    ) {
        Optional<UUID> sampleId = Optional.empty(); // LFQ never does any export or query specified by sample.

        ApplicationStorage applicationStorage = storageFactory.getStorage(graphTaskIds.keyspace());
        return new LfqApplicationGraphImplementation(applicationStorage, graphTaskIds, sampleId);
    }
}
