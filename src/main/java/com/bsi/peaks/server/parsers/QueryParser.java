package com.bsi.peaks.server.parsers;

import com.bsi.peaks.model.dto.FilterList;
import com.bsi.peaks.model.dto.IDFilter;
import com.bsi.peaks.model.dto.SlFilter;
import com.bsi.peaks.model.filter.ProteinHitFilter;
import com.bsi.peaks.model.filter.SlProteinHitFilter;
import java.util.function.Function;

public class QueryParser {
    //TODO take as input
    public static FilterList mergeFilters(FilterList request, FilterList loaded, Function<Float, Float> proteinFdrConverter, Function<Float, Float> pValueFdrConverter) {
        FilterList.Builder builder = loaded.toBuilder();
        //check both loaded and request, just incase request wasn't set correctly
        if (loaded.getIdFiltersCount() > 0 && request.getIdFiltersCount() > 0) {
            final IDFilter requestIdFilter = request.getIdFilters(0);
            final IDFilter.IdentificationType identificationType = requestIdFilter.getIdentificationType();
            builder.removeIdFilters(0);

            if (identificationType == IDFilter.IdentificationType.NONE) {
                builder.addIdFilters(loaded.getIdFilters(0).toBuilder()
                    .setDenovoCandidateFilter(requestIdFilter.getDenovoCandidateFilter()));
            } else {
                builder.addIdFilters(loaded.getIdFilters(0).toBuilder()
                    .setProteinHitFilter(ProteinHitFilter
                        .fromDto(requestIdFilter.getProteinHitFilter(), proteinFdrConverter, pValueFdrConverter)
                        .toDto()));
            }
        } else if (loaded.hasLfqProteinFeatureVectorFilter() && request.hasLfqProteinFeatureVectorFilter()) {
            builder.setLfqProteinFeatureVectorFilter(request.getLfqProteinFeatureVectorFilter());
        } else if (loaded.hasReporterIonIonQProteinFilter() && request.hasReporterIonIonQProteinFilter()) {
            builder.setReporterIonIonQProteinFilter(request.getReporterIonIonQProteinFilter());
        } else if (loaded.hasSlFilter() && request.hasSlFilter()) {
            builder.setSlFilter(loaded.getSlFilter().toBuilder()
                .setProteinHitFilter(SlProteinHitFilter.fromProto(request.getSlFilter().getProteinHitFilter(), proteinFdrConverter, pValueFdrConverter)
                    .proto(SlFilter.ProteinHitFilter.newBuilder()))
            );
        } else if (loaded.hasDiaDbFilter() && request.hasDiaDbFilter()) {
            builder.setDiaDbFilter(loaded.getDiaDbFilter().toBuilder()
                .setProteinHitFilter(SlProteinHitFilter.fromProto(request.getDiaDbFilter().getProteinHitFilter(), proteinFdrConverter, pValueFdrConverter)
                    .proto(SlFilter.ProteinHitFilter.newBuilder()))
            );
        } else if (loaded.hasSilacProteinFilter() && request.hasSilacProteinFilter()) {
            builder.setSilacProteinFilter(request.getSilacProteinFilter());
        }

        return builder.build();
    }
}
