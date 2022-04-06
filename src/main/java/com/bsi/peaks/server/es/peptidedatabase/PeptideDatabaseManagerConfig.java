package com.bsi.peaks.server.es.peptidedatabase;

import com.bsi.peaks.data.storage.application.PeptideDatabaseRepository;
import com.bsi.peaks.model.PeaksImmutable;
import org.immutables.value.Value;

import java.util.regex.Pattern;

@PeaksImmutable
public interface PeptideDatabaseManagerConfig {
    String fileFolder();
    String filenameToTaxonomyIdRegex();
    PeptideDatabaseRepository peptideDatabaseRepository();

    @Value.Derived
    default Pattern filenameToTaxonomyId() {
        return Pattern.compile(filenameToTaxonomyIdRegex());
    }
}
