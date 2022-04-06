package com.bsi.peaks.server.models;

import com.bsi.peaks.model.PeaksImmutable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Map;

/**
 * @author span
 *         created on 6/29/17.
 */
@PeaksImmutable
@JsonDeserialize(builder = PeaksEnumsBuilder.class)
public interface PeaksEnums {
    Map<String, String> activationMethods();
    Map<String, String> fastaIdPatterns();
    Map<String, String> fastaDescriptionPatterns();
    Map<String, String> fastaTypes();
    List<String> modificationTypes();
    List<String> modificationSources();
    List<String> modificationCategories();
}
