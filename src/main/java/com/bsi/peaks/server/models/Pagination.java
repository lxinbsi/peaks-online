package com.bsi.peaks.server.models;

import com.bsi.peaks.model.PeaksImmutable;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * @author span
 *         Created by span on 5/26/2017.
 */
@PeaksImmutable
@JsonDeserialize(builder = PaginationBuilder.class)
public interface Pagination {
    int page();
    int pageSize();
    int totalPages();
    int totalItems();
}
