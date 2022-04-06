package com.bsi.peaks.server.models;

import java.util.List;

/**
 * @author span
 *         Created by span on 5/26/2017.
 */

public class PageResult {
    private List items;
    private Pagination pagination;

    public PageResult() {}

    public PageResult(List items, Pagination pagination) {
        this.items = items;
        this.pagination = pagination;
    }

    public List getItems() {
        return items;
    }

    public void setItems(List items) {
        this.items = items;
    }

    public Pagination getPagination() {
        return pagination;
    }

    public void setPagination(Pagination pagination) {
        this.pagination = pagination;
    }
}
