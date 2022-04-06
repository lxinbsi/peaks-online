package com.bsi.peaks.server.handlers.helper;

import com.bsi.peaks.model.query.ViewFilter;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class ExportHelpers {
    public static <T extends ViewFilter> T getViewFilter(ObjectMapper om, Map<String, String> request, Class<T> viewFilter) {
        String viewFilterQuery = request.get(ViewFilter.VIEW_FILTER_PARAM_NAME);
        try {
            if (viewFilterQuery == null) {
                viewFilterQuery = "{}"; //empty object use default values
            }
            return om.readValue(viewFilterQuery, viewFilter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void writeFastaSequence(OutputStream output, byte[] sequence, int length) throws IOException {
        int startIndex = 0;
        final int charsPerRow = 80;
        while (startIndex < length) {
            int charsInThisRow = Math.min(length - startIndex, charsPerRow);
            output.write(sequence, startIndex, charsInThisRow);
            output.write('\n');
            startIndex += charsPerRow;
        }
    }
}
