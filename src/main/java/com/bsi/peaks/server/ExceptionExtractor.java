package com.bsi.peaks.server;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

class ExceptionExtractor {
    private static Map<String, String> exceptionMapping = ImmutableMap.<String, String>builder()
        .put("maximum number of keyspaces", "You have reached maximum number of time frames, please delete some time frames to proceed")
        .build();

    String extractException(Throwable e) {
        for (String key : exceptionMapping.keySet()) {
            if (!e.getMessage().isEmpty() && e.getMessage().toLowerCase().contains(key.toLowerCase())) {
                return exceptionMapping.get(key);
            }
        }
        return "";
    }

    String beautifyException(Throwable e) {
        if (e.getMessage().toLowerCase().contains("exception: ")) {
            return e.getMessage().substring(e.getMessage().toLowerCase().indexOf("exception: ") + 10);
        } else {
            return e.getMessage();
        }
    }
}
