package com.bsi.peaks.server.es;

import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.server.es.state.AnalysisTaskInformation;

import java.util.List;

@FunctionalInterface
public interface AnalysisWorkCreator {
    List<TaskDescription> workFrom(AnalysisTaskInformation analysis) throws Exception;
}
