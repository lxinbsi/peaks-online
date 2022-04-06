package com.bsi.peaks.server.es;

import akka.Done;
import com.bsi.peaks.model.dto.StepType;
import com.bsi.peaks.model.system.levels.LevelTask;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface TaskDeleter {
    /**
     * Delete data produced by task.
     * @return
     */
    CompletionStage<Done> deleteTask(String keyspace, StepType stepType, UUID taskId, LevelTask level);

    /**
     * Delete project data produced by tasks, but not identifiable by TaskDescription.
     * This should be done after all processing has been cancelled within project.
     * @return
     */
    CompletionStage<Done> deleteProject(UUID projectId);
}
