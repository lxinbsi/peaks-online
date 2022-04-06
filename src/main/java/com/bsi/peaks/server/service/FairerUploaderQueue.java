package com.bsi.peaks.server.service;

import com.bsi.peaks.event.uploader.FileToUpload;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.time.Instant;
import java.util.*;

public class FairerUploaderQueue {
    private final static double WEIGHT_MULTIPLIER = 1.5;
    private final static double POLLING_BONUS_MULTIPLIER = 1.2;
    private final static double MINIMUM_WEIGHT = 0.05;
    private final static int POLLING_BONUS_INTERVAL_MILLIS = 240000;
    // everything is synchronized to make it thread-safe, it's OK for performance as these are not frequently called upon
    private final LinkedHashMap<UUID, LinkedList<FileToUpload>> projectsFiles = new LinkedHashMap<>();
    private final Map<UUID, Instant> lastPolledAt = new HashMap<>();

    public synchronized FileToUpload poll() {
        // expected behaviour:
        // projects added earlier are more likely to get polled
        // projects haven't been polled for a longer time are more likely to get polled
        // every project has a chance to get polled
        if (projectsFiles.isEmpty()) return null;
        List<Pair<UUID, Double>> weights = new ArrayList<>();
        int index = 0;
        for (UUID projectId : projectsFiles.keySet()) {
            long timeElapsed = Math.max(0, Instant.now().toEpochMilli() - lastPolledAt.get(projectId).toEpochMilli());
            double bonusLevel = Math.max(1.0D, Math.ceil((double)timeElapsed / POLLING_BONUS_INTERVAL_MILLIS));
            double bonusTier = Math.log(bonusLevel) / Math.log(2.0);
            double weight = 1.0D / Math.pow(WEIGHT_MULTIPLIER, index) * Math.pow(POLLING_BONUS_MULTIPLIER, bonusTier);
            weights.add(new Pair<>(projectId, Math.max(weight, MINIMUM_WEIGHT)));
            index++;
        }

        UUID pick = new EnumeratedDistribution<>(weights).sample();
        lastPolledAt.put(pick, Instant.now());
        FileToUpload next = projectsFiles.get(pick).poll();
        if (projectsFiles.get(pick).size() == 0) {
            projectsFiles.remove(pick);
            lastPolledAt.remove(pick);
        }

        return next;
    }

    public synchronized boolean isEmpty() {
        return projectsFiles.isEmpty();
    }

    public synchronized void addAllForProject(UUID projectId, Collection<FileToUpload> c) {
        if (c.isEmpty()) return;
        projectsFiles.computeIfAbsent(projectId, ignore -> new LinkedList<>()).addAll(c);
        lastPolledAt.computeIfAbsent(projectId, ignore -> Instant.now());
    }
}
