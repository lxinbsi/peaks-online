package com.bsi.peaks.server;

import akka.actor.ActorSystem;
import akka.persistence.cassandra.ConfigSessionProvider;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import scala.compat.java8.FutureConverters;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class PeaksSessionProvider extends ConfigSessionProvider {

    final List<LatencyTracker> latencyTrackers;

    public PeaksSessionProvider(ActorSystem system, Config config) {
        super(system, config);
        final Config systemConfig = system.settings().config();

        if (systemConfig.getBoolean("peaks.data.storage.application.cassandra.log-query")) {
            final Duration slowQueryThreshold = systemConfig.getDuration("peaks.data.storage.application.cassandra.slow-query-threshold");
            latencyTrackers = ImmutableList.of(QueryLogger.builder()
                .withConstantThreshold(slowQueryThreshold.toMillis())
                .build());
        } else {
            latencyTrackers = Collections.emptyList();
        }
    }

    @Override
    public Future<Session> connect(ExecutionContext ec) {
        final Future<Session> connect = super.connect(ec);
        if (!latencyTrackers.isEmpty()) {
            FutureConverters.toJava(connect)
                .thenAccept(session -> latencyTrackers.forEach(session.getCluster()::register));
        }
        return connect;
    }
}
