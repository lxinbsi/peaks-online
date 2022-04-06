package com.bsi.peaks.server.service;

import akka.Done;
import com.bsi.peaks.event.CassandraMonitorCommandFactory;
import com.bsi.peaks.model.dto.CassandraMonitorInfo;
import com.bsi.peaks.model.dto.CassandraNodeInfo;
import com.bsi.peaks.server.es.communication.CassandraMonitorCommunication;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peaksdata.event.cassandraMonitor.CassandraMonitor;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class CassandraMonitorService {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraMonitorService.class);
    private final CassandraMonitorCommunication cassandraMonitorCommunication;

    @Inject
    public CassandraMonitorService(
        final CassandraMonitorCommunication cassandraMonitorCommunication
    ) {
        this.cassandraMonitorCommunication = cassandraMonitorCommunication;
    }

    public CompletionStage<Boolean> checkFull(UUID actingAsUserId) {
        CassandraMonitorCommandFactory commandFactory = CassandraMonitorCommandFactory.actingAsUserId(actingAsUserId);
        return cassandraMonitorCommunication.query(commandFactory.checkFull())
            .thenApply(CassandraMonitor.CassandraMonitorResponse::getIsFull);
    }

    public CompletionStage<CassandraMonitorInfo> monitorInfo(UUID actingAsUserId) {
        CassandraMonitorCommandFactory commandFactory = CassandraMonitorCommandFactory.actingAsUserId(actingAsUserId);
        return cassandraMonitorCommunication.query(commandFactory.queryCassandraMonitor())
            .thenApply(CassandraMonitor.CassandraMonitorResponse::getCassandraMonitorInfo);
    }

    public CompletionStage<Done> delete(UUID actingAsUserId, String ipAddress) {
        CassandraMonitorCommandFactory commandFactory = CassandraMonitorCommandFactory.actingAsUserId(actingAsUserId);
        return cassandraMonitorCommunication.command(commandFactory.delete(ipAddress));
    }

    public CompletionStage<Done> deleteAll(UUID actingAsUserId) {
        CassandraMonitorCommandFactory commandFactory = CassandraMonitorCommandFactory.actingAsUserId(actingAsUserId);
        return cassandraMonitorCommunication.command(commandFactory.deleteAll());
    }

    public CompletionStage<Done> updateCutOff(UUID actingAsUserId, float newCutOff) {
        CassandraMonitorCommandFactory commandFactory = CassandraMonitorCommandFactory.actingAsUserId(actingAsUserId);
        return cassandraMonitorCommunication.command(commandFactory.updateCutOff(newCutOff));
    }

    public CompletionStage<Done> updateIgnoreMonitor(UUID actingAsUserId, boolean newIgnoreMonitor) {
        CassandraMonitorCommandFactory commandFactory = CassandraMonitorCommandFactory.actingAsUserId(actingAsUserId);
        return cassandraMonitorCommunication.command(commandFactory.updateIgnoreMonitor(newIgnoreMonitor));
    }

    public CompletionStage<Done> updateNodeInfo(UUID actingAsUserId, CassandraNodeInfo nodeInfo) {
        CassandraMonitorCommandFactory commandFactory = CassandraMonitorCommandFactory.actingAsUserId(actingAsUserId);
        return cassandraMonitorCommunication.command(commandFactory.updateNodeInfo(nodeInfo));
    }
}
