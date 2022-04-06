package com.bsi.peaks.server.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClient;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import com.bsi.peaks.model.dto.CassandraNodeInfo;
import com.bsi.peaks.model.dto.NodeStatus;
import com.bsi.peaks.server.config.SystemConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class CassandraMonitorNodeProcess extends AbstractActor {
    public static final String NAME = "CassandraMonitorNodeProcess";
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public CassandraMonitorNodeProcess(
        String nodeIPAddress,
        ActorRef clusterClient,
        SystemConfig sysConfig,
        List<String> drives,
        Config akkaConfig
    ) {
        int intervalInSeconds = akkaConfig.withFallback(ConfigFactory.defaultReference())
            .getInt("peaks.cassandra-monitor.update-interval");

        getContext().system()
            .scheduler()
            .schedule(
                Duration.ofMillis(0),
                Duration.ofSeconds(intervalInSeconds),
                () -> {
                    CassandraNodeInfo.Builder builder =
                        CassandraNodeInfo.newBuilder()
                        .setIPAddress(nodeIPAddress)
                        .setNodeStatus(NodeStatus.ONLINE);
                    long totalUsed = 0L;
                    long totalSpace = 0L;
                    for(String curDrive: drives) {
                        File file = new File(curDrive);
                        if (file.exists()) {
                            long curTotalSpace = file.getTotalSpace();
                            long curFreeSpace = file.getUsableSpace();
                            CassandraNodeInfo.NodeDriveInfo nodeDriveInfo = CassandraNodeInfo.NodeDriveInfo.newBuilder()
                                .setDiskDirectory(file.toString())
                                .setUsedDiskBytes(curTotalSpace - curFreeSpace)
                                .setFreeDiskBytes(curFreeSpace)
                                .setTotalDiskBytes(curTotalSpace)
                                .build();
                            builder.addNodeDriveInfos(nodeDriveInfo);
                            totalSpace += curTotalSpace;
                            totalUsed += curTotalSpace - curFreeSpace;
                        }
                    }

                    CassandraNodeInfo cassandraNodeInfo = builder.setNodeDiskUsagePercent((float) ((double) totalUsed / totalSpace))
                        .setLastTimeCommunicated(Instant.now().getEpochSecond())
                        .build();

                    Patterns.ask(clusterClient, new ClusterClient.SendToAll(Master.PROXY_PATH, cassandraNodeInfo), sysConfig.getWorkerMessageTimeout());
                },
                getContext().system().dispatcher());
    }

    public static Props props(
        String nodeIPAddress,
        ActorRef clusterClient,
        SystemConfig sysConfig,
        List<String> drives,
        Config akkaConfig
    ) {
        return Props.create(CassandraMonitorNodeProcess.class, () ->
            new CassandraMonitorNodeProcess(nodeIPAddress, clusterClient, sysConfig, drives, akkaConfig));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
}
