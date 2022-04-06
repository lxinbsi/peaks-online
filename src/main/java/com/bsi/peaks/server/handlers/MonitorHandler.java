package com.bsi.peaks.server.handlers;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.WorkQueryFactory;
import com.bsi.peaks.event.work.WorkQueryResponse;
import com.bsi.peaks.model.dto.CassandraNodeInfo;
import com.bsi.peaks.model.dto.MonitorLog;
import com.bsi.peaks.model.dto.MonitorMaster;
import com.bsi.peaks.model.dto.TimeFrame;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.WorkManager;
import com.bsi.peaks.server.es.communication.WorkManagerCommunication;
import com.bsi.peaks.server.service.CassandraMonitorService;
import com.bsi.peaks.server.service.LogService;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.server.service.UploaderService;
import com.google.inject.Inject;
import com.sun.management.OperatingSystemMXBean;
import com.typesafe.config.Config;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class MonitorHandler extends ProtoHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MonitorHandler.class);

    private final LogService logService;
    private final Config config;
    private final WorkManagerCommunication workManagerCommunication;
    private final UploaderService uploaderService;
    private final ApplicationStorageFactory storageFactory;
    private final ProjectService projectService;
    private final CassandraMonitorService cassandraMonitorService;

    @Inject
    public MonitorHandler(
        final Vertx vertx,
        final ActorSystem system,
        final LogService logService,
        final Config config,
        final WorkManagerCommunication workManagerCommunication,
        final UploaderService uploaderService,
        final ApplicationStorageFactory storageFactory,
        final ProjectService projectService,
        CassandraMonitorService cassandraMonitorService
    ) {
        super(vertx, system);
        this.logService = logService;
        this.config = config;
        this.workManagerCommunication = workManagerCommunication;
        this.uploaderService = uploaderService;
        this.storageFactory = storageFactory;
        this.projectService = projectService;
        this.cassandraMonitorService = cassandraMonitorService;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.get("/master").handler(this::getMaster);
        subRouter.post("/cassandra/cut-off/:cutOff").handler(this::updateThreshold);
        subRouter.post("/cassandra/ignore-monitor/:isIgnore").handler(this::updateIgnoreMonitor);
        subRouter.get("/cassandra").handler(this::getCassandra);
        subRouter.get("/cassandra/status").handler(this::getStatus);
        subRouter.post("/cassandra/updateNodeInfo").handler(this::updateNodeInfo);
        subRouter.post("/cassandra/deleteAll").handler(this::deleteAllNodes);
        subRouter.post("/cassandra/delete/:ipAddress").handler(this::deleteNode);
        subRouter.get("/master/log/:lines").handler(this::getMasterLogLines);
        subRouter.get("/master/log/:start/:end").handler(this::getMasterLogStartEnd);
        subRouter.get("/worker").handler(this::getWorker);
        subRouter.post("/master/clear-uploader-quota").handler(this::clearUploaderQuota);
        subRouter.get("/master/time-frames").handler(this::listTimeFrames);
        subRouter.delete("/master/keyspace/:keyspace").handler(this::dropKeyspaceAndProjects);
        return subRouter;
    }

    // this endpoint will first drop keyspace and then delete all projects associated to it
    private void dropKeyspaceAndProjects(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final String keyspcae = context.request().getParam("keyspace");
            LOG.info("Trying to drop keyspace " + keyspcae + ", this will force deleting of all projects in the keyspace");
            storageFactory.dropKeyspace(keyspcae)
                .thenCompose(ignore -> projectService.deleteProjectsInKeyspace(user, keyspcae))
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        });
    }

    private void listTimeFrames(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            // there is no payload
            List<TimeFrame> sortedTimeFrames = storageFactory.timeFrames()
                .runWith(StreamConverters.asJavaStream(), storageFactory.getMaterializer())
                .sorted(Comparator.comparingLong(TimeFrame::getStartTimestamp).reversed())
                .collect(Collectors.toList());

            serializationProcedure(context.request(), context.response(), Source.from(sortedTimeFrames), "monitorMaster")
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        context.response().end();
                    }
                });
        });
    }

    private void clearUploaderQuota(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            // there is no payload
            uploaderService.clearQuota();
            ok(context);
        });
    }

    private void getCassandra(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            cassandraMonitorService.monitorInfo(user.userId()).whenComplete((cassandraMonitorInfo, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    ok(context, cassandraMonitorInfo);
                }
            });
        });
    }

    private void getStatus(RoutingContext context) {
        cassandraMonitorService.checkFull(UUID.fromString(UserManager.ADMIN_USERID))
            .whenComplete((isFull, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    context.response()
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(Json.encode(isFull));
                }
            });
    }

    private void updateThreshold(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final String cutOff = context.request().getParam("cutOff");
            cassandraMonitorService.updateCutOff(user.userId(), Float.parseFloat(cutOff))
                .thenCompose(done -> cassandraMonitorService.monitorInfo(user.userId()))
                .whenComplete((cassandraMonitorInfo, innerError) -> {
                    if (innerError != null) {
                        context.fail(innerError);
                    } else {
                        ok(context, cassandraMonitorInfo);
                    }
                });
        });
    }

    private void updateIgnoreMonitor(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final String isIgnore = context.request().getParam("isIgnore");
            cassandraMonitorService.updateIgnoreMonitor(user.userId(), Boolean.parseBoolean(isIgnore))
                .thenCompose(done -> cassandraMonitorService.monitorInfo(user.userId()))
                .whenComplete((cassandraMonitorInfo, innerError) -> {
                    if (innerError != null) {
                        context.fail(innerError);
                    } else {
                        ok(context, cassandraMonitorInfo);
                    }
                });

        });
    }

    private void updateNodeInfo(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            CassandraNodeInfo nodeInfo = getBody(context, CassandraNodeInfo.newBuilder());
            cassandraMonitorService.updateNodeInfo(user.userId(), nodeInfo)
                .thenCompose(done -> cassandraMonitorService.monitorInfo(user.userId()))
                .whenComplete((cassandraMonitorInfo, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context, cassandraMonitorInfo);
                    }
                });
        });
    }


    private void deleteAllNodes(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            cassandraMonitorService.deleteAll(user.userId())
                .thenCompose(done -> cassandraMonitorService.monitorInfo(user.userId()))
                .whenComplete((cassandraMonitorInfo, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context, cassandraMonitorInfo);
                    }
                });
        });
    }

    private void deleteNode(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final String ipAddress = context.request().getParam("nodeIpAddress");
            cassandraMonitorService.delete(user.userId(), ipAddress)
                .thenCompose(done -> cassandraMonitorService.monitorInfo(user.userId()))
                .whenComplete((cassandraMonitorInfo, innerError) -> {
                    if (innerError != null) {
                        context.fail(innerError);
                    } else {
                        ok(context, cassandraMonitorInfo);
                    }
                });
        });
    }

    private void getMaster(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        OperatingSystemMXBean bean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);

        final long totalMem = Runtime.getRuntime().totalMemory();
        final long freeMem = Runtime.getRuntime().freeMemory();
        final long usedMem = totalMem - freeMem;

        String hostAddress;
        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("Unable to get host address", e);
            hostAddress = "Unknown";
        }

        Source<MonitorMaster, NotUsed> source = Source.single(MonitorMaster.newBuilder()
            .setUsedMemoryBytes(usedMem)
            .setAvailableMemoryBytes(totalMem)
            .setFreeMemoryBytes(freeMem)
            .setCpuUsagePercent((float) bean.getProcessCpuLoad())
            .setIpAddress(hostAddress)
            .setMaxFileUploads(Integer.parseInt(config.getConfig("peaks.server.data-loader").getValue("parallelism").render()))
            .build());

        serializationProcedure(request, response, source, "monitorMaster")
            .whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    response.end();
                }
            });
    }

    private void getWorker(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        workManagerCommunication
            .query(WorkQueryFactory.actingAsUserId(WorkManager.ADMIN_USERID).listWorkers())
            .thenApply(WorkQueryResponse::getListWorkers)
            .thenApply(WorkQueryResponse.QueryWorkerResponse::getWorkersList)
            .thenCompose(workers -> serializationProcedure(request, response, Source.from(workers), "monitorWorker"))
            .whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    response.end();
                }
            });
    }

    private void getMasterLogLines(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        int lines = Integer.parseInt(request.getParam("lines"));

        try {
            Source<MonitorLog, NotUsed> source = Source.single(MonitorLog.newBuilder()
                .addAllLines(logService.log(lines))
                .build());
            serializationProcedure(request, response, source, "masterLogs")
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        response.end();
                    }
                });
        } catch (IOException internalException) {
            context.fail(internalException);
        }

    }

    private void getMasterLogStartEnd(RoutingContext context) {
        //date is expected to be in format yyyy-MM-ddTHH:mm
        final HttpServerRequest request = context.request();
        final HttpServerResponse response = context.response();

        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

        final ZonedDateTime start = LocalDateTime.from(formatter.parse(request.getParam("start"))).atZone(ZoneId.systemDefault());
        final ZonedDateTime end = LocalDateTime.from(formatter.parse(request.getParam("end"))).atZone(ZoneId.systemDefault());

        try {
            Source<MonitorLog, NotUsed> source = Source.single(MonitorLog.newBuilder()
                .addAllLines(logService.log(start, end))
                .build());

            serializationProcedure(request, response, source, "masterLogs")
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        response.end();
                    }
                });
        } catch (IOException exception) {
            context.fail(exception);
        }
    }
}
