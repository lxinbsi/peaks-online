package com.bsi.peaks.server.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClient;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.dto.DaemonUploadRequest;
import com.bsi.peaks.model.dto.FilePath;
import com.bsi.peaks.model.dto.FileStructureNode;
import com.bsi.peaks.model.dto.InstrumentDaemon;
import com.bsi.peaks.model.dto.NodeStatus;
import com.bsi.peaks.model.dto.ParsingRule;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.handlers.helper.InstrumentDaemonHelper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.Lists;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.bsi.peaks.server.handlers.helper.InstrumentDaemonHelper.matchesParsingRule;

public class InstrumentDaemonNodeProcess extends AbstractActor {
    public static final String NAME = "InstrumentDaemonNodeProcess";
    public static final int BYTES_IN_A_MB = 1_000_000;
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final ActorRef clusterClient;
    private final SystemConfig sysConfig;
    private final Map<UUID, ProjectScan> currentJobs = new HashMap<>();
    private final Map<UUID, Map<String, FileStatus>> fileProgressTracker = new HashMap<>();
    private final String scanningDirectory;
    private final String name;
    private final String instrument;
    private final String apiKey;
    private final String masterHostName;
    private final int masterPort;
    private final int fileSizeMinimumBytes;
    private final int maxScanningDepth;
    private final String ipAddress;
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    protected static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new ProtobufModule())
        .registerModule(new Jdk8Module());


    public InstrumentDaemonNodeProcess(
        ActorRef clusterClient,
        SystemConfig sysConfig,
        Config akkaConfig
    ) {
        this.clusterClient = clusterClient;
        this.sysConfig = sysConfig;
        scanningDirectory = akkaConfig.withFallback(ConfigFactory.defaultReference())
            .getString("peaks.instrument-daemon.scanning-directory");
        name = akkaConfig.withFallback(ConfigFactory.defaultReference())
            .getString("peaks.instrument-daemon.name");
        instrument = akkaConfig.withFallback(ConfigFactory.defaultReference())
            .getString("peaks.instrument-daemon.instrument-type");
        apiKey = akkaConfig.withFallback(ConfigFactory.defaultReference())
            .getString("peaks.instrument-daemon.api-key");
        ipAddress = akkaConfig.withFallback(ConfigFactory.defaultReference())
            .getString("peaks.instrument-daemon.ip-address");
        // one hour default
        this.masterHostName = akkaConfig.getString("peaks.instrument-daemon.master-url");
        this.masterPort = akkaConfig.getInt("peaks.instrument-daemon.master-http-port");
        this.fileSizeMinimumBytes = akkaConfig.getInt("peaks.instrument-daemon.file-size-min-in-mb") * BYTES_IN_A_MB;
        this.maxScanningDepth = akkaConfig.getInt("peaks.instrument-daemon.scanning-depth");
        int intervalInSeconds = akkaConfig.withFallback(ConfigFactory.defaultReference())
            .getInt("peaks.instrument-daemon.update-interval");

        getContext().system()
            .scheduler()
            .schedule(
                Duration.ofMillis(0),
                Duration.ofSeconds(intervalInSeconds),
                () -> {
                    heartBeat();
                    try {
                        scan();
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                },
                getContext().system().dispatcher());
    }

    public static Props props(
        ActorRef clusterClient,
        SystemConfig sysConfig,
        Config akkaConfig
    ) {
        return Props.create(InstrumentDaemonNodeProcess.class, () ->
            new InstrumentDaemonNodeProcess(clusterClient, sysConfig, akkaConfig));
    }

    private void heartBeat() {
        clusterClient.tell(new ClusterClient.SendToAll(Master.PROXY_PATH,
                InstrumentDaemon.newBuilder()
                    .setName(this.name)
                    .setInstrumentType(this.instrument)
                    .setApiKey(this.apiKey)
                    .addUserId(User.admin().userId().toString())
                    .setStatus(NodeStatus.ONLINE)
                    .setIpAddress(ipAddress)
                    .setLastTimeCommunicated(Instant.now().getEpochSecond())
                    .build()),
            getSelf());
    }

    private void scan() {
        if (currentJobs.isEmpty()) return;

        // only do scanning if there are jobs
        File directory = new File(scanningDirectory);
        recursiveScanFolder(directory, 1);
    }

    private void recursiveScanFolder(File directory, int curDepth) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    if (file.getName().contains(".d")) {
                        scanFile(file);
                    } else if (curDepth < this.maxScanningDepth){
                        recursiveScanFolder(file, curDepth + 1);
                    }
                } else {
                    scanFile(file);
                }
            }
        }
    }

    private void scanFile(File file) {
        String absoluteFilePath = file.getAbsolutePath();
        String fileName = file.getName();
        currentJobs.entrySet().stream().filter(pair -> {
            ProjectScan job = pair.getValue();
            ParsingRule parsingRule = job.parsingRule;
            int curFractionIdx = job.lastUsedIdx + 1;
            int sampleIdx = curFractionIdx / parsingRule.getFractionsPerSample();
            return sampleIdx < job.sampleIds.size();
        }).forEach(pair -> {
            ProjectScan job = pair.getValue();
            ParsingRule parsingRule = job.parsingRule;

            List<String> delimiters = InstrumentDaemonHelper.getDelimiters(parsingRule);
            Path filePath = Paths.get(absoluteFilePath);
            try {
                long newSize = file.isDirectory() ? folderSize(file) : Files.size(filePath);
                if (fileProgressTracker.containsKey(job.projectId) &&
                    fileProgressTracker.get(job.projectId).containsKey(fileName)) {
                    FileStatus fileStatus = fileProgressTracker.get(job.projectId).get(fileName);
                    if (!fileStatus.hasUploaded()) {
                        long oldSize = fileStatus.fileSize();
                        if (newSize >= this.fileSizeMinimumBytes && newSize == oldSize) {
                            List<String> parsedList = matchesParsingRule(file.getName(), delimiters, parsingRule);
                            List<String> exampleList = matchesParsingRule(parsingRule.getExample(), delimiters, parsingRule);
                            if (!parsedList.isEmpty() && !exampleList.isEmpty() && parsedList.get(0).equals(exampleList.get(0))) {
                                int curFractionIdx = job.lastUsedIdx + 1;
                                int sampleIdx = curFractionIdx / parsingRule.getFractionsPerSample();
                                if (sampleIdx < job.sampleIds.size()) {
                                    UUID sampleId = job.sampleIds.get(sampleIdx);
                                    UUID projectId = job.projectId;
                                    UUID fractionId = job.fractionIds.get(curFractionIdx);
                                    // convert parsed list to name and upload with the name
                                    HttpPost post = new HttpPost(masterHostName + ":" + masterPort + sysConfig.getApiRoot() + "/projects/" + projectId + "/uploadDaemon/" + sampleId + "/" + fractionId);
                                    post.setHeader("instrument-daemon-name", name);
                                    post.setHeader("instrument-daemon-api-key", apiKey);
                                    post.setHeader("remotePathName", job.copyLocation.getRemotePathName());
                                    post.setHeader("subPath", job.copyLocation.getSubPath());
                                    post.setHeader("sampleName", parsedList.get(1));
                                    post.setHeader("enzyme", parsedList.get(2));
                                    post.setHeader("activationMethod", parsedList.get(3));
                                    post.setHeader("fractionName", file.getName());
                                    MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
                                    if (file.isFile()) {
                                        multipartEntityBuilder.addPart(absoluteFilePath, new FileBody(file));
                                    } else if (file.isDirectory() && file.getName().contains(".d")) {
                                        FileStructureNode fileStructureNode = dfsDirectory(file, multipartEntityBuilder);
                                        String fileStructureNodeJsonString = OM.writeValueAsString(fileStructureNode);
                                        post.setHeader("fileStructure", fileStructureNodeJsonString);
                                    }
                                    post.setEntity(multipartEntityBuilder.build());
                                    job.lastUsedIdx += 1;
                                    fileStatus = fileStatus.withHasUploaded(true);
                                    CloseableHttpResponse closeableHttpResponse = httpClient.execute(post);
                                    closeableHttpResponse.close();
                                }
                            }
                            fileStatus = fileStatus.withFileSize(newSize)
                                .withTimeStamp(Instant.now());
                            fileProgressTracker.get(job.projectId).put(fileName, fileStatus);
                        } else {
                            fileProgressTracker.get(job.projectId).put(fileName,
                                fileStatus.withFileSize(newSize)
                                    .withTimeStamp(Instant.now()));
                        }
                    }
                } else {
                    fileProgressTracker.computeIfAbsent(job.projectId, projectId -> fileProgressTracker.put(projectId, new HashMap<>()));
                    fileProgressTracker.get(job.projectId).put(fileName,
                        new FileStatusBuilder().fileSize(newSize)
                            .timeStamp(Instant.now())
                            .copyLocation(job.copyLocation)
                            .hasUploaded(false)
                            .build());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(DaemonUploadRequest.class, this::handleUploadRequest)
            .build();
    }

    public FileStructureNode dfsDirectory(File file, MultipartEntityBuilder multipartEntityBuilder) {
        FileStructureNode.Builder builder = FileStructureNode.newBuilder();
        builder.setName(file.getName());
        builder.setIsDirectory(file.isDirectory());
        if (file.isDirectory()){
            List<FileStructureNode> children = new ArrayList<>();
            for (File child: file.listFiles()) {
                children.add(dfsDirectory(child, multipartEntityBuilder));
            }
            builder.addAllChildren(children);
        } else {
            multipartEntityBuilder.addPart(file.getAbsolutePath(), new FileBody(file));
        }
        return builder.build();
    }

    public static long folderSize(File directory) {
        long length = 0;
        for (File file : directory.listFiles()) {
            if (file.isFile())
                length += file.length();
            else
                length += folderSize(file);
        }
        return length;
    }

    public void handleUploadRequest(DaemonUploadRequest daemonUploadRequest) {
        UUID projectId = ModelConversion.uuidFrom(daemonUploadRequest.getProjectId());
        ParsingRule parsingRule = daemonUploadRequest.getParsingRule();
        List<UUID> sampleIds = Lists.newArrayList(ModelConversion.uuidsFrom(daemonUploadRequest.getSampleIds()));
        List<UUID> fractionIds = Lists.newArrayList(ModelConversion.uuidsFrom(daemonUploadRequest.getFractionIds()));
        ProjectScan projectScan = new ProjectScan(projectId, parsingRule, sampleIds, fractionIds, daemonUploadRequest.getCopyLocation());
        this.currentJobs.put(projectId, projectScan);
    }

    private class ProjectScan {
        UUID projectId;
        ParsingRule parsingRule;
        List<UUID> sampleIds;
        List<UUID> fractionIds;
        int lastUsedIdx = -1;
        FilePath copyLocation;
        ProjectScan(
            UUID projectId,
            ParsingRule parsingRule,
            List<UUID> sampleIds,
            List<UUID> fractionIds,
            FilePath copyLocation
        ) {
            this.projectId = projectId;
            this.parsingRule = parsingRule;
            this.sampleIds = sampleIds;
            this.fractionIds = fractionIds;
            this.copyLocation = copyLocation;
        }
    }
}
