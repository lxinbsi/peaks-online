package com.bsi.peaks.server.cluster;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.cluster.client.ClusterClient;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.SharedKillSwitch;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.FractionRepository;
import com.bsi.peaks.data.storage.application.ScanRepository;
import com.bsi.peaks.event.WorkCommandFactory;
import com.bsi.peaks.event.WorkEventFactory;
import com.bsi.peaks.event.tasks.TaskDescription;
import com.bsi.peaks.event.work.LoaderPing;
import com.bsi.peaks.internal.task.FractionLevelTask;
import com.bsi.peaks.message.WorkerMessageFactory;
import com.bsi.peaks.messages.service.CancelService;
import com.bsi.peaks.messages.worker.*;
import com.bsi.peaks.model.core.ms.*;
import com.bsi.peaks.model.core.ms.fraction.FractionAttributes;
import com.bsi.peaks.model.core.ms.fraction.FractionAttributesBuilder;
import com.bsi.peaks.model.proteomics.Instrument;
import com.bsi.peaks.model.proteomics.InstrumentBuilder;
import com.bsi.peaks.model.proto.FractionMsLevelStats;
import com.bsi.peaks.model.proto.FractionStats;
import com.bsi.peaks.reader.*;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.es.tasks.DataLoadingTask;
import com.bsi.peaks.server.es.tasks.Helper;
import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.io.FileUtils;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author span
 *         created on 7/10/17.
 */
@SuppressWarnings("WeakerAccess")
public class Loader extends AbstractActor {
    public enum TimstofReaderType {
        STREAM, JNI
    }
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final OperatingSystemMXBean operatingSystemMXBean;
    private static final String LOADER_CLEANUP_TICK = "LOADER_CLEANUP";
    public static final String NAME = "loader";

    private final UUID workerId = UUID.randomUUID();
    private final UUID masterLoaderId;
    private final Duration timeout;
    private final Cancellable cleanupTask;
    private ActorRef clusterClient;

    private DataLoadingTask currentTask = null;
    private TaskDescription currentTaskDescription = null;

    private final ApplicationStorageFactory storageFactory;
    private final ScanReaderFactory readerFactory;
    private final ActorMaterializer materializer;
    private final Queue<File> deletedFiles = new ConcurrentLinkedDeque<>();

    private final int loaderThreads;
    private SharedKillSwitch killSwitch;
    private final List<Integer> retryInSeconds;
    private final TimstofReaderType timstofReaderType;
    private final LoaderPing loaderPing;

    public static Props props(
        SystemConfig sysConfig,
        ApplicationStorageFactory storageFactory,
        ScanReaderFactory readerFactory,
        ActorRef clusterClient,
        UUID taskId,
        UUID masterLoaderId
    ) {
        return Props.create(Loader.class, () -> new Loader(sysConfig, storageFactory, readerFactory, clusterClient, taskId, masterLoaderId));
    }

    public Loader(
        SystemConfig config,
        ApplicationStorageFactory storageFactory,
        ScanReaderFactory readerFactory,
        ActorRef clusterClient,
        UUID taskId,
        UUID masterLoaderId
    ) {
        this.storageFactory = storageFactory;
        this.readerFactory = readerFactory;
        this.materializer = ActorMaterializer.create(getContext());
        this.timeout = Duration.ofNanos(config.getDataLoaderTimeout().toNanos());
        this.loaderThreads = config.getDataLoaderThreads();
        this.retryInSeconds = config.getDataLoaderRetryInSeconds();
        this.timstofReaderType = config.getTimstofReaderType();
        this.clusterClient = clusterClient;
        this.masterLoaderId = masterLoaderId;

        this.operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        this.cleanupTask = scheduledCleanup(config.getClusterWorkerCleanupInterval());
        this.loaderPing = LoaderPing.newBuilder().setTaskId(taskId.toString()).build();
        tellMaster(loaderPing);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        cleanupTask.cancel();
        log.info("DataLoader {} is stopped", workerId);
    }

    private Cancellable scheduledCleanup(FiniteDuration interval) {
        return getContext().system().scheduler().schedule(
            interval,
            interval,
            getSelf(),
            LOADER_CLEANUP_TICK,
            getContext().dispatcher(),
            getSelf()
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchEquals(Worker.PING, ignore -> pong())
            .match(TaskDescription.class, taskDescription -> {
                currentTaskDescription = taskDescription;
                currentTask = (DataLoadingTask) Helper.convertParameters(taskDescription.getJsonParamaters());
                log.info("DataLoader {} received task {}, become working", workerId, currentTask.taskId());
                doWork(sender());
            })
            .match(CancelService.class, ignore -> handleCancel())
            .matchEquals(LOADER_CLEANUP_TICK, x -> cleanup())
            .build();
    }

    private void pong() {
        tellMaster(loaderPing);
    }

    private void deleteUploadedFile(String uploadedFile) {
        File file = new File(uploadedFile);
        deletedFiles.add(file);
        removeFile(file);
    }

    private boolean removeFile(File file) {
        try {
            if (!file.exists()) {
                return true;
            } else if (file.isDirectory()) {
                FileUtils.deleteDirectory(file);
            } else {
                if (!file.delete()) {
                    throw new IOException("Unable to delete file " + file.getAbsolutePath());
                }
            }
        } catch (IOException e) {
            // log it to debug level as its not very important whether the file is deleted.
            log.debug("Unable to delete uploaded file {}", file.getAbsolutePath());
            return false;
        }

        return true;
    }

    private void handleCancel() {
        final UUID fractionId = currentTask.fractionId();
        try {
            // try to cancel timed out fraction data loading
            log.debug("Try to cancel data loading of fraction {}", fractionId);
            killSwitch.abort(new Exception("Work cancelled"));
        } catch (Exception e) {
            log.error("Unable to cancel data loading of fraction {}", fractionId);
        }
    }

    private void doWork(ActorRef launcher) {
        final FractionLevelTask fractionLevelTask = currentTaskDescription.getFractionLevelTask();
        final UUID fractionId = UUID.fromString(fractionLevelTask.getFractionId());
        final UUID dataLoadingTaskId = currentTask.taskId();
        final String uploadedFile = currentTask.uploadedFile();
        final String sourceFile = currentTask.sourceFile();

        FractionAttributes defaultAttributes = new FractionAttributesBuilder()
            .activationMethod(currentTask.activationMethod())
            .acquisitionMethod(currentTask.acquisitionMethod())
            .enzyme(currentTask.enzyme())
            .instrument(fixInstrument(currentTask.instrument()))
            .srcFile(sourceFile)
            .build();

        final PhysicalFile physicalFile = new PhysicalFileBuilder()
            .filename(sourceFile)
            .physicalFile(new File(uploadedFile))
            .build();

        final long startRealTimeMs = System.currentTimeMillis();
        loadFractionWithRetry(fractionId, physicalFile, defaultAttributes).whenComplete((done, error) -> {
            final int realTimeMs = (int) (System.currentTimeMillis() - startRealTimeMs);
            if (error != null) {
                tellMaster(WorkEventFactory.taskFailed(dataLoadingTaskId, masterLoaderId, error.toString()));
                log.error(error, "Data loading failed for fraction {} file {}", fractionId, sourceFile);
            } else {
                tellMaster(WorkEventFactory.taskCompleted(dataLoadingTaskId, masterLoaderId, realTimeMs, 0));
            }
            deleteUploadedFile(uploadedFile);
            launcher.tell("shut down this subprocess", self());
        });
    }

    CompletionStage<Done> loadFractionWithRetry(UUID fractionId, PhysicalFile physicalFile, FractionAttributes defaultAttributes) {
        return CompletableFuture.supplyAsync(() -> {
            final UUID dataLoadingTaskId = currentTask.taskId();
            final String sourceFile = currentTask.sourceFile();
            final SourceType sourceType = SourceTypeDetector.detect(physicalFile);
            final boolean isTimstof = sourceType == SourceType.BRUKER_TIMSTOF || sourceType == SourceType.PEAKS_RAW_FRAMES;
            final boolean isWiff = sourceType == SourceType.WIFF;

            final Flow<DataLoadingScan, DataLoadingScan, NotUsed> preprocessFlow = isTimstof ?
                new TimstofProcessor(defaultAttributes).flowAsync(loaderThreads) : isWiff ?
                    new WiffCentroidingProcessor(defaultAttributes).flowAsync(loaderThreads) :
                    new CentroidingProcessor(defaultAttributes).flowAsync(loaderThreads);

            for (int i = 0; i <= retryInSeconds.size(); i++) {
                killSwitch = KillSwitches.shared("DataLoaderCanceller" + workerId.toString());
                if (i > 0) {
                    try {
                        log.warning("Retry loading fraction " + fractionId + " file " + sourceFile +
                            ", wait " + retryInSeconds.get(i - 1) + " seconds first");
                        Thread.sleep(retryInSeconds.get(i - 1) * 1000);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                try {
                    ScanRepository scanRepository = storageFactory.getStorage(currentTaskDescription.getKeyspace()).getScanRepository();
                    FractionRepository fractionRepository = storageFactory.getStorage(currentTaskDescription.getKeyspace()).getFractionRepository();

                    // it's OK we write it multiple times, new one will just override old ones
                    final long startRealTimeMs = System.currentTimeMillis();
                    if (sourceType == SourceType.BRUKER_TIMSTOF && timstofReaderType == TimstofReaderType.JNI) {
                        readerFactory.createFrameReader(fractionId, physicalFile, defaultAttributes)
                            .asAkkaSource(this::sendProgress)
                            .via(killSwitch.flow())
                            .completionTimeout(timeout)
                            .toMat(scanRepository.createTimsFrameSink(fractionId, dataLoadingTaskId), Keep.both())
                            .mapMaterializedValue(param -> param.second().thenCompose(done -> param.first())
                                .thenCompose(fraction -> fractionRepository.create(dataLoadingTaskId, fraction))
                            )
                            .run(materializer)
                            .toCompletableFuture().join();
                    } else {
                        readerFactory.create(fractionId, physicalFile, defaultAttributes)
                            .asAkkaSource(this::sendProgress, preprocessFlow)
                            .via(killSwitch.flow())
                            .completionTimeout(timeout)
                            .toMat(scanRepository.createSink(dataLoadingTaskId, isTimstof), Keep.both())
                            .mapMaterializedValue(param -> param.second().thenCompose(done -> param.first())
                                .thenCompose(fraction -> fractionRepository.create(dataLoadingTaskId, fraction))
                            )
                            .run(materializer)
                            .toCompletableFuture().join();
                    }
                    final long loadTimeMs = System.currentTimeMillis() - startRealTimeMs;
                    log.info("Fraction {} file {} loaded in {} seconds", fractionId, sourceFile, (float)loadTimeMs / 1000f);
                    return Done.getInstance();
                } catch (Exception e) {
                    // do nothing, ignore exception, retry it
                    log.error(e, "Something went wrong while loading the file: " + fractionId);
                }
            }

            log.warning("Unable to load fraction " + fractionId + " file " + sourceFile +
                " after " + retryInSeconds.size() + " retries, creating dummy fraction now");

            FractionMsLevelStats dummyLevel = FractionMsLevelStats.newBuilder()
                .setNumberOfScans(0)
                .setMinScanNum(0)
                .setMaxScanNum(0)
                .setMinRetentionTime(com.bsi.peaks.common.Duration.newBuilder().setSecond(0).setNano(0).build())
                .setMaxRetentionTime(com.bsi.peaks.common.Duration.newBuilder().setSecond(0).setNano(0).build())
                .setMinLowMz(0)
                .setMaxHighMz(0)
                .setMaxBasePeakIntensity(0)
                .setTotalTIC(0)
                .setMaxPeaksPerScanOrFrame(0)
                .setTotalPeaks(0)
                .build();

            FractionStats dummyStats = FractionStats.newBuilder()
                .setMinPrecursorCharge(0)
                .setMaxPrecursorCharge(0)
                .setVersion(2)
                .putMsLevelStats(1, dummyLevel)
                .putMsLevelStats(2, dummyLevel)
                .build();

            Fraction dummyFraction = new FractionBuilder()
                .fractionId(fractionId)
                .from(defaultAttributes)
                .fractionStats(dummyStats)
                .hasIonMobility(sourceType == SourceType.BRUKER_TIMSTOF)
                .build();

            storageFactory.getStorage(currentTaskDescription.getKeyspace()).getFractionRepository()
                .create(dataLoadingTaskId, dummyFraction).toCompletableFuture().join();
            return Done.getInstance();
        });
    }

    private Instrument fixInstrument(Instrument instrument) {
        Map<Integer, List<MassAnalyzer>> analyzerMap = new HashMap<>();
        for (int msLevel : instrument.massAnalyzers().keySet()) {
            List<MassAnalyzer> analyzers = new ArrayList<>();
            for (MassAnalyzer analyzer : instrument.massAnalyzers().get(msLevel)) {
                final boolean centroided;
                if (analyzer.type() == MassAnalyzerType.TripleTOF || analyzer.type() == MassAnalyzerType.TOF || analyzer.type() == MassAnalyzerType.Quadrupole) {
                    centroided = analyzer.centroided();
                } else {
                    centroided = true; // for non tripletof/qtof override the centrioding to be true b/c we will do it
                }
                analyzers.add(
                    new MassAnalyzerBuilder().from(analyzer)
                        .centroided(centroided)
                        .build()
                );
            }
            analyzerMap.put(msLevel, analyzers);
        }

        return new InstrumentBuilder().from(instrument).massAnalyzers(analyzerMap).build();
    }

    private void sendProgress(float progress) {
        tellMaster(WorkEventFactory.taskProcessing(currentTask.taskId(), masterLoaderId, progress));
    }

    private void tellMaster(Object message) {
        clusterClient.tell(new ClusterClient.SendToAll(Master.PROXY_PATH, message), getSelf());
    }

    private void cleanup() {
        List<File> leftover = new ArrayList<>();
        File toRemove = deletedFiles.poll();
        while (toRemove != null) {
            if (!removeFile(toRemove)) {
                leftover.add(toRemove);
            }
            toRemove = deletedFiles.poll();
        }
        deletedFiles.addAll(leftover);
    }

    public static DataLoadingScan adaptDDAScan(ScanWithPeaks scanWithPeaks) {
        // Casting to Scan works because internally the `from` method uses reflection, and all values are extracted.
        // This is confirmed in DataLoadingArchiverTest
        return new DataLoadingScanBuilder().from((Scan) scanWithPeaks).build();
    }
}
