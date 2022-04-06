package com.bsi.peaks.server;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.cluster.client.ClusterClient;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.util.Timeout;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.cache.FractionCache;
import com.bsi.peaks.event.work.ServiceRequest;
import com.bsi.peaks.ml.svm.SVM;
import com.bsi.peaks.reader.ScanReaderFactory;
import com.bsi.peaks.server.cluster.CassandraMonitorNodeProcess;
import com.bsi.peaks.server.cluster.InstrumentDaemonNodeProcess;
import com.bsi.peaks.server.cluster.Loader;
import com.bsi.peaks.server.cluster.Master;
import com.bsi.peaks.server.cluster.PeaksTaskResourceCalculator;
import com.bsi.peaks.server.cluster.ServiceContainer;
import com.bsi.peaks.server.cluster.Worker;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.dependency.NodeProcessDependency;
import com.bsi.peaks.server.dependency.GuiceVerticleFactory;
import com.bsi.peaks.server.dependency.GuiceVertxDeploymentManager;
import com.bsi.peaks.server.dependency.LoaderDependency;
import com.bsi.peaks.server.dependency.MasterDependency;
import com.bsi.peaks.server.dependency.WorkerDependency;
import com.bsi.peaks.server.es.CassandraMonitorManager;
import com.bsi.peaks.server.es.EnzymeManager;
import com.bsi.peaks.server.es.FastaDatabaseEntity;
import com.bsi.peaks.server.es.InstrumentDaemonManager;
import com.bsi.peaks.server.es.InstrumentManager;
import com.bsi.peaks.server.es.ModificationEntity;
import com.bsi.peaks.server.es.PeaksWorkCreator;
import com.bsi.peaks.server.es.PeptideDatabaseManager;
import com.bsi.peaks.server.es.ProjectAggregateEntity;
import com.bsi.peaks.server.es.ReporterIonQMethodManager;
import com.bsi.peaks.server.es.SilacQMethodManager;
import com.bsi.peaks.server.es.SpectralLibraryManager;
import com.bsi.peaks.server.es.TaskDeleter;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.WorkManager;
import com.bsi.peaks.server.es.WorkflowManager;
import com.bsi.peaks.server.es.communication.UserManagerCommunication;
import com.bsi.peaks.server.es.peptidedatabase.PeptideDatabaseManagerConfigBuilder;
import com.bsi.peaks.server.es.spectrallibrary.ProcessSpectralLibrary;
import com.bsi.peaks.server.es.work.WorkManagerConfigBuilder;
import com.bsi.peaks.server.service.CassandraMonitorService;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.server.service.ProjectServiceImplementation;
import com.bsi.peaks.server.service.UploaderService;
import com.bsi.peaks.service.services.spectralLibrary.SpectralLibrarySearch;
import com.github.racc.tscg.TypesafeConfigModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.FutureConverters;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Entry point for peaks server
 * @author Shengying Pan
 * Created by span on 1/5/17.
 */
public class Launcher {
    public enum TYPE {
        MASTER, WORKER, PROCESS, LOADER, CASSANDRA_MONITOR, INSTRUMENT_DAEMON
    }

    public static TYPE launcherType = TYPE.MASTER;
    public static String confFile = "";
    public static boolean useCassandraJournal;
    static String taskId;
    private static Logger LOG;
    // entry

    /**
     * Entry point to start the system
     * @param args arguments not used
     */
    public static void main(String[] args) {
        System.out.println("PEAKS Online X build " + SystemConfig.version());

        // parse argument
        Options cmdOptions = new Options();

        Option confOption = new Option("c", "conf", true, "configuration file");
        confOption.setRequired(false);
        cmdOptions.addOption(confOption);

        Option unpackOption = new Option("u", "unpack", false, "unpack resource files");
        unpackOption.setRequired(false);
        cmdOptions.addOption(unpackOption);

        Option processOption = new Option("p", "process", true, "run as subprocess");
        processOption.setRequired(false);
        cmdOptions.addOption(processOption);

        Option migrateOption = new Option("m", "migrate", false, "migrate keyspaces");
        migrateOption.setRequired(false);
        cmdOptions.addOption(migrateOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        final CommandLine cmd;

        try {
            cmd = parser.parse(cmdOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("peaks-server", cmdOptions);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("unpack")) {
            // in unpack model, only unpack resource files, don't start anything
            SVM.unzip();
            SpectralLibrarySearch.unpackResource("svm/orbitrap.model");
            return;
        }

        Injector injector;
        SystemConfig sysConfig;
        Config akkaConfig;
        String taskService = null;
        UUID masterLoaderId= null;
        int taskThreads = 0;
        int workerPort = 0;

        try {
            akkaConfig = ConfigFactory.defaultApplication();
            if (cmd.hasOption("conf")) {
                confFile = cmd.getOptionValue("conf");
                Config overrideConfig = ConfigFactory.parseFile(new File(confFile));
                akkaConfig = overrideConfig.withFallback(akkaConfig);
            }

            // have to set the LOG instance after knowing whether we are starting master or worker
            if (cmd.hasOption("process")) {
                launcherType = TYPE.PROCESS;
                String[] taskTokens = cmd.getOptionValue("process").split("\\|");
                taskId = taskTokens[0];
                taskService = taskTokens[1];
                taskThreads = Integer.parseInt(taskTokens[2]);
                if (taskService.equals("dataLoadingService")) {
                    masterLoaderId = UUID.fromString(taskTokens[3]);
                    launcherType = TYPE.LOADER;
                }
            } else if (akkaConfig.withFallback(ConfigFactory.defaultReference()).getBoolean("peaks.server.cluster.is-master")) {
                launcherType = TYPE.MASTER;
            } else if (akkaConfig.withFallback(ConfigFactory.defaultReference()).getBoolean("peaks.cassandra-monitor.is-cassandra-monitor")) {
                launcherType = TYPE.CASSANDRA_MONITOR;
            } else if (akkaConfig.withFallback(ConfigFactory.defaultReference()).getBoolean("peaks.instrument-daemon.is-instrument-daemon")) {
                launcherType = TYPE.INSTRUMENT_DAEMON;
            } else {
                launcherType = TYPE.WORKER;
            }

            // this is a hack to make logger instantiation after we detect whether we are starting master
            LOG = LoggerFactory.getLogger(Launcher.class);

            if (launcherType == TYPE.MASTER) {
                akkaConfig = ConfigFactory.parseString("akka.cluster.roles = [master]")
                    .withFallback(akkaConfig)
                    .withFallback(ConfigFactory.parseResourcesAnySyntax("master"));
                useCassandraJournal = akkaConfig
                    .getString("akka.persistence.journal.plugin").equals("cassandra-journal");

                if (useCassandraJournal) {
                    akkaConfig = akkaConfig.withFallback(ConfigFactory.parseResourcesAnySyntax("cassandra_journal"));
                } else {
                    akkaConfig = akkaConfig.withFallback(ConfigFactory.parseResourcesAnySyntax("leveldb_journal"));
                }
            } else if (launcherType == TYPE.WORKER) {
                akkaConfig = ConfigFactory.parseString("akka.cluster.roles = [worker]")
                    .withFallback(akkaConfig)
                    .withFallback(ConfigFactory.parseResourcesAnySyntax("worker"));
            } else if (launcherType == TYPE.LOADER) {
                akkaConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port = 0")
                    .withFallback(akkaConfig)
                    .withFallback(ConfigFactory.parseResourcesAnySyntax("worker"));
            } else if (launcherType == TYPE.CASSANDRA_MONITOR) {
                akkaConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port = 0")
                    .withFallback(akkaConfig)
                    .withFallback(ConfigFactory.parseResourcesAnySyntax("monitor"));
            } else if (launcherType == TYPE.INSTRUMENT_DAEMON) {
                akkaConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port = 0")
                    .withFallback(akkaConfig)
                    .withFallback(ConfigFactory.parseResourcesAnySyntax("worker"));
            } else {
                workerPort = akkaConfig.withFallback(ConfigFactory.parseResourcesAnySyntax("worker"))
                    .getInt("akka.remote.netty.tcp.port");
                akkaConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port = 0")
                    .withFallback(akkaConfig)
                    .withFallback(ConfigFactory.parseResourcesAnySyntax("worker"));
            }

            akkaConfig = akkaConfig
                .withFallback(ConfigFactory.parseResourcesAnySyntax("shared_akka"))
                .withFallback(ConfigFactory.defaultReference());

            if (launcherType == TYPE.MASTER) {
                injector = Guice.createInjector(
                        TypesafeConfigModule.fromConfigWithPackage(akkaConfig, "com.bsi.peaks"),
                        new MasterDependency(akkaConfig)
                );
            } else if (launcherType == TYPE.LOADER) {
                injector = Guice.createInjector(
                    TypesafeConfigModule.fromConfigWithPackage(akkaConfig, "com.bsi.peaks"),
                    new LoaderDependency(akkaConfig)
                );
            } else if (launcherType == TYPE.CASSANDRA_MONITOR) {
                injector = Guice.createInjector(
                    TypesafeConfigModule.fromConfigWithPackage(akkaConfig, "com.bsi.peaks"),
                    new NodeProcessDependency(akkaConfig, "CassandraMonitor")
                );
            } else if (launcherType == TYPE.INSTRUMENT_DAEMON) {
                injector = Guice.createInjector(
                    TypesafeConfigModule.fromConfigWithPackage(akkaConfig, "com.bsi.peaks"),
                    new NodeProcessDependency(akkaConfig, "InstrumentDaemon")
                );
            } else {
                injector = Guice.createInjector(
                        TypesafeConfigModule.fromConfigWithPackage(akkaConfig, "com.bsi.peaks"),
                        new WorkerDependency(akkaConfig)
                );
            }

            sysConfig = injector.getInstance(SystemConfig.class);
        } catch (Exception e) {
            // WRite stack trace instead of log, because subsequent exit will exit before log is written.
            e.printStackTrace();
            System.exit(1);
            return;
        }

        LOG.info("PEAKS Online X build " + sysConfig.getVersion());
        LOG.info("Parameters: " + akkaConfig.getObject("peaks.parameters").render(ConfigRenderOptions.concise()));

        // start master or worker actors
        ActorSystem system = injector.getInstance(ActorSystem.class);
        if (launcherType == TYPE.LOADER) {
            // this is launching a loader process
            LOG.info("Starting loader process for task " + taskService + ": " + taskId + "...");
            if (taskService == null) {
                throw new RuntimeException("Missing required argument task service");
            }

            ActorRef clusterClient = injector.getInstance(Key.get(ActorRef.class, Names.named(Master.CLUSTER_CLIENT)));

            ApplicationStorageFactory storageFactory = injector.getInstance(ApplicationStorageFactory.class);
            if (masterLoaderId == null) {
                throw new IllegalStateException("Launching a loader process must include master loader id");
            }
            ActorRef loader = system.actorOf(
                Loader.props(sysConfig, storageFactory, injector.getInstance(ScanReaderFactory.class), clusterClient,
                    UUID.fromString(taskId), masterLoaderId),
                taskService);

            // bridge the message so we can shutdown jvm when service processing is done
            ServiceRequest request = ServiceRequest.newBuilder()
                .setTaskId(taskId)
                .setService(taskService)
                .setContainerPath(loader.path().toSerializationFormat())
                .build();
            Patterns.ask(clusterClient, new ClusterClient.SendToAll(Master.PROXY_PATH, request), sysConfig.getWorkerMessageTimeout())
                .thenCompose(task -> Patterns.ask(loader, task, sysConfig.getWorkerTaskTimeout()))
                .thenCompose(done -> FutureConverters.toJava(system.terminate()))
                .whenComplete((ignore, error) -> {
                    if (error != null) {
                        LOG.error(error.getMessage());
                        System.exit(-1);
                    }
                    System.exit(0);
                });

        } else if (sysConfig.isClusterIsMaster()) {
            final ApplicationStorageFactory storageFactory = injector.getInstance(ApplicationStorageFactory.class);
            if (cmd.hasOption("migrate")) {
                LOG.info("Migrating keyspaces...");
                storageFactory.keyspaces()
                    .runForeach(
                        storageFactory::migrate,
                        ActorMaterializer.create(system)
                    )
                    .toCompletableFuture()
                    .join();
            }

            LOG.info("Starting master...");
            UserManager.start(system);
            FastaDatabaseEntity.start(system, storageFactory.getSystemStorage());
            ProjectAggregateEntity.start(
                system,
                sysConfig.getNumberOfRetriesPerTask(),
                injector.getInstance(PeaksWorkCreator.class),
                injector.getInstance(TaskDeleter.class),
                WorkManager::instance,
                injector.getInstance(UserManagerCommunication.class),
                storageFactory,
                injector.getInstance(FractionCache.class),
                injector.getInstance(UploaderService.class),
                injector.getInstance(CassandraMonitorService.class)
            );

            WorkManager.start(system, new WorkManagerConfigBuilder()
                .minTaskThreads(system.settings().config().getInt("peaks.server.min-task-threads"))
                .maxTaskThreads(system.settings().config().getInt("peaks.server.max-task-threads"))
                .taskThreadsIncrement(system.settings().config().getInt("peaks.server.task-threads-increment"))
                .checkVersion(sysConfig.isClusterWorkerCheckVersion() ? sysConfig.getVersion() : "")
                .unreachableDisconnect(Duration.ofMillis(sysConfig.getClusterMasterWorkTimeout().toMillis()))
                .timeout(sysConfig.getInternalCommunicationTimeout().toMillis())
                .parallelism(sysConfig.getParallelism())
                .build(), injector.getInstance(PeaksTaskResourceCalculator.class),
                injector.getInstance(CassandraMonitorService.class)
            );
            ModificationEntity.start(system);
            EnzymeManager.start(system);
            InstrumentManager.start(system);
            PeptideDatabaseManager.start(
                system,
                new PeptideDatabaseManagerConfigBuilder()
                    .fileFolder(sysConfig.getPeptideDatabaseFolder())
                    .filenameToTaxonomyIdRegex(sysConfig.getPeptideDatabaseFilenameTaxonomyRegex())
                    .peptideDatabaseRepository(storageFactory.getSystemStorage().getPeptideDatabaseRepository())
                    .build()
            );
            ReporterIonQMethodManager.start(system);
            SilacQMethodManager.start(system);
            WorkflowManager.start(system);
            SpectralLibraryManager.start(
                system,
                storageFactory.getSystemStorage().getSpectralLibraryRepository(),
                injector.getInstance(ProcessSpectralLibrary.class)
            );
            CassandraMonitorManager.start(system,
                akkaConfig.withFallback(ConfigFactory.defaultReference()).getInt("peaks.cassandra-monitor.node-communication-timeout"));
            InstrumentDaemonManager.start(system,
                akkaConfig.withFallback(ConfigFactory.defaultReference()).getInt("peaks.instrument-daemon.node-communication-timeout"));
            system.actorOf(ClusterSingletonManager.props(
                    Master.props(sysConfig,
                        storageFactory,
                        injector.getInstance(ScanReaderFactory.class),
                        injector.getInstance(ProjectServiceImplementation.class),
                        injector.getInstance(FastaDatabaseService.class),
                        injector.getInstance(UploaderService.class),
                        CassandraMonitorManager.instance(system),
                        InstrumentDaemonManager.instance(system)
                        ),
                    PoisonPill.getInstance(),
                    ClusterSingletonManagerSettings.create(system).withRole("master")
            ), Master.NAME);

            startServerVerticle(injector);
        } else if (launcherType == TYPE.WORKER) {
            LOG.info("Starting worker...");
            SVM.unzip();
            ActorRef clusterClient = injector.getInstance(Key.get(ActorRef.class, Names.named(Master.CLUSTER_CLIENT)));
            system.actorOf(
                Worker.props(sysConfig, injector.getInstance(ApplicationStorageFactory.class),
                    clusterClient, WorkerDependency.SERVICE_ACTORS
                ),
                Worker.NAME
            );
            FutureConverters.toJava(system.whenTerminated())
                .whenComplete((terminated, throwable) -> {
                    LOG.info("ActorSystem terminated. Now exiting JVM.");
                    System.exit(1);
                });
        } else if (launcherType == TYPE.CASSANDRA_MONITOR) {
            // this is launching a process to process an individual service
            LOG.info("Starting cassandra monitor node process");

            String ipAddress = akkaConfig.getString("peaks.cassandra-monitor.node-ip-address");
            List<String> drives = Arrays.asList(akkaConfig.getString("peaks.cassandra-monitor.drives").split(","));

            ActorRef clusterClient = injector.getInstance(Key.get(ActorRef.class, Names.named(Master.CLUSTER_CLIENT)));

            system.actorOf(
                CassandraMonitorNodeProcess.props(ipAddress, clusterClient, sysConfig, drives, akkaConfig),
                CassandraMonitorNodeProcess.NAME);
        } else if (launcherType == TYPE.INSTRUMENT_DAEMON) {
            // this is launching a process to process an individual service
            LOG.info("Starting instrument daemon node process");

            ActorRef clusterClient = injector.getInstance(Key.get(ActorRef.class, Names.named(Master.CLUSTER_CLIENT)));

            system.actorOf(
                InstrumentDaemonNodeProcess.props(clusterClient, sysConfig, akkaConfig),
                InstrumentDaemonNodeProcess.NAME);
        } else {
            // this is launching a process to process an individual service
            LOG.info("Starting process for task " + taskService + ": " + taskId + "...");
            if (taskService == null) {
                throw new RuntimeException("Missing required argument task service");
            }

            String hostname = akkaConfig.getString("akka.remote.netty.tcp.hostname");
            ActorSelection selection = system.actorSelection("akka.tcp://" + Worker.NAME + "@" + hostname + ":" + workerPort + "/user/" + Worker.NAME);
            ActorRef worker = FutureConverters.toJava(selection.resolveOne(Timeout.create(sysConfig.getWorkerMessageTimeout())))
                .whenComplete((ignore, error) -> {
                    if (error != null) {
                        LOG.error("Unable to resolve worker deamon actor, shutdown it self", error);
                        System.exit(1);
                    }
                })
                .toCompletableFuture().join();

            ApplicationStorageFactory storageFactory = injector.getInstance(ApplicationStorageFactory.class);
            ActorRef container = system.actorOf(
                ServiceContainer.props(taskService, UUID.fromString(taskId), worker, storageFactory, taskThreads), taskService + "_container"
            );

            // bridge the message so we can shutdown jvm when service processing is done
            ServiceRequest request = ServiceRequest.newBuilder()
                .setTaskId(taskId)
                .setService(taskService)
                .setContainerPath(container.path().toSerializationFormat())
                .build();
            Patterns.ask(worker, request, sysConfig.getWorkerMessageTimeout())
                .thenCompose(task -> Patterns.ask(container, task, sysConfig.getWorkerTaskTimeout()))
                .thenCompose(result -> Patterns.ask(worker, result, sysConfig.getWorkerMessageTimeout()))
                .thenCompose(done -> FutureConverters.toJava(system.terminate()))
                .whenComplete((ignore, error) -> {
                    if (error != null) {
                        LOG.error("Caught exception communicating with service container or worker, shut down sub-process", error);
                        System.exit(-1);
                    } else {
                        System.exit(0);
                    }
                });
        }
    }

    private static void startServerVerticle(Injector injector) {
        Vertx vertx = injector.getInstance(Vertx.class);
        SystemConfig sysConfig = injector.getInstance(SystemConfig.class);
        GuiceVerticleFactory verticleFactory = new GuiceVerticleFactory(injector);
        vertx.registerVerticleFactory(verticleFactory);
        GuiceVertxDeploymentManager deploymentManager = new GuiceVertxDeploymentManager(vertx);
        DeploymentOptions options = new DeploymentOptions()
            .setInstances(sysConfig.getServerInstances());
        deploymentManager.deployVerticle(ServerVerticle.class, options, result -> {
            if (result.succeeded()) {
                LOG.info("Server verticle successfully deployed");
            } else {
                LOG.error("Server verticle failed to deploy", result.cause());
                System.exit(1);
            }
        });
    }

}
