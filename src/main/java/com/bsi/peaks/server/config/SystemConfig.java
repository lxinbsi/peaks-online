package com.bsi.peaks.server.config;

import com.bsi.peaks.server.cluster.Loader;
import com.github.racc.tscg.TypesafeConfig;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Server configuration file
 * @author Shengying Pan
 * Created by span on 1/12/17.
 */
@Singleton
public class SystemConfig {
    public static final String CLUSTER_NAME_KEY = "peaks.server.cluster.name";
    public static final String version = version();

    private final int serverInstances;
    private final int serverHttpPort;
    private final String serverRootURL;
    private final int numberOfRetriesPerTask;
    private final boolean serverWebIntegration;
    private final String serverJWTSecret;
    private final FiniteDuration serverWorkRegistrationTimeout;
    private final boolean isDebug;
    private final FiniteDuration dataLoaderLoadInterval;
    private final int dataLoaderParallelism;
    private final int parallelism;
    private final FiniteDuration dataLoaderTimeout;
    private final FiniteDuration fastaDatabaseTimeout;
    private final FiniteDuration serverSessionTimeout;
    private final FiniteDuration internalCommunicationTimeout;
    private final int dataLoaderThreads;

    private final String apiRoot;
    private final boolean apiAllowCors;
    private final int apiSearchLimit;

    private final String certificateLocation;
    private final String certificatePassword;

    private final String emailDomainName;

    private final String clusterName;
    private final boolean clusterIsMaster;
    private final FiniteDuration clusterMasterCleanupInterval;
    private final FiniteDuration clusterMasterWorkTimeout;
    private final FiniteDuration clusterWorkerCleanupInterval;
    private final FiniteDuration clusterWorkerResultAckInterval;

    private final String peptideDatabaseFolder;
    private final String peptideDatabaseFilenameTaxonomyRegex;

    private final int uploaderQuota;
    private final boolean clusterWorkerCheckVersion;
    private final List<Integer> dataLoaderRetryInSeconds;
    private final Loader.TimstofReaderType timstofReaderType;
    private final boolean autoKeyspaceGeneration;

    private final List<String> workerGPUs;
    private final int workerMaxCPU;
    private final int workerMaxMemory;
    private final int workerDefaultCPU;
    private final int workerDefaultGPU;
    private final int workerDefaultMemory;
    private final boolean workerDetached;
    private final java.time.Duration workerTaskTimeout;
    private final java.time.Duration workerMessageTimeout;
    private final java.time.Duration workerHeartbeatTimeout;

    @Inject
    public SystemConfig(
        @TypesafeConfig("peaks.server.server-instances") int serverInstances,
        @TypesafeConfig("peaks.server.web-integration") boolean serverWebIntegration,
        @TypesafeConfig("peaks.server.environment") String serverEnvironment,
        @TypesafeConfig("peaks.server.root-url") String serverRootURL,
        @TypesafeConfig("peaks.server.auto-keyspace-generation") boolean autoKeyspaceGeneration,

        @TypesafeConfig("peaks.server.http-port") int serverHttpPort,
        @TypesafeConfig("peaks.server.jwt-secret") String serverJWTSecret,
        @TypesafeConfig("peaks.server.session-timeout") java.time.Duration serverSessionTimeout,
        @TypesafeConfig("peaks.server.work-registration-timeout") java.time.Duration serverWorkRegistrationTimeout,
        @TypesafeConfig("peaks.server.data-loader.load-interval") java.time.Duration dataLoaderLoadInterval,
        @TypesafeConfig("peaks.server.data-loader.parallelism") int dataLoaderParallelism,
        @TypesafeConfig("peaks.server.data-loader.timeout") java.time.Duration dataLoaderTimeout,
        @TypesafeConfig("peaks.server.data-loader.threads-per-loader") int threadsPerLoader,
        @TypesafeConfig("peaks.server.fasta-database.timeout") java.time.Duration fastaDatabaseTimeout,
        @TypesafeConfig("peaks.server.uploader.extra-quota") int uploaderExtraQuota,
        @TypesafeConfig("peaks.server.data-loader.retry-in-seconds") List<Integer> dataLoaderRetryInSeconds,
        @TypesafeConfig("peaks.server.data-loader.timstof-reader") String timstofReaderType,

        @TypesafeConfig("peaks.server.peptide-database.folder") String peptideDatabaseFolder,
        @TypesafeConfig("peaks.server.peptide-database.filename-taxonomy-regex") String peptideDatabaseFilenameTaxonomyRegex,

        @TypesafeConfig("peaks.server.api.root") String apiRoot,
        @TypesafeConfig("peaks.server.api.allow-cors") boolean apiAllowCors,
        @TypesafeConfig("peaks.server.api.search-limit") int apiSearchLimit,

        @TypesafeConfig("peaks.server.ssl.certificate-location") String certificateLocation,
        @TypesafeConfig("peaks.server.ssl.certificate-password") String certificatePassword,

        @TypesafeConfig("peaks.server.email.domain-name") String emailDomainName,

        @TypesafeConfig(CLUSTER_NAME_KEY) String clusterName,
        @TypesafeConfig("peaks.server.cluster.is-master") boolean clusterIsMaster,
        @TypesafeConfig("peaks.server.cluster.master.cleanup-interval") java.time.Duration clusterMasterCleanupInterval,
        @TypesafeConfig("peaks.server.cluster.master.work-timeout") java.time.Duration clusterMasterWorkTimeout,
        @TypesafeConfig("peaks.server.cluster.master.number-of-retries-per-task") int numberOfRetriesPerTask,
        @TypesafeConfig("peaks.server.cluster.worker.cleanup-interval") java.time.Duration clusterWorkerCleanupInterval,
        @TypesafeConfig("peaks.server.cluster.worker.result-ack-interval") java.time.Duration clusterWorkerResultAckInterval,
        @TypesafeConfig("peaks.server.cluster.worker.check-version") boolean clusterWorkerCheckVersion,
        @TypesafeConfig("peaks.server.cluster.master.parallelism") int parallelism,
        @TypesafeConfig("peaks.server.cluster.master.internal-communication-timeout") java.time.Duration internalCommunicationTimeout,

        @TypesafeConfig("peaks.worker.max-cpu") int workerMaxCPU,
        @TypesafeConfig("peaks.worker.gpus") List<String> workerGPUs,
        @TypesafeConfig("peaks.worker.max-memory") int workerMaxMemory,
        @TypesafeConfig("peaks.worker.default-cpu") int workerDefaultCPU,
        @TypesafeConfig("peaks.worker.default-gpu") int workerDefaultGPU,
        @TypesafeConfig("peaks.worker.default-memory") int workerDefaultMemory,
        @TypesafeConfig("peaks.worker.detached") boolean workerDetached,
        @TypesafeConfig("peaks.worker.task-timeout") java.time.Duration workerTaskTimeout,
        @TypesafeConfig("peaks.worker.message-timeout") java.time.Duration workerMessageTimeout,
        @TypesafeConfig("peaks.worker.heartbeat-timeout") java.time.Duration workerHeartbeatTimeout
        ) {
        this.serverInstances = serverInstances;
        this.serverHttpPort = serverHttpPort;
        this.isDebug = serverEnvironment.toLowerCase().equals("debug");
        this.serverRootURL = serverRootURL;
        this.numberOfRetriesPerTask = numberOfRetriesPerTask;
        this.autoKeyspaceGeneration = autoKeyspaceGeneration;

        //this.serverJWTSecret = serverJWTSecret;
        this.serverJWTSecret = "p&secre^t"; // hard code JWT secret
        this.serverSessionTimeout = FiniteDuration.fromNanos(serverSessionTimeout.toNanos());
        this.serverWebIntegration = serverWebIntegration;
        this.serverWorkRegistrationTimeout = FiniteDuration.fromNanos(serverWorkRegistrationTimeout.toNanos());
        this.dataLoaderLoadInterval = FiniteDuration.fromNanos(dataLoaderLoadInterval.toNanos());
        this.dataLoaderParallelism = dataLoaderParallelism;
        this.dataLoaderTimeout = FiniteDuration.fromNanos(dataLoaderTimeout.toNanos());
        this.dataLoaderThreads = threadsPerLoader;
        this.fastaDatabaseTimeout = FiniteDuration.fromNanos(fastaDatabaseTimeout.toNanos());
        this.dataLoaderRetryInSeconds = dataLoaderRetryInSeconds;
        this.timstofReaderType = Loader.TimstofReaderType.valueOf(timstofReaderType.toUpperCase());

        this.peptideDatabaseFolder = peptideDatabaseFolder;
        this.peptideDatabaseFilenameTaxonomyRegex = peptideDatabaseFilenameTaxonomyRegex;

        this.apiRoot = apiRoot;
        this.apiAllowCors = apiAllowCors;
        this.apiSearchLimit = apiSearchLimit;

        this.certificateLocation = certificateLocation;
        this.certificatePassword = certificatePassword;

        this.emailDomainName = emailDomainName;
        
        this.clusterName = clusterName;
        this.clusterIsMaster = clusterIsMaster;
        this.clusterMasterCleanupInterval = FiniteDuration.fromNanos(clusterMasterCleanupInterval.toNanos());
        this.clusterMasterWorkTimeout = FiniteDuration.fromNanos(clusterMasterWorkTimeout.toNanos());
        this.clusterWorkerCleanupInterval = FiniteDuration.fromNanos(clusterWorkerCleanupInterval.toNanos());
        this.clusterWorkerResultAckInterval = FiniteDuration.fromNanos(clusterWorkerResultAckInterval.toNanos());
        this.clusterWorkerCheckVersion = clusterWorkerCheckVersion;
        this.parallelism = parallelism;
        this.internalCommunicationTimeout = FiniteDuration.fromNanos(internalCommunicationTimeout.toNanos());

        this.uploaderQuota = dataLoaderParallelism + uploaderExtraQuota;

        this.workerMaxCPU = workerMaxCPU;
        this.workerGPUs = workerGPUs;
        this.workerMaxMemory = workerMaxMemory;
        this.workerDefaultCPU = workerDefaultCPU;
        this.workerDefaultGPU = workerDefaultGPU;
        this.workerDefaultMemory = workerDefaultMemory;
        this.workerDetached = workerDetached;
        this.workerTaskTimeout = workerTaskTimeout;
        this.workerMessageTimeout = workerMessageTimeout;
        this.workerHeartbeatTimeout = workerHeartbeatTimeout;
    }

    public static String version() {
        final Properties properties = new Properties();
        try {
            InputStream resourceAsStream = Resources.getResource(SystemConfig.class, "/project.properties").openStream();
            properties.load(resourceAsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties.getProperty("version");
    }

    public int getUploaderQuota() {
        return uploaderQuota;
    }

    public int getServerInstances() {
        return serverInstances;
    }

    public String getServerRootURL() {
        return serverRootURL;
    }

    public int getServerHttpPort() {
        return serverHttpPort;
    }

    public boolean isServerWebIntegration() {
        return serverWebIntegration;
    }

    public String getServerJWTSecret() {
        return serverJWTSecret;
    }

    public FiniteDuration getServerWorkRegistrationTimeout() {
        return serverWorkRegistrationTimeout;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public FiniteDuration getDataLoaderLoadInterval() {
        return dataLoaderLoadInterval;
    }

    public int getDataLoaderParallelism() {
        return dataLoaderParallelism;
    }

    public Loader.TimstofReaderType getTimstofReaderType() {
        return timstofReaderType;
    }

    public int getParallelism() { return parallelism; }

    public FiniteDuration getDataLoaderTimeout() {
        return dataLoaderTimeout;
    }

    public int getDataLoaderThreads() {
        return dataLoaderThreads;
    }

    public FiniteDuration getFastaDatabaseTimeout() {
        return fastaDatabaseTimeout;
    }

    public FiniteDuration getServerSessionTimeout() {
        return serverSessionTimeout;
    }

    public String getApiRoot() {
        return apiRoot;
    }

    public boolean getApiAllowCors() {
        return apiAllowCors;
    }

    public int getApiSearchLimit() {
        return apiSearchLimit;
    }

    public String getCertificateLocation() {
        return certificateLocation;
    }

    public String getCertificatePassword() { return certificatePassword; }

    public String getEmailDomainName() { return emailDomainName; }

    public String getClusterName() {
        return clusterName;
    }

    public boolean isClusterIsMaster() {
        return clusterIsMaster;
    }

    public FiniteDuration getClusterMasterCleanupInterval() {
        return clusterMasterCleanupInterval;
    }

    public FiniteDuration getClusterMasterWorkTimeout() {
        return clusterMasterWorkTimeout;
    }

    public FiniteDuration getInternalCommunicationTimeout() { return internalCommunicationTimeout; }

    public FiniteDuration getClusterWorkerCleanupInterval() {
        return clusterWorkerCleanupInterval;
    }

    public FiniteDuration getClusterWorkerResultAckInterval() {
        return clusterWorkerResultAckInterval;
    }

    public boolean isClusterWorkerCheckVersion() {
        return clusterWorkerCheckVersion;
    }

    public String getVersion() {
        return version;
    }

    public final boolean getClusterIsWorker() {
        return !clusterIsMaster;
    }

    public int getNumberOfRetriesPerTask() {
        return numberOfRetriesPerTask;
    }

    public String getPeptideDatabaseFolder() {
        return peptideDatabaseFolder;
    }

    public String getPeptideDatabaseFilenameTaxonomyRegex() {
        return peptideDatabaseFilenameTaxonomyRegex;
    }

    public List<Integer> getDataLoaderRetryInSeconds() {
        return dataLoaderRetryInSeconds;
    }

    public boolean isAutoKeyspaceGeneration() {
        return autoKeyspaceGeneration;
    }

    public int getWorkerMaxCPU() {
        return workerMaxCPU;
    }

    public List<String> getWorkerGPUs() {
        return workerGPUs;
    }

    public int getWorkerMaxMemory() {
        return workerMaxMemory;
    }

    public int getWorkerDefaultCPU() {
        return workerDefaultCPU;
    }

    public int getWorkerDefaultGPU() {
        return workerDefaultGPU;
    }

    public int getWorkerDefaultMemory() {
        return workerDefaultMemory;
    }

    public boolean isWorkerDetached() {
        return workerDetached;
    }

    public java.time.Duration getWorkerTaskTimeout() {
        return workerTaskTimeout;
    }

    public java.time.Duration getWorkerMessageTimeout() {
        return workerMessageTimeout;
    }

    public Duration getWorkerHeartbeatTimeout() {
        return workerHeartbeatTimeout;
    }
}