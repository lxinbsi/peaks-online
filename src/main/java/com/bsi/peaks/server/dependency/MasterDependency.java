package com.bsi.peaks.server.dependency;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.client.ClusterClientReceptionist;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import com.bsi.peaks.common.security.PasswordHasher;
import com.bsi.peaks.common.security.SHAPasswordHasher;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.cache.FractionCache;
import com.bsi.peaks.data.storage.application.cassandra.CassandraStorageFactory;
import com.bsi.peaks.io.reader.taxonomy.Taxonomy;
import com.bsi.peaks.io.reader.taxonomy.implementation.ArrayTaxonomy;
import com.bsi.peaks.reader.ScanReaderFactory;
import com.bsi.peaks.server.auth.PeaksAuthProvider;
import com.bsi.peaks.server.cluster.EventSourceTaskDeleter;
import com.bsi.peaks.server.cluster.Master;
import com.bsi.peaks.server.cluster.PeaksTaskResourceCalculator;
import com.bsi.peaks.server.es.TaskDeleter;
import com.bsi.peaks.server.service.EmailService;
import com.bsi.peaks.server.service.LogService;
import com.bsi.peaks.server.service.ProjectService;
import com.bsi.peaks.server.service.ProjectServiceImplementation;
import com.bsi.peaks.server.service.UploaderService;
import com.bsi.peaks.service.services.libgenerator.AdjustPsmRtFunctionBuilder;
import com.bsi.peaks.service.services.libgenerator.BuildAdjustPsmRtFunction;
import com.bsi.peaks.service.services.libgenerator.BuildLibrary;
import com.bsi.peaks.service.services.libgenerator.BuildLibraryGenerator;
import com.github.racc.tscg.TypesafeConfigModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.vertx.ext.auth.AuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Shengying Pan
 * Created by span on 1/6/17.
 */
public class MasterDependency extends AbstractModule {
    private static final Logger LOG = LoggerFactory.getLogger(MasterDependency.class);
    public static final String CLUSTER_NAME_KEY = "peaks.server.cluster.name";
    private final Config config;

    public MasterDependency(Config config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        install(new VertxModule());
        bind(Config.class).toInstance(config);
        // worker node only need application storage
        bind(ApplicationStorageFactory.class).to(CassandraStorageFactory.class).asEagerSingleton();
        // the following things are only used by master node
        bind(Taxonomy.class).toProvider(ArrayTaxonomy.TaxonomyProvider.class).asEagerSingleton();
        bind(PasswordHasher.class).to(SHAPasswordHasher.class);
        bind(AuthProvider.class).to(PeaksAuthProvider.class).asEagerSingleton();
        bind(ScanReaderFactory.class);
        bind(PeaksTaskResourceCalculator.class);
        bind(FractionCache.class);

        // purge old readers, have to do this before creation of actor system because of jni conflicts
        // this is a hack to make dependency injection workaround the issue
        Injector configInjector = Guice.createInjector(
            TypesafeConfigModule.fromConfigWithPackage(config, "com.bsi.peaks")
        );
        ScanReaderFactory readerFactory = configInjector.getInstance(ScanReaderFactory.class);
        readerFactory.purgeReaders();
        readerFactory.unzipReaders();

        // now we can create the actor system
        LOG.info("Starting actor system.");
        ActorSystem system = ActorSystem.create(config.getString(CLUSTER_NAME_KEY), config);
        bind(ActorSystem.class).toInstance(system);

        bind(TaskDeleter.class).to(EventSourceTaskDeleter.class);

        ActorRef masterProxy = system.actorOf(
            ClusterSingletonProxy.props(
                Master.MASTER_PATH,
                ClusterSingletonProxySettings.create(system).withRole("master")
            ), Master.PROXY_NAME
        );
        ClusterClientReceptionist.get(system).registerService(masterProxy);
        bind(ActorRef.class).annotatedWith(Names.named(Master.PROXY_NAME)).toInstance(masterProxy);

        bind(ProjectService.class).to(ProjectServiceImplementation.class).asEagerSingleton();
        bind(UploaderService.class).asEagerSingleton();
        bind(EmailService.class).asEagerSingleton();
        bind(LogService.class).asEagerSingleton();
        bind(BuildAdjustPsmRtFunction.class).to(AdjustPsmRtFunctionBuilder.class);
        bind(BuildLibraryGenerator.class).to(BuildLibrary.class);
    }
}
