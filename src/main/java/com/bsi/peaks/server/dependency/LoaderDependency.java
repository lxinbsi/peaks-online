package com.bsi.peaks.server.dependency;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.client.ClusterClient;
import akka.cluster.client.ClusterClientSettings;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.cassandra.CassandraStorageFactory;
import com.bsi.peaks.reader.ScanReaderFactory;
import com.bsi.peaks.server.Launcher;
import com.bsi.peaks.server.cluster.Master;
import com.github.racc.tscg.TypesafeConfigModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoaderDependency extends AbstractModule {
    private static final Logger LOG = LoggerFactory.getLogger(LoaderDependency.class);
    private final Config config;

    public LoaderDependency(Config config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        Injector configInjector = Guice.createInjector(
            TypesafeConfigModule.fromConfigWithPackage(config, "com.bsi.peaks")
        );
        ScanReaderFactory readerFactory = configInjector.getInstance(ScanReaderFactory.class);
        readerFactory.loadReaderDependency();

        bind(ApplicationStorageFactory.class).to(CassandraStorageFactory.class).asEagerSingleton();
        ActorSystem system = ActorSystem.create("loader", config);
        bind(ActorSystem.class).toInstance(system);
        ActorRef clusterClient = system.actorOf(ClusterClient.props(ClusterClientSettings.create(system)), Master.CLUSTER_CLIENT);
        bind(ActorRef.class).annotatedWith(Names.named(Master.CLUSTER_CLIENT)).toInstance(clusterClient);
    }
}
