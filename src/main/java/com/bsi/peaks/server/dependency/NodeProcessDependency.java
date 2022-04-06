package com.bsi.peaks.server.dependency;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.client.ClusterClient;
import akka.cluster.client.ClusterClientSettings;
import com.bsi.peaks.server.cluster.Master;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeProcessDependency extends AbstractModule {
    private static final Logger LOG = LoggerFactory.getLogger(LoaderDependency.class);
    private final Config config;
    private final String name;

    public NodeProcessDependency(Config config, String name) {
        this.config = config;
        this.name = name;
    }

    @Override
    protected void configure() {
        ActorSystem system = ActorSystem.create(name, config);
        bind(ActorSystem.class).toInstance(system);
        ActorRef clusterClient = system.actorOf(ClusterClient.props(ClusterClientSettings.create(system)), Master.CLUSTER_CLIENT);
        bind(ActorRef.class).annotatedWith(Names.named(Master.CLUSTER_CLIENT)).toInstance(clusterClient);
    }
}
