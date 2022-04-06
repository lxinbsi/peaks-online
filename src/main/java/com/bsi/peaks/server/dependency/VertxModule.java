package com.bsi.peaks.server.dependency;

import com.google.inject.AbstractModule;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.file.FileSystem;
import io.vertx.core.shareddata.SharedData;

/**
 * @author Shengying Pan
 * Created by span on 1/12/17.
 */
public class VertxModule extends AbstractModule {

    private Vertx vertx;

    public VertxModule() {
        this(Vertx.vertx());
    }

    public VertxModule(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    protected void configure() {
        bind(Vertx.class).toInstance(vertx);
        bind(EventBus.class).toInstance(vertx.eventBus());
        bind(FileSystem.class).toInstance(vertx.fileSystem());
        bind(SharedData.class).toInstance(vertx.sharedData());
    }
}
