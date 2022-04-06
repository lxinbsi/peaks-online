package com.bsi.peaks.server.dependency;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * @author Shengying Pan
 * Created by span on 1/12/17.
 */
public class GuiceVertxDeploymentManager {
    private final Vertx vertx;
    
    /**
     * Create GuiceVertxDeploymentManager
     * @param vertx Vertx instance to bind this manager to
     */
    public GuiceVertxDeploymentManager(final Vertx vertx) {
        this.vertx = vertx;
    }
    
    /**
     * Function to deploy a verticle
     * @param verticleClass class of the verticle to be deployed
     */
    public void deployVerticle(final Class verticleClass) {
        deployVerticle(verticleClass, new DeploymentOptions());
    }
    
    /**
     * Function to deploy a verticle
     * @param verticleClass class of the verticle to be deployed
     * @param options Vertx deployment options
     */
    public void deployVerticle(final Class verticleClass, final DeploymentOptions options) {
        vertx.deployVerticle(getFullVerticleName(verticleClass), options);
    }

    /**
     * Function to deploy a verticle, with completion handler
     * @param verticleClass class of the verticle to be deployed
     * @param options Vertx deployment options
     * @param completionHandler handler will be called when deployment is done, asynchronously
     */
    public void deployVerticle(final Class verticleClass, final DeploymentOptions options,
        Handler<AsyncResult<String>> completionHandler) {
        vertx.deployVerticle(getFullVerticleName(verticleClass), options, completionHandler);
    }


    private String getFullVerticleName(final Class verticleClass) {
        return GuiceVerticleFactory.PREFIX + ":" + verticleClass.getCanonicalName();
    }
}
