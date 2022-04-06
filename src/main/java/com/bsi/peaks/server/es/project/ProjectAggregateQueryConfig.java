package com.bsi.peaks.server.es.project;

import akka.stream.Materializer;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.cache.FractionCache;
import com.bsi.peaks.event.UserQueryFactory;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.communication.UserManagerCommunication;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface ProjectAggregateQueryConfig {

    UUID projectId();
    String keyspace();
    int parallelism();
    Materializer materializer();
    ApplicationStorage applicationStorage();
    CompletionStage<? extends ApplicationStorage> applicationStorageAsync();
    UserManagerCommunication userManagerCommunication();
    FractionCache fractionCache();

    default CompletionStage<Map<String, String>> userNamesById() {
        return userManagerCommunication().query(UserQueryFactory.actingAsUserId(UserManager.ADMIN_USERID).requestUserList())
            .thenApply(response -> {
                if (!response.hasListUsers()) {
                    throw new IllegalStateException("Unable to query user list");
                }
                return Maps.transformValues(
                    response.getListUsers().getUsersMap(),
                    userState -> userState.getUserInformation().getUserName()
                );
            });
    }
}
