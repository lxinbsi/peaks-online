package com.bsi.peaks.server.service;

import akka.Done;
import com.bsi.peaks.model.system.User;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public abstract class CRUDService<T> {

    // COMMANDS
    public abstract CompletionStage<Done> create(T data, User creator);
    public abstract CompletionStage<Done> delete(String key, User user);
    public abstract CompletionStage<Done> update(String key, T data, User user);
    public abstract CompletionStage<Done> grantPermission(String key, UUID newOwnerId, UUID oldOwnerId);
    public abstract CompletionStage<List<T>> readById(User user);
    public abstract CompletionStage<List<T>> readAllBuiltin();
    public abstract CompletionStage<List<T>> readByNames(List<String> names);

}
