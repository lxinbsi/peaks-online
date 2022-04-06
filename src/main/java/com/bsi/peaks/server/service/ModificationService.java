package com.bsi.peaks.server.service;

import akka.Done;
import com.bsi.peaks.data.model.vm.ModificationVM;
import com.bsi.peaks.event.ModificationCommandFactory;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.communication.ModificationCommunication;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class ModificationService extends CRUDService<ModificationVM> {

    private final ModificationCommunication modificationCommunication;

    @Inject
    public ModificationService(ModificationCommunication modificationCommunication) {
        this.modificationCommunication = modificationCommunication;
    }

    @Override
    public CompletionStage<Done> create(ModificationVM data, User creator) {
        final String userId = creator.userId().toString();
        return modificationCommunication.command(ModificationCommandFactory.addModification(DtoAdaptors.convertModificationProto(data), userId));
    }

    @Override
    public CompletionStage<Done> delete(String key, User user) {
        final String userId = user.userId().toString();
        return modificationCommunication.command(ModificationCommandFactory.deleteModification(key, userId));
    }

    @Override
    public CompletionStage<Done> update(String key, ModificationVM data, User user) {
        final String userId = user.userId().toString();
        return modificationCommunication.command(ModificationCommandFactory.updateModification(key, DtoAdaptors.convertModificationProto(data), userId));
    }

    @Override
    public CompletionStage<Done> grantPermission(String key, UUID oldOwnerId, UUID newOwnerId) {
        return modificationCommunication.command(ModificationCommandFactory.grantPermission(key, oldOwnerId.toString(), newOwnerId.toString()));
    }

    @Override
    public CompletionStage<List<ModificationVM>> readById(User user) {
        final String userId = user.userId().toString();
        return modificationCommunication
            .query(ModificationCommandFactory.queryModificationById(userId))
            .thenApply(modificationResponse -> {
                List<Modification> modifications = modificationResponse.getModificationList();
                Map<String, String> permissions = modificationResponse.getPermissionMap();
                return modifications.stream()
                        .map(modification -> DtoAdaptors.convertModificationVM(modification, UUID.fromString(permissions.get(modification.getName()))))
                        .collect(Collectors.toList());
            });
    }

    @Override
    public CompletionStage<List<ModificationVM>> readAllBuiltin() {
        final String userId = UserManager.ADMIN_USERID;
        return modificationCommunication
            .query(ModificationCommandFactory.queryAllBuiltInModification(userId))
            .thenApply(modificationResponse -> {
                List<Modification> modifications = modificationResponse.getModificationList();
                Map<String, String> permissions = modificationResponse.getPermissionMap();
                return modifications.stream()
                        .map(modification -> DtoAdaptors.convertModificationVM(modification, UUID.fromString(permissions.get(modification.getName()))))
                        .collect(Collectors.toList());
            });
    }

    @Override
    public CompletionStage<List<ModificationVM>> readByNames(List<String> names) {
        final String userId = UserManager.ADMIN_USERID;
        return modificationCommunication
            .query(ModificationCommandFactory.queryModificationByName(userId, names))
            .thenApply(modificationResponse -> {
                List<Modification> modifications = modificationResponse.getModificationList();
                Map<String, String> permissions = modificationResponse.getPermissionMap();
                return modifications.stream()
                        .map(modification -> DtoAdaptors.convertModificationVM(modification, UUID.fromString(permissions.get(modification.getName()))))
                        .collect(Collectors.toList());
            });
    }

}
