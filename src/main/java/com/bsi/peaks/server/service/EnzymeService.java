package com.bsi.peaks.server.service;

import akka.Done;
import com.bsi.peaks.data.model.vm.EnzymeVM;
import com.bsi.peaks.event.EnzymeCommandFactory;
import com.bsi.peaks.model.proto.Enzyme;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.es.communication.EnzymeCommunication;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;

public class EnzymeService extends CRUDService<EnzymeVM> {

    private final EnzymeCommunication enzymeCommunication;

    @Inject
    public EnzymeService(EnzymeCommunication enzymeCommunication) {
        this.enzymeCommunication = enzymeCommunication;
    }

    @Override
    public CompletionStage<Done> create(EnzymeVM data, User creator) {
        return enzymeCommunication.command(
            EnzymeCommandFactory.addEnzyme(DtoAdaptors.convertEnzymeProto(data), creator.userId().toString())
        );
    }

    @Override
    public CompletionStage<Done> delete(String key, User user) {
        return enzymeCommunication.command(EnzymeCommandFactory.deleteEnzyme(key, user.userId().toString()));
    }

    @Override
    public CompletionStage<Done> update(String key, EnzymeVM data, User user) {
        if (!key.equals(data.name())) {
            throw new IllegalStateException("Invalid update for enzyme");
        }
        
        return enzymeCommunication.command(EnzymeCommandFactory.updateEnzyme(DtoAdaptors.convertEnzymeProto(data), user.userId().toString()));
    }

    @Override
    public CompletionStage<Done> grantPermission(String key, UUID oldOwnerId, UUID newOwnerId) {
        return enzymeCommunication.command(EnzymeCommandFactory.enzymeGrantPermission(key, oldOwnerId.toString(), newOwnerId.toString()));
    }

    @Override
    public CompletionStage<List<EnzymeVM>> readById(User user) {
        return enzymeCommunication
            .query(EnzymeCommandFactory.queryEnzymeById(user.userId().toString()))
            .thenApply(enyzmeResponse -> {
                List<Enzyme> enyzmes = enyzmeResponse.getEnzymeList();
                Map<String, String> permissions = enyzmeResponse.getNameToOwnerIdMap();
                return enyzmes.stream()
                    .map(enyzme -> DtoAdaptors.convertEnzymeVM(enyzme, UUID.fromString(permissions.get(enyzme.getName()))))
                    .collect(Collectors.toList());
            });
    }

    @Override
    public CompletionStage<List<EnzymeVM>> readAllBuiltin() {
        return enzymeCommunication
            .query(EnzymeCommandFactory.queryAllEnzymes(ADMIN_USERID))
            .thenApply(enyzmeResponse -> {
                List<Enzyme> enyzmes = enyzmeResponse.getEnzymeList();
                Map<String, String> permissions = enyzmeResponse.getNameToOwnerIdMap();
                return enyzmes.stream()
                    .map(enyzme -> DtoAdaptors.convertEnzymeVM(enyzme, UUID.fromString(permissions.get(enyzme.getName()))))
                    .collect(Collectors.toList());
            });
    }

    @Override
    public CompletionStage<List<EnzymeVM>> readByNames(List<String> names) {
        return enzymeCommunication
            .query(EnzymeCommandFactory.queryEnzymeByName(ADMIN_USERID, names))
            .thenApply(enyzmeResponse -> {
                List<Enzyme> enyzmes = enyzmeResponse.getEnzymeList();
                Map<String, String> permissions = enyzmeResponse.getNameToOwnerIdMap();
                return enyzmes.stream()
                    .map(enyzme -> DtoAdaptors.convertEnzymeVM(enyzme, UUID.fromString(permissions.get(enyzme.getName()))))
                    .collect(Collectors.toList());
            });
    }
}
