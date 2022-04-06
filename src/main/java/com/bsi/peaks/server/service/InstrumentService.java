package com.bsi.peaks.server.service;

import akka.Done;
import com.bsi.peaks.data.model.vm.InstrumentVM;
import com.bsi.peaks.event.InstrumentCommandFactory;
import com.bsi.peaks.model.proto.Instrument;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.dto.DtoAdaptors;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.communication.InstrumentCommunication;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class InstrumentService extends CRUDService<InstrumentVM> {

    private final InstrumentCommunication instrumentCommunication;

    @Inject
    public InstrumentService(InstrumentCommunication instrumentCommunication) {
        this.instrumentCommunication = instrumentCommunication;
    }

    @Override
    public CompletionStage<Done> create(InstrumentVM data, User creator) {
        return instrumentCommunication.command(InstrumentCommandFactory.addInstrument(DtoAdaptors.convertInstrumentProto(data), creator.userId().toString()));
    }

    @Override
    public CompletionStage<Done> delete(String key, User user) {
        return instrumentCommunication.command(InstrumentCommandFactory.deleteInstrument(key, user.userId().toString()));
    }

    @Override
    public CompletionStage<Done> update(String key, InstrumentVM data, User user) {
        if (!key.equals(data.name())) {
            throw new IllegalStateException("Invalid update for instrument");
        }

        return instrumentCommunication.command(InstrumentCommandFactory.updateInstrument(DtoAdaptors.convertInstrumentProto(data), user.userId().toString()));
    }

    @Override
    public CompletionStage<Done> grantPermission(String key, UUID oldOwnerId, UUID newOwnerId) {
        return instrumentCommunication.command(InstrumentCommandFactory.instrumentGrantPermission(key, oldOwnerId.toString(), newOwnerId.toString()));
    }

    @Override
    public CompletionStage<List<InstrumentVM>> readById(User user) {
        final String actingUserId = user.userId().toString();
        return instrumentCommunication
            .query(InstrumentCommandFactory.queryInstrumentById(actingUserId))
            .thenApply(instrumentResponse -> {
                List<Instrument> instruments = instrumentResponse.getInstrumentList();
                Map<String, String> permissions = instrumentResponse.getNameToOwnerIdMap();
                return instruments.stream()
                    .map(instrument -> DtoAdaptors.convertInstrumentVM(instrument, UUID.fromString(permissions.get(instrument.getName()))))
                    .collect(Collectors.toList());
            });
    }

    @Override
    public CompletionStage<List<InstrumentVM>> readAllBuiltin() {
        final String actingUserId = UserManager.ADMIN_USERID;
        return instrumentCommunication
            .query(InstrumentCommandFactory.queryAllInstruments(actingUserId))
            .thenApply(instrumentResponse -> {
                List<Instrument> instruments = instrumentResponse.getInstrumentList();
                Map<String, String> permissions = instrumentResponse.getNameToOwnerIdMap();
                return instruments.stream()
                    .map(instrument -> DtoAdaptors.convertInstrumentVM(instrument, UUID.fromString(permissions.get(instrument.getName()))))
                    .collect(Collectors.toList());
            });
    }

    @Override
    public CompletionStage<List<InstrumentVM>> readByNames(List<String> names) {
        final String actingUserId = UserManager.ADMIN_USERID;
        return instrumentCommunication
            .query(InstrumentCommandFactory.queryInstrumentByName(actingUserId, names))
            .thenApply(instrumentResponse -> {
                List<Instrument> instrument = instrumentResponse.getInstrumentList();
                Map<String, String> permissions = instrumentResponse.getNameToOwnerIdMap();
                return instrument.stream()
                    .map(instruments -> DtoAdaptors.convertInstrumentVM(instruments, UUID.fromString(permissions.get(instruments.getName()))))
                    .collect(Collectors.toList());
            });
    }

}
