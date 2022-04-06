package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import com.bsi.peaks.data.model.vm.InstrumentVM;
import com.bsi.peaks.server.service.InstrumentService;
import com.google.inject.Inject;
import io.vertx.core.Vertx;

import java.util.UUID;

/**
 * @author Shengying Pan
 * Created by span on 3/9/2017.
 */
public class InstrumentHandler extends EsCRUDHandler<InstrumentVM> {
    @Inject
    public InstrumentHandler(Vertx vertx, final ActorSystem system, final InstrumentService instrumentService) {
        super(vertx, system, instrumentService, InstrumentVM.class);
    }

    @Override
    String dataId(InstrumentVM instrumentVM) {
        return instrumentVM.name();
    }

    @Override
    String dataName(InstrumentVM instrumentVM) {
        return instrumentVM.name();
    }

    @Override
    UUID creatorId(InstrumentVM instrumentVM) {
        return instrumentVM.creatorId();
    }
}
