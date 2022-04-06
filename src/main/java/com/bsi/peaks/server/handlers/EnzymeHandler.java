package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import com.bsi.peaks.data.model.vm.EnzymeVM;
import com.bsi.peaks.server.service.EnzymeService;
import com.google.inject.Inject;
import io.vertx.core.Vertx;

import java.util.UUID;

/**
 * @author Shengying Pan
 * Created by span on 3/9/2017.
 */
public class EnzymeHandler extends EsCRUDHandler<EnzymeVM> {
    @Inject
    public EnzymeHandler(Vertx vertx, final ActorSystem system, final EnzymeService enzymeService) {
        super(vertx, system, enzymeService, EnzymeVM.class);
    }

    @Override
    String dataId(EnzymeVM enzymeVM) {
        return enzymeVM.name();
    }

    @Override
    String dataName(EnzymeVM enzymeVM) {
        return enzymeVM.name();
    }

    @Override
    UUID creatorId(EnzymeVM enzymeVM) {
        return enzymeVM.creatorId();
    }
}
