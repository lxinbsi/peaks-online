package com.bsi.peaks.server.handlers;

import com.bsi.peaks.data.model.vm.ModificationVM;
import com.bsi.peaks.server.service.ModificationService;

import akka.actor.ActorSystem;
import com.google.inject.Inject;
import io.vertx.core.Vertx;

import java.util.UUID;

/**
 * @author span
 *         Created by span on 3/16/2017.
 */
public class ModificationHandler extends EsCRUDHandler<ModificationVM> {

    @Inject
    public ModificationHandler(Vertx vertx, final ActorSystem system, final ModificationService modificationService) {
        super(vertx,system, modificationService, ModificationVM.class);
    }

    @Override
    String dataId(ModificationVM modificationVM) {
        return modificationVM.name();
    }

    @Override
    String dataName(ModificationVM modificationVM) {
        return modificationVM.name();
    }

    @Override
    UUID creatorId(ModificationVM modificationVM) {
        return modificationVM.creatorId();
    }
}
