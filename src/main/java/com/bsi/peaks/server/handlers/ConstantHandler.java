package com.bsi.peaks.server.handlers;


import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import com.bsi.peaks.io.writer.dto.DtoConversion;
import com.bsi.peaks.model.dto.peptide.Constant;
import com.bsi.peaks.server.config.SystemConfig;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstantHandler extends ProtoHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ConstantHandler.class);
    private final SystemConfig systemConfig;

    @Inject
    public ConstantHandler(
        final Vertx vertx,
        final ActorSystem system,
        final SystemConfig systemConfig
    ) {
        super(vertx, system);
        this.systemConfig = systemConfig;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.get("/").handler(this::getConstant);
        return subRouter;
    }

    private void getConstant(RoutingContext context) {
        Source<Constant, NotUsed> source = Source.single(DtoConversion.residues())
            .map(residues -> Constant.newBuilder()
                .setVersion(systemConfig.getVersion())
                .addAllResidues(residues)
                .build()
            );

        respondWithProtobufSource(context, "constant", source);
    }

}
