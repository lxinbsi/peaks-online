package com.bsi.peaks.server.handlers;

import akka.actor.ActorRef;
import akka.util.Timeout;
import com.bsi.peaks.model.core.ActivationMethod;
import com.bsi.peaks.model.proteomics.Modification;
import com.bsi.peaks.model.proteomics.fasta.FastaFormat;
import com.bsi.peaks.model.proteomics.fasta.TaxonomyMapper;
import com.bsi.peaks.server.cluster.Master;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.models.PeaksEnums;
import com.bsi.peaks.server.models.PeaksEnumsBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Shengying Pan
 * Created by span on 2/27/2017.
 */
public class ClusterStateHandler extends ApiHandler {
    private final ActorRef masterProxy;
    private final Timeout proxyTimeout;

    @Inject
    public ClusterStateHandler(final Vertx vertx, final SystemConfig sysConfig,
                               @Named(Master.PROXY_NAME) final ActorRef masterProxy) {
        super(vertx);
        this.masterProxy = masterProxy;
        this.proxyTimeout = new Timeout(sysConfig.getServerWorkRegistrationTimeout());
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.get("/enums").handler(this::retrieveEnums);
        subRouter.get("/parameters/defaults").handler(this::retrieveDefaultParameters);
        subRouter.get("/taxonomies").handler(this::retrieveTaxonomies);
        return subRouter;
    }

    private void retrieveTaxonomies(final RoutingContext context) {
        ok(context, TaxonomyMapper.getTaxonomy());
    }

    private void retrieveDefaultParameters(final RoutingContext context) {
        Config defaultConfig = ConfigFactory.defaultApplication()
            .withFallback(ConfigFactory.defaultReference())
            .getConfig("peaks.parameters");
        ok(context, new JsonObject(defaultConfig.root().render(ConfigRenderOptions.concise())));
    }

    private void retrieveEnums(final RoutingContext context) {
        // display name => name
        Map<String, String> activationMethods = new HashMap<>();
        for (ActivationMethod method : ActivationMethod.values()) {
            activationMethods.put(method.displayName(), method.name());
        }

        // display name => name
        Map<String, String> fastaTypes = new HashMap<>();
        // name => pattern
        Map<String, String> fastaIdPatterns = new HashMap<>();
        Map<String, String> fastaDescriptionPatterns = new HashMap<>();
        for (FastaFormat format : FastaFormat.values()) {
            fastaIdPatterns.put(format.name(), format.idPatternString());
            fastaDescriptionPatterns.put(format.name(), format.descriptionPatternString());
            fastaTypes.put(format.name, format.name());
        }

        PeaksEnums enums = new PeaksEnumsBuilder()
                .activationMethods(activationMethods)
                .fastaIdPatterns(fastaIdPatterns)
                .fastaDescriptionPatterns(fastaDescriptionPatterns)
                .fastaTypes(fastaTypes)
                .modificationTypes(Arrays.stream(Modification.Type.values()).map(Enum::name).collect(Collectors.toList()))
                .modificationSources(Arrays.stream(Modification.Source.values()).map(Enum::name).collect(Collectors.toList()))
                .modificationCategories(Arrays.stream(Modification.Category.values()).map(Enum::name).collect(Collectors.toList()))
                .build();
        ok(context, enums);
    }
}
