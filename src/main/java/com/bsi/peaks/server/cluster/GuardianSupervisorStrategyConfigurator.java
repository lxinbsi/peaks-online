package com.bsi.peaks.server.cluster;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;
import static akka.actor.SupervisorStrategy.escalate;

/**
 * @author span
 *         created on 8/29/17.
 */
public class GuardianSupervisorStrategyConfigurator implements SupervisorStrategyConfigurator {
    private static final Logger LOG = LoggerFactory.getLogger(GuardianSupervisorStrategyConfigurator.class);
    @Override
    public SupervisorStrategy create() {
        return new OneForOneStrategy(-1, Duration.Inf(), DeciderBuilder
            .matchAny(o -> {
                LOG.error("Guardian supervised actor encountered error", o);
                return escalate();
            }).build());
    }
}
