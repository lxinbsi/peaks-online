package com.bsi.peaks.server.service;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.BackoffSupervisor;
import akka.pattern.PatternsCS;
import com.bsi.peaks.event.EmailCommandFactory;
import com.bsi.peaks.event.common.AckStatus;
import com.bsi.peaks.event.email.EmailCommand;
import com.bsi.peaks.event.email.EmailCommandAck;
import com.bsi.peaks.event.email.SmtpConfig;
import com.bsi.peaks.model.dto.SmtpServer;
import com.bsi.peaks.model.dto.SmtpTlsOption;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.email.EmailCreator;
import com.bsi.peaks.server.email.Mailer;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * @author span
 * Created by span on 2019-03-07
 */
@Singleton
public class EmailService {
    private static final Duration TIMEOUT = Duration.ofMillis(3000);
    private final ActorRef mailer;

    @Inject
    public EmailService(final Vertx vertx, final ActorSystem system, final EmailCreator emailCreator) {
        this.mailer = system.actorOf(
            BackoffSupervisor.props(
                Mailer.props(vertx, emailCreator),
                Mailer.NAME + "Instance",
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                0.2
            ), Mailer.NAME
        );
    }

    public CompletionStage<SmtpServer> getServer(User user) {
        EmailCommand cmd = EmailCommandFactory.userId(user.userId().toString()).getSmtpConfig();
        return PatternsCS.ask(mailer, cmd, TIMEOUT).thenApply(response -> parse((EmailCommandAck) response));
    }

    public CompletionStage<SmtpServer> updateServer(User user, SmtpServer server) {
        EmailCommand cmd = EmailCommandFactory.userId(user.userId().toString()).setSmtpConfig(server);
        return PatternsCS.ask(mailer, cmd, TIMEOUT).thenApply(response -> parse((EmailCommandAck) response));
    }

    public ActorRef getMailer() {
        return mailer;
    }

    private SmtpServer parse(EmailCommandAck ack) {
        if (ack.getAckStatus() == AckStatus.REJECT) {
            throw new IllegalStateException("Unable to get SMTP server information: " + ack.getAckStatusDescription());
        } else if (!ack.hasSmtpConfig()) {
            throw new IllegalStateException("Received empty SMTP server information");
        } else {
            SmtpConfig config = ack.getSmtpConfig();
            return SmtpServer.newBuilder()
                .setHostname(config.getHostname())
                .setPort(config.getPort())
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .setSsl(config.getSsl())
                .setTrustAll(config.getTrustAll())
                .setStartTls(convertSmtpTlsOption(config.getStartTls()))
                .setFromAddress(config.getFromAddress())
                .setCc(config.getCc())
                .build();
        }
    }

    private SmtpTlsOption convertSmtpTlsOption(com.bsi.peaks.event.email.SmtpTlsOption option) {
        switch (option) {
            case SMTP_TLS_OPTIONAL:
                return SmtpTlsOption.SMTP_TLS_OPTIONAL;
            case SMTP_TLS_DISABLED:
                return SmtpTlsOption.SMTP_TLS_DISABLED;
            case SMTP_TLS_REQUIRED:
                return SmtpTlsOption.SMTP_TLS_REQUIRED;
            default:
                throw new IllegalArgumentException("Unexpected SmtpTlsOption " + option);
        }
    }
}
