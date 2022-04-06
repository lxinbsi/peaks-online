package com.bsi.peaks.server.email;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.SnapshotOffer;
import com.bsi.peaks.event.EmailCommandFactory;
import com.bsi.peaks.event.common.AckStatus;
import com.bsi.peaks.event.email.Email;
import com.bsi.peaks.event.email.EmailCommand;
import com.bsi.peaks.event.email.EmailCommandAck;
import com.bsi.peaks.event.email.SmtpConfig;
import com.bsi.peaks.event.projectAggregate.ProjectAggregateEvent;
import com.bsi.peaks.server.es.ProjectAggregateEntity;
import io.vertx.core.Vertx;
import io.vertx.ext.mail.MailClient;
import io.vertx.ext.mail.MailConfig;
import io.vertx.ext.mail.MailMessage;
import io.vertx.ext.mail.StartTLSOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.bsi.peaks.model.ModelConversion.uuidFrom;

/**
 * @author span
 * Created by span on 2019-03-07
 */
public class Mailer extends AbstractPersistentActor {
    public static final String NAME = "mailer";
    private final Vertx vertx; // need a reference to vertx to send email
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final EmailCreator creator;
    private SmtpConfig state = EmailUtilities.DEFAULT_CONFIG;


    private Mailer(
        Vertx vertx,
        EmailCreator emailCreator
    ) {
        this.vertx = vertx;
        this.creator = emailCreator;
    }

    public static Props props(Vertx vertx, EmailCreator emailCreator) {
        return Props.create(Mailer.class, vertx, emailCreator);
    }

    @Override
    public String persistenceId() {
        return NAME + "-persistence";
    }

    @Override
    public void preStart() throws Exception {
        final ActorRef mediator = DistributedPubSub.get(getContext().getSystem()).mediator();
        final String analysisDoneEventTag = ProjectAggregateEntity.eventTag(ProjectAggregateEvent.AnalysisEvent.EventCase.ANALYSISDONE);
        mediator.tell(new DistributedPubSubMediator.Subscribe(analysisDoneEventTag, getSelf()), getSelf());
    }

    @Override
    public Receive createReceiveRecover() {
        return receiveBuilder()
            .match(SnapshotOffer.class, ss -> state = (SmtpConfig) ss.snapshot())
            .match(EmailCommand.class, cmd -> updateServer(cmd.getSetSmtpConfig()))
            .matchAny(msg -> log.warning("Mailer received unexpected recovery message: {}", msg))
            .build();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(EmailCommand.class, EmailCommand::hasGetSmtpConfig, this::fetchConfig)
            .match(EmailCommand.class, EmailCommand::hasSetSmtpConfig, this::validatePersistAndAck)
            .match(EmailCommand.class, EmailCommand::hasSendEmail, this::sendEmail)
            .match(ProjectAggregateEvent.class, this::handleProjectAggregateEvent)
            .matchAny(msg -> log.warning("Mailer received unexpected message: {}", msg))
            .build();
    }

    private void handleProjectAggregateEvent(ProjectAggregateEvent event) {
        final UUID projectId = uuidFrom(event.getProjectId());
        switch (event.getEventCase()) {
            case ANALYSIS:
                final ProjectAggregateEvent.AnalysisEvent analysisEvent = event.getAnalysis();
                final UUID analysisId = uuidFrom(analysisEvent.getAnalysisId());
                switch (analysisEvent.getEventCase()) {
                    case ANALYSISDONE:
                        // this will be very slow, we can off load it asynchronously, just notify self when we get the email content
                        creator.generateEmailForAnalysis(projectId, analysisId, state.getCc().trim())
                            .whenComplete((email, error) -> {
                                if (error != null) {
                                    log.error("Unable to create email for project {}, analysis {}: {}", projectId, analysisId, error.getMessage());
                                } else if (email == null) {
                                    log.debug("No need to send email for project {}, analysis {}, ignored", projectId, analysisId);
                                } else {
                                    // This email is sent by the system, not real user
                                    EmailCommand cmd = EmailCommandFactory.userId("").send(email);
                                    self().tell(cmd, self());
                                }
                            });
                }
                break;
        }
    }

    private void validatePersistAndAck(EmailCommand command) {
        try {
            //TODO: work on more complete validation
            //only do basic validation for now
            SmtpConfig smtpConfig = command.getSetSmtpConfig();
            if (smtpConfig.getHostname().equals("")) {
                throw new IllegalArgumentException("Hostname cannot be empty");
            }
            if (smtpConfig.getPort() == 0) {
                throw new IllegalArgumentException("Port cannot be zero");
            }
            if (smtpConfig.getFromAddress().equals("")) {
                throw new IllegalArgumentException("From address cannot be empty");
            }
        } catch (Exception e) {
            sendError(command.getDeliveryId(), e);
            return;
        }

        persist(command, cmd -> {
            updateServer(cmd.getSetSmtpConfig());
            ackWithConfig(command.getDeliveryId());
        });
    }

    private void fetchConfig(EmailCommand cmd) {
        ackWithConfig(cmd.getDeliveryId());
    }

    private void updateServer(SmtpConfig smtpConfig) {
        state = SmtpConfig.newBuilder().mergeFrom(smtpConfig).build();
    }

    private void sendEmail(EmailCommand command) {
        final Email email = command.getSendEmail();
        final MailClient client = getMailClient();
        final String from = state.getFromAddress();
        //after obtaining the same client, now send emails asynchronously
        // don't care about error, which will already be logged
        broadcastEmail(client, from, email);
        // no need to ack email, this is a message to myself
    }

    private MailClient getMailClient() {
        MailConfig config = new MailConfig();
        config.setHostname(state.getHostname());
        config.setPort(state.getPort());
        config.setUsername(state.getUsername());
        config.setPassword(state.getPassword());
        config.setSsl(state.getSsl());
        config.setTrustAll(state.getTrustAll());
        switch (state.getStartTls()) {
            case SMTP_TLS_OPTIONAL:
                config.setStarttls(StartTLSOptions.OPTIONAL);
                break;
            case SMTP_TLS_REQUIRED:
                config.setStarttls(StartTLSOptions.REQUIRED);
                break;
            case SMTP_TLS_DISABLED:
                config.setStarttls(StartTLSOptions.DISABLED);
                break;
        }

        return MailClient.createNonShared(vertx, config);
    }

    private void broadcastEmail(MailClient client, String from, Email email) {
        List<String> toList;
        if (email.getToCount() > 0) {
            toList = email.getToList();
        } else if (email.getCcCount() > 0) {
            toList = email.getCcList();
        } else {
            // otherwise there is no list so don't send anything
            return;
        }

        List<String> ccList = email.getToCount() > 0 ? email.getCcList() : new ArrayList<>(); // moved to to list

        // toList will always have something here
        MailMessage message = new MailMessage();
        message.setFrom(from.trim());
        message.setTo(toList);

        if (ccList.size() > 0) {
            message.setCc(ccList);
        }

        message.setSubject(email.getSubject());
        // there is not text, only html

        // To force encoding (see: io.vertx.ext.mail.mailencoder.Utils.mustEncode(java.lang.String):
        // 1. We include special carriage return character at end (\r).
        // 2. However, convention is to always follow (\r) with (\n). So we include that as well.
        // Otherwise, we experience with Content-Transfer-Encoding: 7bit, that new line statements are inserted during transfer.
        message.setHtml(email.getHtml() + "\r\n");

        // send email asynchronously
        client.sendMail(message, result -> {
            // don't care whether the message was sent correctly, just log error here if any
            if (result.succeeded()) {
                log.debug("Email sent to " + String.join(",", toList));
            } else {
                log.error(result.cause(), "Unable to send email to " + String.join(",", toList));
            }
        });
    }

    private void ackWithConfig(long deliveryId) {
        EmailCommandAck ack = EmailCommandAck.newBuilder()
            .setDeliveryId(deliveryId)
            .setAckStatus(AckStatus.ACCEPT)
            .setSmtpConfig(state)
            .build();

        sender().tell(ack, self());
    }

    private void sendError(long deliveryId, Exception e) {
        log.error(e, "Mailer encountered error, reject command");
        String message = e == null || e.getMessage() == null ? "Unknown error" : e.getMessage();
        EmailCommandAck ackWithError = EmailCommandAck.newBuilder()
            .setDeliveryId(deliveryId)
            .setAckStatus(AckStatus.REJECT)
            .setAckStatusDescription(message)
            .build();

        sender().tell(ackWithError, self());
    }
}
