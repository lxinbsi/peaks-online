package com.bsi.peaks.server.es.communication;

import akka.actor.ActorSystem;
import akka.japi.Procedure;
import akka.japi.pf.Match;
import com.bsi.peaks.event.UserAuthenticationFactory;
import com.bsi.peaks.event.user.AuthenticationAccept;
import com.bsi.peaks.event.user.AuthenticationChallengeQuestion;
import com.bsi.peaks.event.user.AuthenticationReject;
import com.bsi.peaks.event.user.UserCommand;
import com.bsi.peaks.event.user.UserQueryRequest;
import com.bsi.peaks.event.user.UserQueryResponse;
import com.bsi.peaks.server.es.UserManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

@Singleton
public class UserManagerCommunication extends AbstractCommunication<UserCommand, UserQueryRequest, UserQueryResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(UserManagerCommunication.class);

    @Inject
    public UserManagerCommunication(
        final ActorSystem system
    ) {
        super(system, UserQueryResponse.class, UserManager::instance);
    }

    public void authenticate(
        String username,
        String password,
        Procedure<AuthenticationAccept> onAccept,
        Procedure<AuthenticationReject> onReject,
        Procedure<Throwable> onError
    ) {
        ask(UserAuthenticationFactory.authenticationRequest(username))
            .thenCompose(Match
                .match(AuthenticationChallengeQuestion.class, question -> ask(UserAuthenticationFactory.authenticationChallengeAnswer(question, password)))
                .matchAny(CompletableFuture::completedFuture)
                .build()::apply)
            .whenComplete((o, throwable) -> {
                if (throwable == null) {
                    if (o instanceof AuthenticationAccept) {
                        try {
                            onAccept.apply((AuthenticationAccept) o);
                        } catch (Exception e) {
                            LOG.error("Exception during authentication onAccept", e);
                        }
                    } else if (o instanceof AuthenticationReject) {
                        try {
                            onReject.apply((AuthenticationReject) o);
                        } catch (Exception e) {
                            LOG.error("Exception during authentication onReject", e);
                        }
                    }
                } else {
                    LOG.error("Exception during authentication", throwable);
                }
            });
    }

    @Override
    protected String tagFromCommand(UserCommand command) {
        return "UserManagerCommand " + command.getCommandCase().name();
    }

}
