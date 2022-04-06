package com.bsi.peaks.server.auth;

import com.bsi.peaks.event.user.UserInformation;
import com.bsi.peaks.event.user.UserRole;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.model.system.UserBuilder;
import com.bsi.peaks.server.es.communication.UserManagerCommunication;
import com.bsi.peaks.server.handlers.HandlerException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;

import java.util.UUID;

/**
 * @author Shengying Pan
 * Created by span on 1/24/17.
 */
@Singleton
public class PeaksAuthProvider implements AuthProvider {

    private final UserManagerCommunication userManagerCommunication;
    private final JWTTool jwtTool;
    
    /**
     * Create an auth provider with system storage
     * @param storage system storage for user repository
     * @param hasher password hasher
     */
    @Inject
    public PeaksAuthProvider(UserManagerCommunication userManagerCommunication, JWTTool jwtTool) {
        this.userManagerCommunication = userManagerCommunication;
        this.jwtTool = jwtTool;
    }
    
    @Override
    public void authenticate(JsonObject jsonObject, Handler<AsyncResult<io.vertx.ext.auth.User>> handler) {
        final String username = jsonObject.getString("username");
        final String password = jsonObject.getString("password");
        authenticate(username, password, handler);
    }

    public void authenticate(String username, String password, Handler<AsyncResult<io.vertx.ext.auth.User>> handler) {
        userManagerCommunication.authenticate(username, password,
            ackAccept -> {
                final String userId = ackAccept.getUserid();
                final UserRole role = ackAccept.getRole();
                final UserInformation userInformation = ackAccept.getUserInformation();
                final String salt = ackAccept.getSalt();
                final User user = new UserBuilder()
                    .userId(UUID.fromString(userId))
                    .username(userInformation.getUserName())
                    .fullName(userInformation.getFullName())
                    .email(userInformation.getEmail())
                    .salt(salt)
                    .role(role.equals(UserRole.SYS_ADMIN) ? User.Role.SYS_ADMIN : User.Role.USER)
                    .active(true)
                    .receiveEmail(userInformation.getReceiveEmail())
                    .defaultPriority(userInformation.getDefaultPriority())
                    .maximumPriority(userInformation.getMaximumPriority())
                    .locale(userInformation.getLocale())
                    .build();
                String jwt = jwtTool.encode(user);
                VertxUser vUser = new VertxUser(user, jwt, this);
                handler.handle(Future.succeededFuture(vUser));
            },
            param -> {
                HandlerException ex = new HandlerException(401, "Unauthorized user");
                handler.handle(Future.failedFuture(ex));
            },
            error -> {
                HandlerException ex = new HandlerException(500, "Error authenticating user", error);
                handler.handle(Future.failedFuture(ex));
            }
        );
    }
}
