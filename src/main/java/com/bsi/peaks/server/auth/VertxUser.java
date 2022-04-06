package com.bsi.peaks.server.auth;

import com.bsi.peaks.model.system.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Throwables;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AbstractUser;
import io.vertx.ext.auth.AuthProvider;

/**
 * @author Shengying Pan
 * Created by span on 1/24/17.
 */
public class VertxUser extends AbstractUser {
    private User user;
    private String jwt;
    private AuthProvider authProvider;

    private final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new Jdk8Module());

    /**
     * Create a Vertx user from peaks user
     * @param user associated peaks user
     */
    public VertxUser(User user, String jwt, AuthProvider authProvider) {
        this.user = user;
        this.jwt = jwt;
        this.authProvider = authProvider;
    }
    
    @Override
    protected void doIsPermitted(String s, Handler<AsyncResult<Boolean>> handler) {
        handler.handle(Future.succeededFuture(true));
    }
    
    @Override
    public void setAuthProvider(AuthProvider authProvider) {
        this.authProvider = authProvider;
    }
    
    @Override
    public JsonObject principal() {
        try {
            return new JsonObject(OM.writeValueAsString(user));
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    public User getUser() {
        return user;
    }
    
    public String getJWT() {
        return jwt;
    }
}
