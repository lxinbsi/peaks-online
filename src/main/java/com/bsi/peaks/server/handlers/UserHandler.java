package com.bsi.peaks.server.handlers;

import akka.Done;
import akka.japi.Pair;
import com.bsi.peaks.event.UserAuthenticationFactory;
import com.bsi.peaks.event.user.UserInformation;
import com.bsi.peaks.event.user.UserState;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.auth.JWTTool;
import com.bsi.peaks.server.auth.VertxUser;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.es.communication.UserManagerCommunication;
import com.bsi.peaks.server.service.UserService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * @author Shengying Pan
 * Created by span on 1/12/17.
 */
public class UserHandler extends ApiHandler {
    private static final Logger LOG = LoggerFactory.getLogger(UserHandler.class);
    private final AuthProvider authProvider;
    private final JWTTool jwtTool;
    private final UserService userService;
    private final UserManagerCommunication userManagerCommunication;
    private final SystemConfig config;

    private final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new Jdk8Module());

    @Inject
    public UserHandler(
        final Vertx vertx,
        final SystemConfig config,
        final UserService userService,
        final JWTTool jwtTool,
        final AuthProvider authProvider,
        final UserManagerCommunication userManagerCommunication
    ) {
        super(vertx);
        this.userService = userService;
        this.jwtTool = jwtTool;
        this.authProvider = authProvider;
        this.userManagerCommunication = userManagerCommunication;
        this.config = config;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.route("/").handler(BodyHandler.create());
        subRouter.route("/:id").handler(BodyHandler.create());
        subRouter.post("/").consumes("*/json").handler(this::create);
        subRouter.get("/").handler(this::readAll);
        subRouter.get("/:id").handler(this::readOne);
        subRouter.put("/:id").consumes("*/json").handler(this::update);
        subRouter.post("/action/login").handler(BodyHandler.create()).consumes("*/json").handler(this::login);
        subRouter.post("/action/logout").handler(this::logout);
        subRouter.get("/action/check").handler(this::check);
        subRouter.get("/:id/jwt").handler(this::getJWT);
        subRouter.get("/:username/salt").handler(this::getSalt);
        return subRouter;
    }

    private void readAll(RoutingContext context) {
        auth(context, user -> {
            final CompletionStage<Pair<Integer, List>> futureUserList = userService.allUsers(user)
                .thenApply(users -> {
                    final List<User> userList = users.entrySet().stream()
                        .map(entry -> user(entry.getKey(), entry.getValue(), getSalt(entry.getValue())))
                        .collect(Collectors.toList());
                    return Pair.create(userList.size(), userList);
                });
            sendPage(context, futureUserList, 0, 0, "");
        });
    }

    protected void readOne(RoutingContext context) {
        auth(context, user -> {
            final String userId = context.request().getParam("id");
            if (userId == null) {
                badRequest(context, "Invalid parameter id");
                return;
            }
            userService.getUserByUserId(userId).whenComplete((userState, error) -> {
                if (error == null) {
                    ok(context, user(userId, userState, getSalt(userState)));
                } else {
                    LOG.error("Unable to load userId " + userId + "", error);
                    context.fail(error);
                }
            });
        });
    }

    private void getJWT(RoutingContext context) {
        auth(context, user -> getId(context, "id", userId -> userService.getUserByUserId(userId).whenComplete((found, error) -> {
            if (error != null) {
                LOG.error("unable to find user in database", error);
                notFound(context);
            } else {
                context.response().setStatusCode(200).end(jwtTool.encode(user(userId.toString(), found, getSalt(found))));
            }
        })));
    }

    private void getSalt(RoutingContext context) {
        final String username = context.request().getParam("username");
        if (username != null) {
            userService.getSaltByUsername(username).whenComplete((salt, error) -> {
                if (error != null) {
                    LOG.error("unable to find user salt in database", error);
                    // Generate random salt, so as not to reveal that user does not exist.
                    salt = UserAuthenticationFactory.generateSalt();
                }
                context.response().setStatusCode(200).end(salt);
            });
        } else {
            badRequest(context, "Unable to parse username");
        }
    }

    private void login(RoutingContext context) {
        JsonObject authInfo = context.getBodyAsJson();
        authProvider.authenticate(authInfo, res -> {
            if (res.succeeded()) {
                VertxUser user = (VertxUser) res.result();
                context.setUser(res.result());
                context.response()
                    .setStatusCode(200)
                    .putHeader("content-type", "application/json; charset=utf-8")
                    .putHeader("authorization", user.getJWT())
                    .end(user.principal().encode());
            } else if (res.cause() instanceof HandlerException && ((HandlerException) res.cause()).getErrorCode() == 401) {
                HandlerException he = (HandlerException) res.cause();
                context.response().setStatusCode(he.getErrorCode()).end(he.getMessage());
            } else {
                context.fail(res.cause());
            }
        });
    }

    private void logout(RoutingContext context) {
        context.setUser(null);
        ok(context);
    }

    private void check(RoutingContext context) {
        auth(context, user -> ok(context, user));
    }

    protected void create(RoutingContext context) {
        auth(context, user -> {
            final User newUser;
            try {
                newUser = OM.readValue(context.getBodyAsString(), User.class);
            } catch (IOException e) {
                badRequest(context, "Unable to parse request " + context.getBodyAsString());
                return;
            }
            userService.createNewUser(user, newUser)
                .thenCompose(newUserid -> userService.countActiveUsers()
                    .thenCompose(userCount -> {
                        CompletionStage<Done> future;
                        future = userService.activateUser(user, newUserid.toString());
                        return future.thenApply(done -> userCount);
                    })
                )
                .whenComplete((userCount, error) -> {
                    if (error != null) {
                        LOG.error("Unable to add user: " + error.getMessage(), error);
                        context.fail(error);
                    } else {
                        ok(context, userCount.toString(), context.normalisedPath() + "/" + userCount);
                    }
                });
        });
    }

    @SuppressWarnings("ConstantConditions")
    protected void update(RoutingContext context) {
        auth(context, actingAsUser -> {
            final User newUserInformation;
            try {
                newUserInformation = OM.readValue(context.getBodyAsString(), User.class);
            } catch (IOException e) {
                badRequest(context, "Unable to parse request " + context.getBodyAsString());
                return;
            }
            final String userId = newUserInformation.userId().toString();
            final String newPassword = newUserInformation.password();

            // TODO: This is horrible! The URL is readable by anyone, even with SSL!!! Putting password in query string is a NO NO!
            final String oldPassword = context.request().getParam("password");

            // Update password id required.
            final boolean isAdmin = actingAsUser.role().equals(User.Role.SYS_ADMIN);
            final boolean actingUserIsTarget = newUserInformation.userId().equals(actingAsUser.userId());

            CompletionStage<Done> futureDone;
            if (!Strings.isNullOrEmpty(newPassword)) {
                if (isAdmin) {
                    futureDone = userService.setPassword(actingAsUser, userId, newPassword, newUserInformation.salt());
                } else {
                    if (actingUserIsTarget) {
                        futureDone = userService.verifyThenSetPassword(actingAsUser, userId, oldPassword, newPassword, newUserInformation.salt());
                    } else {
                        badRequest(context, "Unauthorized attempt to change password");
                        return;
                    }
                }
            } else {
                futureDone = CompletableFuture.completedFuture(Done.getInstance());
            }

            if (isAdmin) {
                if (newUserInformation.active()) {
                    futureDone = futureDone
                        .thenCompose(done -> userService.countActiveUsers())
                        .thenCompose(userCount -> {
                            return userService.activateUser(actingAsUser, userId);
                        });
                } else {
                    futureDone = futureDone.thenCompose(done -> userService.deactivateUser(actingAsUser, userId));
                }
            }

            futureDone
                .thenCompose(done -> userService.updateUserInformation(actingAsUser, userId, UserInformation.newBuilder()
                    .setUserName(newUserInformation.username())
                    .setFullName(newUserInformation.fullName())
                    .setEmail(newUserInformation.email())
                    .setReceiveEmail(newUserInformation.receiveEmail())
                    .setDefaultPriority(newUserInformation.defaultPriority())
                    .setMaximumPriority(newUserInformation.maximumPriority())
                    .setLocale(newUserInformation.locale())
                    .build()
                ))
                .whenComplete((updated, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        userService.getUserByUserId(newUserInformation.userId())
                            .whenComplete((userState, throwable) -> {
                                if (throwable == null) {
                                    if (actingUserIsTarget) {
                                        context.setUser(vertxUser(userId, userState));
                                    }
                                    ok(context, user(userId, userState, getSalt(userState)));
                                } else {
                                    LOG.error("Error retrieving user, after update.", throwable);
                                    context.fail(error);
                                }
                            });
                    }
                });
        });
    }

    private String getSalt(UserState userState) {
        return userService.getSaltByUsername(userState.getUserInformation().getUserName())
            .toCompletableFuture().join();
    }

    private VertxUser vertxUser(String userId, UserState userState) {
        final User user = user(userId, userState, getSalt(userState));
        return new VertxUser(user, jwtTool.encode(user), authProvider);
    }

}
