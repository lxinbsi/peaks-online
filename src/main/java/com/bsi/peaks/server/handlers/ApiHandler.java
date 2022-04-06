package com.bsi.peaks.server.handlers;

import akka.dispatch.Futures;
import akka.japi.Pair;
import com.bsi.peaks.event.user.UserInformation;
import com.bsi.peaks.event.user.UserRole;
import com.bsi.peaks.event.user.UserState;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.model.system.UserBuilder;
import com.bsi.peaks.server.auth.VertxUser;
import com.bsi.peaks.server.models.PageResult;
import com.bsi.peaks.server.models.Pagination;
import com.bsi.peaks.server.models.PaginationBuilder;
import com.bsi.peaks.server.parsers.ParserException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.pcollections.PCollectionsModule;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.HttpServerResponseImpl;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Shengying Pan
 * Created by span on 1/12/17.
 */
public abstract class ApiHandler {
    protected static final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        .registerModule(new ProtobufModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .registerModule(new PCollectionsModule());
    private static final Logger LOG = LoggerFactory.getLogger(ApiHandler.class);
    protected final Vertx vertx;
    private final JsonFormat.Printer protoPrinter = JsonFormat.printer()
        .includingDefaultValueFields()
        .omittingInsignificantWhitespace();

    public ApiHandler(Vertx vertx) {
        this.vertx = vertx;
    }

    public static void failChunkedResponse(HttpServerResponse response) {
        response.write("ERROR");
        HttpConnection conn;
        try {
            HttpServerResponseImpl responseImpl = (HttpServerResponseImpl) response;
            Field f = responseImpl.getClass().getDeclaredField("conn");
            f.setAccessible(true);
            conn = (HttpConnection) f.get(responseImpl);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        conn.close();
    }

    protected static User user(String userId, UserState userState, String salt) {
        final UserInformation userInformation = userState.getUserInformation();
        return new UserBuilder()
            .userId(UUID.fromString(userId))
            .username(userInformation.getUserName())
            .fullName(userInformation.getFullName())
            .email(userInformation.getEmail())
            .role(userState.getUserRole().equals(UserRole.SYS_ADMIN) ? User.Role.SYS_ADMIN : User.Role.USER)
            .active(userState.getActive())
            .receiveEmail(userInformation.getReceiveEmail())
            .defaultPriority(userInformation.getDefaultPriority())
            .maximumPriority(userInformation.getMaximumPriority())
            .salt(salt)
            .locale(userInformation.getLocale())
            .build();
    }

    protected static void cleanUploads(RoutingContext context) {
        for (FileUpload fileUpload : context.fileUploads()) {
            String uploadedFileName = fileUpload.uploadedFileName();
            context.vertx().fileSystem().delete(uploadedFileName, handler -> {
                if (handler.succeeded()) {
                    LOG.info("Deleted uploaded filename " + uploadedFileName);
                } else {
                    LOG.error("Unable to delete uploaded filename " + uploadedFileName, handler.cause());
                }
            });
        }
    }

    public abstract Router createSubRouter();

    @NotNull
    protected static <T> Function<T, CompletionStage<T>> futureErrorIf(Predicate<T> predicate, int errorCode, String message) {
        return (T data) -> {
            if (predicate.test(data)) {
                return futureError(errorCode, message);
            } else {
                return CompletableFuture.completedFuture(data);
            }
        };
    }

    @NotNull
    protected static <T> CompletionStage<T> futureError(int errorCode, String message) {
        return Futures.failedCompletionStage(new HandlerException(errorCode, message));
    }

    @NotNull
    protected static <T> Function<T, CompletionStage<T>> futureErrorIfNull(int errorCode, String message) {
        return futureErrorIf(Objects::isNull, errorCode, message);
    }

    protected final void ok(RoutingContext context) {
        context.response().setStatusCode(204).end();
    }

    protected final void ok(RoutingContext context, Object obj) {
        if (obj == null) {
            notFound(context);
        } else {
            try {
                final String json;
                if (obj instanceof String) {
                    json = (String) obj;
                } else if (obj instanceof GeneratedMessageV3) {
                    json = protoPrinter.print((MessageOrBuilder) obj);
                } else {
                    json = OM.writeValueAsString(obj);
                }
                context.response()
                    .setStatusCode(200)
                    .putHeader("content-type", "application/json; charset=utf-8")
                    .end(json);
            } catch (Throwable e) {
                handleError(context, new HandlerException(500, "Unable to JSON serialize object", e));
            }
        }
    }

    /**
     * This is used for response of HTTP creation
     * @param context  associated routing context
     * @param content  content to send back to client
     * @param location location where the newly created object can be accessed
     */
    protected final void ok(RoutingContext context, String content, String location) {
        context.response()
                .setStatusCode(201)
                .putHeader("location", location)
                .end(content);
    }

    protected final void badRequest(RoutingContext context, String error) {
        context.response().setStatusCode(400).setStatusMessage(error).end();
    }

    protected final void notFound(RoutingContext context) {
        context.response().setStatusCode(404).setStatusMessage("Required data cannot be found").end();
    }

    protected final void unauthorized(RoutingContext context) {
        context.response().setStatusCode(401).setStatusMessage("No permission to perform given action").end();
    }
    
    // authentication without role
    protected final void auth(RoutingContext context, Consumer<User> userConsumer) {
        if (context.user() == null) {
            unauthorized(context);
        } else {
            final User user = ((VertxUser) context.user()).getUser();
            if (user.active()) {
                userConsumer.accept(user);
            } else {
                unauthorized(context);
            }
        }
    }

    // role based authentication
    protected final void auth(RoutingContext context, List<User.Role> roles, Consumer<User> userConsumer) {
        if (context.user() == null) {
            unauthorized(context);
        } else {
            User user = ((VertxUser) context.user()).getUser();
            if (user.active() && roles.contains(user.role())) {
                userConsumer.accept(user);
            } else {
                unauthorized(context);
            }
        }
    }

    protected final void getId(RoutingContext context, String param, Consumer<UUID> uuidConsumer) {
        final UUID uuid;
        try {
            uuid = UUID.fromString(context.request().getParam(param));
        } catch (IllegalArgumentException e) {
            badRequest(context, "Unable to parse id for " + param);
            return;
        }
        uuidConsumer.accept(uuid);
    }

    protected final <T extends GeneratedMessageV3> T getBody(RoutingContext context, GeneratedMessageV3.Builder builder) {
        try {
            if (context.getBody() == null) {
                throw new ParserException("Error request has not body.");
            }
            JsonFormat.parser()
                .ignoringUnknownFields()
                .merge(context.getBodyAsString(), builder);
            return (T) builder.build();
        } catch (Exception e) {
            throw new ParserException("Unable to parse body to: " + builder.getClass().getName(), e);
        }
    }

    protected final void handleError(RoutingContext context, Throwable e) {
        LOG.error("Error occurred", e);
        if (e instanceof ParserException || e.getCause() instanceof ParserException) {
            badRequest(context, e.getMessage());
        } else {
            context.fail(e);
        }
    }

    protected final PageResult pageResult(List items, int total, int pageSize, int pageNum) {
        Pagination pagination = new PaginationBuilder()
            .page(pageNum)
            .pageSize(items.size())
            .totalPages(pageSize == 0 ? 0 : (int) (Math.ceil((double) total / (double) pageSize)))
            .totalItems(total)
            .build();
        return new PageResult(items, pagination);
    }

    protected final void sendPage(
        RoutingContext context,
        CompletionStage<Pair<Integer, List>> stage,
        int pageSize,
        int pageNum,
        String extraErrorMessage
    ) {
        String pageInfo = pageSize == 0 ? "" : " page " + pageNum + ", size " + pageSize;
        stage.whenComplete((pair, ex) -> {
            if (ex != null) {
                LOG.error("Unable to load" + pageInfo + " from database" + extraErrorMessage, ex);
                context.fail(ex);
            } else {
                ok(context, pageResult(pair.second(), pair.first(), pageSize, pageNum));
            }
        });
    }

    protected final boolean getBoolean(RoutingContext context, String parameterName) {
        String includeDecoyStr = context.request().getParam(parameterName);
        if (includeDecoyStr == null || includeDecoyStr.isEmpty()) {
            return false;
        } else {
            return Boolean.parseBoolean(includeDecoyStr);
        }
    }
}

