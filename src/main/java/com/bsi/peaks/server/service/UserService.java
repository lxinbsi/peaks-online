package com.bsi.peaks.server.service;

import akka.Done;
import akka.dispatch.Futures;
import com.bsi.peaks.event.UserAuthenticationFactory;
import com.bsi.peaks.event.UserCommandFactory;
import com.bsi.peaks.event.UserQueryFactory;
import com.bsi.peaks.event.user.UserCommand;
import com.bsi.peaks.event.user.UserCredentials;
import com.bsi.peaks.event.user.UserInformation;
import com.bsi.peaks.event.user.UserState;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.model.system.UserBuilder;
import com.bsi.peaks.server.es.UserManager;
import com.bsi.peaks.server.es.communication.UserManagerCommunication;
import com.bsi.peaks.server.handlers.HandlerException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;
import static com.bsi.peaks.server.es.UserManager.MESSAGE_DIGEST;

@Singleton
public class UserService {
    private final static int CACHE_SIZE = 16;
    private final UserManagerCommunication userManagerCommunication;
    private static final User ADMIN = new UserBuilder()
        .username("admin")
        .userId(UUID.fromString(UserManager.ADMIN_USERID))
        .role(User.Role.SYS_ADMIN)
        .locale("en")
        .build();
    private final LoadingCache<String, CompletionStage<UserState>> userCache;

    @Inject
    public UserService(UserManagerCommunication userManagerCommunication) {
        this.userManagerCommunication = userManagerCommunication;
        this.userCache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .build(CacheLoader.from(this::userCacheLoader));
    }

    private CompletionStage<UserState> userCacheLoader(String userId) {
        return allUsers(ADMIN).thenApply(users -> {
            final UserState userState = users.get(userId);
            return Optional.ofNullable(userState).orElseThrow(NoSuchElementException::new);
        });
    }

    public CompletionStage<String> getUsernameByUserId(UUID userId) {
        return getUserByUserId(userId).thenApply(us -> us.getUserInformation().getUserName());
    }

    public CompletionStage<UserState> getUserByUserId(UUID userId) {
        return getUserByUserId(userId.toString());
    }

    public CompletionStage<UserState> getUserByUserId(String userId) {
        try {
            return userCache.get(userId);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletionStage<String> getSaltByUsername(String username) {
        return userManagerCommunication
            .query(UserQueryFactory.requestUserSalt(username))
            .thenApply(response -> response.getQuerySalt().getSalt());
    }

    public CompletionStage<Map<String, UserState>> allUsers(User actingAsUser) {
        return userManagerCommunication
            .query(queryFactory(actingAsUser).requestUserList())
            .thenApply(response -> response.getListUsers().getUsersMap());
    }

    public CompletionStage<Integer> countActiveUsers() {
        return userManagerCommunication
            .query(UserQueryFactory.actingAsUserId(ADMIN_USERID).requestUserList())
            .thenApply(x -> (int) x.getListUsers().getUsersMap().values().stream()
                .filter(UserState::getActive)
                .count()
            );
    }

    public CompletionStage<UUID> createNewUser(User actingAsUser, User user) {
        final UserCommand command;
        final UUID newUserId = user.userId() == null ? UUID.randomUUID() : user.userId();
        final UserCredentials credentials = UserAuthenticationFactory.makeCredentials(newUserId.toString(), MESSAGE_DIGEST, user.password(), user.salt());
        command = commandFactory(actingAsUser).createUser(
            credentials,
            UserInformation.newBuilder()
                .setUserName(user.username())
                .setFullName(user.fullName())
                .setEmail(user.email())
                .setReceiveEmail(user.receiveEmail())
                .setMaximumPriority(user.maximumPriority())
                .setDefaultPriority(3)
                .setLocale(user.locale())
                .build()
        );
        return userManagerCommunication.command(command).thenApply(ignore -> newUserId);
    }

    public UserCommandFactory commandFactory(User actingAsUser) {
        return UserCommandFactory.actingAsUserId(actingAsUser.userId().toString());
    }

    private UserQueryFactory queryFactory(User actingAsUser) {
        return UserQueryFactory.actingAsUserId(actingAsUser.userId().toString());
    }

    private Function<Done, Done> invalidateCache(String userId) {
        return done -> {
            userCache.invalidate(userId);
            return done;
        };
    }

    public CompletionStage<Done> activateUser(User actingAsUser, String userId) {
        final UserCommand command = commandFactory(actingAsUser).activateUser(userId);
        return userManagerCommunication.command(command).thenApply(invalidateCache(userId));
    }

    public CompletionStage<Done> deactivateUser(User actingAsUser, String userId) {
        final UserCommand command = commandFactory(actingAsUser).deactivateUser(userId);
        return userManagerCommunication.command(command).thenApply(invalidateCache(userId));
    }

    public CompletionStage<Done> setPassword(User actingAsUser, String userId, String newPassword, String salt) {
        final UserCredentials credentials;
        credentials = UserAuthenticationFactory.makeCredentials(userId, MESSAGE_DIGEST, newPassword, salt);
        final UserCommand command = commandFactory(actingAsUser).updateCredentials(credentials);
        return userManagerCommunication.command(command).thenApply(invalidateCache(userId));
    }

    public CompletionStage<Done> verifyThenSetPassword(User actingAsUser, String userId, String oldPaassword, String newPassword, String salt) {
        if (!actingAsUser.userId().toString().equals(userId)) {
            return Futures.failedCompletionStage(new HandlerException(400, "Not authorized to change password"));
        }
        final UserCredentials credentials;
        credentials = UserAuthenticationFactory.makeCredentials(userId, MESSAGE_DIGEST, newPassword, salt);
        final CompletableFuture<Done> futureDone = new CompletableFuture<>();
        userManagerCommunication.authenticate(actingAsUser.username(), oldPaassword,
            onAccept -> futureDone.complete(Done.getInstance()),
            onReject -> futureDone.completeExceptionally(new HandlerException(400, "The old password is not matched")),
            futureDone::completeExceptionally
        );
        return futureDone.thenCompose(done -> {
            final UserCommand command = commandFactory(actingAsUser).updateCredentials(credentials);
            return userManagerCommunication.command(command).thenApply(invalidateCache(userId));
        });
    }

    public CompletionStage<Done> updateUserInformation(User actingAsUser, String userId, UserInformation userInformation) {
        final UserCommand command = commandFactory(actingAsUser).updateUserInformation(userId, userInformation);
        return userManagerCommunication.command(command).thenApply(invalidateCache(userId));
    }
}
