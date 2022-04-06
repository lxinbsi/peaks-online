package com.bsi.peaks.server.es;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import akka.japi.Function;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.BackoffSupervisor;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.UserAuthenticationFactory;
import com.bsi.peaks.event.user.AuthenticationAccept;
import com.bsi.peaks.event.user.AuthenticationChallengeAnswer;
import com.bsi.peaks.event.user.AuthenticationChallengeQuestion;
import com.bsi.peaks.event.user.AuthenticationReject;
import com.bsi.peaks.event.user.AuthenticationRequest;
import com.bsi.peaks.event.user.CreateUser;
import com.bsi.peaks.event.user.ListUsersResponse;
import com.bsi.peaks.event.user.QuerySaltResponse;
import com.bsi.peaks.event.user.UpdateUserInformation;
import com.bsi.peaks.event.user.UserCommand;
import com.bsi.peaks.event.user.UserCredentials;
import com.bsi.peaks.event.user.UserDisabled;
import com.bsi.peaks.event.user.UserEnabled;
import com.bsi.peaks.event.user.UserEvent;
import com.bsi.peaks.event.user.UserInformation;
import com.bsi.peaks.event.user.UserInformationSet;
import com.bsi.peaks.event.user.UserManagerState;
import com.bsi.peaks.event.user.UserQueryRequest;
import com.bsi.peaks.event.user.UserQueryResponse;
import com.bsi.peaks.event.user.UserRole;
import com.bsi.peaks.event.user.UserRoleSet;
import com.bsi.peaks.event.user.UserState;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class UserManager extends PeaksCommandPersistentActor<UserManagerState, UserEvent, UserCommand> {

    public static final String PERSISTENCE_ID = "UserManager";
    public static final String ADMIN_USERID = "00000000-0000-0000-0000-000000000000";
    public static final String MESSAGE_DIGEST = "SHA-512";
    public static final String NAME = "usermanager";

    public UserManager() {
        super(UserEvent.class);
    }

    public static void start(ActorSystem system) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(UserManager.class),
                    NAME + "Instance",
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(10),
                    0.2
                ),
                PoisonPill.getInstance(),
                ClusterSingletonManagerSettings.create(system)
            ),
            NAME
        );
        system.actorOf(
            ClusterSingletonProxy.props("/user/" + NAME, ClusterSingletonProxySettings.create(system)),
            NAME + "Proxy"
        );
    }

    public static ActorRef instance(ActorSystem system) {
        return system.actorFor("/user/" + NAME + "Proxy");
    }

    public static UserManagerState withUserState(UserManagerState state, String userId, Function<UserState.Builder, UserState.Builder> modificiation) throws Exception {
        final UserState oldUserState = state.getStatebyUserIdOrDefault(userId, UserState.getDefaultInstance());
        final UserState newUserState = modificiation.apply(oldUserState.toBuilder()).build();
        final UserManagerState build = state.toBuilder().removeStatebyUserId(userId).putStatebyUserId(userId, newUserState).build();
        return build;
    }

    public static UserManagerState withUserCredentials(UserManagerState state, String userName, UserCredentials userCredentials) {
        return state.toBuilder().putCredentialsByUserName(userName, userCredentials).build();
    }

    @Override
    protected UserManagerState defaultState() {
        return UserManagerState.getDefaultInstance();
    }

    @Override
    protected void onRecoveryComplete(List<UserEvent> recoveryEvents) throws Exception {
        super.onRecoveryComplete(recoveryEvents);
        if (lastSequenceNr() == 0) {
            persistNewUser(
                ADMIN_USERID,
                UserAuthenticationFactory.makeCredentials(ADMIN_USERID, MESSAGE_DIGEST, "onlineuser"),
                UserInformation.newBuilder()
                    .setUserName("onlineuser")
                    .setFullName("Online User")
                    .setEmail("onlineuser@localhost.com")
                    .setReceiveEmail(false)
                    .setMaximumPriority(5)
                    .setDefaultPriority(3)
                    .build(),
                UserRole.SYS_ADMIN
            );
            persist(eventBuilder(ADMIN_USERID)
                .setUserEnabled(UserEnabled.newBuilder()
                    .setUserId(ADMIN_USERID)
                    .build()
                )
                .build()
            );
        }
    }

    private void persistNewUser(
        String byUserId,
        UserCredentials credentials,
        UserInformation userInformation,
        UserRole role
    ) throws Exception {
        final String userId = credentials.getUserId();
        persist(eventBuilder(byUserId)
            .setUserInformationSet(UserInformationSet.newBuilder()
                .setUserId(userId)
                .setUserInformation(userInformation)
                .build()
            )
            .build()
        );
        persist(eventBuilder(byUserId)
            .setUserRoleSet(UserRoleSet.newBuilder()
                .setUserId(userId)
                .setUserRole(role)
                .build()
            )
            .build()
        );
        persist(eventBuilder(byUserId)
            .setUserCredentials(credentials)
            .build()
        );
    }

    @Override
    protected ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(UserCommand.class, UserCommand::hasCreateUser, this::createUser)
            .match(UserCommand.class, UserCommand::hasActivateUser, this::activateUser)
            .match(UserCommand.class, UserCommand::hasDectivateUser, this::deactivateUser)
            .match(UserCommand.class, UserCommand::hasUpdateCredentials, this::updateCredentials)
            .match(UserCommand.class, UserCommand::hasUpdateUserInformation, this::updateUserInformation)
            .match(UserQueryRequest.class, UserQueryRequest::hasListUsers, this::queryListUsers)
            .match(UserQueryRequest.class, UserQueryRequest::hasQueryUserSalt, this::queryUserSalt)
            .match(AuthenticationRequest.class, this::authenticationRequest)
            .match(AuthenticationChallengeAnswer.class, this::authenticationAnswer);
    }

    @Override
    protected long deliveryId(UserCommand command) {
        return command.getDeliveryId();
    }

    @Override
    protected UserManagerState nextState(UserManagerState data, UserEvent event) throws Exception {
        switch (event.getEventCase()) {
            case USERINFORMATIONSET: {
                final UserInformationSet userInformationSet = event.getUserInformationSet();
                final String userId = userInformationSet.getUserId();
                // Force user names to lowercase.
                UserInformation userInformation = userInformationSet.getUserInformation()
                    .toBuilder()
                    .setUserName(userInformationSet.getUserInformation().getUserName().toLowerCase())
                    .build();
                return withUserState(data, userId, b -> b.setUserInformation(userInformation));
            }
            case USERCREDENTIALS: {
                final UserCredentials userCredentials = event.getUserCredentials();
                final String userName = Optional.ofNullable(data.getStatebyUserIdMap().get(userCredentials.getUserId()))
                    .orElseThrow(() -> new IllegalStateException("Credentials event encountered for non-existent userId"))
                    .getUserInformation()
                    .getUserName();
                return data.toBuilder().putCredentialsByUserName(userName, userCredentials).build();
            }
            case USERROLESET: {
                final UserRoleSet userRoleSet = event.getUserRoleSet();
                final String userId = userRoleSet.getUserId();
                final UserRole userRole = userRoleSet.getUserRole();
                return withUserState(data, userId, b -> b.setUserRole(userRole));
            }
            case USERENABLED: {
                final UserEnabled userEnabled = event.getUserEnabled();
                final String userId = userEnabled.getUserId();
                return withUserState(data, userId, b -> b.setActive(true));
            }
            case USERDISABLED: {
                final UserDisabled userDisabled = event.getUserDisabled();
                final String userId = userDisabled.getUserId();
                return withUserState(data, userId, b -> b.setActive(false));
            }
            default:
                return data;
        }
    }

    @Override
    protected String commandTag(UserCommand command) {
        return command.getCommandCase().name();
    }

    @Override
    public String persistenceId() {
        return PERSISTENCE_ID;
    }

    private void createUser(UserCommand command) {
        try {
            if (!command.getByUserId().equals(ADMIN_USERID)) {
                reply(ackReject(command, "Only Admin is allowed to create users"));
                return;
            }
            final CreateUser createUser = command.getCreateUser();
            final UserCredentials userCredentials = createUser.getUserCredentials();
            final UserInformation userInformation = createUser.getUserInformation();
            if (lookupCredentials(userInformation.getUserName()).isPresent()) {
                reply(ackReject(command, "Username already in use"));
                return;
            }
            if (lookupUserState(userCredentials.getUserId()).isPresent()) {
                reply(ackReject(command, "UserId already exists"));
                return;
            }
            persistNewUser(
                command.getByUserId(),
                userCredentials,
                userInformation,
                UserRole.USER
            );
            defer(ackAccept(command), this::reply);
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void activateUser(UserCommand command) {
        try {
            if (!command.getByUserId().equals(ADMIN_USERID)) {
                reply(ackReject(command, "Only Admin is allowed to activate users"));
                return;
            }
            final String userId = command.getActivateUser().getUserId();
            final Optional<UserState> userState = lookupUserState(userId);
            if (!userState.isPresent()) {
                reply(ackReject(command, "User does not exist"));
                return;
            }
            if (!userState.get().getActive()) {
                persist(eventBuilder(command.getByUserId())
                    .setUserEnabled(UserEnabled.newBuilder()
                        .setUserId(userId)
                        .build()
                    )
                    .build()
                );
            }
            reply(ackAccept(command));
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void deactivateUser(UserCommand command) {
        try {
            if (!command.getByUserId().equals(ADMIN_USERID)) {
                reply(ackReject(command, "Only Admin is allowed to deactivate users"));
                return;
            }
            final String userId = command.getDectivateUser().getUserId();
            if (userId.equals(ADMIN_USERID)) {
                reply(ackReject(command, "Admin cannot be deactivated"));
                return;
            }
            final Optional<UserState> userState = lookupUserState(userId);
            if (!userState.isPresent()) {
                reply(ackReject(command, "User does not exist"));
                return;
            }
            if (userState.get().getActive()) {
                persist(eventBuilder(command.getByUserId())
                    .setUserDisabled(UserDisabled.newBuilder()
                        .setUserId(userId)
                        .build())
                    .build()
                );
            }
            reply(ackAccept(command));
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void updateCredentials(UserCommand command) {
        try {
            final UserCredentials credentials = command.getUpdateCredentials().getUserCredentials();
            final String byUserId = command.getByUserId();
            if (!byUserId.equals(ADMIN_USERID) && !byUserId.equals(credentials.getUserId())) {
                reply(ackReject(command, "Unauthorized password change"));
                return;
            }
            persist(eventBuilder(byUserId).setUserCredentials(credentials).build(), () -> reply(ackAccept(command)));
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void queryListUsers(UserQueryRequest request) {
        final UserQueryResponse response = queryResponseBuilder(request)
            .setListUsers(ListUsersResponse.newBuilder()
                .putAllUsers(data.getStatebyUserIdMap())
                .build())
            .build();
        reply(response);
    }

    private void queryUserSalt(UserQueryRequest request) {
        try {
            final String username = request.getQueryUserSalt().getUsername().toLowerCase();
            final String salt = data.getCredentialsByUserNameMap().get(username).getSalt();
            final UserQueryResponse response = queryResponseBuilder(request)
                    .setQuerySalt(QuerySaltResponse.newBuilder()
                            .setSalt(salt)
                            .build())
                    .build();
            reply(response);
        } catch (Throwable e) {
            log.error("Unable to query user information caused by", e);
            reply(e);
        }
    }

    private void updateUserInformation(UserCommand command) {
        try {
            final UpdateUserInformation updateUserInformation = command.getUpdateUserInformation();
            final String userId = updateUserInformation.getUserId();
            final UserInformation userInformation = updateUserInformation.getUserInformation();
            final String actingAsUserId = command.getByUserId();
            if (actingAsUserId.equals(userId)
                || lookupUserState(actingAsUserId).map(x -> x.getUserRole().equals(UserRole.SYS_ADMIN)).orElse(false)) {
                persist(eventBuilder(actingAsUserId)
                        .setUserInformationSet(UserInformationSet.newBuilder()
                            .setUserId(userId)
                            .setUserInformation(userInformation)
                            .build()
                        )
                        .build(),
                    () -> reply(ackAccept(command))
                );
            } else {
                reply(ackReject(command, "Unauthorzed to update User Information"));
            }
        } catch (Throwable e) {
            reply(ackReject(command, e.getMessage()));
        }
    }

    private void authenticationRequest(AuthenticationRequest request) {
        try {
            final String username = request.getUsername();
            final Optional<UserCredentials> optionalUserCredentials = lookupCredentials(username);
            if (optionalUserCredentials.isPresent()) {
                final UserCredentials userCredentials = optionalUserCredentials.get();
                reply(AuthenticationChallengeQuestion.newBuilder()
                    .setChallengeId(request.getChallengeId())
                    .setUsername(username)
                    .setMessageDigest(userCredentials.getMessageDigest())
                    .setSalt(userCredentials.getSalt())
                    .build()
                );
            } else {
                // Return fake challenge to avoid revealing whether user exists.
                reply(AuthenticationChallengeQuestion.newBuilder()
                    .setChallengeId(request.getChallengeId())
                    .setUsername(username)
                    .setMessageDigest(MESSAGE_DIGEST)
                    .setSalt(UserAuthenticationFactory.generateSalt())
                    .build()
                );
            }
        } catch (Throwable e) {
            log.error(e, "Exception during authentication request");
        }
    }

    @NotNull
    private Optional<UserCredentials> lookupCredentials(String username) {
        return Optional.ofNullable(data.getCredentialsByUserNameMap().get(username.toLowerCase()));
    }

    @NotNull
    private Optional<UserState> lookupUserState(String userId) {
        return Optional.ofNullable(data.getStatebyUserIdMap().get(userId));
    }

    private void authenticationAnswer(AuthenticationChallengeAnswer answer) {
        try {
            final String username = answer.getUsername();
            final Optional<UserCredentials> credentialsOptional = lookupCredentials(username);
            final Optional<UserState> userStateOptional = credentialsOptional
                .filter(credentials -> answer.getHashedPassword().equals(credentials.getHashedPassword()))
                .flatMap(credentials -> lookupUserState(credentials.getUserId()))
                .filter(UserState::getActive);
            if (userStateOptional.isPresent()) {
                final UserState userState = userStateOptional.get();
                reply(AuthenticationAccept.newBuilder()
                    .setUserid(credentialsOptional.get().getUserId())
                    .setChallengeId(answer.getChallengeId())
                    .setUserInformation(userState.getUserInformation())
                    .setSalt(credentialsOptional.get().getSalt())
                    .setRole(userState.getUserRole())
                    .build()
                );
            } else {
                reply(AuthenticationReject.newBuilder()
                    .setChallengeId(answer.getChallengeId())
                    .setUsername(username)
                    .build()
                );
            }
        } catch (Throwable e) {
            log.error(e, "Exception during authentication challenge");
        }
    }

    @NotNull
    private UserQueryResponse.Builder queryResponseBuilder(UserQueryRequest request) {
        return UserQueryResponse.newBuilder()
            .setQueryId(request.getQueryId());
    }

    private UserEvent.Builder eventBuilder(String byUserId) {
        return UserEvent.newBuilder()
            .setByUserId(byUserId)
            .setTimeStamp(CommonFactory.convert(Instant.now()));
    }

}
