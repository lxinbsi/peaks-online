package com.bsi.peaks.server.es;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ClusterShardingSettings;
import akka.cluster.sharding.ShardRegion;
import akka.japi.pf.ReceiveBuilder;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.FastaSequenceRepository;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.common.CommandAck;
import com.bsi.peaks.event.fasta.*;
import com.bsi.peaks.model.proteomics.fasta.TaxonomyMapper;
import com.google.common.base.Strings;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;

public class FastaDatabaseEntity extends PeaksEntityCommandPersistentActor<FastaDatabaseState, FastaDatabaseEvent, FastaDatabaseCommand> {

    public static final String CLUSTER_NAMESPACE = "FastaDatabase";
    public static final FastaDatabaseMessageExtractor MESSAGE_EXTRACTOR = new FastaDatabaseMessageExtractor();
    private final FastaSequenceRepository fastaSequenceRepository;

    public FastaDatabaseEntity(FastaSequenceRepository fastaSequenceRepository) {
        super(FastaDatabaseEvent.class, 5);
        this.fastaSequenceRepository = fastaSequenceRepository;
    }

    public static void start(ActorSystem system, ApplicationStorage applicationStorage) {
        final ClusterShardingSettings settings = ClusterShardingSettings.create(system);
        final ClusterSharding clusterSharding = ClusterSharding.get(system);
        clusterSharding.start(CLUSTER_NAMESPACE, Props.create(FastaDatabaseEntity.class, applicationStorage.getFastaSequenceRepository()), settings, MESSAGE_EXTRACTOR);
    }

    public static ActorRef instance(ActorSystem system) {
        return ClusterSharding.get(system).shardRegion(CLUSTER_NAMESPACE);
    }

    public static final String persistenceId(String databaseId) {
        return MessageFormat.format("{0}-{1}", CLUSTER_NAMESPACE, databaseId);
    }

    public static String commandTag(FastaDatabaseCommand.CommandCase commandCase) {
        return "FastaDatabaseCommand." + commandCase.name();
    }

    public static String eventTag(FastaDatabaseEvent.EventCase eventCase) {
        return "FastaDatabaseEvent." + eventCase.name();
    }

    @Override
    protected FastaDatabaseState defaultState() {
        final int prefixLength = CLUSTER_NAMESPACE.length() + 1;
        final String databaseId = getSelf().path().name().substring(prefixLength);
        return FastaDatabaseState.newBuilder()
            .setDatabaseId(databaseId)
            .build();
    }

    @Override
    protected ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder)
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasCreate, ignoreThenAckOr(this::ignoreCreate, this::create))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasDelete, ignoreThenAckOr(this::ignoreDelete, this::delete))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasSetOwner, ignoreThenAckOr(this::ignoreSetOwner, this::setOwner))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasSetInformation, ignoreThenAckOr(this::ignoreSetInformation, this::setInformation))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasUploadStarted, ignoreThenAckOr(this::ignoreUploadStarted, this::uploadStarted))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasUploadCompleted, ignoreThenAckOr(this::ignoreUploadCompleted, this::uploadCompleted))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasUploadFailed, ignoreThenAckOr(this::ignoreUploadFailed, this::uploadFailed))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasProcessingStarted, ignoreThenAckOr(this::ignoreProcessingStarted, this::processingStarted))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasProcessingCompleted, ignoreThenAckOr(this::ignoreProcessingCompleted, this::processingCompleted))
            .match(FastaDatabaseCommand.class, FastaDatabaseCommand::hasProcessingFailed, ignoreThenAckOr(this::ignoreProcessingFailed, this::processingFailed))
            .match(FastaDatabaseQuery.class, FastaDatabaseQuery::hasState, this::queryState)
            .match(FastaDatabaseQuery.class, FastaDatabaseQuery::hasQueryFastaDatabaseTaxonomyCount, this::queryFastaDatabaseTaxonomyCount);
    }

    private boolean ignoreCreate(FastaDatabaseCommand command) {
        final CreateFastaDatabase create = command.getCreate();
        validateFastaDatabaseInformation(create.getInformation());

        // If events exist, then FastaDatabase is already created.
        return lastSequenceNr() > 0;
    }

    private CommandAck create(FastaDatabaseCommand command) throws Exception {
        final CreateFastaDatabase create = command.getCreate();
        persist(eventBuilder(command.getActingUserId())
            .setOwnerSet(FastaDatabaseOwnerSet.newBuilder()
                .setOwnerId(command.getActingUserId())
                .build())
            .build()
        );
        persist(eventBuilder(command.getActingUserId())
            .setCreated(FastaDatabaseCreated.newBuilder().setTaxonomyParser(create.getTaxonomy()))
            .build()
        );
        persist(eventBuilder(command.getActingUserId())
            .setInformationSet(FastaDatabaseInformationSet.newBuilder()
                .setInformation(create.getInformation())
                .build()
            )
            .build()
        );
        return ackAccept(command);
    }

    private boolean ignoreDelete(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        return data.getStatus().equals(FastaDatabaseStatus.FASTA_DELETED);
    }

    private CommandAck delete(FastaDatabaseCommand command) throws Exception {
        deleteFromRespository(); // Fire and forget ... better would be to save an event when delete compeletes successfully.
        persist(eventBuilder(command.getActingUserId())
            .setDeleted(FastaDatabaseDeleted.getDefaultInstance())
            .build()
        );
        return ackAccept(command);
    }

    private boolean ignoreSetOwner(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        final SetOwner setOwner = command.getSetOwner();
        if (Strings.isNullOrEmpty(setOwner.getOwnerId())) {
            throw new IllegalArgumentException("OwnerId must have a value");
        }
        return false;
    }

    private CommandAck setOwner(FastaDatabaseCommand command) throws Exception {
        final SetOwner setOwner = command.getSetOwner();
        persist(eventBuilder(command.getActingUserId())
            .setOwnerSet(FastaDatabaseOwnerSet.newBuilder().setOwnerId(setOwner.getOwnerId()).build())
            .build());
        return ackAccept(command);
    }

    private boolean ignoreSetInformation(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        final FastaDatabaseInformation information = command.getSetInformation();
        validateFastaDatabaseInformation(information);
        if (!information.getName().equals(data.getInformation().getName())) {
            throw new IllegalArgumentException("Change of name is not permitted");
        }
        return false;
    }

    private CommandAck setInformation(FastaDatabaseCommand command) throws Exception {
        final FastaDatabaseInformation information = command.getSetInformation();
        persist(eventBuilder(command.getActingUserId())
            .setInformationSet(FastaDatabaseInformationSet.newBuilder()
                .setInformation(information)
                .build()
            )
            .build()
        );
        return ackAccept(command);
    }

    private void validateFastaDatabaseInformation(FastaDatabaseInformation information) {
        if (Strings.isNullOrEmpty(information.getName())) {
            throw new IllegalArgumentException("FastaDatabaseInformation name cannot be empty");
        }
        final String idPattern = information.getIdPattern();
        if (Strings.isNullOrEmpty(idPattern)) {
            throw new IllegalArgumentException("FastaDatabaseInformation idPattern cannot be empty");
        }
        try {
            Pattern.compile(idPattern);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("FastaDatabaseInformation idPattern is invalid regular expression");
        }

        final String descriptionPattern = information.getDescriptionPattern();
        if (Strings.isNullOrEmpty(idPattern)) {
            throw new IllegalArgumentException("FastaDatabaseInformation descriptionPattern cannot be empty");
        }
        try {
            Pattern.compile(descriptionPattern);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("FastaDatabaseInformation descriptionPattern is invalid regular expression");
        }
    }

    private boolean ignoreUploadStarted(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        final String uploadTaskId = data.getUploadTaskId();
        final FastaDatabaseUploadStarted uploadStarted = command.getUploadStarted();
        if (Strings.isNullOrEmpty(uploadTaskId)) {
            return false;
        }
        if (uploadTaskId.equals(uploadStarted.getTaskId())) {
            return true;
        }
        throw new IllegalStateException("Upload already started under a different taskId");
    }

    private CommandAck uploadStarted(FastaDatabaseCommand command) throws Exception {
        persist(eventBuilder(command.getDatabaseId())
            .setUploadStarted(command.getUploadStarted())
            .build()
        );
        return ackAccept(command);
    }

    private boolean ignoreUploadCompleted(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        final String uploadTaskId = data.getUploadTaskId();
        final FastaDatabaseUploadCompleted uploadCompleted = command.getUploadCompleted();
        if (uploadTaskId.equals(uploadCompleted.getTaskId())) {
            final FastaDatabaseStatus status = data.getStatus();
            switch (status) {
                case FASTA_PENDING:
                case FASTA_FAILED:
                    throw new IllegalStateException("Unexpected upload completed when current state is " + status.name());
                case FASTA_UPLOADING:
                    return false;
                default:
                    return true;
            }
        }
        throw new IllegalStateException("Upload already started under a different taskId");
    }

    private CommandAck uploadCompleted(FastaDatabaseCommand command) throws Exception {
        persist(eventBuilder(command.getDatabaseId())
            .setUploadCompleted(command.getUploadCompleted())
            .build()
        );
        return ackAccept(command);
    }

    private boolean ignoreUploadFailed(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        final String uploadTaskId = data.getUploadTaskId();
        final FastaDatabaseUploadFailed uploadFailed = command.getUploadFailed();
        if (uploadTaskId.equals(uploadFailed.getTaskId())) {
            final FastaDatabaseStatus status = data.getStatus();
            switch (status) {
                case FASTA_PENDING:
                case FASTA_FAILED:
                    throw new IllegalStateException("Unexpected upload failed when current state is " + status.name());
                case FASTA_UPLOADING:
                    return false;
                default:
                    return true;
            }
        }
        throw new IllegalStateException("Upload already started under a different taskId");
    }

    private CommandAck uploadFailed(FastaDatabaseCommand command) throws Exception {
        persist(eventBuilder(command.getDatabaseId())
            .setUploadFailed(command.getUploadFailed())
            .build()
        );
        return ackAccept(command);
    }

    private boolean ignoreProcessingStarted(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        final String processTaskId = data.getProcessTaskId();
        final FastaDatabaseProcessingStarted processingStarted = command.getProcessingStarted();
        if (Strings.isNullOrEmpty(processTaskId)) {
            final FastaDatabaseStatus status = data.getStatus();
            switch (status) {
                case FASTA_PENDING:
                case FASTA_FAILED:
                case FASTA_UPLOADING:
                    throw new IllegalStateException("Unexpected processing started when current state is " + status.name());
                case FASTA_PROCESSING:
                    return false;
                default:
                    return true;
            }
        }
        if (processTaskId.equals(processingStarted.getTaskId())) {
            return true;
        }
        throw new IllegalStateException("Processing already started under a different taskId");
    }

    private CommandAck processingStarted(FastaDatabaseCommand command) throws Exception {
        persist(eventBuilder(command.getDatabaseId())
            .setProcessingStarted(command.getProcessingStarted())
            .build()
        );
        return ackAccept(command);
    }

    private boolean ignoreProcessingCompleted(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        final String processTaskId = data.getProcessTaskId();
        final FastaDatabaseProcessingCompleted processingCompleted = command.getProcessingCompleted();
        if (processTaskId.equals(processingCompleted.getTaskId())) {
            final FastaDatabaseStatus status = data.getStatus();
            switch (status) {
                case FASTA_PENDING:
                case FASTA_UPLOADING:
                case FASTA_FAILED:
                    throw new IllegalStateException("Unexpected process completed when current state is " + status.name());
                case FASTA_PROCESSING:
                    return false;
                default:
                    return true;
            }
        }
        throw new IllegalStateException("Processing already started under a different taskId");
    }

    private CommandAck processingCompleted(FastaDatabaseCommand command) throws Exception {
        persist(eventBuilder(command.getDatabaseId())
            .setProcessingCompleted(command.getProcessingCompleted())
            .build()
        );
        return ackAccept(command);
    }

    private boolean ignoreProcessingFailed(FastaDatabaseCommand command) {
        validateCommandAccess(command);
        final String processTaskId = data.getProcessTaskId();
        final FastaDatabaseProcessingFailed processingFailed = command.getProcessingFailed();
        if (processTaskId.equals(processingFailed.getTaskId())) {
            final FastaDatabaseStatus status = data.getStatus();
            switch (status) {
                case FASTA_PENDING:
                case FASTA_UPLOADING:
                case FASTA_FAILED:
                    throw new IllegalStateException("Unexpected processing failed when current state is " + status.name());
                case FASTA_PROCESSING:
                    return false;
                default:
                    return true;
            }
        }
        throw new IllegalStateException("Processing already started under a different taskId");
    }

    private CommandAck processingFailed(FastaDatabaseCommand command) throws Exception {
        persist(eventBuilder(command.getDatabaseId())
            .setProcessingFailed(command.getProcessingFailed())
            .build()
        );
        return ackAccept(command);
    }

    private void queryState(FastaDatabaseQuery query) {
        try {
            if (lastSequenceNr() == 0 || data.getStatus() == FastaDatabaseStatus.FASTA_DELETED) {
                reply(new NoSuchElementException(String.format(
                    "FastaDatabase %s does not exists",
                    data.getDatabaseId()
                )));
            } else {
                validateQueryAccess(query);
                reply(FastaDatabaseQueryResponse.newBuilder()
                    .setDatabaseId(data.getDatabaseId())
                    .setSequenceNr(lastSequenceNr())
                    .setState(data)
                    .build()
                );
            }
        } catch (Throwable e) {
            reply(e);
        }
    }
    private void queryFastaDatabaseTaxonomyCount(FastaDatabaseQuery query) {
        try {
            if (lastSequenceNr() == 0 || data.getStatus() == FastaDatabaseStatus.FASTA_DELETED) {
                reply(new NoSuchElementException(String.format(
                    "FastaDatabase %s does not exists",
                    data.getDatabaseId()
                )));
            } else {
                List<Integer> taxonomyIdsList = query.getQueryFastaDatabaseTaxonomyCount().getTaxonomyIdsList();
                long totalSequenceCount = 0L;
                Map<Integer, FastaDatabaseTaxonomyStats> taxonomyIdStatsMap = data.getStats().getTaxonomyIdStatsMap();
                for(Integer taxonomyId: taxonomyIdsList) {
                    if (taxonomyIdStatsMap.containsKey(taxonomyId)) {
                        totalSequenceCount += taxonomyIdStatsMap.get(taxonomyId).getSequenceCount();
                    }
                }
                validateQueryAccess(query);
                reply(FastaDatabaseQueryResponse.newBuilder()
                    .setDatabaseId(data.getDatabaseId())
                    .setSequenceNr(lastSequenceNr())
                    .setFastSequenceCount(totalSequenceCount)
                    .build()
                );
            }
        } catch (Throwable e) {
            reply(e);
        }
    }

    @Override
    protected FastaDatabaseState nextState(FastaDatabaseState data, FastaDatabaseEvent event) throws Exception {
        if (!data.hasCreated()) {
            data = data.toBuilder()
                .setCreated(event.getTimeStamp())
                .build();
        }
        switch (event.getEventCase()) {
            case CREATED: {
                final FastaDatabaseCreated created = event.getCreated();
                return data.toBuilder()
                    .setTaxonomy(created.getTaxonomyParser())
                    .build();
            }
            case DELETED: {
                return data.toBuilder()
                    .setStatus(FastaDatabaseStatus.FASTA_DELETED)
                    .build();
            }
            case OWNERSET: {
                final FastaDatabaseOwnerSet ownerSet = event.getOwnerSet();
                return data.toBuilder()
                    .setOwnerId(ownerSet.getOwnerId())
                    .build();
            }
            case INFORMATIONSET: {
                final FastaDatabaseInformationSet informationSet = event.getInformationSet();
                return data.toBuilder()
                    .setInformation(informationSet.getInformation())
                    .build();
            }
            case UPLOADSTARTED: {
                final FastaDatabaseUploadStarted uploadStarted = event.getUploadStarted();
                return data.toBuilder()
                    .setUploadTaskId(uploadStarted.getTaskId())
                    .setStatus(FastaDatabaseStatus.FASTA_UPLOADING)
                    .build();
            }
            case UPLOADCOMPLETED: {
                return data.toBuilder()
                    .setStatus(FastaDatabaseStatus.FASTA_PROCESSING)
                    .build();
            }
            case UPLOADFAILED: {
                return data.toBuilder()
                    .setStatus(FastaDatabaseStatus.FASTA_FAILED)
                    .build();
            }
            case PROCESSINGSTARTED: {
                final FastaDatabaseProcessingStarted processingStarted = event.getProcessingStarted();
                return data.toBuilder()
                    .setProcessTaskId(processingStarted.getTaskId())
                    .setStatus(FastaDatabaseStatus.FASTA_PROCESSING)
                    .build();
            }
            case PROCESSINGCOMPLETED: {
                final FastaDatabaseProcessingCompleted processingCompleted = event.getProcessingCompleted();
                return data.toBuilder()
                    .setStats(processingCompleted.getStats())
                    .setStatus(FastaDatabaseStatus.FASTA_DONE)
                    .build();
            }
            case PROCESSINGFAILED: {
                return data.toBuilder()
                    .setStatus(FastaDatabaseStatus.FASTA_FAILED)
                    .build();
            }
            default: {
                log.warning("{}: Unexpected event {}", persistenceId(), event.getEventCase());
                return data;
            }
        }
    }

    @Override
    protected String eventTag(FastaDatabaseEvent event) {
        return eventTag(event.getEventCase());
    }

    @Override
    protected String commandTag(FastaDatabaseCommand command) {
        return commandTag(command.getCommandCase());
    }

    @Override
    protected long deliveryId(FastaDatabaseCommand command) {
        return command.getDeliveryId();
    }

    private FastaDatabaseEvent.Builder eventBuilder(String actingAsUserId) {
        return FastaDatabaseEvent.newBuilder()
            .setDatabaseId(data.getDatabaseId())
            .setActingUserId(actingAsUserId)
            .setTimeStamp(CommonFactory.convert(Instant.now()));
    }

    private void validateQueryAccess(FastaDatabaseQuery query) {
        final String actingUserId = query.getActingUserId();
        final String ownerId = data.getOwnerId();
        if (!actingUserId.equals(ADMIN_USERID) && !actingUserId.equals(ownerId) && !ownerId.equals(ADMIN_USERID)) {
            throw new IllegalArgumentException("UserId " + actingUserId + " does not have query access to FastaDatabase " + data.getDatabaseId());
        }
    }

    private void validateCommandAccess(FastaDatabaseCommand command) {
        final String actingUserId = command.getActingUserId();
        final String ownerId = data.getOwnerId();
        if (!actingUserId.equals(ADMIN_USERID) && !actingUserId.equals(ownerId)) {
            throw new IllegalArgumentException("UserId " + actingUserId + " does not have command access to FastaDatabase " + data.getDatabaseId());
        }
    }

    private CompletionStage<Done> deleteFromRespository() {
        final UUID databaseId = UUID.fromString(data.getDatabaseId());
        final Set<Integer> taxonomyIds;
        if (data.hasStats()) {
            taxonomyIds = data.getStats().getTaxonomyIdStatsMap().keySet();
        } else {
            taxonomyIds = TaxonomyMapper.TOP_LEVEL_TAXONOMY_IDS;
        }
        return fastaSequenceRepository.deleteGroups(databaseId, taxonomyIds);
    }

    static class FastaDatabaseMessageExtractor implements ShardRegion.MessageExtractor {

        @Override
        public String entityId(Object message) {
            if (message instanceof FastaDatabaseCommand) {
                final FastaDatabaseCommand command = (FastaDatabaseCommand) message;
                return persistenceId(command.getDatabaseId());
            }
            if (message instanceof FastaDatabaseEvent) {
                final FastaDatabaseEvent event = (FastaDatabaseEvent) message;
                return persistenceId(event.getDatabaseId());
            }
            if (message instanceof FastaDatabaseQuery) {
                final FastaDatabaseQuery query = (FastaDatabaseQuery) message;
                return persistenceId(query.getDatabaseId());
            }
            return null;
        }

        @Override
        public Object entityMessage(Object message) {
            return message;
        }

        @Override
        public String shardId(Object message) {
            // We only have 1 shard.
            return "1";
        }
    }

}
