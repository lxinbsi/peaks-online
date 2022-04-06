package com.bsi.peaks.server.service;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.storage.application.ApplicationStorage;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.FastaDatabaseCommandFactory;
import com.bsi.peaks.event.FastaDatabaseQueryFactory;
import com.bsi.peaks.event.fasta.FastaDatabaseCommand;
import com.bsi.peaks.event.fasta.FastaDatabaseInformation;
import com.bsi.peaks.event.fasta.FastaDatabaseQueryResponse;
import com.bsi.peaks.event.fasta.FastaDatabaseState;
import com.bsi.peaks.event.fasta.FastaDatabaseStats;
import com.bsi.peaks.event.fasta.FastaDatabaseStatus;
import com.bsi.peaks.event.fasta.FastaDatabaseTaxonomyStats;
import com.bsi.peaks.event.fasta.SetOwner;
import com.bsi.peaks.event.fasta.Taxonomy;
import com.bsi.peaks.model.proteomics.fasta.FastaFormat;
import com.bsi.peaks.model.proteomics.fasta.FastaSequenceDatabase;
import com.bsi.peaks.model.proteomics.fasta.FastaSequenceDatabaseBuilder;
import com.bsi.peaks.model.proteomics.fasta.ProteinHash;
import com.bsi.peaks.model.proteomics.fasta.fastasequencedatabase.FastaSequenceDatabaseAttributes;
import com.bsi.peaks.model.proteomics.fasta.fastasequencedatabase.FastaSequenceDatabaseStats;
import com.bsi.peaks.server.es.communication.FastaDatabaseCommunication;
import com.bsi.peaks.server.es.fasta.FastaDatabaseView;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static com.bsi.peaks.server.es.UserManager.ADMIN_USERID;

public class FastaDatabaseService {

    private static final Logger LOG = LoggerFactory.getLogger(FastaDatabaseService.class);

    public static final UUID ADMIN_USER_ID = UUID.fromString(ADMIN_USERID);
    private final ActorSystem system;
    private final FastaDatabaseCommunication fastaDatabaseCommunication;
    private final FastaDatabaseView fastaDatabaseView;
    private final UserService userService;
    private final int parallelism;

    @Inject
    public FastaDatabaseService(
        ActorSystem system,
        FastaDatabaseCommunication fastaDatabaseCommunication,
        FastaDatabaseView fastaDatabaseView,
        ApplicationStorageFactory storageFactory,
        UserService userService
    ) {
        this.system = system;
        this.fastaDatabaseCommunication = fastaDatabaseCommunication;
        this.fastaDatabaseView = fastaDatabaseView;
        this.parallelism = storageFactory.getParallelism();
        this.userService = userService;
    }

    public static FastaDatabaseStats convertToFastaDatabaseStats(FastaSequenceDatabaseStats stats) {
        final Map<Integer, Long> aminoAcidByTaxonomyId = stats.totalAminoAcidByTaxonomyId();
        final Map<Integer, Integer> sequenceByTaxonomyId = stats.totalSequenceByTaxonomyId();
        final Map<Integer, ProteinHash> proteinHashByTaxonomyId = stats.proteinHashByTaxonomyId();
        final FastaDatabaseStats.Builder builder = FastaDatabaseStats.newBuilder();
        sequenceByTaxonomyId.forEach((taxonomyId, sequenceCount) -> {
            FastaDatabaseTaxonomyStats.Builder statsBuilder = FastaDatabaseTaxonomyStats.newBuilder();
            statsBuilder.setSequenceCount(sequenceCount);
            statsBuilder.setAminoAcidCount(aminoAcidByTaxonomyId.get(taxonomyId));
            ProteinHash proteinHash = proteinHashByTaxonomyId.get(taxonomyId);
            if (proteinHash != null) {
                statsBuilder.setProteinsHash(ByteString.copyFrom(proteinHash.toBytes()));
            }
            builder.putTaxonomyIdStats(taxonomyId, statsBuilder.build());
        });
        return builder.build();
    }

    public boolean nameAlreadyExists(String name) {
        return fastaDatabaseView.lookupDatabaseId(name).isPresent();
    }

    public CompletionStage<FastaSequenceDatabase> findDatabaseById(UUID dbId) {
        final FastaDatabaseQueryFactory fastaDatabaseQueryFactory = FastaDatabaseQueryFactory.actingAsUserId(ADMIN_USERID);
        return fastaDatabaseCommunication.query(fastaDatabaseQueryFactory.queryState(dbId.toString()))
            .thenApply(FastaDatabaseQueryResponse::getState)
            .thenApply(this::convert);
    }

    public CompletionStage<Optional<FastaSequenceDatabase>> safeFindDatabaseById(UUID dbId) {
        final FastaDatabaseQueryFactory fastaDatabaseQueryFactory = FastaDatabaseQueryFactory.actingAsUserId(ADMIN_USERID);
        return fastaDatabaseCommunication.query(fastaDatabaseQueryFactory.queryState(dbId.toString())).handle((response, error) -> {
            if (error != null) {
                return Optional.empty();
            } else {
                return Optional.of(convert(response.getState()));
            }
        });
    }

    public CompletionStage<List<FastaSequenceDatabase>> databasesAccessibleByUser(UUID userId) {
        final FastaDatabaseQueryFactory fastaDatabaseQueryFactory = FastaDatabaseQueryFactory.actingAsUserId(userId.toString());
        final Iterable<UUID> databases;
        if (userId.equals(ADMIN_USER_ID)) {
            databases = fastaDatabaseView.databases();
        } else {
            databases = fastaDatabaseView.databases(ADMIN_USER_ID, userId);
        }
        return Source.from(databases)
            .map(databaseId -> fastaDatabaseQueryFactory.queryState(databaseId.toString()))
            .mapAsync(2, query -> fastaDatabaseCommunication.query(query)
                .thenApply(FastaDatabaseQueryResponse::getState)
                .handle((fastaDatabaseState, throwable) -> Optional.ofNullable(fastaDatabaseState))
            )
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(this::convert)
            .runWith(Sink.seq(), ActorMaterializer.create(system));
    }

    public void timeoutDatabases(int fastaDatabaseTimeout) {
        final FastaDatabaseQueryFactory fastaDatabaseQueryFactory = FastaDatabaseQueryFactory.actingAsUserId(ADMIN_USERID);
        final Instant now = Instant.now();
        Source.from(fastaDatabaseView.databases())
            .map(databaseId -> fastaDatabaseQueryFactory.queryState(databaseId.toString()))
            .mapAsync(parallelism, query -> fastaDatabaseCommunication.query(query)
                .thenApply(FastaDatabaseQueryResponse::getState)
                .handle((fastaDatabaseState, throwable) -> Optional.ofNullable(fastaDatabaseState))
            )
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(state -> {
                switch (state.getStatus()) {
                    case FASTA_DELETED:
                    case FASTA_DONE:
                    case FASTA_FAILED:
                        return false;
                    default:
                        return true;
                }
            })
            .filter(state -> {
                final Instant created = CommonFactory.convert(state.getCreated());
                return Duration.between(created, now).getSeconds() > fastaDatabaseTimeout;
            })
            .map(FastaDatabaseState::getDatabaseId)
            .mapAsync(2, databaseId -> {
                LOG.info("Processing timeout. Deleting FastDabatase " + databaseId);
                final FastaDatabaseCommand deleteCommand = FastaDatabaseCommandFactory.actingAsUserId(ADMIN_USERID).delete(databaseId);
                return fastaDatabaseCommunication.command(deleteCommand);
            })
            .runWith(Sink.ignore(), ActorMaterializer.create(system))
            .whenComplete((done, throwable) -> {
                if (throwable != null) {
                    LOG.error("Error during timeout of FastaDatabases", throwable);
                }
            });
    }

    private FastaSequenceDatabase convert(FastaDatabaseState state) {
        final FastaDatabaseInformation information = state.getInformation();
        final String creatorName = userService.getUsernameByUserId(UUID.fromString(state.getOwnerId()))
            .toCompletableFuture().join();
        final FastaSequenceDatabaseBuilder builder = new FastaSequenceDatabaseBuilder()
            .creatorId(UUID.fromString(state.getOwnerId()))
            .creatorName(creatorName)
            .id(UUID.fromString(state.getDatabaseId()))
            .format(convert(state.getTaxonomy()))
            .name(information.getName())
            .sourceFile(information.getSourceFile())
            .idPatternString(information.getIdPattern())
            .descriptionPatternString(information.getDescriptionPattern())
            .comments(information.getComment())
            .noDecoyGeneration(information.getNoDecoyGeneration())
            .state(convert(state.getStatus()));
        state.getStats().getTaxonomyIdStatsMap()
            .forEach((taxonmyId, fastaDatabaseTaxonomyStats) -> {
                builder.putTotalAminoAcidByTaxonomyId(taxonmyId, fastaDatabaseTaxonomyStats.getAminoAcidCount());
                builder.putTotalSequenceByTaxonomyId(taxonmyId, (int) fastaDatabaseTaxonomyStats.getSequenceCount());
                ByteString proteinsHash = fastaDatabaseTaxonomyStats.getProteinsHash();
                if (!proteinsHash.isEmpty()) {
                    builder.putProteinHashByTaxonomyId(taxonmyId, ProteinHash.fromBytes(proteinsHash.toByteArray()));
                }
            });
        return builder.build();
    }

    private FastaSequenceDatabase.State convert(FastaDatabaseStatus status) {
        switch (status) {
            case FASTA_PENDING:
            case FASTA_UPLOADING:
            case FASTA_PROCESSING:
                return FastaSequenceDatabase.State.PROCESSING;
            case FASTA_FAILED:
                return FastaSequenceDatabase.State.ERROR;
            case FASTA_DONE:
                return FastaSequenceDatabase.State.READY;
            case FASTA_DELETED:
                return FastaSequenceDatabase.State.DELETED;
            default:
                return FastaSequenceDatabase.State.ERROR;
        }
    }

    private FastaFormat convert(Taxonomy taxonomy) {
        switch (taxonomy) {
            case NCBI_GI:
                return FastaFormat.NCBI_GI;
            case NCBI_Accession:
                return FastaFormat.NCBI_Accession;
            case UniProtKb:
                return FastaFormat.UniProtKb;
            case Ensembl:
                return FastaFormat.Ensembl;
            default:
                return FastaFormat.None;
        }
    }

    private Taxonomy convert(FastaFormat taxonomy) {
        switch (taxonomy) {
            case NCBI_GI:
                return Taxonomy.NCBI_GI;
            case NCBI_Accession:
                return Taxonomy.NCBI_Accession;
            case UniProtKb:
                return Taxonomy.UniProtKb;
            case Ensembl:
                return Taxonomy.Ensembl;
            default:
                return Taxonomy.None;
        }
    }

    public CompletionStage<Done> create(UUID userId, String databaseId, FastaSequenceDatabaseAttributes database) {
        final FastaDatabaseCommand createCommand = FastaDatabaseCommandFactory.actingAsUserId(userId.toString())
            .create(
                convert(database.format()),
                convertToInformation(database)
            )
            .toBuilder()
            .setDatabaseId(databaseId)
            .build();
        return fastaDatabaseCommunication
            .command(createCommand);
    }

    public FastaDatabaseInformation convertToInformation(FastaSequenceDatabaseAttributes databaseAttributes) {
        return FastaDatabaseInformation.newBuilder()
            .setName(databaseAttributes.name())
            .setComment(databaseAttributes.comments())
            .setIdPattern(databaseAttributes.idPatternString())
            .setDescriptionPattern(databaseAttributes.descriptionPatternString())
            .setSourceFile(databaseAttributes.sourceFile())
            .setNoDecoyGeneration(databaseAttributes.noDecoyGeneration())
            .build();
    }

    public CompletionStage<FastaSequenceDatabase> setOwner(UUID userId, String databaseId, SetOwner setOwner) {
        final FastaDatabaseCommandFactory commandFactory = FastaDatabaseCommandFactory.actingAsUserId(userId.toString());
        final FastaDatabaseQueryFactory queryFactory = FastaDatabaseQueryFactory.actingAsUserId(userId.toString());
        final FastaDatabaseCommand setOwnerCommand = commandFactory.setOwner(databaseId, setOwner);
        return fastaDatabaseCommunication.command(setOwnerCommand)
            .thenCompose(done -> fastaDatabaseCommunication.query(queryFactory.queryState(databaseId)))
            .thenApply(FastaDatabaseQueryResponse::getState)
            .thenApply(this::convert);
    }

    public CompletionStage<FastaSequenceDatabase> setInformation(UUID userId, String databaseId, FastaSequenceDatabaseAttributes databaseAttributes) {
        final FastaDatabaseCommandFactory commandFactory = FastaDatabaseCommandFactory.actingAsUserId(userId.toString());
        final FastaDatabaseQueryFactory queryFactory = FastaDatabaseQueryFactory.actingAsUserId(userId.toString());
        final FastaDatabaseCommand setInformationCommand = commandFactory.setInformation(databaseId, convertToInformation(databaseAttributes));
        return fastaDatabaseCommunication.command(setInformationCommand)
            .thenCompose(done -> fastaDatabaseCommunication.query(queryFactory.queryState(databaseId)))
            .thenApply(FastaDatabaseQueryResponse::getState)
            .thenApply(this::convert);
    }

    public CompletionStage<Done> delete(UUID userId, String databaseId) {
        final FastaDatabaseCommandFactory commandFactory = FastaDatabaseCommandFactory.actingAsUserId(userId.toString());
        return fastaDatabaseCommunication.command(commandFactory.delete(databaseId));
    }

    public CompletionStage<Pair<String, Iterable<Integer>>> queryNameTaxonomyIds(UUID userId, UUID databaseId) {
        final FastaDatabaseQueryFactory queryFactory = FastaDatabaseQueryFactory.actingAsUserId(userId.toString());
        return fastaDatabaseCommunication.query(queryFactory.queryState(databaseId.toString()))
            .thenApply(FastaDatabaseQueryResponse::getState)
            .thenApply(state -> Pair.create(state.getInformation().getName(), state.getStats().getTaxonomyIdStatsMap().keySet()));
    }

    public CompletionStage<Double> queryFastaDatabaseTaxonomyCount(UUID userId, UUID databaseId, List<Integer> taxonomyIds) {
        final FastaDatabaseQueryFactory queryFactory = FastaDatabaseQueryFactory.actingAsUserId(userId.toString());
        return fastaDatabaseCommunication.query(queryFactory.queryFastaDatabaseTaxonomyCount(databaseId.toString(), taxonomyIds))
            .thenApply(FastaDatabaseQueryResponse::getFastSequenceCount);
    }

}
