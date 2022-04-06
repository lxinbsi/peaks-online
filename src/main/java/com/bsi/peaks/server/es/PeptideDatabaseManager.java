package com.bsi.peaks.server.es;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.BackoffSupervisor;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.bsi.peaks.data.storage.application.PeptideDatabaseRepository;
import com.bsi.peaks.event.CommonFactory;
import com.bsi.peaks.event.modification.PeptideDatabaseCommand;
import com.bsi.peaks.event.modification.PeptideDatabaseEvent;
import com.bsi.peaks.event.modification.PeptideDatabaseEvent.DeletedDatabase;
import com.bsi.peaks.event.modification.PeptideDatabaseEvent.EventCase;
import com.bsi.peaks.event.modification.PeptideDatabaseEvent.LoadingStarted;
import com.bsi.peaks.event.modification.PeptideDatabaseQueryRequest;
import com.bsi.peaks.event.modification.PeptideDatabaseQueryResponse;
import com.bsi.peaks.event.modification.PeptideDatabaseReady;
import com.bsi.peaks.event.modification.PeptideDatabaseState;
import com.bsi.peaks.model.proteomics.fasta.FastaSequenceRemoveAminoAcidFromSequence;
import com.bsi.peaks.server.es.peptidedatabase.PeptideDatabaseManagerConfig;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PeptideDatabaseManager extends PeaksCommandPersistentActor<PeptideDatabaseState, PeptideDatabaseEvent, PeptideDatabaseCommand> {

    public static final String NAME = "peptidedatabasemanager";
    public static final String PERSISTENCE_ID = "PeptideDatabase";

    private final PeptideDatabaseManagerConfig config;
    private final PeptideDatabaseRepository peptideDatabaseRepository;

    public PeptideDatabaseManager(
        PeptideDatabaseManagerConfig config
    ) {
        super(PeptideDatabaseEvent.class);
        this.config = config;
        peptideDatabaseRepository = config.peptideDatabaseRepository();
    }

    public static void start(
        ActorSystem system,
        PeptideDatabaseManagerConfig config
    ) {
        system.actorOf(
            ClusterSingletonManager.props(
                BackoffSupervisor.props(
                    Props.create(PeptideDatabaseManager.class, config),
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

    @Override
    protected void onRecoveryComplete(List<PeptideDatabaseEvent> recoveryEvents) throws Exception {
        super.onRecoveryComplete(recoveryEvents);
        loadData();
        cleanupOldData(recoveryEvents);
    }

    /**
     * If changes are detected in fileFolder, load new data, then delete old data.
     */
    private void loadData() {
        final Path path = Paths.get(config.fileFolder());
        if (!Files.exists(path)) {
            log.warning("PeptideDatabase folder does not exist. Peptides will not be loaded.");
            return;
        }

        Map<Integer, List<File>> taxonomyIdFiles;
        try (Stream<Path> walk = Files.walk(path)) {
            Pattern filenameToTaxonomyId = config.filenameToTaxonomyId();
            taxonomyIdFiles = walk
                .filter(Files::isRegularFile)
                .map(Path::toFile)
                .map(file -> Pair.create(filenameToTaxonomyId.matcher(file.getName()), file))
                .filter(pair -> pair.first().find())
                .collect(Collectors.groupingBy(
                    pair -> Integer.parseInt(pair.first().group(1)),
                    Collectors.mapping(Pair::second, Collectors.toList())
                ));

            long hash = taxonomyIdFiles.values().stream()
                .flatMap(Collection::stream)
                .mapToLong(file -> file.getName().hashCode() + file.length())
                .reduce(0L, (a, b) -> a ^ b);

            final PeptideDatabaseReady originalReadyDatabase = data.getReadyDatabase();
            if (data.getReadyDatabase().getFileFolderHash() == hash) {
                log.info("No PeptideDatabase changes detected. Continueing to use PeptideDatabase {}", data.getReadyDatabase().getDatabaseId());
            } else {
                final ActorMaterializer materializer = ActorMaterializer.create(getContext());
                final UUID databaseId = UUID.randomUUID();
                log.info("Loading new PeptideDatabase {}", databaseId);
                Source.from(taxonomyIdFiles.entrySet())
                    .mapAsync(2, entry -> {
                        final int taxonomyId = entry.getKey();
                        sendSelfEvent(eventBuilder()
                            .setLoadingStarted(LoadingStarted.newBuilder()
                                .setDatabaseId(databaseId.toString())
                                .setTaxonomyId(taxonomyId)
                                .build()
                            )
                            .build()
                        );
                        return Source.from(entry.getValue())
                            .flatMapMerge(2, FileIO::fromFile)
                            .via(Framing.delimiter(ByteString.fromString("\n"), 8192, FramingTruncation.DISALLOW))
                            .map(ByteString::utf8String)
                            .map(FastaSequenceRemoveAminoAcidFromSequence.commonAminoAcidResidues()::remove)
                            .runWith(peptideDatabaseRepository.sink(databaseId, taxonomyId), materializer)
                            .whenComplete((done, throwable) -> {
                                if (throwable == null) {
                                    sendSelfEvent(eventBuilder()
                                        .setLoadingFinsihed(PeptideDatabaseEvent.LoadingFinished.newBuilder()
                                            .setDatabaseId(databaseId.toString())
                                            .setTaxonomyId(taxonomyId)
                                            .build()
                                        )
                                        .build()
                                    );
                                } else {
                                    log.error(throwable, "Error occured during loading of PeptideDatabse {} taxonomy {}. Proceeding to delete partially loaded taxnonomy.", databaseId, taxonomyId);
                                    delete(databaseId, taxonomyId);
                                }
                            });
                    })
                    .runWith(Sink.ignore(), materializer)
                    .whenComplete((done, throwable) -> {
                        if (throwable == null) {
                            log.info("Done loading new PeptideDatabase {}", databaseId);
                            sendSelfEvent(eventBuilder()
                                .setReady(PeptideDatabaseReady.newBuilder()
                                    .setDatabaseId(databaseId.toString())
                                    .addAllTaxonomyIds(taxonomyIdFiles.keySet())
                                    .setFileFolderHash(hash)
                                    .build()
                                )
                                .build()
                            );
                            if (originalReadyDatabase.getDatabaseId() != null) {
                                final UUID originalDatabaseId = UUID.fromString(originalReadyDatabase.getDatabaseId());
                                final List<Integer> originalTaxonomyIds = originalReadyDatabase.getTaxonomyIdsList();
                                deletedDatabase(originalDatabaseId, originalTaxonomyIds);
                            }
                        } else {
                            log.error("Error during loading of new PeptideDatabase {}", databaseId);
                        }
                    });
            }

        } catch (Exception e) {
            log.error(e, "Unable to read file folder names.");
        }
    }

    /**
     * Normally old data should be deleted when new data is uplodaed.
     * In rare case, this does not happen. We cleanup during restart by identifying missing deleted events.
     *
     * @param recoveryEvents
     */
    private void cleanupOldData(List<PeptideDatabaseEvent> recoveryEvents) {
        final String currentDatabaseId = data.getReadyDatabase().getDatabaseId();

        final ImmutableList<EventCase> eventsOFinterest = ImmutableList.of(
            EventCase.DELETEDDATABASE,
            EventCase.LOADINGSTARTED
        );
        final Map<EventCase, List<PeptideDatabaseEvent>> eventCases = recoveryEvents.stream()
            .filter(event -> eventsOFinterest.contains(event.getEventCase()))
            .collect(Collectors.groupingBy(
                PeptideDatabaseEvent::getEventCase,
                Collectors.toList()
            ));

        final Set<String> alreadyDeletedOrCurrentDatabaseIds = eventCases
            .getOrDefault(EventCase.DELETEDDATABASE, ImmutableList.of())
            .stream()
            .map(PeptideDatabaseEvent::getDeletedDatabase)
            .map(DeletedDatabase::getDatabaseId)
            .collect(Collectors.toSet());

        if (currentDatabaseId != null) {
            alreadyDeletedOrCurrentDatabaseIds.add(currentDatabaseId);
        }

        final Map<String, List<Integer>> toBeDeleted = eventCases.getOrDefault(EventCase.LOADINGSTARTED, ImmutableList.of())
            .stream()
            .map(PeptideDatabaseEvent::getLoadingStarted)
            .filter(loadingStarted -> !alreadyDeletedOrCurrentDatabaseIds.contains(loadingStarted.getDatabaseId()))
            .collect(Collectors.groupingBy(
                LoadingStarted::getDatabaseId,
                Collectors.mapping(LoadingStarted::getTaxonomyId, Collectors.toList())
            ));

        final ActorMaterializer materializer = ActorMaterializer.create(getContext());
        Source.from(toBeDeleted.keySet())
            .mapAsync(2, toBeDeletedDatabaseId -> {
                final UUID databaseId = UUID.fromString(toBeDeletedDatabaseId);
                final List<Integer> taxonomyIds = toBeDeleted.get(toBeDeletedDatabaseId);
                return deletedDatabase(databaseId, taxonomyIds);
            })
            .runWith(Sink.ignore(), materializer);
    }

    private CompletionStage<Done> deletedDatabase(UUID databaseId, List<Integer> taxonomyIds) {
        log.info("Deleting old PeptideDatabase {}", databaseId);
        return Source.from(taxonomyIds)
            .mapAsync(2, taxonomyId -> delete(databaseId, taxonomyId))
            .runWith(Sink.ignore(), ActorMaterializer.create(getContext()))
            .whenComplete((deleteDone, deleteThrowable) -> {
                if (deleteThrowable == null) {
                    sendSelfEvent(eventBuilder()
                        .setDeletedDatabase(DeletedDatabase.newBuilder()
                            .setDatabaseId(databaseId.toString())
                            .addAllTaxonomyIds(taxonomyIds)
                            .build())
                        .build()
                    );
                } else {
                    log.error(deleteThrowable, "Error during delete of old PeptideDatase {}", databaseId);
                }
            });
    }

    private CompletionStage<Done> delete(UUID databaseId, int taxonomyId) {
        return peptideDatabaseRepository.delete(databaseId, taxonomyId)
            .whenComplete((done, throwable) -> {
                if (throwable == null) {
                    sendSelfEvent(eventBuilder()
                        .setDeletedTaxonomy(PeptideDatabaseEvent.DeletedTaxonomy.newBuilder()
                            .setDatabaseId(databaseId.toString())
                            .setTaxonomyId(taxonomyId)
                            .build()
                        ).build()
                    );
                } else {
                    log.error(throwable, "Error during delete of PeptideDtabase {} taxonomy {}", databaseId, taxonomyId);
                }
            });
    }

    @Override
    protected ReceiveBuilder createReceive(ReceiveBuilder receiveBuilder) {
        return super.createReceive(receiveBuilder
            .match(PeptideDatabaseQueryRequest.class, PeptideDatabaseQueryRequest::hasDatabaseReady, this::handleDatabaseReadyQuery)
            .match(PeptideDatabaseEvent.class, this::persistOnlyFromSelf)
        );
    }

    @Override
    protected long deliveryId(PeptideDatabaseCommand command) {
        return command.getDeliveryId();
    }

    private void handleDatabaseReadyQuery(PeptideDatabaseQueryRequest query) {
        queryReply(query, b -> b.setDatabaseReady(data.getReadyDatabase()).build());
    }

    @Override
    protected PeptideDatabaseState defaultState() {
        return PeptideDatabaseState.getDefaultInstance();
    }

    @Override
    protected PeptideDatabaseState nextState(PeptideDatabaseState state, PeptideDatabaseEvent event) {
        switch (event.getEventCase()) {
            case READY: {
                final PeptideDatabaseReady ready = event.getReady();
                return state.toBuilder()
                    .setReadyDatabase(ready)
                    .build();
            }
            default:
                return state;
        }
    }

    @Override
    protected String commandTag(PeptideDatabaseCommand command) {
        // At time of writing, there were no commands.
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public String persistenceId() {
        return PERSISTENCE_ID;
    }

    private PeptideDatabaseEvent.Builder eventBuilder() {
        return PeptideDatabaseEvent.newBuilder()
            .setTimeStamp(CommonFactory.nowTimeStamp());
    }

    private void queryReply(
        PeptideDatabaseQueryRequest request,
        Function<PeptideDatabaseQueryResponse.Builder, PeptideDatabaseQueryResponse> build
    ) {
        try {
            reply(build.apply(PeptideDatabaseQueryResponse.newBuilder()
                .setQueryId(request.getQueryId())
            ));
        } catch (Exception e) {
            log.error(e, "Unable to respond to query {}", request.getRequestCase());
        }
    }
}
