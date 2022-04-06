package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.FastaSequenceRepository;
import com.bsi.peaks.event.FastaDatabaseCommandFactory;
import com.bsi.peaks.event.fasta.FastaDatabaseStats;
import com.bsi.peaks.event.fasta.SetOwner;
import com.bsi.peaks.io.reader.fasta.FastaSequenceFileReader;
import com.bsi.peaks.io.reader.taxonomy.TaxonomyClassifierFactory;
import com.bsi.peaks.model.proteomics.fasta.FastaFormat;
import com.bsi.peaks.model.proteomics.fasta.fastasequencedatabase.FastaSequenceDatabaseAttributes;
import com.bsi.peaks.model.proteomics.fasta.fastasequencedatabase.FastaSequenceDatabaseAttributesBuilder;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.es.communication.FastaDatabaseCommunication;
import com.bsi.peaks.server.handlers.helper.ExportHelpers;
import com.bsi.peaks.server.service.FastaDatabaseService;
import com.bsi.peaks.service.streams.DecoyGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.bsi.peaks.server.handlers.ProtoHandler.responseSink;

/**
 * @author Tom Andersen
 * Created by tandersen on 2017-04-24.
 */
public class DatabaseHandler extends ApiHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseHandler.class);
    private final Materializer materializer;
    private final TaxonomyClassifierFactory taxonomyClassifierFactory;
    private final FastaDatabaseService fastaDatabaseService;
    private final FastaDatabaseCommunication fastaDatabaseCommunication;
    private final FastaSequenceRepository fastaSequenceRepository;

    @Inject
    public DatabaseHandler(
        final Vertx vertx,
        final SystemConfig config,
        final ActorSystem system,
        final ApplicationStorageFactory storageFactory,
        final TaxonomyClassifierFactory taxonomyClassifierFactory,
        final FastaDatabaseService fastaDatabaseService,
        FastaDatabaseCommunication fastaDatabaseCommunication
    ) {
        super(vertx);
        this.materializer = ActorMaterializer.create(system);
        this.taxonomyClassifierFactory = taxonomyClassifierFactory;
        this.fastaDatabaseService = fastaDatabaseService;
        this.fastaDatabaseCommunication = fastaDatabaseCommunication;
        this.fastaSequenceRepository = storageFactory.getSystemStorage().getFastaSequenceRepository();

    }

    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.post("/").handler(BodyHandler.create()).handler(this::create);
        subRouter.route("/:id").handler(BodyHandler.create());
        subRouter.post("/:id/setOwner").handler(BodyHandler.create()).handler(this::setOwner);
        subRouter.put("/:id").consumes("*/json").handler(this::update);
        subRouter.delete("/:id").handler(this::delete);
        subRouter.get("/").handler(this::readAll);
        subRouter.get("/:id/download").handler(this::download);
        subRouter.get("/:id/headers").handler(this::getHeaders);
        return subRouter;
    }

    protected void readAll(RoutingContext context) {
        auth(context, user -> {
            CompletionStage<Pair<Integer, List>> stage = fastaDatabaseService.databasesAccessibleByUser(user.userId())
                .thenApply(l -> Pair.create(l.size(), l));
            sendPage(context, stage, 0, 0, "");
        });
    }

    public void create(RoutingContext context) {
        auth(context, user -> {
            final FastaDatabaseCommandFactory commandFactory = FastaDatabaseCommandFactory.actingAsUserId(user.userId().toString());
            final FastaSequenceDatabaseAttributes attributes;
            try {
                attributes = getFastaSequenceDatabaseAttributes(context);
            } catch (PatternSyntaxException e) {
                badRequest(context, "Invalid pattern - " + e.getDescription());
                cleanUploads(context);
                return;
            } catch (Exception e) {
                handleError(context, e);
                cleanUploads(context);
                return;
            }

            if (fastaDatabaseService.nameAlreadyExists(attributes.name())) {
                LOG.error("Database name is already in use " + attributes.name());
                badRequest(context, "Database name is already in use");
                cleanUploads(context);
                return;
            }

            final UUID fastaDatabaseId = UUID.randomUUID();
            final String databaseId = fastaDatabaseId.toString();
            final String uploadTaskId = UUID.randomUUID().toString();
            final String processingTaskId = UUID.randomUUID().toString();
            final String uploadedFileName = getUploadedFile(context).uploadedFileName();
            fastaDatabaseService.create(user.userId(), databaseId, attributes)
                .thenCompose(done -> fastaDatabaseCommunication.command(commandFactory.uploadStarted(databaseId, uploadTaskId)))
                .thenCompose(done -> fastaDatabaseCommunication.command(commandFactory.uploadCompleted(databaseId, uploadTaskId, uploadedFileName)))
                .whenComplete((done, throwable) -> {
                    if (throwable != null) {
                        handleError(context, new RuntimeException("Unable to create database", throwable));
                    } else {
                        // ok the client no matter what
                        ok(context, fastaDatabaseId.toString(), context.normalisedPath() + "/" + fastaDatabaseId);
                    }
                })
                .thenCompose(done -> {
                    final FastaSequenceFileReader fileReader = new FastaSequenceFileReader(Paths.get(uploadedFileName));
                    return fastaDatabaseCommunication.command(commandFactory.processingStarted(databaseId, processingTaskId))
                        .thenApply(ignore -> taxonomyClassifierFactory.create(attributes.format()).get())
                        .thenCompose(taxonomyClassifier -> fileReader.asAkkaSource()
                            .via(attributes.noDecoyGeneration() ? DecoyGenerator.appendBlankDecoy() : DecoyGenerator.appendDecoy())
                            .runWith(fastaSequenceRepository.createSinkWithMappingFunction(fastaDatabaseId, taxonomyClassifier::classifyHeader), materializer)
                        ).whenComplete((stats, processError) -> {
                            if (processError != null) {
                                LOG.error("Processing uploaded fasta database failed", processError);
                                fastaDatabaseCommunication.command(commandFactory.processingFailed(databaseId, processingTaskId, processError.toString()));
                            } else {
                                LOG.info("Fasta loading complete " + databaseId);
                                final FastaDatabaseStats fastaDatabaseStats = fastaDatabaseService.convertToFastaDatabaseStats(stats);
                                fastaDatabaseCommunication.command(commandFactory.processingCompleted(databaseId, processingTaskId, fastaDatabaseStats));
                            }
                        });
                })
                .whenComplete((fastaSequenceDatabaseStats, throwable) -> cleanUploads(context));
        });
    }

    private void setOwner(RoutingContext context) {
        auth(context, user -> {
            final String databaseId = context.request().getParam("id");
            if (Strings.isNullOrEmpty(databaseId)) {
                badRequest(context, "Invalid parameter id");
                return;
            }
            SetOwner setOwner = getBody(context, SetOwner.newBuilder());
            fastaDatabaseService.setOwner(user.userId(), databaseId, setOwner)
                .whenComplete((updated, error) -> {
                    if (error != null) {
                        LOG.error("Unable to setOwner on " + databaseId + " FastaSequenceDatabase", error);
                        context.fail(error);
                    } else {
                        ok(context, updated);
                    }
                });
        });
    }

    protected void update(RoutingContext context) {
        auth(context, user -> {
            final String databaseId = context.request().getParam("id");
            if (Strings.isNullOrEmpty(databaseId)) {
                badRequest(context, "Invalid parameter id");
                return;
            }
            final FastaSequenceDatabaseAttributes data;
            try {
                data = OM.readValue(context.getBodyAsString(), FastaSequenceDatabaseAttributes.class);
            } catch (JsonMappingException e) {
                if (e.getCause() instanceof PatternSyntaxException) {
                    PatternSyntaxException patternSyntaxException = (PatternSyntaxException) e.getCause();
                    badRequest(context, "Invalid pattern - " + patternSyntaxException.getDescription());
                    return;
                } else {
                    badRequest(context, "Unable to parse request " + context.getBodyAsString());
                    return;
                }
            } catch (IOException e) {
                badRequest(context, "Unable to parse request " + context.getBodyAsString());
                return;
            }
            fastaDatabaseService.setInformation(user.userId(), databaseId, data)
                .whenComplete((updated, error) -> {
                    if (error != null) {
                        LOG.error("Unable to update " + databaseId + " FastaSequenceDatabase in database", error);
                        handleError(context, error);
                    } else {
                        ok(context, updated);
                    }
                });
        });
    }

    protected void delete(RoutingContext context) {
        auth(context, user -> {
            final String databaseId = context.request().getParam("id");
            if (Strings.isNullOrEmpty(databaseId)) {
                badRequest(context, "Invalid parameter id");
                return;
            }
            fastaDatabaseService.delete(user.userId(), databaseId)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        LOG.error("Unable to delete FastaDatabase " + databaseId, error);
                        handleError(context, error);
                    } else {
                        ok(context);
                    }
                });
        });
    }

    private FastaSequenceDatabaseAttributes getFastaSequenceDatabaseAttributes(RoutingContext context) {
        return new FastaSequenceDatabaseAttributesBuilder()
            .creatorId(UUID.fromString(context.request().getFormAttribute("creatorId")))
            .name(context.request().getFormAttribute("name"))
            .comments(context.request().getFormAttribute("comments"))
            .format(FastaFormat.valueOf(context.request().getFormAttribute("format")))
            .idPatternString(context.request().getFormAttribute("idPatternString"))
            .descriptionPatternString(context.request().getFormAttribute("descriptionPatternString"))
            .noDecoyGeneration(Boolean.parseBoolean(context.request().getFormAttribute("noDecoyGeneration")))
            .sourceFile(getUploadedFile(context).fileName())
            .build();
    }

    private FileUpload getUploadedFile(final RoutingContext context) {
        try {
            return Iterators.getOnlyElement(context.fileUploads().iterator());
        } catch (Exception e) {
            throw new HandlerException(400, "Unable to access uploaded file");
        }
    }

    private void getHeaders(RoutingContext context) {
        auth(context, user -> {
            final UUID databaseId;
            final int limit;
            final List<Integer> taxIds;
            try {
                databaseId = UUID.fromString(context.request().getParam("id"));
                limit = Integer.parseInt(context.request().getParam("top"));
                if (context.request().getParam("taxIds") == null) {
                    taxIds = new ArrayList<>();
                    fastaDatabaseService.queryNameTaxonomyIds(user.userId(), databaseId)
                        .toCompletableFuture().join().second().forEach(taxIds::add);
                } else {
                    taxIds = Arrays.stream(context.request().getParam("taxIds").split(","))
                        .map(Integer::parseInt).collect(Collectors.toList());
                }
            } catch (Exception e) {
                badRequest(context, "Invalid request, please verify parameters");
                return;
            }
            fastaSequenceRepository.headerSource(databaseId, taxIds, limit)
                .runWith(Sink.seq(), materializer)
                .whenComplete((headers, error) -> {
                    if (error != null) {
                        LOG.error("Unable to fetch headers for database " + databaseId, error);
                        handleError(context, error);
                    } else {
                        ok(context, headers);
                    }
                });
        });
    }

    private void download(RoutingContext context) {
        auth(context, user -> {
            final UUID databaseId;
            final boolean includeDecoy;
            try {
                databaseId = UUID.fromString(context.request().getParam("id"));
                includeDecoy = getBoolean(context, "includeDecoy");
            } catch (IllegalArgumentException e) {
                badRequest(context, "Invalid request. Please verify the parameter id and all query parameters.");
                return;
            }
            final HttpServerResponse response = context.response();

            fastaDatabaseService.queryNameTaxonomyIds(user.userId(), databaseId)
                .thenCompose(p -> {
                    String databaseName = p.first();
                    Iterable<Integer> taxIds = p.second();
                    return fastaSequenceRepository.sourceUnorderedNoCache(databaseId, ImmutableList.copyOf(taxIds))
                        .async()
                        .map(fasta -> Pair.create(fasta.header().getBytes(), fasta.sequence().getBytes()))
                        .buffer(256, OverflowStrategy.backpressure())
                        .runWith(responseSink(
                            response,
                            r -> {
                                r.putHeader("Content-Disposition", "attachment; filename=\"" + databaseName + ".fasta\"");
                                r.putHeader("Content-Type", "text/plain");
                            },
                            (output, pair) -> {
                                final byte[] header = pair.first();
                                final byte[] sequence = pair.second();

                                output.write(header);
                                output.write('\n');

                                final int sequenceLength;
                                if (includeDecoy) {
                                    sequenceLength = sequence.length;
                                } else {
                                    /*
                                    * We assume all fasta sequences are of the form ____X____ where
                                    * (1) the sequence before the first X is non decoy
                                    * (2) the sequence after the X is decoy
                                    * (3) the length of the sequence before and after the X are equal
                                    * */
                                    sequenceLength = sequence.length / 2;
                                }
                                ExportHelpers.writeFastaSequence(output, sequence, sequenceLength);
                            }
                        ), materializer);
                })
                .whenComplete((p, error) -> {
                    if (error != null) {
                        LOG.error("Unable to download FastaDatabase " + databaseId, error);
                        handleError(context, error);
                    } else {
                        response.end();
                    }
                });
        });
    }
}
