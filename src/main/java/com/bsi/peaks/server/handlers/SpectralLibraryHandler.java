package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.SpectralLibraryRepository;
import com.bsi.peaks.io.writer.csv.SpectralLibrary.SpectralLibraryExporterTsv;
import com.bsi.peaks.io.writer.mgf.Mgf;
import com.bsi.peaks.model.dto.CreateSpectralLibrary;
import com.bsi.peaks.model.dto.SpectralLibraryListItem;
import com.bsi.peaks.server.handlers.helper.VertxChunkedOutputStream;
import com.bsi.peaks.server.service.SpectralLibraryService;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.bsi.peaks.server.handlers.ExportHandler.closeOutputStream;

public class SpectralLibraryHandler extends ProtoHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SpectralLibraryHandler.class);
    private final SpectralLibraryService service;
    private final SpectralLibraryRepository spectralLibraryRepository;

    @Inject
    public SpectralLibraryHandler(
        Vertx vertx,
        ActorSystem system,
        SpectralLibraryService service,
        ApplicationStorageFactory storageFactory
    ) {
        super(vertx, system);
        this.service = service;
        spectralLibraryRepository = storageFactory.getSystemStorage().getSpectralLibraryRepository();
    }


    @Override
    public Router createSubRouter() {
        BodyHandler bodyHandler = BodyHandler.create();

        Router subRouter = Router.router(vertx);
        subRouter.get("/").handler(this::listSL);
        subRouter.post("/").handler(bodyHandler).handler(this::createSL);
        subRouter.post("/import").handler(bodyHandler).handler(this::importSL);
        subRouter.get("/:spectrumLibraryId").handler(this::getSL);
        subRouter.delete("/:spectrumLibraryId").handler(this::deleteSL);
        subRouter.post("/:spectrumLibraryId/setOwner").handler(bodyHandler).handler(this::setOwnerSL);
        subRouter.post("/:spectrumLibraryId/setName").handler(bodyHandler).handler(this::setNameSL);
        subRouter.get("/:spectrumLibraryId/mgf").handler(this::mgf);
        subRouter.get("/:spectrumLibraryId/export").handler(this::export);

        return subRouter;
    }

    private void listSL(RoutingContext context) {
        auth(context, user -> {
            HttpServerResponse response = context.response();
            serializationProcedure(
                context.request(),
                response,
                service.list(user.userId()),
                "spectrumLibraries"
            ).whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    response.end();
                }
            });
        });
    }

    private void getSL(RoutingContext context) {
        auth(context, user -> getId(context, "spectrumLibraryId", spectrumLibraryId -> {
            HttpServerResponse response = context.response();
            serializationProcedure(
                context.request(),
                response,
                Source.fromCompletionStage(service.get(user.userId(), spectrumLibraryId)),
                "spectrumLibraries"
            ).whenComplete((done, error) -> {
                if (error != null) {
                    context.fail(error);
                } else {
                    response.end();
                }
            });
        }));
    }

    private void deleteSL(RoutingContext context) {
        auth(context, user -> getId(context, "spectrumLibraryId", spectrumLibraryId -> {
            service.delete(user.userId(), spectrumLibraryId)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        }));
    }

    private void setOwnerSL(RoutingContext context) {
        auth(context, user -> getId(context, "spectrumLibraryId", spectrumLibraryId -> {
            final UUID newOwnerId;
            try {
                newOwnerId = UUID.fromString(context.getBodyAsJson().getString("ownerId"));
            } catch (Exception e) {
                badRequest(context, "Unable to parse JSON body for ownerId.");
                return;
            }
            service.setOwner(user.userId(), spectrumLibraryId, newOwnerId)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        }));
    }

    private void setNameSL(RoutingContext context) {
        auth(context, user -> getId(context, "spectrumLibraryId", spectrumLibraryId -> {
            final String name;
            try {
                name = context.getBodyAsJson().getString("slName");
            } catch (Exception e) {
                badRequest(context, "Unable to parse JSON body for name.");
                return;
            }
            service.setName(user.userId(), spectrumLibraryId, name)
                .whenComplete((done, error) -> {
                    if (error != null) {
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
        }));
    }

    private void export(RoutingContext context) {
        auth(context, user -> getId(context, "spectrumLibraryId", id -> service.get(user.userId(), id)
            .thenCompose(lib -> service.meta(user.userId(), id).thenApply(meta -> Pair.create(lib, meta)))
            .whenComplete((pair, libraryError) -> {
                if (libraryError != null) {
                    LOG.error("Unable to process export spectral library", libraryError);
                    context.fail(libraryError);
                    return;
                }

                SpectralLibraryListItem spectralLibrary = pair.first();
                String meta = pair.second();

                String label = MessageFormat.format("SpectralLibrary-{0}", spectralLibrary.getSlName());
                HttpServerResponse response = context.response();
                response.putHeader("Content-Disposition", "attachment; filename=\"" + label + ".zip\"");
                response.putHeader("Content-Type", "zip");
                response.setChunked(true);
                ZipOutputStream output = new ZipOutputStream(new VertxChunkedOutputStream(response));

                try {
                    output.putNextEntry(new ZipEntry(label + ".info"));
                    output.write(new GsonBuilder().setPrettyPrinting().create().toJson(new JsonParser().parse(meta)).getBytes());
                    output.closeEntry();

                    output.putNextEntry(new ZipEntry(label + ".tsv"));
                    SpectralLibraryExporterTsv spectralLibraryCsv = SpectralLibraryExporterTsv.create(spectralLibrary.getType());
                    output.write(spectralLibraryCsv.header().getBytes());
                    output.write('\n');
                    spectralLibraryRepository.sourceOrderedOnlyTarget(id)
                        .map(proto -> spectralLibraryCsv.export(proto).getBytes())
                        .async()
                        .runWith(Sink.foreach(bytes -> {
                                output.write(bytes);
                                output.write('\n');
                            }),
                            ActorMaterializer.create(system)
                        )
                        .whenComplete((done, error) -> {
                            closeOutputStream(output);
                            if (error != null) {
                                LOG.error("Unable to process export " + label, error);
                                context.fail(error);
                            } else {
                                response.end();
                            }
                        });
                } catch (IOException e) {
                    closeOutputStream(output);
                    LOG.error("Unable to export spectral library zip file", e);
                    context.fail(e);
                    return;
                }

            })
        ));
    }

    private void mgf(RoutingContext context) {
        auth(context, user -> getId(context, "spectrumLibraryId", id -> service.get(user.userId(), id)
            .whenComplete((spectralLibrary, libraryError) -> {
                if (libraryError != null) {
                    LOG.error("Unable to process export spectral library", libraryError);
                    context.fail(libraryError);
                    return;
                }
                String label = MessageFormat.format("SpectralLibrary-{0}.mgf", spectralLibrary.getSlName());
                HttpServerResponse response = context.response();
                response.putHeader("Content-Disposition", "attachment; filename=\"" + label + "\"");
                response.putHeader("Content-Type", "text/mgf");
                response.setChunked(true);

                LOG.info("Export " + label);
                final OutputStream output = new BufferedOutputStream(new VertxChunkedOutputStream(response));
                spectralLibraryRepository.sourceOrdered(id)
                    .map(proto -> Mgf.toLocalString(spectralLibrary.getSlName(), proto).getBytes())
                    .async()
                    .runWith(Sink.foreach(output::write), ActorMaterializer.create(system))
                    .whenComplete((done, error) -> {
                        closeOutputStream(output);
                        if (error != null) {
                            LOG.error("Unable to process export " + label, error);
                            context.fail(error);
                        } else {
                            response.end();
                        }
                    });
            })
        ));
    }

    private void createSL(RoutingContext context) {
        auth(context, user -> {
            CreateSpectralLibrary createSL = getBody(context, CreateSpectralLibrary.newBuilder());
            service.create(user.userId(), createSL).whenComplete(((spectralLibraryListItem, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    try {
                        ok(context, spectralLibraryListItem);
                    } catch (Exception e) {
                        handleError(context, e);
                    }
                }
            }));

        });
    }

    private void importSL(RoutingContext context) {
        auth(context, user -> {
            try {
                UUID actingAsUserId = user.userId();
                String name = context.request().getFormAttribute("name");
                FileUpload metaInfoUpload = null;
                FileUpload spectralLibraryUpload = null;
                for (FileUpload fileUpload : context.fileUploads()) {
                    switch (fileUpload.name()) {
                        case "metaInfo":
                            metaInfoUpload = fileUpload;
                            continue;
                        case "spectralLibrary":
                            spectralLibraryUpload = fileUpload;
                            continue;
                    }
                }
                if (name == null) {
                    throw new HandlerException(400, "name is required.");
                }
                if (spectralLibraryUpload == null) {
                    throw new HandlerException(400, "spectralLibrary is required.");
                }
                String spectralLibraryFilename = spectralLibraryUpload.uploadedFileName();
                Optional<Path> path;
                if (metaInfoUpload != null) {
                    String metaInforFilename = metaInfoUpload.uploadedFileName();
                    path = Optional.of(Paths.get(metaInforFilename));
                } else {
                    path = Optional.empty();
                }
                Path spectralLibraryPath = Paths.get(spectralLibraryFilename);
                service.createLibraryFromImport(actingAsUserId, name, spectralLibraryPath, path)
                    .whenComplete((slId, throwable) -> {
                        if (throwable == null) {
                            String slIdString = slId.toString();
                            ok(context, slIdString, context.normalisedPath() + "/" + slIdString);
                        } else {
                            handleError(context, throwable);
                        }
                    })
                    .thenCompose(slId -> service
                        .importLibraryCsv(actingAsUserId, slId, spectralLibraryPath)
                        .whenComplete((done, throwable) -> {
                            if (throwable != null) {
                                LOG.error("Error importing SL " + slId, throwable);
                            }
                        })
                    )
                    .whenComplete((done, throwable) -> cleanUploads(context));
            } catch (Throwable e) {
                handleError(context, e);
                cleanUploads(context);
            }
        });
    }
}
