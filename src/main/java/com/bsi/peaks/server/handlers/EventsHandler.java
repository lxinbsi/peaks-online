package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.javadsl.CurrentEventsByPersistenceIdQuery;
import akka.persistence.query.journal.leveldb.javadsl.LeveldbReadJournal;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.Launcher;
import com.bsi.peaks.server.es.PeptideDatabaseManager;
import com.bsi.peaks.server.es.ProjectAggregateEntity;
import com.bsi.peaks.server.es.WorkManager;
import com.google.inject.Inject;
import com.google.protobuf.GeneratedMessageV3;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.BiConsumer;

import static java.util.Collections.singletonList;

public class EventsHandler extends ApiHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ExportHandler.class);

    private final ActorSystem system;
    private final CurrentEventsByPersistenceIdQuery journal;
    private final Materializer materializer;

    @Inject
    public EventsHandler(
        final Vertx vertx,
        final ActorSystem system
    ) {
        super(vertx);
        this.system = system;
        if (Launcher.useCassandraJournal) {
            this.journal = PersistenceQuery.get(system)
                .getReadJournalFor(CassandraReadJournal.class, CassandraReadJournal.Identifier());
        } else {
            this.journal = PersistenceQuery.get(system)
                .getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());
        }
        materializer = ActorMaterializer.create(system);
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);

        subRouter.get("/project/:pid").handler(this::projectHandler);
        subRouter.get("/workmanager").handler(this::workManagerHandler);
        subRouter.get("/peptidedatabase").handler(this::peptideDatabaseHandler);
        subRouter.get("/:persistenceId").handler(this::peristenceHandler);

        return subRouter;
    }

    private void peptideDatabaseHandler(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            dumpEvents(context, PeptideDatabaseManager.PERSISTENCE_ID);
        });
    }

    private void workManagerHandler(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            dumpEvents(context, WorkManager.PERSISTENCE_ID);
        });
    }

    private void projectHandler(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final UUID projectId;
            try {
                projectId = UUID.fromString(context.request().getParam("pid"));
            } catch (Exception e) {
                LOG.warn("Unable to parse ids", e);
                badRequest(context, "Unable to parse ids");
                return;
            }
            dumpEvents(context, ProjectAggregateEntity.persistenceId(projectId));
        });
    }

    private void peristenceHandler(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            dumpEvents(context, context.request().getParam("persistenceId"));
        });
    }

    private void dumpEvents(RoutingContext context, String persistenceId) {
        final HttpServerResponse response = context.response();
        response.exceptionHandler(e -> {
            LOG.error("Response exception", e);
            response.close();
        });
        journal.currentEventsByPersistenceId(persistenceId,0, Long.MAX_VALUE)
            .map(EventEnvelope::event)
            .map(GeneratedMessageV3.class::cast)
            .runWith(ProtoHandler.json(persistenceId, response), materializer)
            .whenComplete((BiConsumer<Object,Throwable>) (done, error) -> {
                if (error != null) {
                    if (error instanceof NoSuchElementException) {
                        notFound(context);
                    } else {
                        context.fail(error);
                    }
                } else {
                    response.end();
                }
            });
    }

}
