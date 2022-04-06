package com.bsi.peaks.server.es;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.pf.PFBuilder;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.Offset;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.javadsl.EventsByTagQuery;
import akka.persistence.query.journal.leveldb.javadsl.LeveldbReadJournal;
import akka.stream.javadsl.Source;
import com.bsi.peaks.server.Launcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;

public final class View {

    private static final Logger LOG = LoggerFactory.getLogger(View.class);
    static int sleepMillis = 5000;

    public static final Source<EventEnvelope, NotUsed> events(ActorSystem system, String... eventTags) {
        final PersistenceQuery persistenceQuery = PersistenceQuery.get(system);
        if (Launcher.useCassandraJournal) {
            final CassandraReadJournal journal = persistenceQuery.getReadJournalFor(
                CassandraReadJournal.class,
                CassandraReadJournal.Identifier()
            );
            return events(journal, Offset.timeBasedUUID(journal.firstOffset()), eventTags);
        } else {
            final LeveldbReadJournal journal = persistenceQuery.getReadJournalFor(
                LeveldbReadJournal.class,
                LeveldbReadJournal.Identifier()
            );
            return events(journal, Offset.noOffset(), eventTags);
        }
    }

    static final Source<EventEnvelope, NotUsed> events(EventsByTagQuery journal, Offset firstOffset, String... eventTags) {
        return Source.from(() -> {
            // Remember last encountered offset, such that recovering the source can start from here.
            final Offset[] offsets = new Offset[eventTags.length];
            return IntStream
                .range(0, eventTags.length)
                .mapToObj(i -> {
                        offsets[i] = firstOffset;
                        final String eventTag = eventTags[i];
                        return journal.eventsByTag(eventTag, firstOffset)
                            .recoverWith(new PFBuilder<Throwable, Source<EventEnvelope, NotUsed>>()
                                .match(Throwable.class, e -> {
                                    LOG.error("Event source for tag " + eventTag + " failed, recovering after " + sleepMillis + "ms.", e);
                                    try {
                                        Thread.sleep(sleepMillis);
                                    } catch (InterruptedException interruptedException) {
                                        LOG.warn("Event source for tag " + eventTag + " failed, recovering interrupted.", interruptedException);
                                        return Source.empty();
                                    }
                                    return journal.eventsByTag(eventTag, offsets[i]);
                                })
                                .build()
                            )
                            .map(envelope -> {
                                offsets[i] = envelope.offset();
                                return envelope;
                            });
                    }
                ).iterator();
        }).flatMapMerge(eventTags.length, x -> x);
    }
}
