package com.bsi.peaks.server.es.fasta;

import akka.actor.ActorSystem;
import akka.persistence.query.EventEnvelope;
import akka.stream.ActorMaterializer;
import com.bsi.peaks.server.es.FastaDatabaseEntity;
import com.bsi.peaks.server.es.View;
import com.bsi.peaks.event.fasta.FastaDatabaseEvent;
import com.bsi.peaks.event.fasta.FastaDatabaseInformation;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;
import org.pcollections.PSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.bsi.peaks.event.fasta.FastaDatabaseEvent.EventCase.DELETED;
import static com.bsi.peaks.event.fasta.FastaDatabaseEvent.EventCase.INFORMATIONSET;
import static com.bsi.peaks.event.fasta.FastaDatabaseEvent.EventCase.OWNERSET;

@Singleton
public class FastaDatabaseView {

    private static final Logger LOG = LoggerFactory.getLogger(FastaDatabaseView.class);
    private PMap<UUID, String> databaseIdNamenMap = HashTreePMap.empty();
    private PMap<UUID, UUID> databaseIdOwnerMap = HashTreePMap.empty();
    private PSet<UUID> deletedDatabases = HashTreePSet.empty();

    @Inject
    public FastaDatabaseView(ActorSystem system) {
        View
            .events(
                system,
                FastaDatabaseEntity.eventTag(OWNERSET),
                FastaDatabaseEntity.eventTag(INFORMATIONSET),
                FastaDatabaseEntity.eventTag(DELETED)
            )
            .map(EventEnvelope::event)
            .collectType(FastaDatabaseEvent.class)
            .runForeach(this::processEvent, ActorMaterializer.create(system));
    }

    private void processEvent(FastaDatabaseEvent event) {
        try {
            final UUID databaseId = UUID.fromString(event.getDatabaseId());
            switch (event.getEventCase()) {
                case INFORMATIONSET: {
                    final FastaDatabaseInformation information = event.getInformationSet().getInformation();
                    plusDatabaseIdName(databaseId, information.getName());
                    break;
                }
                case OWNERSET: {
                    final UUID ownerId = UUID.fromString(event.getOwnerSet().getOwnerId());
                    plusDatabaseIdOwnerId(databaseId, ownerId);
                    break;
                }
                case DELETED: {
                    remove(databaseId);
                    break;
                }
            }
        } catch (Exception e) {
            LOG.error("Unable to handle event in FastaDatabase view", e);
        }
    }

    private void plusDatabaseIdOwnerId(UUID databaseId, UUID ownerId) {
        if (deletedDatabases.contains(databaseId)) {
            return;
        }
        databaseIdOwnerMap = databaseIdOwnerMap.plus(databaseId, ownerId);
    }

    private void plusDatabaseIdName(UUID databaseId, String name) {
        if (deletedDatabases.contains(databaseId)) {
            return;
        }
        databaseIdNamenMap = databaseIdNamenMap.plus(databaseId, name);
    }

    private void remove(UUID databaseId) {
        databaseIdNamenMap = databaseIdNamenMap.minus(databaseId);
        databaseIdOwnerMap = databaseIdOwnerMap.minus(databaseId);
        deletedDatabases = deletedDatabases.plus(databaseId);
    }

    public Iterable<UUID> databases() {
        return databaseIdNamenMap.keySet();
    }

    public Iterable<UUID> databases(UUID... ownerIds) {
        final List<UUID> ownerIdsList = Arrays.asList(ownerIds);
        return databaseIdOwnerMap.entrySet().stream()
            .filter(x -> ownerIdsList.contains(x.getValue()))
            .map(Map.Entry::getKey)
            ::iterator;
    }

    public Optional<UUID> lookupDatabaseId(String name) {
        return databaseIdNamenMap.entrySet()
            .stream()
            .filter(x -> x.getValue().equalsIgnoreCase(name))
            .map(Map.Entry::getKey)
            .findFirst();
    }

}
