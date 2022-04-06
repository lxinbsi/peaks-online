package com.bsi.peaks.server.es.state;


import com.bsi.peaks.model.PeaksImmutable;
import com.bsi.peaks.model.dto.CassandraNodeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peaksdata.event.cassandraMonitor.CassandraMonitor.CassandraMonitorEvent;

@PeaksImmutable
@JsonDeserialize(builder = CassandraMonitorStateBuilder.class)
public interface CassandraMonitorState {
    static CassandraMonitorStateBuilder builder() {
        return new CassandraMonitorStateBuilder();
    }

    Logger LOG = LoggerFactory.getLogger(CassandraMonitorStateBuilder.class);

    default PMap<String, CassandraNodeInfo> ipAddressNodeInfo() {
        return HashTreePMap.empty();
    }

    default float cutOff() { return 0.9f;}

    default boolean ignoreMonitor() { return false; }

    static CassandraMonitorState getDefaultInstance() {
        return builder().ipAddressNodeInfo(HashTreePMap.empty()).ignoreMonitor(true).build();
    }

    CassandraMonitorState withCutOff(float newCutOff);

    CassandraMonitorState withIgnoreMonitor(boolean newIgnoreMonitor);

    CassandraMonitorState withIpAddressNodeInfo(PMap<String, CassandraNodeInfo> value);

    default CassandraMonitorState update(CassandraMonitorEvent event) {
        try {
            switch(event.getEventCase()) {
                case NODEINFOUPDATED:
                    return withNodeUpdate(event);
                case CUTOFFUPDATED:
                    return withCutOffUpdate(event);
                case NODEDELETED:
                    return withDelete(event);
                case DELETEDALL:
                    return withDeleteAllNodes(event);
                case IGNOREMONITORUPDATED:
                    return withIgnoreMonitorUpdate(event);
                default:
                    throw new IllegalArgumentException("Unexpected event " + event.getEventCase());
            }
        } catch (Exception e) {
            LOG.error("Unable to update cassandra monitor state, event {}", event, e);
        }
        return this;
    }

    default CassandraMonitorState withNodeUpdate(CassandraMonitorEvent event) {
        CassandraNodeInfo newNodeInfo = event.getNodeInfoUpdated().getNodeInfo();
        String ipAddress = newNodeInfo.getIPAddress();
        return plus(ipAddress, newNodeInfo);
    }

    default CassandraMonitorState withCutOffUpdate(CassandraMonitorEvent event) {
        return withCutOff(event.getCutOffUpdated().getCutOff());
    }

    default CassandraMonitorState withIgnoreMonitorUpdate(CassandraMonitorEvent event) {
        return withIgnoreMonitor(event.getIgnoreMonitorUpdated().getIgnoreMonitor());
    }

    default CassandraMonitorState withDelete(CassandraMonitorEvent event) {
        return minus(event.getNodeDeleted().getIpAddress());
    }

    default CassandraMonitorState withDeleteAllNodes(CassandraMonitorEvent event) {
        return withIpAddressNodeInfo(HashTreePMap.empty());
    }

    default CassandraMonitorState plus(String ipAddress, CassandraNodeInfo newNodeInfo) {
        PMap<String, CassandraNodeInfo> cassandraNodeInfoPMap = ipAddressNodeInfo().plus(ipAddress, newNodeInfo);
        return withIpAddressNodeInfo(cassandraNodeInfoPMap);
    }

    default CassandraMonitorState minus(String ipAddress) {
        PMap<String, CassandraNodeInfo> cassandraNodeInfoPMap = ipAddressNodeInfo().minus(ipAddress);
        return withIpAddressNodeInfo(cassandraNodeInfoPMap);
    }
}
