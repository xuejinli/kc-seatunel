package org.apache.seatunnel.engine.server.rest.service;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class SystemMonitoringService extends BaseService {
    public SystemMonitoringService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonArray getSystemMonitoringInformationJsonValues() {
        return super.getSystemMonitoringInformationJsonValues();
    }
}
