package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.stream.Collectors;

public class UpdateTagsService extends BaseService {
    public UpdateTagsService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonObject updateTags(byte[] requestBody) {
        Map<String, Object> params = JsonUtils.toMap(requestHandle(requestBody));
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);

        NodeEngineImpl nodeEngine = seaTunnelServer.getNodeEngine();
        MemberImpl localMember = nodeEngine.getLocalMember();

        Map<String, String> tags =
                params.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        value ->
                                                value.getValue() != null
                                                        ? value.getValue().toString()
                                                        : ""));
        localMember.updateAttribute(tags);
        return new JsonObject().add("status", "success").add("message", "update node tags done.");
    }
}
