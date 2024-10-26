package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class EncryptConfigService extends BaseService {
    public EncryptConfigService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonObject encryptConfig(byte[] requestBody) {
        Config config = RestUtil.buildConfig(requestHandle(requestBody), true);
        Config encryptConfig = ConfigShadeUtils.encryptConfig(config);
        String encryptString =
                encryptConfig.root().render(ConfigRenderOptions.concise().setJson(true));
        return Json.parse(encryptString).asObject();
    }
}
