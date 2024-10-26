package org.apache.seatunnel.engine.server.rest.service;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Comparator;

public class RunningThreadService extends BaseService {
    public RunningThreadService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonArray getRunningThread() {
        return Thread.getAllStackTraces().keySet().stream()
                .sorted(Comparator.comparing(Thread::getName))
                .map(
                        stackTraceElements -> {
                            JsonObject jobInfoJson = new JsonObject();
                            jobInfoJson.add("threadName", stackTraceElements.getName());
                            jobInfoJson.add(
                                    "classLoader",
                                    String.valueOf(stackTraceElements.getContextClassLoader()));
                            return jobInfoJson;
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }
}
