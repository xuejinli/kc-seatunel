package org.apache.seatunnel.engine.server.rest.service;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;

public class ThreadDumpService extends BaseService {
    public ThreadDumpService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public JsonArray getThreadDump() {

        Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
        JsonArray threadInfoList = new JsonArray();
        for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
            StringBuilder stackTraceBuilder = new StringBuilder();
            for (StackTraceElement element : entry.getValue()) {
                stackTraceBuilder.append(element.toString()).append("\n");
            }
            String stackTrace = stackTraceBuilder.toString().trim();
            JsonObject threadInfo = new JsonObject();
            threadInfo.add("threadName", entry.getKey().getName());
            threadInfo.add("threadId", entry.getKey().getId());
            threadInfo.add("threadState", entry.getKey().getState().name());
            threadInfo.add("stackTrace", stackTrace);
            threadInfoList.add(threadInfo);
        }

        return threadInfoList;
    }
}
