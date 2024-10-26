/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.servlet;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.operation.GetClusterHealthMetricsOperation;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.rest.RestJobExecutionEnvironment;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class BaseServlet extends HttpServlet {

    protected final NodeEngineImpl nodeEngine;

    public BaseServlet(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    protected void writeJson(HttpServletResponse resp, Object obj) throws IOException {
        resp.setContentType("application/json");
        resp.getWriter().write(new Gson().toJson(obj));
    }

    protected void writeJson(HttpServletResponse resp, JsonArray jsonArray) throws IOException {
        resp.setContentType("application/json");
        resp.getWriter().write(jsonArray.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonObject jsonObject) throws IOException {
        resp.setContentType("application/json");
        resp.getWriter().write(jsonObject.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonArray jsonArray, int statusCode)
            throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        resp.getWriter().write(jsonArray.toString());
    }

    protected void writeJson(HttpServletResponse resp, JsonObject jsonObject, int statusCode)
            throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        resp.getWriter().write(jsonObject.toString());
    }

    protected void writeJson(HttpServletResponse resp, Object obj, int statusCode)
            throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        resp.getWriter().write(new Gson().toJson(obj));
    }

    protected void write(HttpServletResponse resp, Object obj) throws IOException {
        resp.setContentType("text/plain");
        resp.getWriter().write(obj.toString());
    }

    protected void writeHtml(HttpServletResponse resp, Object obj) throws IOException {
        resp.setContentType("text/html; charset=UTF-8");
        resp.getWriter().write(obj.toString());
    }

    protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
        Map<String, Object> extensionServices =
                nodeEngine.getNode().getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        if (shouldBeMaster && !seaTunnelServer.isMasterNode()) {
            return null;
        }
        return seaTunnelServer;
    }

    protected byte[] requestBody(HttpServletRequest req) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        try (BufferedReader reader = req.getReader()) {
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
        }

        String requestBody = stringBuilder.toString();
        return requestBody.getBytes(StandardCharsets.UTF_8);
    }

    protected Map<String, String> getParameterMap(HttpServletRequest req) {
        Map<String, String> reqParameterMap = new HashMap<>();

        Map<String, String[]> parameterMap = req.getParameterMap();

        for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
            String paramName = entry.getKey();
            String[] paramValues = entry.getValue();

            for (String value : paramValues) {
                reqParameterMap.put(paramName, value);
            }
        }
        return reqParameterMap;
    }

    protected JsonNode requestHandle(byte[] requestBody) {
        if (requestBody.length == 0) {
            throw new IllegalArgumentException("Request body is empty.");
        }
        JsonNode requestBodyJsonNode;
        try {
            requestBodyJsonNode = RestUtil.convertByteToJsonNode(requestBody);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON format in request body.");
        }
        return requestBodyJsonNode;
    }

    protected String mapToUrlParams(Map<String, String> params) {
        return params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&", "?", ""));
    }

    protected JsonObject submitJobInternal(
            Config config,
            Map<String, String> requestParams,
            SeaTunnelServer seaTunnelServer,
            Node node) {
        ReadonlyConfig envOptions = ReadonlyConfig.fromConfig(config.getConfig("env"));
        String jobName = envOptions.get(EnvCommonOptions.JOB_NAME);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(
                StringUtils.isEmpty(requestParams.get(RestConstant.JOB_NAME))
                        ? jobName
                        : requestParams.get(RestConstant.JOB_NAME));

        boolean startWithSavePoint =
                Boolean.parseBoolean(requestParams.get(RestConstant.IS_START_WITH_SAVE_POINT));
        String jobIdStr = requestParams.get(RestConstant.JOB_ID);
        Long finalJobId = StringUtils.isNotBlank(jobIdStr) ? Long.parseLong(jobIdStr) : null;
        RestJobExecutionEnvironment restJobExecutionEnvironment =
                new RestJobExecutionEnvironment(
                        seaTunnelServer, jobConfig, config, node, startWithSavePoint, finalJobId);
        JobImmutableInformation jobImmutableInformation = restJobExecutionEnvironment.build();
        long jobId = jobImmutableInformation.getJobId();
        if (!seaTunnelServer.isMasterNode()) {

            NodeEngineUtil.sendOperationToMasterNode(
                            node.nodeEngine,
                            new SubmitJobOperation(
                                    jobId,
                                    node.nodeEngine.toData(jobImmutableInformation),
                                    jobImmutableInformation.isStartWithSavePoint()))
                    .join();

        } else {
            submitJob(node, seaTunnelServer, jobImmutableInformation, jobConfig);
        }

        return new JsonObject()
                .add(RestConstant.JOB_ID, String.valueOf(jobId))
                .add(RestConstant.JOB_NAME, jobConfig.getName());
    }

    private void submitJob(
            Node node,
            SeaTunnelServer seaTunnelServer,
            JobImmutableInformation jobImmutableInformation,
            JobConfig jobConfig) {
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        Data data = node.nodeEngine.getSerializationService().toData(jobImmutableInformation);
        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                coordinatorService.submitJob(
                        Long.parseLong(jobConfig.getJobContext().getJobId()),
                        data,
                        jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }

    protected JsonArray getSystemMonitoringInformationJsonValues() {
        Cluster cluster = nodeEngine.getHazelcastInstance().getCluster();

        Set<Member> members = cluster.getMembers();
        JsonArray jsonValues =
                members.stream()
                        .map(
                                member -> {
                                    Address address = member.getAddress();
                                    String input = null;
                                    try {
                                        input =
                                                (String)
                                                        NodeEngineUtil.sendOperationToMemberNode(
                                                                        nodeEngine,
                                                                        new GetClusterHealthMetricsOperation(),
                                                                        address)
                                                                .get();
                                    } catch (InterruptedException | ExecutionException e) {
                                        log.error("Failed to get cluster health metrics", e);
                                    }
                                    String[] parts = input.split(", ");
                                    JsonObject jobInfo = new JsonObject();
                                    Arrays.stream(parts)
                                            .forEach(
                                                    part -> {
                                                        String[] keyValue = part.split("=");
                                                        jobInfo.add(keyValue[0], keyValue[1]);
                                                    });
                                    return jobInfo;
                                })
                        .collect(JsonArray::new, JsonArray::add, JsonArray::add);
        return jsonValues;
    }
}
