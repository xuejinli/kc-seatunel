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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.server.NodeExtension;
import org.apache.seatunnel.engine.server.log.FormatType;
import org.apache.seatunnel.engine.server.log.Log4j2HttpGetCommandProcessor;
import org.apache.seatunnel.engine.server.operation.GetClusterHealthMetricsOperation;
import org.apache.seatunnel.engine.server.rest.service.JobInfoService;
import org.apache.seatunnel.engine.server.rest.service.OverviewService;
import org.apache.seatunnel.engine.server.rest.service.RunningThreadService;
import org.apache.seatunnel.engine.server.rest.service.SystemMonitoringService;
import org.apache.seatunnel.engine.server.rest.service.ThreadDumpService;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.builder.api.Component;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.properties.PropertiesConfiguration;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpGetCommand;
import com.hazelcast.internal.ascii.rest.RestValue;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.JsonUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.spi.impl.NodeEngineImpl;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_400;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.FINISHED_JOBS_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.GET_ALL_LOG_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.GET_LOG;
import static org.apache.seatunnel.engine.server.rest.RestConstant.GET_LOGS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.JOB_INFO_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.OVERVIEW;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOBS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_JOB_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.RUNNING_THREADS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.SYSTEM_MONITORING_INFORMATION;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TELEMETRY_METRICS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.TELEMETRY_OPEN_METRICS_URL;
import static org.apache.seatunnel.engine.server.rest.RestConstant.THREAD_DUMP;

public class RestHttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    private final Log4j2HttpGetCommandProcessor original;
    private NodeEngineImpl nodeEngine;
    private OverviewService overviewService;
    private JobInfoService jobInfoService;
    private SystemMonitoringService systemMonitoringService;
    private ThreadDumpService threadDumpService;
    private RunningThreadService runningThreadService;

    public RestHttpGetCommandProcessor(TextCommandService textCommandService) {

        this(textCommandService, new Log4j2HttpGetCommandProcessor(textCommandService));
        this.nodeEngine = this.textCommandService.getNode().getNodeEngine();
        this.overviewService = new OverviewService(nodeEngine);
        this.jobInfoService = new JobInfoService(nodeEngine);
        this.systemMonitoringService = new SystemMonitoringService(nodeEngine);
        this.threadDumpService = new ThreadDumpService(nodeEngine);
        this.runningThreadService = new RunningThreadService(nodeEngine);
    }

    public RestHttpGetCommandProcessor(
            TextCommandService textCommandService,
            Log4j2HttpGetCommandProcessor log4j2HttpGetCommandProcessor) {
        super(
                textCommandService,
                textCommandService.getNode().getLogger(Log4j2HttpGetCommandProcessor.class));
        this.original = log4j2HttpGetCommandProcessor;
        this.nodeEngine = this.textCommandService.getNode().getNodeEngine();
        this.overviewService = new OverviewService(nodeEngine);
    }

    @Override
    public void handle(HttpGetCommand httpGetCommand) {
        String uri = httpGetCommand.getURI();

        try {
            if (uri.startsWith(CONTEXT_PATH + RUNNING_JOBS_URL)) {
                handleRunningJobsInfo(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + FINISHED_JOBS_INFO)) {
                handleFinishedJobsInfo(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + RUNNING_JOB_URL)
                    || uri.startsWith(CONTEXT_PATH + JOB_INFO_URL)) {
                handleJobInfoById(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + SYSTEM_MONITORING_INFORMATION)) {
                getSystemMonitoringInformation(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + RUNNING_THREADS)) {
                getRunningThread(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + OVERVIEW)) {
                overView(httpGetCommand, uri);
            } else if (uri.equals(TELEMETRY_METRICS_URL)) {
                handleMetrics(httpGetCommand, TextFormat.CONTENT_TYPE_004);
            } else if (uri.equals(TELEMETRY_OPEN_METRICS_URL)) {
                handleMetrics(httpGetCommand, TextFormat.CONTENT_TYPE_OPENMETRICS_100);
            } else if (uri.startsWith(CONTEXT_PATH + THREAD_DUMP)) {
                getThreadDump(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + GET_ALL_LOG_NAME)) {
                getAllLogName(httpGetCommand);
            } else if (uri.startsWith(CONTEXT_PATH + GET_LOGS)) {
                getAllNodeLog(httpGetCommand, uri);
            } else if (uri.startsWith(CONTEXT_PATH + GET_LOG)) {
                getCurrentNodeLog(httpGetCommand, uri);
            } else {
                original.handle(httpGetCommand);
            }
        } catch (IndexOutOfBoundsException e) {
            httpGetCommand.send400();
        } catch (IllegalArgumentException e) {
            prepareResponse(SC_400, httpGetCommand, exceptionResponse(e));
        } catch (Throwable e) {
            logger.warning("An error occurred while handling request " + httpGetCommand, e);
            prepareResponse(SC_500, httpGetCommand, exceptionResponse(e));
        }

        this.textCommandService.sendResponse(httpGetCommand);
    }

    @Override
    public void handleRejection(HttpGetCommand httpGetCommand) {
        handle(httpGetCommand);
    }

    public void overView(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        String tagStr;
        if (uri.contains("?")) {
            int index = uri.indexOf("?");
            tagStr = uri.substring(index + 1);
        } else {
            tagStr = "";
        }
        Map<String, String> tags =
                Arrays.stream(tagStr.split("&"))
                        .map(variable -> variable.split("=", 2))
                        .filter(pair -> pair.length == 2)
                        .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));

        this.prepareResponse(
                command,
                JsonUtil.toJsonObject(
                        JsonUtils.toMap(
                                JsonUtils.toJsonString(overviewService.getOverviewInfo(tags)))));
    }

    public void getThreadDump(HttpGetCommand command) {

        this.prepareResponse(command, threadDumpService.getThreadDump());
    }

    private void getSystemMonitoringInformation(HttpGetCommand command) {
        this.prepareResponse(
                command, systemMonitoringService.getSystemMonitoringInformationJsonValues());
    }

    private JsonArray getSystemMonitoringInformationJsonValues() {
        Cluster cluster = textCommandService.getNode().hazelcastInstance.getCluster();

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
                                        logger.severe("get system monitoring information fail", e);
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

    private void handleRunningJobsInfo(HttpGetCommand command) {
        this.prepareResponse(command, jobInfoService.getRunningJobsJson());
    }

    private void handleFinishedJobsInfo(HttpGetCommand command, String uri) {

        uri = StringUtil.stripTrailingSlash(uri);

        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String state;
        if (indexEnd == -1) {
            state = "";
        } else {
            state = uri.substring(indexEnd + 1);
        }

        this.prepareResponse(command, jobInfoService.getJobsByStateJson(state));
    }

    private void handleJobInfoById(HttpGetCommand command, String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String jobId = uri.substring(indexEnd + 1);
        this.prepareResponse(command, jobInfoService.getJobInfoJson(Long.valueOf(jobId)));
    }

    private void getRunningThread(HttpGetCommand command) {
        this.prepareResponse(command, runningThreadService.getRunningThread());
    }

    private void handleMetrics(HttpGetCommand httpGetCommand, String contentType) {
        StringWriter stringWriter = new StringWriter();
        NodeExtension nodeExtension =
                (NodeExtension) textCommandService.getNode().getNodeExtension();
        try {
            TextFormat.writeFormat(
                    contentType,
                    stringWriter,
                    nodeExtension.getCollectorRegistry().metricFamilySamples());
            this.prepareResponse(httpGetCommand, stringWriter.toString());
        } catch (IOException e) {
            httpGetCommand.send400();
        } finally {
            try {
                stringWriter.close();
            } catch (IOException e) {
                logger.warning("An error occurred while handling request " + httpGetCommand, e);
                prepareResponse(SC_500, httpGetCommand, exceptionResponse(e));
            }
        }
    }

    private PropertiesConfiguration getLogConfiguration() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        return (PropertiesConfiguration) context.getConfiguration();
    }

    private void getAllNodeLog(HttpGetCommand httpGetCommand, String uri)
            throws NoSuchFieldException, IllegalAccessException {

        // Analysis uri, get logName and jobId param
        String param = getParam(uri);
        boolean isLogFile = param.contains(".log");
        String logName = isLogFile ? param : StringUtils.EMPTY;
        String jobId = !isLogFile ? param : StringUtils.EMPTY;

        String logPath = getLogPath();
        if (StringUtils.isBlank(logPath)) {
            logger.warning(
                    "Log file path is empty, no log file path configured in the current configuration file");
            httpGetCommand.send404();
            return;
        }
        JsonArray systemMonitoringInformationJsonValues =
                getSystemMonitoringInformationJsonValues();

        if (StringUtils.isBlank(logName)) {
            StringBuffer logLink = new StringBuffer();
            ArrayList<Tuple3<String, String, String>> allLogNameList = new ArrayList<>();

            systemMonitoringInformationJsonValues.forEach(
                    systemMonitoringInformation -> {
                        String host = systemMonitoringInformation.asObject().get("host").asString();
                        int port =
                                Integer.valueOf(
                                        systemMonitoringInformation
                                                .asObject()
                                                .get("port")
                                                .asString());
                        String url = "http://" + host + ":" + port + CONTEXT_PATH;
                        String allName = sendGet(url + GET_ALL_LOG_NAME);
                        logger.fine(String.format("Request: %s , Result: %s", url, allName));
                        ArrayNode jsonNodes = JsonUtils.parseArray(allName);

                        jsonNodes.forEach(
                                jsonNode -> {
                                    String fileName = jsonNode.asText();
                                    if (StringUtils.isNotBlank(jobId)
                                            && !fileName.contains(jobId)) {
                                        return;
                                    }
                                    allLogNameList.add(
                                            Tuple3.tuple3(
                                                    host + ":" + port,
                                                    url + GET_LOGS + "/" + fileName,
                                                    fileName));
                                });
                    });
            FormatType formatType = getFormatType(uri);
            switch (formatType) {
                case JSON:
                    JsonArray jsonArray =
                            allLogNameList.stream()
                                    .map(
                                            tuple -> {
                                                JsonObject jsonObject = new JsonObject();
                                                jsonObject.add("node", tuple.f0());
                                                jsonObject.add("logLink", tuple.f1());
                                                jsonObject.add("logName", tuple.f2());
                                                return jsonObject;
                                            })
                                    .collect(JsonArray::new, JsonArray::add, JsonArray::add);
                    this.prepareResponse(httpGetCommand, jsonArray);
                    return;
                case HTML:
                default:
                    allLogNameList.forEach(
                            tuple ->
                                    logLink.append(
                                            buildLogLink(
                                                    tuple.f1(), tuple.f0() + "-" + tuple.f2())));
                    String logContent = buildWebSiteContent(logLink);
                    this.prepareResponse(httpGetCommand, getRestValue(logContent));
            }
        } else {
            prepareLogResponse(httpGetCommand, logPath, logName);
        }
    }

    private FormatType getFormatType(String uri) {
        Map<String, String> uriParam = getUriParam(uri);
        return FormatType.fromString(uriParam.get("format"));
    }

    private Map<String, String> getUriParam(String uri) {
        String queryString = uri.contains("?") ? uri.substring(uri.indexOf("?") + 1) : "";
        return Arrays.stream(queryString.split("&"))
                .map(param -> param.split("=", 2))
                .filter(pair -> pair.length == 2)
                .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));
    }

    private String getParam(String uri) {
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        if (indexEnd != -1) {
            String param = uri.substring(indexEnd + 1);
            logger.fine(String.format("Request: %s , Param: %s", uri, param));
            return param;
        }
        return StringUtils.EMPTY;
    }

    private static RestValue getRestValue(String logContent) {
        RestValue restValue = new RestValue();
        restValue.setContentType("text/html; charset=UTF-8".getBytes(StandardCharsets.UTF_8));
        restValue.setValue(logContent.getBytes(StandardCharsets.UTF_8));
        return restValue;
    }

    private static String buildWebSiteContent(StringBuffer logLink) {
        return "<html><head><title>Seatunnel log</title></head>\n"
                + "<body>\n"
                + " <h2>Seatunnel log</h2>\n"
                + " <ul>\n"
                + logLink.toString()
                + " </ul>\n"
                + "</body></html>";
    }

    private String getFileLogPath(PropertiesConfiguration config)
            throws NoSuchFieldException, IllegalAccessException {
        Field propertiesField = BuiltConfiguration.class.getDeclaredField("appendersComponent");
        propertiesField.setAccessible(true);
        Component propertiesComponent = (Component) propertiesField.get(config);
        StrSubstitutor substitutor = config.getStrSubstitutor();
        return propertiesComponent.getComponents().stream()
                .filter(component -> "fileAppender".equals(component.getAttributes().get("name")))
                .map(component -> substitutor.replace(component.getAttributes().get("fileName")))
                .findFirst()
                .orElse(null);
    }

    /** Get configuration log path */
    private String getLogPath() throws NoSuchFieldException, IllegalAccessException {
        String routingAppender = "routingAppender";
        String fileAppender = "fileAppender";
        PropertiesConfiguration config = getLogConfiguration();
        // Get routingAppender log file path
        String routingLogFilePath = getRoutingLogFilePath(config);

        // Get fileAppender log file path
        String fileLogPath = getFileLogPath(config);
        String logRef =
                config.getLoggerConfig(StringUtils.EMPTY).getAppenderRefs().stream()
                        .map(Object::toString)
                        .filter(ref -> ref.contains(routingAppender) || ref.contains(fileAppender))
                        .findFirst()
                        .orElse(StringUtils.EMPTY);
        if (logRef.equals(routingAppender)) {
            return routingLogFilePath.substring(0, routingLogFilePath.lastIndexOf("/"));
        } else if (logRef.equals(fileAppender)) {
            return fileLogPath.substring(0, routingLogFilePath.lastIndexOf("/"));
        } else {
            logger.warning(String.format("Log file path is empty, get logRef : %s", logRef));
            return null;
        }
    }

    /** Get Current Node Log By /log request */
    private void getCurrentNodeLog(HttpGetCommand httpGetCommand, String uri)
            throws NoSuchFieldException, IllegalAccessException {
        String logName = getParam(uri);
        String logPath = getLogPath();

        if (StringUtils.isBlank(logName)) {
            // Get Current Node Log List
            List<File> logFileList = FileUtils.listFile(logPath);
            StringBuffer logLink = new StringBuffer();
            for (File file : logFileList) {
                logLink.append(buildLogLink("log/" + file.getName(), file.getName()));
            }
            this.prepareResponse(httpGetCommand, getRestValue(buildWebSiteContent(logLink)));
        } else {
            // Get Current Node Log Content
            prepareLogResponse(httpGetCommand, logPath, logName);
        }
    }

    /** Prepare Log Response */
    private void prepareLogResponse(HttpGetCommand httpGetCommand, String logPath, String logName) {
        String logFilePath = logPath + "/" + logName;
        try {
            String logContent = FileUtils.readFileToStr(new File(logFilePath).toPath());
            this.prepareResponse(httpGetCommand, logContent);
        } catch (SeaTunnelRuntimeException e) {
            // If the log file does not exist, return 400
            httpGetCommand.send400();
            logger.warning(
                    String.format("Log file content is empty, get log path : %s", logFilePath));
        }
    }

    public String buildLogLink(String href, String name) {
        return "<li><a href=\"" + href + "\">" + name + "</a></li>\n";
    }

    private static String sendGet(String urlString) {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();

            if (connection.getResponseCode() == 200) {
                try (InputStream is = connection.getInputStream();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = is.read(buffer)) != -1) {
                        baos.write(buffer, 0, len);
                    }
                    return baos.toString();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void getAllLogName(HttpGetCommand httpGetCommand)
            throws NoSuchFieldException, IllegalAccessException {
        String logPath = getLogPath();
        List<File> logFileList = FileUtils.listFile(logPath);
        List<String> fileNameList =
                logFileList.stream().map(File::getName).collect(Collectors.toList());
        try {
            this.prepareResponse(httpGetCommand, JsonUtils.toJsonString(fileNameList));
        } catch (SeaTunnelRuntimeException e) {
            httpGetCommand.send400();
            logger.warning(String.format("Log file name get failed, get log path: %s", logPath));
        }
    }

    private static String getRoutingLogFilePath(PropertiesConfiguration config)
            throws NoSuchFieldException, IllegalAccessException {
        Field propertiesField = BuiltConfiguration.class.getDeclaredField("appendersComponent");
        propertiesField.setAccessible(true);
        Component propertiesComponent = (Component) propertiesField.get(config);
        StrSubstitutor substitutor = config.getStrSubstitutor();
        return propertiesComponent.getComponents().stream()
                .filter(
                        component ->
                                "routingAppender".equals(component.getAttributes().get("name")))
                .flatMap(component -> component.getComponents().stream())
                .flatMap(component -> component.getComponents().stream())
                .flatMap(component -> component.getComponents().stream())
                .map(component -> substitutor.replace(component.getAttributes().get("fileName")))
                .findFirst()
                .orElse(null);
    }
}
