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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.NODE_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.SQL;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.USERNAME;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.TypeConvertUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(SeaTunnelSource.class)
public class ClickhouseSource implements SeaTunnelSource<SeaTunnelRow, ClickhouseSourceSplit, ClickhouseSourceState> {

    private SeaTunnelContext seaTunnelContext;
    private List<ClickHouseNode> servers;
    private SeaTunnelRowTypeInfo rowTypeInfo;
    private String sql;

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, NODE_ADDRESS, DATABASE, SQL, USERNAME, PASSWORD);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        servers = Arrays.stream(config.getString(NODE_ADDRESS).split(",")).map(address -> {
            String[] nodeAndPort = address.split(":", 2);
            return ClickHouseNode.builder().host(nodeAndPort[0]).port(ClickHouseProtocol.HTTP,
                            Integer.parseInt(nodeAndPort[1])).database(config.getString(DATABASE))
                    .credentials(ClickHouseCredentials.fromUserAndPassword(config.getString(USERNAME),
                            config.getString(PASSWORD))).build();
        }).collect(Collectors.toList());

        sql = config.getString(SQL);
        try (ClickHouseClient client = ClickHouseClient.newInstance(servers.get(0).getProtocol());
             ClickHouseResponse response =
                     client.connect(servers.get(0)).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                             .query(modifySQLToLimit1(config.getString(SQL))).executeAndWait()) {

            int columnSize = response.getColumns().size();
            String[] fieldNames = new String[columnSize];
            SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[columnSize];

            for (int i = 0; i < columnSize; i++) {
                fieldNames[i] = response.getColumns().get(i).getColumnName();
                seaTunnelDataTypes[i] = TypeConvertUtil.convert(response.getColumns().get(i).getDataType());
            }

            this.rowTypeInfo = new SeaTunnelRowTypeInfo(fieldNames, seaTunnelDataTypes);

        } catch (ClickHouseException e) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, e.getMessage());
        }

    }

    private String modifySQLToLimit1(String sql) {
        return String.format("SELECT * FROM (%s) s LIMIT 1", sql);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowTypeInfo getRowTypeInfo() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, ClickhouseSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new ClickhouseSourceReader(servers, readerContext, this.rowTypeInfo, sql);
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> createEnumerator(SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext) throws Exception {
        return new ClickhouseSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> restoreEnumerator(SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext, ClickhouseSourceState checkpointState) throws Exception {
        return new ClickhouseSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public Serializer<ClickhouseSourceState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }
}
