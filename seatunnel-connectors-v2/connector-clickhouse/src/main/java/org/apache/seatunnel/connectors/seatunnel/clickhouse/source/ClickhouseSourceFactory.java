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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.TypeConvertUtil;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.USERNAME;

@AutoService(Factory.class)
public class ClickhouseSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Clickhouse";
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        List<ClickHouseNode> nodes = ClickhouseUtil.createNodes(readonlyConfig);

        String sql = readonlyConfig.get(SQL);
        ClickHouseNode currentServer = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
        try (ClickHouseClient client = ClickHouseClient.newInstance(currentServer.getProtocol());
                ClickHouseResponse response =
                        client.connect(currentServer)
                                .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                                .query(modifySQLToLimit1(sql))
                                .executeAndWait()) {
            TableSchema.Builder builder = TableSchema.builder();
            int columnSize = response.getColumns().size();
            for (int i = 0; i < columnSize; i++) {
                String columnName = response.getColumns().get(i).getColumnName();
                SeaTunnelDataType<?> seaTunnelDataType =
                        TypeConvertUtil.convert(response.getColumns().get(i));
                builder.column(
                        PhysicalColumn.of(
                                columnName, seaTunnelDataType, Long.MAX_VALUE, true, null, null));
            }
            CatalogTable catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(
                                    factoryIdentifier(), readonlyConfig.get(DATABASE), "default"),
                            builder.build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            "",
                            factoryIdentifier());
            return () ->
                    (SeaTunnelSource<T, SplitT, StateT>)
                            new ClickhouseSource(nodes, catalogTable, sql);
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            factoryIdentifier(), PluginType.SOURCE, e.getMessage()));
        }
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(HOST, DATABASE, SQL, USERNAME, PASSWORD).build();
    }

    private String modifySQLToLimit1(String sql) {
        return String.format("SELECT * FROM (%s) s LIMIT 1", sql);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ClickhouseSource.class;
    }
}
