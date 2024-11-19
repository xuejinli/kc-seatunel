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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksCsvSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksISerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.serialize.StarRocksJsonSerializer;
import org.apache.seatunnel.connectors.seatunnel.starrocks.util.SchemaUtils;

import org.apache.commons.lang3.StringUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

@Slf4j
public class StarRocksSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private StarRocksISerializer serializer;
    private StarRocksSinkManager manager;
    private TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private TablePath sinkTablePath;

    public StarRocksSinkWriter(
            SinkConfig sinkConfig, TableSchema tableSchema, TablePath tablePath) {
        this.tableSchema = tableSchema;
        SeaTunnelRowType seaTunnelRowType = tableSchema.toPhysicalRowDataType();
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);
        this.manager = new StarRocksSinkManager(sinkConfig, tableSchema);
        this.sinkConfig = sinkConfig;
        this.sinkTablePath = tablePath;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String record;
        try {
            record = serializer.serialize(element);
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "serialize failed. Row={" + element + "}",
                    e);
        }
        manager.write(record);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        if (event instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent alterTableColumnsEvent = (AlterTableColumnsEvent) event;
            List<AlterTableColumnEvent> events = alterTableColumnsEvent.getEvents();
            for (AlterTableColumnEvent alterTableColumnEvent : events) {
                String sourceDialectName = alterTableColumnEvent.getSourceDialectName();
                if (StringUtils.isBlank(sourceDialectName)) {
                    throw new SeaTunnelException(
                            "The sourceDialectName in AlterTableColumnEvent can not be empty. event: "
                                    + event);
                }
                processSchemaChangeEvent(alterTableColumnEvent);
            }
        } else {
            log.warn("We only support AlterTableColumnsEvent, but actual event is " + event);
        }

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            Connection conn =
                    DriverManager.getConnection(
                            sinkConfig.getJdbcUrl(),
                            sinkConfig.getUsername(),
                            sinkConfig.getPassword());
            SchemaUtils.applySchemaChange(event, conn, sinkTablePath);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed connecting to %s via JDBC.", sinkConfig.getJdbcUrl()), e);
        }
    }

    protected void processSchemaChangeEvent(AlterTableColumnEvent event) throws IOException {
        TableSchema newTableSchema = this.tableSchema.copy();
        List<Column> columns = newTableSchema.getColumns();
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                AlterTableAddColumnEvent alterTableAddColumnEvent =
                        (AlterTableAddColumnEvent) event;
                Column addColumn = alterTableAddColumnEvent.getColumn();
                String afterColumn = alterTableAddColumnEvent.getAfterColumn();
                if (StringUtils.isNotBlank(afterColumn)) {
                    Optional<Column> columnOptional =
                            columns.stream()
                                    .filter(column -> afterColumn.equals(column.getName()))
                                    .findFirst();
                    if (!columnOptional.isPresent()) {
                        columns.add(addColumn);
                        break;
                    }
                    columnOptional.ifPresent(
                            column -> {
                                int index = columns.indexOf(column);
                                columns.add(index + 1, addColumn);
                            });
                } else {
                    columns.add(addColumn);
                }
                break;
            case SCHEMA_CHANGE_DROP_COLUMN:
                String dropColumn = ((AlterTableDropColumnEvent) event).getColumn();
                columns.removeIf(column -> column.getName().equalsIgnoreCase(dropColumn));
                break;
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                Column modifyColumn = ((AlterTableModifyColumnEvent) event).getColumn();
                replaceColumnByIndex(
                        event.getEventType(), columns, modifyColumn.getName(), modifyColumn);
                break;
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                        (AlterTableChangeColumnEvent) event;
                Column changeColumn = alterTableChangeColumnEvent.getColumn();
                String oldColumnName = alterTableChangeColumnEvent.getOldColumn();
                replaceColumnByIndex(event.getEventType(), columns, oldColumnName, changeColumn);
                break;
            default:
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent for event type: " + event.getEventType());
        }
        this.tableSchema = newTableSchema;
        SeaTunnelRowType seaTunnelRowType = newTableSchema.toPhysicalRowDataType();
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);
        this.manager = new StarRocksSinkManager(sinkConfig, newTableSchema);
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        // Flush to storage before snapshot state is performed
        manager.flush();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (IOException e) {
            log.error("Close starRocks manager failed.", e);
            throw new StarRocksConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, e);
        }
    }

    public StarRocksISerializer createSerializer(
            SinkConfig sinkConfig, SeaTunnelRowType seaTunnelRowType) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            return new StarRocksCsvSerializer(
                    sinkConfig.getColumnSeparator(),
                    seaTunnelRowType,
                    sinkConfig.isEnableUpsertDelete());
        }
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            return new StarRocksJsonSerializer(seaTunnelRowType, sinkConfig.isEnableUpsertDelete());
        }
        throw new StarRocksConnectorException(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                "Failed to create row serializer, unsupported `format` from stream load properties.");
    }

    protected void replaceColumnByIndex(
            EventType eventType, List<Column> columns, String oldColumnName, Column newColumn) {
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (column.getName().equalsIgnoreCase(oldColumnName)) {
                // rename ...... to ......  which just has column name
                if (eventType.equals(EventType.SCHEMA_CHANGE_CHANGE_COLUMN)
                        && newColumn.getDataType() == null) {
                    columns.set(i, column.rename(newColumn.getName()));
                } else {
                    columns.set(i, newColumn);
                }
            }
        }
    }
}
