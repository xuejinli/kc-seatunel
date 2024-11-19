/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.starrocks.util;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksTypeConverter;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class SchemaUtils {

    private SchemaUtils() {}

    /**
     * Refresh physical table schema by schema change event
     *
     * @param event schema change event
     * @param connection jdbc connection
     * @param tablePath sink table path
     */
    public static void applySchemaChange(
            SchemaChangeEvent event, Connection connection, TablePath tablePath)
            throws SQLException {
        if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                applySchemaChange(columnEvent, connection, tablePath);
            }
        } else {
            if (event instanceof AlterTableChangeColumnEvent) {
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                if (!changeColumnEvent
                        .getOldColumn()
                        .equals(changeColumnEvent.getColumn().getName())) {
                    if (!columnExists(connection, tablePath, changeColumnEvent.getOldColumn())
                            && columnExists(
                                    connection,
                                    tablePath,
                                    changeColumnEvent.getColumn().getName())) {
                        log.warn(
                                "Column {} already exists in table {}. Skipping change column operation. event: {}",
                                changeColumnEvent.getColumn().getName(),
                                tablePath.getFullName(),
                                event);
                        return;
                    }
                }
                applySchemaChange(connection, tablePath, changeColumnEvent);
            } else if (event instanceof AlterTableModifyColumnEvent) {
                applySchemaChange(connection, tablePath, (AlterTableModifyColumnEvent) event);
            } else if (event instanceof AlterTableAddColumnEvent) {
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                if (columnExists(connection, tablePath, addColumnEvent.getColumn().getName())) {
                    log.warn(
                            "Column {} already exists in table {}. Skipping add column operation. event: {}",
                            addColumnEvent.getColumn().getName(),
                            tablePath.getFullName(),
                            event);
                    return;
                }
                applySchemaChange(connection, tablePath, addColumnEvent);
            } else if (event instanceof AlterTableDropColumnEvent) {
                AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
                if (!columnExists(connection, tablePath, dropColumnEvent.getColumn())) {
                    log.warn(
                            "Column {} does not exist in table {}. Skipping drop column operation. event: {}",
                            dropColumnEvent.getColumn(),
                            tablePath.getFullName(),
                            event);
                    return;
                }
                applySchemaChange(connection, tablePath, dropColumnEvent);
            } else {
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent : " + event.getEventType());
            }
        }
    }

    public static void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableChangeColumnEvent event)
            throws SQLException {
        String tableIdentifierWithQuoted = tablePath.getFullName();
        Column changeColumn = event.getColumn();
        String oldColumnName = event.getOldColumn();
        String afterColumn = event.getAfterColumn();
        String changeColumnSQL =
                buildAlterTableSql(
                        AlterType.RENAME.name(),
                        changeColumn,
                        tableIdentifierWithQuoted,
                        oldColumnName,
                        afterColumn);
        try (Statement statement = connection.createStatement()) {
            log.info("Executing change column SQL: " + changeColumnSQL);
            statement.execute(changeColumnSQL);
        }
    }

    public static void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableModifyColumnEvent event)
            throws SQLException {
        String tableIdentifierWithQuoted = tablePath.getFullName();
        Column modifyColumn = event.getColumn();
        String afterColumn = event.getAfterColumn();
        String modifyColumnSQL =
                buildAlterTableSql(
                        AlterType.MODIFY.name(),
                        modifyColumn,
                        tableIdentifierWithQuoted,
                        StringUtils.EMPTY,
                        afterColumn);

        try (Statement statement = connection.createStatement()) {
            log.info("Executing modify column SQL: " + modifyColumnSQL);
            statement.execute(modifyColumnSQL);
        }
    }

    public static void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableAddColumnEvent event)
            throws SQLException {
        String tableIdentifierWithQuoted = tablePath.getFullName();
        Column addColumn = event.getColumn();
        String afterColumn = event.getAfterColumn();
        String addColumnSQL =
                buildAlterTableSql(
                        AlterType.ADD.name(),
                        addColumn,
                        tableIdentifierWithQuoted,
                        StringUtils.EMPTY,
                        afterColumn);
        try (Statement statement = connection.createStatement()) {
            log.info("Executing add column SQL: " + addColumnSQL);
            statement.execute(addColumnSQL);
        }
    }

    public static void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableDropColumnEvent event)
            throws SQLException {
        String tableIdentifierWithQuoted = tablePath.getFullName();
        String dropColumn = event.getColumn();
        String dropColumnSQL =
                buildAlterTableSql(
                        AlterType.DROP.name(), null, tableIdentifierWithQuoted, dropColumn, null);
        try (Statement statement = connection.createStatement()) {
            log.info("Executing drop column SQL: " + dropColumnSQL);
            statement.execute(dropColumnSQL);
        }
    }

    /**
     * build alter table sql
     *
     * @param alterOperation alter operation of ddl
     * @param newColumn new column after ddl
     * @param tableName table name of sink table
     * @param oldColumnName old column name before ddl
     * @param afterColumn column before the new column
     * @return alter table sql for sink table after schema change
     */
    public static String buildAlterTableSql(
            String alterOperation,
            Column newColumn,
            String tableName,
            String oldColumnName,
            String afterColumn) {
        if (StringUtils.equals(alterOperation, AlterType.DROP.name())) {
            return String.format(
                    "ALTER TABLE %s DROP COLUMN %s", tableName, quoteIdentifier(oldColumnName));
        }

        if (alterOperation.equalsIgnoreCase(AlterType.RENAME.name())) {
            return String.format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    tableName, oldColumnName, newColumn.getName());
        }

        BasicTypeDefine<StarRocksType> typeDefine =
                StarRocksTypeConverter.INSTANCE.reconvert(newColumn);
        String basicSql = buildAlterTableBasicSql(alterOperation, tableName);
        basicSql = decorateWithColumnNameAndType(basicSql, newColumn, typeDefine.getColumnType());
        basicSql = decorateWithComment(basicSql, newColumn.getComment());
        basicSql = decorateWithAfterColumn(basicSql, afterColumn);
        return basicSql + ";";
    }

    /**
     * Check if the column exists in the table
     *
     * @param connection
     * @param tablePath
     * @param column
     * @return
     */
    public static boolean columnExists(Connection connection, TablePath tablePath, String column) {
        String selectColumnSQL =
                String.format(
                        "SELECT %s FROM %s WHERE 1 != 1",
                        quoteIdentifier(column), tablePath.getTableName());
        try (Statement statement = connection.createStatement()) {
            return statement.execute(selectColumnSQL);
        } catch (SQLException e) {
            log.debug("Column {} does not exist in table {}", column, tablePath.getFullName(), e);
            return false;
        }
    }

    /**
     * decorate the sql with column name and type
     *
     * @param basicSql basic sql of alter table for sink table
     * @param newColumn new column after ddl
     * @param columnType column type of new column
     * @return basic sql with column name and type of alter table for sink table
     */
    public static String decorateWithColumnNameAndType(
            String basicSql, Column newColumn, String columnType) {
        StringBuilder sql = new StringBuilder(basicSql);
        String newColumnNameWithQuoted = quoteIdentifier(newColumn.getName());
        sql.append(newColumnNameWithQuoted).append(StringUtils.SPACE);
        sql.append(columnType).append(StringUtils.SPACE);
        return sql.toString();
    }

    /**
     * build the body of alter table sql
     *
     * @param alterOperation alter operation of ddl
     * @param tableName table name of sink table
     * @return basic sql of alter table for sink table
     */
    public static String buildAlterTableBasicSql(String alterOperation, String tableName) {
        StringBuilder sql =
                new StringBuilder(
                        "ALTER TABLE "
                                + tableName
                                + StringUtils.SPACE
                                + alterOperation
                                + StringUtils.SPACE
                                + "COLUMN"
                                + StringUtils.SPACE);
        return sql.toString();
    }

    /**
     * decorate with comment
     *
     * @param basicSql alter table sql for sink table
     * @param comment comment of new column
     * @return alter table sql with comment for sink table
     */
    public static String decorateWithComment(String basicSql, String comment) {
        StringBuilder sql = new StringBuilder(basicSql);
        if (StringUtils.isNotBlank(comment)) {
            sql.append("COMMENT '").append(comment).append("'");
        }
        return sql.toString();
    }

    /**
     * decorate with after
     *
     * @param basicSql alter table sql for sink table
     * @param afterColumn column before the new column
     * @return alter table sql with after for sink table
     */
    public static String decorateWithAfterColumn(String basicSql, String afterColumn) {
        StringBuilder sql = new StringBuilder(basicSql);
        if (StringUtils.isNotBlank(afterColumn)) {
            sql.append("AFTER ").append(afterColumn).append(StringUtils.SPACE);
        }
        return sql.toString();
    }

    public static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    enum AlterType {
        ADD,
        DROP,
        MODIFY,
        RENAME
    }
}
