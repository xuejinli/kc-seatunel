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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;

import io.debezium.relational.Column;
import lombok.extern.slf4j.Slf4j;

/** Utilities for converting from MySQL types to SeaTunnel types. */
@Slf4j
public class MySqlTypeUtils {

    public static SeaTunnelDataType<?> convertFromColumn(Column column) {
        return convertToSeaTunnelColumn(column).getDataType();
    }

    public static org.apache.seatunnel.api.table.catalog.Column convertToSeaTunnelColumn(
            io.debezium.relational.Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.name())
                        .columnType(column.typeName())
                        .dataType(column.typeName())
                        .length((long) column.length())
                        .precision((long) column.length())
                        .scale(column.scale().orElse(0))
                        .defaultValue(column.defaultValueExpression().orElse("NULL"));
        switch (column.typeName().toUpperCase()) {
            case MySqlTypeConverter.MYSQL_CHAR:
            case MySqlTypeConverter.MYSQL_VARCHAR:
                if (column.length() <= 0) {
                    // set default length
                    builder.columnType(MySqlTypeConverter.MYSQL_VARCHAR);
                    builder.length(TypeDefineUtils.charTo4ByteLength(1L));
                } else {
                    // parse length from ddl sql
                    builder.columnType(
                            String.format(
                                    "%s(%s)", MySqlTypeConverter.MYSQL_VARCHAR, column.length()));
                    builder.length(TypeDefineUtils.charTo4ByteLength((long) column.length()));
                }
                break;
            case MySqlTypeConverter.MYSQL_TIME:
                if (column.length() <= 0) {
                    builder.columnType(MySqlTypeConverter.MYSQL_TIME);
                } else {
                    builder.columnType(
                            String.format(
                                    "%s(%s)", MySqlTypeConverter.MYSQL_TIME, column.length()));
                    builder.scale(column.length());
                }
                break;
            case MySqlTypeConverter.MYSQL_TIMESTAMP:
                if (column.length() <= 0) {
                    builder.columnType(MySqlTypeConverter.MYSQL_TIMESTAMP);
                } else {
                    builder.columnType(
                            String.format(
                                    "%s(%s)", MySqlTypeConverter.MYSQL_TIMESTAMP, column.length()));
                    builder.scale(column.length());
                }
                break;
            case MySqlTypeConverter.MYSQL_DATETIME:
                if (column.length() <= 0) {
                    builder.columnType(MySqlTypeConverter.MYSQL_DATETIME);
                } else {
                    builder.columnType(
                            String.format(
                                    "%s(%s)", MySqlTypeConverter.MYSQL_DATETIME, column.length()));
                    builder.scale(column.length());
                }
                break;
            default:
                break;
        }
        return MySqlTypeConverter.DEFAULT_INSTANCE.convert(builder.build());
    }
}
