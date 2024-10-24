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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.trino;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TrinoTypeMapper implements JdbcDialectTypeMapper {
    // ============================data types=====================

    private static final String TRINO_BOOLEAN = "BOOLEAN";

    // -------------------------Structural----------------------------
    private static final String TRINO_ARRAY = "ARRAY";
    private static final String TRINO_MAP = "MAP";
    private static final String TRINO_ROW = "ROW";

    // -------------------------number----------------------------
    private static final String TRINO_TINYINT = "TINYINT";
    private static final String TRINO_SMALLINT = "SMALLINT";
    private static final String TRINO_INTEGER = "INTEGER";
    private static final String TRINO_BIGINT = "BIGINT";
    private static final String TRINO_DECIMAL = "DECIMAL";
    private static final String TRINO_REAL = "REAL";
    private static final String TRINO_DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    private static final String TRINO_CHAR = "CHAR";
    private static final String TRINO_VARCHAR = "VARCHAR";
    private static final String TRINO_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String TRINO_DATE = "DATE";
    private static final String TRINO_TIME = "TIME";
    private static final String TRINO_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String TRINO_BINARY = "BINARY";
    private static final String TRINO_VARBINARY = "VARBINARY";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String columnType = metadata.getColumnTypeName(colIndex).toUpperCase();
        // VARCHAR(x)      --->      VARCHAR
        if (columnType.indexOf("(") > -1) {
            columnType = columnType.split("\\(")[0];
        }
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (columnType) {
            case TRINO_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TRINO_TINYINT:
                return BasicType.BYTE_TYPE;
            case TRINO_INTEGER:
                return BasicType.INT_TYPE;
            case TRINO_SMALLINT:
                return BasicType.SHORT_TYPE;
            case TRINO_BIGINT:
                return BasicType.LONG_TYPE;
            case TRINO_DECIMAL:
                return new DecimalType(precision, scale);
            case TRINO_REAL:
                return BasicType.FLOAT_TYPE;
            case TRINO_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case TRINO_CHAR:
            case TRINO_VARCHAR:
            case TRINO_JSON:
                return BasicType.STRING_TYPE;
            case TRINO_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TRINO_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TRINO_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case TRINO_VARBINARY:
            case TRINO_BINARY:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case TRINO_MAP:
            case TRINO_ARRAY:
            case TRINO_ROW:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support TRINO type '%s' on column '%s'  yet.",
                                columnType, jdbcColumnName));
        }
    }
}
