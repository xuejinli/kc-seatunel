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

package org.apache.seatunnel.api.arrow.reader;

import org.apache.seatunnel.shade.org.apache.arrow.memory.RootAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.vector.FieldVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;
import org.apache.seatunnel.shade.org.apache.arrow.vector.util.Text;

import org.apache.seatunnel.api.arrow.converter.Converter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;

@Slf4j
public class Arrow2SeatunnelRowReader implements AutoCloseable {

    private final SeaTunnelDataType<?>[] seaTunnelDataTypes;
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<FieldVector> fieldVectors;
    private VectorSchemaRoot root;
    private final ArrowStreamReader arrowStreamReader;
    private final RootAllocator rootAllocator;
    private final Map<String, Integer> fieldIndexMap = new HashMap<>();
    private final List<SeaTunnelRow> seatunnelRowBatch = new ArrayList<>();
    private final List<Converter> converters = new ArrayList<>();
    private final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public Arrow2SeatunnelRowReader(byte[] byteArray, SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelDataTypes = seaTunnelRowType.getFieldTypes();
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader =
                new ArrowStreamReader(new ByteArrayInputStream(byteArray), rootAllocator);
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            fieldIndexMap.put(seaTunnelRowType.getFieldNames()[i], i);
        }
        ServiceLoader<Converter> load = ServiceLoader.load(Converter.class);
        load.forEach(converters::add);
    }

    public Arrow2SeatunnelRowReader readArrow() {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                this.fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                    log.debug("one batch in arrow has no data.");
                    continue;
                }
                log.info("one batch in arrow row count size '{}'", root.getRowCount());
                this.rowCountInOneBatch = root.getRowCount();
                for (int i = 0; i < rowCountInOneBatch; i++) {
                    seatunnelRowBatch.add(new SeaTunnelRow(this.seaTunnelDataTypes.length));
                }
                convertSeatunnelRow();
                this.readRowCount += root.getRowCount();
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            close();
        }
    }

    public boolean hasNext() {
        return offsetInRowBatch < readRowCount;
    }

    public SeaTunnelRow next() {
        if (!hasNext()) {
            throw new IllegalStateException("no more rows to read.");
        }
        return seatunnelRowBatch.get(offsetInRowBatch++);
    }

    private void convertSeatunnelRow() {
        for (FieldVector fieldVector : fieldVectors) {
            String name = fieldVector.getField().getName();
            Integer fieldIndex = fieldIndexMap.get(name);
            Types.MinorType minorType = fieldVector.getMinorType();
            for (int i = 0; i < seatunnelRowBatch.size(); i++) {
                // arrow field not in the Seatunnel Sechma field, skip it
                if (fieldIndex != null) {
                    SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[fieldIndex];
                    Object fieldValue =
                            convertArrowData(
                                    readRowCount + i, minorType, fieldVector, seaTunnelDataType);
                    fieldValue =
                            convertSeatunnelRowValue(
                                    seaTunnelDataType.getSqlType(), minorType, fieldValue);
                    seatunnelRowBatch.get(readRowCount + i).setField(fieldIndex, fieldValue);
                }
            }
        }
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    private Object convertSeatunnelRowValue(
            SqlType currentType, Types.MinorType minorType, Object fieldValue) {
        switch (currentType) {
            case STRING:
                if (fieldValue instanceof byte[]) {
                    return new String((byte[]) fieldValue);
                } else if (fieldValue instanceof Text) {
                    return ((Text) fieldValue).toString();
                } else {
                    return fieldValue;
                }
            case DECIMAL:
                if (fieldValue instanceof String) {
                    return new BigDecimal((String) fieldValue);
                } else if (fieldValue instanceof Text) {
                    return new BigDecimal(((Text) fieldValue).toString());
                } else {
                    return fieldValue;
                }
            case DATE:
                if (fieldValue instanceof Integer) {
                    return LocalDate.ofEpochDay((Integer) fieldValue);
                } else if (fieldValue instanceof Long) {
                    return LocalDate.ofEpochDay((Long) fieldValue);
                } else if (fieldValue instanceof String) {
                    return LocalDate.parse((String) fieldValue, DATE_FORMATTER);
                } else if (fieldValue instanceof Text) {
                    return LocalDate.parse(((Text) fieldValue).toString(), DATE_FORMATTER);
                } else if (fieldValue instanceof LocalDateTime) {
                    return ((LocalDateTime) fieldValue).toLocalDate();
                } else {
                    return fieldValue;
                }
            case TIME:
                if (fieldValue instanceof Integer) {
                    return LocalTime.ofSecondOfDay((Integer) fieldValue);
                } else if (fieldValue instanceof Long) {
                    return Instant.ofEpochMilli((Long) fieldValue)
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime()
                            .toLocalTime();
                } else if (fieldValue instanceof String) {
                    return LocalTime.parse((String) fieldValue, TIME_FORMATTER);
                } else if (fieldValue instanceof Text) {
                    return LocalTime.parse(((Text) fieldValue).toString(), TIME_FORMATTER);
                } else {
                    return fieldValue;
                }
            case TIMESTAMP:
                if (fieldValue instanceof Long) {
                    return Instant.ofEpochMilli((Long) fieldValue)
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime();
                } else if (fieldValue instanceof String) {
                    return LocalDateTime.parse((String) fieldValue, DATETIME_FORMATTER);
                } else if (fieldValue instanceof Text) {
                    return LocalDateTime.parse(((Text) fieldValue).toString(), DATETIME_FORMATTER);
                } else {
                    return fieldValue;
                }
            default:
                return fieldValue;
        }
    }

    private Object convertArrowData(
            int rowIndex,
            Types.MinorType minorType,
            FieldVector fieldVector,
            SeaTunnelDataType<?> seaTunnelDataType) {
        for (Converter converter : converters) {
            if (converter.support(minorType)) {
                SqlType sqlType = seaTunnelDataType.getSqlType();
                if (SqlType.MAP == sqlType) {
                    MapType mapType = (MapType) seaTunnelDataType;
                    SqlType keyType = mapType.getKeyType().getSqlType();
                    SqlType valueType = mapType.getValueType().getSqlType();
                    return converter.convert(
                            rowIndex,
                            fieldVector,
                            Arrays.asList(genericsConvert(keyType), genericsConvert(valueType)));
                } else if (SqlType.ARRAY == sqlType) {
                    ArrayType arrayType = (ArrayType) seaTunnelDataType;
                    SqlType elementType = arrayType.getElementType().getSqlType();
                    return converter.convert(
                            rowIndex,
                            fieldVector,
                            Collections.singletonList(genericsConvert(elementType)));
                } else {
                    return converter.convert(rowIndex, fieldVector);
                }
            }
        }
        log.error("No converter found for Arrow MinorType: {}", minorType);
        throw new IllegalStateException("Unsupported Arrow MinorType: " + minorType);
    }

    private Function<Object, Object> genericsConvert(SqlType sqlType) {
        return value -> convertSeatunnelRowValue(sqlType, null, value);
    }

    @Override
    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
            if (root != null) {
                root.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to close arrow stream reader.", e);
        }
    }
}
