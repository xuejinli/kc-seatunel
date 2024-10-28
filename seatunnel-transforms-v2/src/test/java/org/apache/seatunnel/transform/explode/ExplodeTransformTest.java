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

package org.apache.seatunnel.transform.explode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExplodeTransformTest {

    static Map<String, String> explodeFields = new HashMap<>();
    static CatalogTable catalogTable;
    static Object[] values;

    @BeforeAll
    static void setUp() {

        explodeFields.put("key1", ",");
        explodeFields.put("key2", ";");

        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key2",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key3",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key5",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
        values = new Object[] {"value1,value2", "value3;value4", "value5", "value6", "value7"};
    }

    @Test
    void testConfig() {
        // test both not set
        try {
            new ExplodeTransform(ReadonlyConfig.fromMap(new HashMap<>()), catalogTable);
        } catch (Exception e) {
            assertEquals(
                    "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, these options('include_fields', 'exclude_fields') are mutually exclusive, allowing only one set(\"[] for a set\") of options to be configured.",
                    e.getMessage());
        }

        // test both include and exclude set
        try {
            new ExplodeTransform(
                    ReadonlyConfig.fromMap(
                            new HashMap<String, Object>() {
                                {
                                    put(ExplodeTransformConfig.EXPLODE_FIELDS.key(), explodeFields);
                                }
                            }),
                    catalogTable);
        } catch (Exception e) {
            assertEquals(
                    "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('include_fields', 'exclude_fields') are mutually exclusive, allowing only one set(\"[] for a set\") of options to be configured.",
                    e.getMessage());
        }

        // not exception should be thrown now
        new ExplodeTransform(
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(ExplodeTransformConfig.EXPLODE_FIELDS.key(), explodeFields);
                            }
                        }),
                catalogTable);
    }

    @Test
    void testExplode() {
        // default include
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ExplodeTransformConfig.EXPLODE_FIELDS.key(), explodeFields);

        ExplodeTransform explodeTransform =
                new ExplodeTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        // test output schema
        TableSchema resultSchema = explodeTransform.transformTableSchema();
        Assertions.assertNotNull(resultSchema);

        // test output row
        SeaTunnelRow input = new SeaTunnelRow(values);
        List<SeaTunnelRow> output = explodeTransform.transformRow(input);
        Assertions.assertNotNull(output);
        List<Object[]> result =
                Lists.newArrayList(
                        new Object[] {"value1", "value3", "value5", "value6", "value7"},
                        new Object[] {"value1", "value4", "value5", "value6", "value7"},
                        new Object[] {"value2", "value3", "value5", "value6", "value7"},
                        new Object[] {"value2", "value4", "value5", "value6", "value7"});

        List<Object[]> outputValues =
                output.stream().map(SeaTunnelRow::getFields).collect(Collectors.toList());
        assertEquals(outputValues.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertTrue(arraysEqual(result.get(i), outputValues.get(i)));
        }
    }

    private boolean arraysEqual(Object[] array1, Object[] array2) {
        if (array1.length != array2.length) {
            return false;
        }
        for (int i = 0; i < array1.length; i++) {
            if (!array1[i].equals(array2[i])) {
                return false;
            }
        }
        return true;
    }
}
