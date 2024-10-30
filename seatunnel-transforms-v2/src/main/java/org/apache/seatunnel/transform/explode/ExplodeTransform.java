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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogMultiRowTransform;
import org.apache.seatunnel.transform.common.CommonOptions;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.replace.ReplaceTransformConfig;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class ExplodeTransform extends AbstractCatalogMultiRowTransform {
    protected CatalogTable inputCatalogTable;
    public static final String PLUGIN_NAME = "Explode";
    protected Map<String, String> explodeStringFields;
    protected List<String> explodeListFields;
    protected SeaTunnelRowType seaTunnelRowType;
    private int[] fieldsIndex;
    private ReadonlyConfig config;

    public ExplodeTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable, CommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.defaultValue());
        this.config = config;
        this.explodeStringFields = config.get(ExplodeTransformConfig.EXPLODE_STRING_FIELDS);
        this.explodeListFields = config.get(ExplodeTransformConfig.EXPLODE_LIST_FIELDS);
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.inputCatalogTable = catalogTable;
    }

    /**
     * Outputs transformed row data.
     *
     * @param inputRow upstream input row data
     */
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        List<SeaTunnelRow> rows = Lists.newArrayList(inputRow);
        if (MapUtils.isNotEmpty(explodeStringFields)) {
            for (Map.Entry<String, String> entry : explodeStringFields.entrySet()) {
                List<SeaTunnelRow> next = new ArrayList<>();
                for (SeaTunnelRow row : rows) {

                    String field = entry.getKey();
                    int fieldIndex = seaTunnelRowType.indexOf(field);
                    Object splitFieldValue = inputRow.getField(fieldIndex);
                    if (splitFieldValue == null) {
                        continue;
                    }
                    String separator = entry.getValue();
                    String[] splitFieldValues = splitFieldValue.toString().split(separator);
                    for (String fieldValue : splitFieldValues) {
                        SeaTunnelRow outputRow = row.copy();
                        outputRow.setField(fieldIndex, fieldValue);
                        next.add(outputRow);
                    }
                }
                rows = next;
            }
        }
        if (!CollectionUtils.isEmpty(explodeListFields)) {
            for (String field : explodeListFields) {
                List<SeaTunnelRow> next = new ArrayList<>();
                for (SeaTunnelRow row : rows) {
                    int fieldIndex = seaTunnelRowType.indexOf(field);
                    Object splitFieldValue = inputRow.getField(fieldIndex);
                    if (splitFieldValue == null) {
                        continue;
                    }
                    if (splitFieldValue instanceof Object[]) {
                        Object[] rowList = (Object[]) splitFieldValue;
                        for (Object fieldValue : rowList) {
                            SeaTunnelRow outputRow = row.copy();
                            outputRow.setField(fieldIndex, fieldValue);
                            next.add(outputRow);
                        }
                    }
                }
                rows = next;
            }
        }

        return rows;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected TableSchema transformTableSchema() {
        Column[] outputColumns = getOutputColumns();

        List<ConstraintKey> copiedConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        TableSchema.Builder builder = TableSchema.builder();
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null) {
            builder.primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey().copy());
        }
        builder.constraintKey(copiedConstraintKeys);
        List<Column> columns =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .map(Column::copy)
                        .collect(Collectors.toList());

        int addFieldCount = 0;
        this.fieldsIndex = new int[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) {
            Column outputColumn = outputColumns[i];
            Optional<Column> optional =
                    columns.stream()
                            .filter(c -> c.getName().equals(outputColumn.getName()))
                            .findFirst();
            if (optional.isPresent()) {
                Column originalColumn = optional.get();
                int originalColumnIndex = columns.indexOf(originalColumn);
                if (!originalColumn.getDataType().equals(outputColumn.getDataType())) {
                    columns.set(
                            originalColumnIndex, originalColumn.copy(outputColumn.getDataType()));
                }
                fieldsIndex[i] = originalColumnIndex;
            } else {
                addFieldCount++;
                columns.add(outputColumn);
                fieldsIndex[i] = columns.indexOf(outputColumn);
            }
        }

        TableSchema outputTableSchema = builder.columns(columns).build();
        log.info(
                "Changed input table schema: {} to output table schema: {}",
                inputCatalogTable.getTableSchema(),
                outputTableSchema);

        return outputTableSchema;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    protected Column[] getOutputColumns() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();

        List<Column> collect =
                columns.stream()
                        .filter(
                                column ->
                                        explodeStringFields.containsKey(column.getName())
                                                || explodeListFields.contains(column.getName()))
                        .map(
                                column -> {
                                    if (explodeListFields.contains(column.getName())) {
                                        ArrayType arrayType = (ArrayType) column.getDataType();
                                        return PhysicalColumn.of(
                                                column.getName(),
                                                arrayType.getElementType(),
                                                200,
                                                true,
                                                "",
                                                "");
                                    }
                                    return column;
                                })
                        .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(collect)) {
            throw TransformCommonError.cannotFindInputFieldError(
                    getPluginName(), config.get(ReplaceTransformConfig.KEY_REPLACE_FIELD));
        }
        return collect.toArray(new Column[0]);
    }
}
