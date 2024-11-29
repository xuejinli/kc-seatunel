package org.apache.seatunnel.transform.rename;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableRenametransformTest {

    private static final CatalogTable DEFAULT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("mysql-1", "database-x", null, "Table-x"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "f1",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            false,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f2",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            true,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f3",
                                            BasicType.LONG_TYPE,
                                            null,
                                            null,
                                            true,
                                            null,
                                            null))
                            .primaryKey(PrimaryKey.of("pk1", Arrays.asList("f1")))
                            .constraintKey(
                                    ConstraintKey.of(
                                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                                            "uk1",
                                            Arrays.asList(
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f2", ConstraintKey.ColumnSortType.ASC),
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f3",
                                                            ConstraintKey.ColumnSortType.ASC))))
                            .build(),
                    Collections.emptyMap(),
                    Collections.singletonList("f2"),
                    null);

    @Test
    public void testRename() {
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, 1L, 1L});
        inputRow.setTableId(DEFAULT_TABLE.getTablePath().getFullName());
        AlterTableAddColumnEvent inputEvent =
                AlterTableAddColumnEvent.add(
                        DEFAULT_TABLE.getTableId(),
                        PhysicalColumn.of("f4", BasicType.LONG_TYPE, null, null, true, null, null));

        TableRenameConfig config = new TableRenameConfig().setConvertCase(ConvertCase.LOWER);

        TableRenameTransform transform = new TableRenameTransform(config, DEFAULT_TABLE);
        List<CatalogTable> outputCatalogTable = transform.getProducedCatalogTables();
        SeaTunnelRow outputRow = transform.map(inputRow);
        SchemaChangeEvent outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "database-x.table-x",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-x.table-x", outputRow.getTableId());
        Assertions.assertEquals("database-x.table-x", outputEvent.tablePath().getFullName());

        config = new TableRenameConfig().setConvertCase(ConvertCase.UPPER);
        transform = new TableRenameTransform(config, DEFAULT_TABLE);
        outputCatalogTable = transform.getProducedCatalogTables();
        outputRow = transform.map(inputRow);
        outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "DATABASE-X.TABLE-X",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("DATABASE-X.TABLE-X", outputRow.getTableId());
        Assertions.assertEquals("DATABASE-X.TABLE-X", outputEvent.tablePath().getFullName());

        config = new TableRenameConfig().setPrefix("user-").setSuffix("-table");
        transform = new TableRenameTransform(config, DEFAULT_TABLE);
        outputCatalogTable = transform.getProducedCatalogTables();
        outputRow = transform.map(inputRow);
        outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "database-x.user-Table-x-table",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-x.user-Table-x-table", outputRow.getTableId());
        Assertions.assertEquals(
                "database-x.user-Table-x-table", outputEvent.tablePath().getFullName());

        config =
                new TableRenameConfig()
                        .setReplacementsWithRegex(
                                Arrays.asList(
                                        new TableRenameConfig.ReplacementsWithRegex("Table", "t1"),
                                        new TableRenameConfig.ReplacementsWithRegex(
                                                "Table", "t2")));
        transform = new TableRenameTransform(config, DEFAULT_TABLE);
        outputCatalogTable = transform.getProducedCatalogTables();
        outputRow = transform.map(inputRow);
        outputEvent = transform.mapSchemaChangeEvent(inputEvent);
        Assertions.assertEquals(
                "database-x.t2-x",
                outputCatalogTable.get(0).getTableId().toTablePath().getFullName());
        Assertions.assertEquals("database-x.t2-x", outputRow.getTableId());
        Assertions.assertEquals("database-x.t2-x", outputEvent.tablePath().getFullName());
    }
}
