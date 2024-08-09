package org.apache.seatunnel.connectors.seatunnel.sls.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.schema.ReadonlyConfigParser;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.sls.serialization.FastLogDeserialization;
import org.apache.seatunnel.connectors.seatunnel.sls.serialization.FastLogDeserializationContent;
import org.apache.seatunnel.connectors.seatunnel.sls.serialization.FastLogDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.ACCESS_KEY_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.AUTO_CURSOR_RESET;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.CONSUMER_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.LOGSTORE;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.START_MODE;

public class SlsSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @Getter private final String endpoint;
    @Getter private final String accessKeyId;
    @Getter private final String accessKeySecret;
    @Getter private final CatalogTable catalogTable;
    @Getter private final ConsumerMetaData consumerMetaData;

    public SlsSourceConfig(ReadonlyConfig readonlyConfig) {
        this.endpoint = readonlyConfig.get(ENDPOINT);
        this.accessKeyId = readonlyConfig.get(ACCESS_KEY_ID);
        this.accessKeySecret = readonlyConfig.get(ACCESS_KEY_SECRET);
        this.catalogTable = createCatalogTable(readonlyConfig);
        this.consumerMetaData = createMetaData(readonlyConfig);
    }

    /** only single endpoint logstore */
    public ConsumerMetaData createMetaData(ReadonlyConfig readonlyConfig) {
        ConsumerMetaData consumerMetaData = new ConsumerMetaData();
        consumerMetaData.setProject(readonlyConfig.get(PROJECT));
        consumerMetaData.setLogstore(readonlyConfig.get(LOGSTORE));
        consumerMetaData.setConsumerGroup(readonlyConfig.get(CONSUMER_GROUP));
        consumerMetaData.setStartMode(readonlyConfig.get(START_MODE));
        consumerMetaData.setFetchSize(readonlyConfig.get(BATCH_SIZE));
        consumerMetaData.setAutoCursorReset(readonlyConfig.get(AUTO_CURSOR_RESET));
        consumerMetaData.setDeserializationSchema(createDeserializationSchema(readonlyConfig));
        consumerMetaData.setCatalogTable(catalogTable);
        return consumerMetaData;
    }

    private CatalogTable createCatalogTable(ReadonlyConfig readonlyConfig) {
        Optional<Map<String, Object>> schemaOptions =
                readonlyConfig.getOptional(TableSchemaOptions.SCHEMA);
        TablePath tablePath = TablePath.of(readonlyConfig.get(LOGSTORE));
        TableSchema tableSchema;
        if (schemaOptions.isPresent()) {
            tableSchema = new ReadonlyConfigParser().parse(readonlyConfig);
        } else {
            // no scheam, all value in content filed
            tableSchema =
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "content", BasicType.STRING_TYPE, 0, false, "{}", null))
                            .build();
        }
        return CatalogTable.of(
                TableIdentifier.of("", tablePath),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private FastLogDeserialization<SeaTunnelRow> createDeserializationSchema(
            ReadonlyConfig readonlyConfig) {
        Optional<Map<String, Object>> schemaOptions =
                readonlyConfig.getOptional(TableSchemaOptions.SCHEMA);
        FastLogDeserialization fastLogDeserialization;
        if (schemaOptions.isPresent()) {
            fastLogDeserialization = new FastLogDeserializationSchema(catalogTable);

        } else {
            fastLogDeserialization = new FastLogDeserializationContent(catalogTable);
        }
        return fastLogDeserialization;
    }

    private DeserializationSchema<SeaTunnelRow> createDeserializationSchema(
            CatalogTable catalogTable) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        return TextDeserializationSchema.builder()
                .seaTunnelRowType(seaTunnelRowType)
                .delimiter(TextFormatConstant.PLACEHOLDER)
                .build();
    }
}
