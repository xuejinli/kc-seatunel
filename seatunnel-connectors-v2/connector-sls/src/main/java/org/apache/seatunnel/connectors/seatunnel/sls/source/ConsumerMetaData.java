package org.apache.seatunnel.connectors.seatunnel.sls.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.sls.config.StartMode;
import org.apache.seatunnel.connectors.seatunnel.sls.serialization.FastLogDeserialization;

import com.aliyun.openservices.log.common.Consts;
import lombok.Data;

import java.io.Serializable;

@Data
public class ConsumerMetaData implements Serializable {
    private String project;
    private String logstore;
    private String consumerGroup;
    private StartMode startMode;
    private Consts.CursorMode autoCursorReset;
    private int fetchSize;
    private FastLogDeserialization<SeaTunnelRow> deserializationSchema;
    private CatalogTable catalogTable;
}
