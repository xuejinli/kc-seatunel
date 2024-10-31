package org.apache.seatunnel.connectors.seatunnel.file.juicefs.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.juicefs.source.config.MultipleTableJuicefsFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseMultipleTableFileSource;

public class JuicefsFileSource extends BaseMultipleTableFileSource {

    public JuicefsFileSource(ReadonlyConfig readonlyConfig) {
        super(new MultipleTableJuicefsFileSourceConfig(readonlyConfig));
    }

    @Override
    public String getPluginName() {
        return FileSystemType.JUICEFS.getFileSystemPluginName();
    }
}
