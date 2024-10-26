package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.common.env.EnvironmentUtil;
import org.apache.seatunnel.engine.common.env.Version;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.GetOverviewOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.OverviewInfo;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;

public class OverviewService extends BaseService {

    private final NodeEngineImpl nodeEngine;

    public OverviewService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.nodeEngine = nodeEngine;
    }

    public OverviewInfo getOverviewInfo(Map<String, String> tags) {
        Version version = EnvironmentUtil.getVersion();
        OverviewInfo overviewInfo;

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);

        if (seaTunnelServer == null) {
            overviewInfo =
                    (OverviewInfo)
                            NodeEngineUtil.sendOperationToMasterNode(
                                            nodeEngine, new GetOverviewOperation(tags))
                                    .join();
        } else {
            overviewInfo = GetOverviewOperation.getOverviewInfo(seaTunnelServer, nodeEngine, tags);
        }

        overviewInfo.setProjectVersion(version.getProjectVersion());
        overviewInfo.setGitCommitAbbrev(version.getGitCommitAbbrev());

        return overviewInfo;
    }
}
