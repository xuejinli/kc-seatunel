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

package org.apache.seatunnel.connectors.seatunnel.redshift.commit;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.redshift.RedshiftJdbcClient;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfig;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftJdbcException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class S3RedshiftSinkAggregatedCommitter extends FileSinkAggregatedCommitter {

    private final String executeSql;

    private Config pluginConfig;

    public S3RedshiftSinkAggregatedCommitter(HadoopConf hadoopConf, Config pluginConfig) {
        super(hadoopConf);
        this.pluginConfig = pluginConfig;
        this.executeSql = pluginConfig.getString(S3RedshiftConfig.EXECUTE_SQL.key());
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        List<FileAggregatedCommitInfo> errorAggregatedCommitInfoList = new ArrayList<>();
        aggregatedCommitInfos.forEach(aggregatedCommitInfo -> {
            try {
                for (Map.Entry<String, Map<String, String>> entry : aggregatedCommitInfo.getTransactionMap().entrySet()) {
                    for (Map.Entry<String, String> tmpFileEntry : entry.getValue().entrySet()) {
                        String sql = convertSql(tmpFileEntry.getKey());
                        log.debug("execute redshift sql is:" + sql);
                        RedshiftJdbcClient.getInstance(pluginConfig).execute(sql);
                        try {
                            FileSystemUtils.deleteFile(tmpFileEntry.getKey());
                        } catch (IOException e) {
                            log.warn("delete tmp file error:" + tmpFileEntry.getKey());
                        }
                    }

                }

            } catch (Exception e) {
                log.error("commit aggregatedCommitInfo error ", e);
                errorAggregatedCommitInfoList.add(aggregatedCommitInfo);
            }
        });
        return errorAggregatedCommitInfoList;
    }

    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfos) {
        if (aggregatedCommitInfos == null || aggregatedCommitInfos.isEmpty()) {
            return;
        }
        aggregatedCommitInfos.forEach(aggregatedCommitInfo -> {
            try {
                for (Map.Entry<String, Map<String, String>> entry : aggregatedCommitInfo.getTransactionMap().entrySet()) {
                    // delete the transaction dir
                    FileSystemUtils.deleteFile(entry.getKey());
                }
            } catch (Exception e) {
                log.error("abort aggregatedCommitInfo error ", e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            RedshiftJdbcClient.getInstance(pluginConfig).close();
        } catch (SQLException e) {
            log.error("close redshift jdbc client error", e);
            throw new S3RedshiftJdbcException("close redshift jdbc client error", e);
        }
    }

    private String convertSql(String path) {
        return StringUtils.replace(executeSql, "${path}", path);
    }

}
