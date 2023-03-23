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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.committer.HudiSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer.HudiSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiSinkState;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class HudiSink
        implements SeaTunnelSink<
                SeaTunnelRow, HudiSinkState, HudiCommitInfo, HudiAggregatedCommitInfo> {

    private Config pluginConfig;
    private HudiSinkConfig hudiSinkConfig;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "Hudi";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
        this.hudiSinkConfig = HudiSinkConfig.of(config);
        this.pluginConfig = pluginConfig;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> restoreWriter(
            SinkWriter.Context context, List<HudiSinkState> states) throws IOException {
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<Serializer<HudiSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<HudiCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<HudiCommitInfo, HudiAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(new HudiSinkAggregatedCommitter(hudiSinkConfig));
    }

    @Override
    public Optional<Serializer<HudiAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        HudiSinkWriter hudiSinkWriter =
                new HudiSinkWriter(
                        context, Collections.emptyList(), seaTunnelRowType, pluginConfig);
        return hudiSinkWriter;
    }
}
