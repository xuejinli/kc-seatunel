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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TransformFlowLifeCycle<T> extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>> {

    private final TransformChainAction<T> action;

    private final List<SeaTunnelTransform<T>> transform;

    private final Collector<Record<?>> collector;

    public TransformFlowLifeCycle(TransformChainAction<T> action,
                                  SeaTunnelTask runningTask,
                                  Collector<Record<?>> collector,
                                  CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
        this.action = action;
        this.transform = action.getTransforms();
        this.collector = collector;
    }

    @Override
    public void received(Record<?> record) {
        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            runningTask.ack(barrier);
            if (barrier.prepareClose()) {
                prepareClose = true;
            }
            if (barrier.snapshot()) {
                runningTask.addState(barrier, action.getId(), Collections.emptyList());
            }
            collector.collect(record);
        } else {
            if (prepareClose) {
                return;
            }
            T r = (T) record.getData();
            for (SeaTunnelTransform<T> t : transform) {
                r = t.map(r);
            }
            collector.collect(new Record<>(r));
        }
    }
}
