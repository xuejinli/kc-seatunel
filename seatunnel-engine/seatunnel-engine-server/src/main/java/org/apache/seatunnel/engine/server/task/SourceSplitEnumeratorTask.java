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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.core.dag.actions.PhysicalSourceAction;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class SourceSplitEnumeratorTask extends CoordinatorTask {

    private static final long serialVersionUID = -3713701594297977775L;

    private final PhysicalSourceAction<?, ?, ?> source;
    private SourceSplitEnumerator<?, ?> enumerator;

    @Override
    public void init() throws Exception {
        enumerator = this.source.getSource()
                .createEnumerator(new SeaTunnelSplitEnumeratorContext<>(this.source.getParallelism(), this.operationService));
        enumerator.open();
    }

    @Override
    public void close() throws IOException {
        if (enumerator != null) {
            enumerator.close();
        }
    }

    public SourceSplitEnumeratorTask(long taskID, PhysicalSourceAction<?, ?, ?> source) {
        super(taskID);
        this.source = source;
    }

    private void receivedReader(int readId) {
        enumerator.registerReader(readId);
    }

    private void requestSplit(int taskID) {
        enumerator.handleSplitRequest(taskID);
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(source.getJarUrls());
    }
}
