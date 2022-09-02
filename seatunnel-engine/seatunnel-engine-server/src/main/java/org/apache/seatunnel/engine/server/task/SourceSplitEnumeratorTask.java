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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.context.SeaTunnelSplitEnumeratorContext;
import org.apache.seatunnel.engine.server.task.operation.source.PrepareCloseOperation;
import org.apache.seatunnel.engine.server.task.statemachine.EnumeratorState;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class SourceSplitEnumeratorTask<SplitT extends SourceSplit> extends CoordinatorTask {

    private static final ILogger LOGGER = Logger.getLogger(SourceSplitEnumeratorTask.class);

    private static final long serialVersionUID = -3713701594297977775L;

    private final SourceAction<?, SplitT, ?> source;
    private SourceSplitEnumerator<?, ?> enumerator;
    private int maxReaderSize;
    private Set<Long> unfinishedReaders;
    private Map<TaskLocation, Address> taskMemberMapping;
    private Map<Long, TaskLocation> taskIDToTaskLocationMapping;

    private volatile EnumeratorState currState;

    private volatile boolean readerRegisterComplete;
    private volatile boolean readerFinish;

    @Override
    public void init() throws Exception {
        currState = EnumeratorState.INIT;
        super.init();
        readerRegisterComplete = false;
        readerFinish = false;
        LOGGER.info("starting seatunnel source split enumerator task, source name: " + source.getName());
        SeaTunnelSplitEnumeratorContext<SplitT> context = new SeaTunnelSplitEnumeratorContext<>(this.source.getParallelism(), this);
        enumerator = this.source.getSource().createEnumerator(context);
        taskMemberMapping = new ConcurrentHashMap<>();
        taskIDToTaskLocationMapping = new ConcurrentHashMap<>();
        maxReaderSize = source.getParallelism();
        unfinishedReaders = new CopyOnWriteArraySet<>();
        enumerator.open();
    }

    @Override
    public void close() throws IOException {
        if (enumerator != null) {
            enumerator.close();
        }
        progress.done();
    }

    public SourceSplitEnumeratorTask(long jobID, TaskLocation taskID, SourceAction<?, SplitT, ?> source) {
        super(jobID, taskID);
        this.source = source;
        this.currState = EnumeratorState.CREATED;
    }

    @NonNull
    @Override
    public ProgressState call() throws Exception {
        stateProcess();
        return progress.toState();
    }

    public void receivedReader(TaskLocation readerId, Address memberAddr) {
        LOGGER.info("received reader register, readerID: " + readerId);
        this.addTaskMemberMapping(readerId, memberAddr);
        enumerator.registerReader((int) readerId.getTaskID());
        if (maxReaderSize == taskMemberMapping.size()) {
            readerRegisterComplete = true;
        }
    }

    public void requestSplit(long taskID) {
        enumerator.handleSplitRequest((int) taskID);
    }

    public void addTaskMemberMapping(TaskLocation taskID, Address memberAddr) {
        taskMemberMapping.put(taskID, memberAddr);
        taskIDToTaskLocationMapping.put(taskID.getTaskID(), taskID);
    }

    public Address getTaskMemberAddr(long taskID) {
        return taskMemberMapping.get(taskIDToTaskLocationMapping.get(taskID));
    }

    public TaskLocation getTaskMemberLocation(long taskID) {
        return taskIDToTaskLocationMapping.get(taskID);
    }

    public void readerFinished(long taskID) {
        unfinishedReaders.remove(taskID);
        if (unfinishedReaders.isEmpty()) {
            readerFinish = true;
        }
    }

    private void stateProcess() throws Exception {
        switch (currState) {
            case INIT:
                reportReadyRestore();
                currState = EnumeratorState.WAITING_RESTORE;
                break;
            case WAITING_RESTORE:
                if (restoreComplete) {
                    reportReadyStart();
                    currState = EnumeratorState.READY_START;
                }
                break;
            case READY_START:
                if (startCalled) {
                    currState = EnumeratorState.STARTING;
                }
                break;
            case STARTING:
                if (readerRegisterComplete) {
                    currState = EnumeratorState.READER_REGISTER_COMPLETE;
                }
                break;
            case READER_REGISTER_COMPLETE:
                currState = EnumeratorState.ASSIGN;
                LOGGER.info("received enough reader, starting enumerator...");
                enumerator.run();
                break;
            case ASSIGN:
                currState = EnumeratorState.WAITING_READER_FEEDBACK;
                break;
            case WAITING_READER_FEEDBACK:
                if (readerFinish) {
                    prepareCloseAllReader();
                    prepareClose();
                    currState = EnumeratorState.PREPARE_CLOSE;
                }
                break;
            case PREPARE_CLOSE:
                if (closeCalled) {
                    currState = EnumeratorState.CLOSED;
                }
                break;
            case CLOSED:
                this.close();
                return;
            // TODO support cancel by outside
            case CANCELLING:
                this.close();
                currState = EnumeratorState.CANCELED;
                return;
            default:
                throw new IllegalArgumentException("Unknown Enumerator State: " + currState);
        }
    }

    public Set<Long> getRegisteredReaders() {
        return taskMemberMapping.keySet().stream().map(TaskLocation::getTaskID).collect(Collectors.toSet());
    }

    private void prepareCloseAllReader() {
        List<InvocationFuture<?>> futures = new ArrayList<>();
        taskMemberMapping.forEach((location, address) -> {
            futures.add(this.getExecutionContext().sendToMember(new PrepareCloseOperation(location),
                    address));
        });
        futures.forEach(InvocationFuture::join);
    }

    @Override
    public Set<URL> getJarsUrl() {
        return new HashSet<>(source.getJarUrls());
    }
}
