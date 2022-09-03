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

package org.apache.seatunnel.engine.server.checkpoint.operation;

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneakyThrow;

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;

@Getter
@AllArgsConstructor
public class CheckpointFinishedOperation extends Operation implements IdentifiedDataSerializable {

    private long checkpointId;

    private TaskLocation taskLocation;

    private boolean successful;

    public CheckpointFinishedOperation() {
    }

    @Override
    public int getFactoryId() {
        return CheckpointDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CheckpointDataSerializerHook.CHECKPOINT_FINISHED_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(checkpointId);
        out.writeObject(taskLocation);
        out.writeBoolean(successful);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        checkpointId = in.readLong();
        taskLocation = in.readObject(TaskLocation.class);
        successful = in.readBoolean();
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        RetryUtils.retryWithException(() -> {
            Task task = server.getTaskExecutionService().getExecutionContext(taskLocation.getTaskGroupLocation())
                .getTaskGroup().getTask(taskLocation.getTaskID());
            try {
                if (successful) {
                    task.notifyCheckpointComplete(checkpointId);
                } else {
                    task.notifyCheckpointAborted(checkpointId);
                }
            } catch (Exception e) {
                sneakyThrow(e);
            }
            return null;
        }, new RetryUtils.RetryMaterial(Constant.OPERATION_RETRY_TIME, true,
            exception -> exception instanceof NullPointerException, Constant.OPERATION_RETRY_SLEEP));
    }
}
