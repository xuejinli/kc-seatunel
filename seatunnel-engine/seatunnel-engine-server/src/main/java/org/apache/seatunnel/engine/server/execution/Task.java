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

package org.apache.seatunnel.engine.server.execution;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.NonNull;

import java.io.IOException;
import java.io.Serializable;

public interface Task extends Serializable {

    default void init() throws Exception {
    }

    @NonNull
    ProgressState call() throws Exception;

    @NonNull
    Long getTaskID();

    default boolean isCooperative() {
        return false;
    }

    default void close() throws IOException {
    }

    default void setOperationService(OperationService operationService) {
    }

    default <E> InvocationFuture<E> sendToMaster(Operation operation) {
        // TODO add method send operation to master
        return null;
    }

    default <E> InvocationFuture<E> sendToMember(Operation operation, Address memberID) {
        // TODO add method send operation to member
        return null;
    }

    default void receivedMessage(Object message) {

    }

}
