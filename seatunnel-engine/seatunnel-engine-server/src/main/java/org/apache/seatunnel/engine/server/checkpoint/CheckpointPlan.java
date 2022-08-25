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

package org.apache.seatunnel.engine.server.checkpoint;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * checkpoint plan info
 */
@Getter
@Builder(builderClassName = "Builder")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CheckpointPlan {

    private final int pipelineId;

    /**
     * all task ids of the pipeline.
     * <br> key: task id;
     * <br> value: group id;
     */
    private final Map<Long, Long> pipelineTaskIds;

    /**
     * All starting task ids of a pipeline.
     */
    private final Set<Long> startingTasks;

    /**
     * Restored task state.
     * <br> key: job vertex id;
     * <br> value: job vertex state;
     */
    private final Map<Long, TaskState> restoredTaskState;

    /**
     * All stateful vertices in this pipeline.
     * <br> key: the job vertex id;
     * <br> value: the parallelism of the job vertex;
     */
    private final Map<Long, Integer> statefulVertices;

    /**
     * All vertices of this pipeline.
     * <br> key: the job vertex id;
     * <br> value: the parallelism of the job vertex;
     */
    private final Map<Long, Integer> allVertices;

    public static final class Builder {
        private final Map<Long, Long> pipelineTaskIds = new HashMap<>();
        private final Set<Long> startingTasks = new HashSet<>();
        private final Map<Long, TaskState> restoredTaskState = new HashMap<>();
        private final Map<Long, Integer> statefulVertices = new HashMap<>();

        private final Map<Long, Integer> allVertices = new HashMap<>();

        private Builder() {
        }

        public Builder pipelineTaskIds(Map<Long, Long> pipelineTaskIds) {
            this.pipelineTaskIds.putAll(pipelineTaskIds);
            return this;
        }

        public Builder startingTasks(Set<Long> startingVertices) {
            this.startingTasks.addAll(startingVertices);
            return this;
        }

        public Builder restoredTaskState(Map<Long, TaskState> restoredTaskState) {
            this.restoredTaskState.putAll(restoredTaskState);
            return this;
        }

        public Builder statefulVertices(Map<Long, Integer> statefulVertices) {
            this.statefulVertices.putAll(statefulVertices);
            return this;
        }

        public Builder allVertices(Map<Long, Integer> allVertices) {
            this.allVertices.putAll(allVertices);
            return this;
        }
    }
}
