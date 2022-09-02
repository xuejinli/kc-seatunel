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

package org.apache.seatunnel.engine.server.task.statemachine;

import java.io.Serializable;

/**
 * The state of {@link  org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask},
 * The task usually start in the state {@code CREATED} and switch states according to this diagram:
 * <p>
 * CREATED -> INIT  -> WAITING_RESTORE -> READY_START -> READER_REGISTER_COMPLETE  -> ASSIGN -> WAITING_FEEDBACK -> PREPARE_CLOSE -> CLOSED
 * |        |          |                          |              |                       |              |
 * |        |          |                          |              |                       |              |
 * |        |          |                          |              |                       |              |
 * |        |          |                          |              |                       |              |
 * +--------+----------+--------------------------+--------------+-----------------------+--------------+> CANCELLING ----> CANCELED
 * ... -> FAILED
 */
public enum EnumeratorState implements Serializable {
    CREATED,
    INIT,
    WAITING_RESTORE,
    READY_START,
    STARTING,
    READER_REGISTER_COMPLETE,
    ASSIGN,
    WAITING_READER_FEEDBACK,
    PREPARE_CLOSE,
    CLOSED,
    CANCELLING,
    CANCELED,
    FAILED
}
