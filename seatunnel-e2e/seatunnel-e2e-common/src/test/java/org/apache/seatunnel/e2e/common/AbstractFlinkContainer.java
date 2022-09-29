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

package org.apache.seatunnel.e2e.common;

import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.io.IOException;

/**
 * This class is the base class of FlinkEnvironment test.
 * The before method will create a Flink cluster, and after method will close the Flink cluster.
 * You can use {@link AbstractFlinkContainer#executeJob} to submit a seatunnel config and run a seatunnel job.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractFlinkContainer extends AbstractTestFlinkContainer {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        log.info("The TestContainer[{}] is running.", identifier());
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
        log.info("The TestContainer[{}] is closed.", identifier());
    }

    public Container.ExecResult executeSeaTunnelFlinkJob(String confFile) throws IOException, InterruptedException {
        return executeJob(confFile);
    }
}
