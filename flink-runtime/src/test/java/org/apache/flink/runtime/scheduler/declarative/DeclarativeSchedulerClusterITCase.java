/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.dispatcher.SchedulerNGFactoryFactory;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * This class contains integration tests for the declarative scheduler which start a {@link
 * org.apache.flink.runtime.minicluster.MiniCluster} per test case.
 */
public class DeclarativeSchedulerClusterITCase extends TestLogger {

    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;

    private final Configuration configuration = createConfiguration();

    @Rule
    public final MiniClusterResource miniClusterResource =
            new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .build());

    private Configuration createConfiguration() {
        final Configuration configuration = new Configuration();

        configuration.set(
                JobManagerOptions.SCHEDULER, SchedulerNGFactoryFactory.DECLARATIVE_SCHEDULER);
        configuration.set(ClusterOptions.ENABLE_DECLARATIVE_RESOURCE_MANAGEMENT, true);

        return configuration;
    }

    @Test
    public void testAutomaticScaleDownInCaseOfLostSlots() throws InterruptedException {
        assumeTrue(ClusterOptions.isDeclarativeResourceManagementEnabled(configuration));

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();
        final CompletableFuture<JobResult> resultFuture =
                miniCluster.requestJobResult(jobGraph.getJobID());

        OnceBlockingNoOpInvokable.waitUntilOpsAreRunning();

        miniCluster.terminateTaskManager(0);

        final JobResult jobResult = resultFuture.join();

        assertTrue(jobResult.isSuccess());
    }

    private JobGraph createBlockingJobGraph(int parallelism) {
        final JobVertex blockingOperator = new JobVertex("Blocking operator");

        OnceBlockingNoOpInvokable.resetFor(parallelism);
        blockingOperator.setInvokableClass(OnceBlockingNoOpInvokable.class);

        blockingOperator.setParallelism(parallelism);

        final JobGraph jobGraph = new JobGraph("Blocking job.", blockingOperator);

        return jobGraph;
    }
}
