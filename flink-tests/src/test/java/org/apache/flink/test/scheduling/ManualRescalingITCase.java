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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobVertexParallelism;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.function.Supplier;

/** Tests for the manual rescaling of Flink jobs using the REST API. */
public class ManualRescalingITCase extends TestLogger {

    public static final Duration TIMEOUT_DURATION = Duration.ofSeconds(10);

    @ClassRule
    public static MiniClusterWithClientResource miniClusterWithClientResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(createConfiguration())
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();

        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(WebOptions.REFRESH_INTERVAL, 50L);

        return configuration;
    }

    @Test
    public void testManualUpScalingWithNewSlotAllocation() throws Exception {
        final JobVertex jobVertex = new JobVertex("Single operator");
        final int initialParallelism = 1;
        final int parallelismAfterRescaling = 2;
        jobVertex.setParallelism(initialParallelism);
        jobVertex.setInvokableClass(BlockingNoOpInvokable.class);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);

        runRescalingTest(
                jobGraph,
                () ->
                        JobVertexParallelism.newBuilder()
                                .setParallelismForJobVertex(jobVertex.getID(), 2)
                                .build(),
                initialParallelism,
                parallelismAfterRescaling);
    }

    @Test
    public void testManualUpScalingWithNoNewSlotAllocation() throws Exception {
        final int initialRunningTasks = 3;
        final int runningTasksAfterRescale = 4;

        final JobVertex jobVertex1 = new JobVertex("Operator1");
        jobVertex1.setParallelism(1);
        jobVertex1.setInvokableClass(BlockingNoOpInvokable.class);
        final JobVertex jobVertex2 = new JobVertex("Operator2");
        jobVertex2.setParallelism(2);
        jobVertex2.setInvokableClass(BlockingNoOpInvokable.class);

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        jobVertex1.setSlotSharingGroup(slotSharingGroup);
        jobVertex2.setSlotSharingGroup(slotSharingGroup);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex1, jobVertex2);

        runRescalingTest(
                jobGraph,
                () ->
                        JobVertexParallelism.newBuilder()
                                .setParallelismForJobVertex(jobVertex1.getID(), 2)
                                .build(),
                initialRunningTasks,
                runningTasksAfterRescale);
    }

    @Test
    public void testManualDownScaling() throws Exception {
        final int initialRunningTasks = 2;
        final int runningTasksAfterRescale = 1;

        final JobVertex jobVertex = new JobVertex("Operator");
        jobVertex.setParallelism(initialRunningTasks);
        jobVertex.setInvokableClass(BlockingNoOpInvokable.class);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);

        runRescalingTest(
                jobGraph,
                () ->
                        JobVertexParallelism.newBuilder()
                                .setParallelismForJobVertex(jobVertex.getID(), 1)
                                .build(),
                initialRunningTasks,
                runningTasksAfterRescale);
    }

    private void runRescalingTest(
            JobGraph jobGraph,
            Supplier<JobVertexParallelism> newJobVertexParallelismSupplier,
            int initialRunningTasks,
            int runningTasksAfterRescale)
            throws Exception {
        final Deadline timeout = Deadline.fromNow(TIMEOUT_DURATION);

        final RestClusterClient<?> restClusterClient =
                miniClusterWithClientResource.getRestClusterClient();

        restClusterClient.submitJob(jobGraph).join();

        try {
            final JobID jobId = jobGraph.getJobID();

            CommonTestUtils.waitUntilCondition(
                    () -> getNumberRunningTasks(restClusterClient, jobId) == initialRunningTasks,
                    timeout);

            final JobVertexParallelism jobVertexParallelism = newJobVertexParallelismSupplier.get();

            restClusterClient.changeParallelismOfJob(jobId, jobVertexParallelism).join();

            CommonTestUtils.waitUntilCondition(
                    () ->
                            getNumberRunningTasks(restClusterClient, jobId)
                                    == runningTasksAfterRescale,
                    timeout);
        } finally {
            miniClusterWithClientResource.getClusterClient().cancel(jobGraph.getJobID()).join();
        }
    }

    private int getNumberRunningTasks(RestClusterClient<?> restClusterClient, JobID jobId) {
        final JobDetailsInfo jobDetailsInfo = restClusterClient.getJobDetails(jobId).join();

        return jobDetailsInfo.getJobVertexInfos().stream()
                .map(
                        jobVertexDetailsInfo ->
                                jobVertexDetailsInfo.getTasksPerState().get(ExecutionState.RUNNING))
                .mapToInt(Integer::intValue)
                .sum();
    }
}
