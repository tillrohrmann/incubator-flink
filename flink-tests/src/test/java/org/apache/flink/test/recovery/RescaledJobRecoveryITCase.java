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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobVertexParallelism;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.test.scheduling.ManualRescalingITCase;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;

/** Tests for recovering of rescaled jobs. */
public class RescaledJobRecoveryITCase extends TestLogger {

    @ClassRule public static final ZooKeeperResource ZOO_KEEPER_RESOURCE = new ZooKeeperResource();

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    /** Tests that a rescaled job graph will be recovered with the latest parallelism. */
    @Test
    public void testRescaledJobGraphsWillBeRecoveredCorrectly() throws Exception {
        final JobVertex jobVertex = new JobVertex("operator");
        jobVertex.setParallelism(1);
        jobVertex.setInvokableClass(BlockingNoOpInvokable.class);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);
        final JobID jobId = jobGraph.getJobID();

        final Configuration configuration = new Configuration();
        configuration.set(WebOptions.REFRESH_INTERVAL, 50L);
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                ZOO_KEEPER_RESOURCE.getConnectString());
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                TEMPORARY_FOLDER.newFolder().getAbsolutePath());

        final MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumSlotsPerTaskManager(2)
                        .build();

        final MiniCluster firstMiniCluster = new MiniCluster(miniClusterConfiguration);
        firstMiniCluster.start();

        final RestClusterClient<?> restClusterClient =
                new RestClusterClient<>(configuration, "foobar");

        restClusterClient.submitJob(jobGraph).join();

        ClientUtils.waitUntilJobInitializationFinished(
                () -> restClusterClient.getJobStatus(jobId).get(),
                () -> restClusterClient.requestJobResult(jobId).get(),
                getClass().getClassLoader());

        restClusterClient
                .changeParallelismOfJob(
                        jobGraph.getJobID(),
                        JobVertexParallelism.newBuilder()
                                .setParallelismForJobVertex(jobVertex.getID(), 2)
                                .build())
                .join();

        firstMiniCluster.close();

        log.info("Start second mini cluster to recover the persisted job.");

        final MiniCluster secondMiniCluster = new MiniCluster(miniClusterConfiguration);
        secondMiniCluster.start();

        final Deadline timeout = Deadline.fromNow(Duration.ofSeconds(10L));

        CommonTestUtils.waitUntilCondition(
                () -> ManualRescalingITCase.getNumberRunningTasks(restClusterClient, jobId) == 2,
                timeout);
    }
}
