/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.driver;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.StoppingException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.RescalingBehaviour;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.AutoCloseableAsync;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Component which is responsible for executing the {@link ExecutionGraph}.
 */
public interface ExecutionGraphDriver extends AutoCloseableAsync {

	void start();

	void cancel();

	void stop() throws StoppingException;

	boolean updateState(TaskExecutionState taskExecutionState);

	InputSplit requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws ExecutionGraphDriverException;

	void fail(Throwable reason);

	Optional<ExecutionState> requestPartitionState(IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId);

	void scheduleOrUpdateConsumers(ResultPartitionID partitionID) throws ExecutionGraphException;

	void acknowledgeCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot checkpointState);

	void declineCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointID, Throwable reason);

	KvStateLocationRegistry getKvStateLocationRegistry();

	AccessExecutionGraph getExecutionGraph();

	CompletableFuture<String> triggerSavepoint(@Nullable String targetDirectory);

	void startCheckpointScheduler();

	void stopCheckpointScheduler();

	AccessExecutionJobVertex getJobVertex(JobVertexID jobVertexId);

	void suspend(Exception cause);

	void updateAccumulators(AccumulatorSnapshot snapshot);

	CompletableFuture<ArchivedExecutionGraph> getResultFuture();

	JobStatus getState();

	CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(JobVertexID jobVertexId);

	CompletableFuture<Acknowledge> rescaleOperators(Collection<JobVertexID> operators, int newParallelism, RescalingBehaviour rescalingBehaviour);
}
