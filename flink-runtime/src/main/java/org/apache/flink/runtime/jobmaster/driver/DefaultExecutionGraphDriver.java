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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.StoppingException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphException;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of the {@link ExecutionGraphDriver}.
 */
public class DefaultExecutionGraphDriver implements ExecutionGraphDriver {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionGraphDriver.class);

	private final ExecutionGraph executionGraph;

	private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

	private final BackPressureStatsTracker backPressureStatsTracker;

	public DefaultExecutionGraphDriver(
		@Nonnull ExecutionGraph executionGraph,
		@Nonnull JobManagerJobMetricGroup jobManagerJobMetricGroup,
		@Nonnull BackPressureStatsTracker backPressureStatsTracker) {
		this.executionGraph = executionGraph;
		this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
		this.backPressureStatsTracker = backPressureStatsTracker;
	}

	@Override
	public void schedule() throws Exception {
		executionGraph.scheduleForExecution();
	}

	@Override
	public void cancel() {
		executionGraph.cancel();
	}

	@Override
	public void stop() throws StoppingException {
		executionGraph.stop();
	}

	@Override
	public boolean updateState(TaskExecutionState taskExecutionState) {
		return executionGraph.updateState(taskExecutionState);
	}

	@Override
	public InputSplit requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws ExecutionGraphDriverException {
		final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
		if (execution == null) {
			// can happen when JobManager had already unregistered this execution upon on task failure,
			// but TaskManager get some delay to aware of that situation
			if (LOG.isDebugEnabled()) {
				LOG.debug("Can not find Execution for attempt {}.", executionAttempt);
			}
			// but we should TaskManager be aware of this
			throw new ExecutionGraphDriverException("Can not find Execution for attempt " + executionAttempt);
		}

		final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
		if (vertex == null) {
			LOG.error("Cannot find execution vertex for vertex ID {}.", vertexID);
			throw new ExecutionGraphDriverException("Cannot find execution vertex for vertex ID " + vertexID);
		}

		final InputSplitAssigner splitAssigner = vertex.getSplitAssigner();
		if (splitAssigner == null) {
			LOG.error("No InputSplitAssigner for vertex ID {}.", vertexID);
			throw new ExecutionGraphDriverException("No InputSplitAssigner for vertex ID " + vertexID);
		}

		final LogicalSlot slot = execution.getAssignedResource();
		final int taskId = execution.getVertex().getParallelSubtaskIndex();
		final String host = slot != null ? slot.getTaskManagerLocation().getHostname() : null;
		final InputSplit nextInputSplit = splitAssigner.getNextInputSplit(host, taskId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Send next input split {}.", nextInputSplit);
		}

		return nextInputSplit;
	}

	@Override
	public void fail(Throwable reason) {
		executionGraph.failGlobal(reason);
	}

	@Override
	public Optional<ExecutionState> requestPartitionState(IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId) {
		final Execution execution = executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());

		if (execution != null) {
			return Optional.of(execution.getState());
		} else {
			final IntermediateResult intermediateResult =
				executionGraph.getAllIntermediateResults().get(intermediateResultId);

			if (intermediateResult != null) {
				// Try to find the producing execution
				Execution producerExecution = intermediateResult
					.getPartitionById(resultPartitionId.getPartitionId())
					.getProducer()
					.getCurrentExecutionAttempt();

				if (producerExecution.getAttemptId().equals(resultPartitionId.getProducerId())) {
					return Optional.of(producerExecution.getState());
				} else {
					return Optional.empty();
				}
			} else {
				throw new IllegalArgumentException("Intermediate data set with ID " + intermediateResultId + " not found.");
			}
		}
	}

	@Override
	public void scheduleOrUpdateConsumers(ResultPartitionID partitionID) throws ExecutionGraphException {
		executionGraph.scheduleOrUpdateConsumers(partitionID);
	}

	@Override
	public void acknowledgeCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot checkpointState) {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		final AcknowledgeCheckpoint ackMessage = new AcknowledgeCheckpoint(
			jobID,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			checkpointState);

		if (checkpointCoordinator != null) {
			try {
				checkpointCoordinator.receiveAcknowledgeMessage(ackMessage);
			} catch (Throwable t) {
				LOG.warn("Error while processing checkpoint acknowledgement message.", t);
			}
		} else {
			LOG.error("Received AcknowledgeCheckpoint message for job {} with no checkpointing enabled.",
				executionGraph.getJobID());
		}
	}

	@Override
	public void declineCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointID, Throwable reason) {
		final DeclineCheckpoint decline = new DeclineCheckpoint(
			jobID, executionAttemptID, checkpointID, reason);
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			try {
				checkpointCoordinator.receiveDeclineMessage(decline);
			} catch (Throwable t) {
				LOG.error("Error in CheckpointCoordinator while processing {}.", decline, t);
			}
		} else {
			LOG.error("Received DeclineCheckpoint message for job {} with no checkpointing enabled.",
				executionGraph.getJobID());
		}
	}

	@Override
	public KvStateLocationRegistry getKvStateLocationRegistry() {
		return executionGraph.getKvStateLocationRegistry();
	}

	@Override
	public AccessExecutionGraph getExecutionGraph() {
		return executionGraph;
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String targetDirectory) {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator == null) {
			return FutureUtils.completedExceptionally(new IllegalStateException(
				String.format("Job %s is not a streaming job.", executionGraph.getJobID())));
		} else if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			LOG.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", executionGraph.getJobID());

			return FutureUtils.completedExceptionally(new IllegalStateException(
				"No savepoint directory configured. You can either specify a directory " +
					"while cancelling via -s :targetDirectory or configure a cluster-wide " +
					"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'."));
		}

		return checkpointCoordinator
			.triggerSavepoint(System.currentTimeMillis(), targetDirectory)
			.thenApply(CompletedCheckpoint::getExternalPointer);
	}

	@Override
	public void startCheckpointScheduler() {
		final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator();
		if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
			try {
				checkpointCoordinator.startCheckpointScheduler();
			} catch (IllegalStateException ignored) {
				// Concurrent shut down of the coordinator
			}
		}
	}

	@Nonnull
	private CheckpointCoordinator getCheckpointCoordinator() {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		Preconditions.checkState(checkpointCoordinator != null, "Job %s is not a streaming job.", executionGraph.getJobID());
		return checkpointCoordinator;
	}

	@Override
	public void stopCheckpointScheduler() {
		final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator();

		checkpointCoordinator.stopCheckpointScheduler();
	}

	@Override
	public AccessExecutionJobVertex getJobVertex(JobVertexID jobVertexId) {
		return executionGraph.getJobVertex(jobVertexId);
	}

	@Override
	public void suspend(Exception cause) {
		executionGraph.suspend(cause);
		jobManagerJobMetricGroup.close();
	}

	@Override
	public void updateAccumulators(AccumulatorSnapshot snapshot) {
		executionGraph.updateAccumulators(snapshot);
	}

	@Override
	public CompletableFuture<JobStatus> getTerminationFuture() {
		return executionGraph.getTerminationFuture();
	}

	@Override
	public JobStatus getState() {
		return executionGraph.getState();
	}

	@Override
	public void registerJobStatusListener(JobStatusListener jobStatusListener) {
		executionGraph.registerJobStatusListener(jobStatusListener);
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(JobVertexID jobVertexId) {
		final ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexId);
		if (jobVertex == null) {
			return FutureUtils.completedExceptionally(new FlinkException("JobVertexID not found " +
				jobVertexId));
		}

		final Optional<OperatorBackPressureStats> operatorBackPressureStats =
			backPressureStatsTracker.getOperatorBackPressureStats(jobVertex);
		return CompletableFuture.completedFuture(OperatorBackPressureStatsResponse.of(
			operatorBackPressureStats.orElse(null)));
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		suspend(new FlinkException("Closing ExecutionGraphDriver."));
		return CompletableFuture.completedFuture(null);
	}
}
