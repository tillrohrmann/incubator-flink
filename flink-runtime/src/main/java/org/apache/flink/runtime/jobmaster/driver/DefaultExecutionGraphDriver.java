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
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.RescalingBehaviour;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.messages.Acknowledge;
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

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of the {@link ExecutionGraphDriver}.
 */
public class DefaultExecutionGraphDriver implements ExecutionGraphDriver {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionGraphDriver.class);

	private final JobGraph jobGraph;

	private final ExecutionGraphFactory executionGraphFactory;

	private final JobManagerJobMetricGroupFactory jobMetricGroupFactory;

	private final BackPressureStatsTracker backPressureStatsTracker;

	private final ClassLoader userCodeClassLoader;

	private final ExecutionGraph executionGraph;

	private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

	public DefaultExecutionGraphDriver(
			@Nonnull JobGraph jobGraph,
			@Nonnull ExecutionGraphFactory executionGraphFactory,
			@Nonnull JobManagerJobMetricGroupFactory jobMetricGroupFactory,
			@Nonnull BackPressureStatsTracker backPressureStatsTracker,
			@Nonnull ClassLoader userCodeClassLoader) throws Exception {
		this.jobGraph = jobGraph;
		this.executionGraphFactory = executionGraphFactory;
		this.jobMetricGroupFactory = jobMetricGroupFactory;
		this.backPressureStatsTracker = backPressureStatsTracker;
		this.userCodeClassLoader = userCodeClassLoader;

		this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
		this.executionGraph = createAndRestoreExecutionGraph(jobGraph, jobManagerJobMetricGroup);
	}

	@Override
	public void schedule() throws Exception {
		executionGraph.scheduleForExecution();
	}
//
//	private void resetAndScheduleExecutionGraph() throws Exception {
//		validateRunsInMainThread();
//
//		final CompletableFuture<Void> executionGraphAssignedFuture;
//
//		if (executionGraphDriver.getState() == JobStatus.CREATED) {
//			executionGraphAssignedFuture = CompletableFuture.completedFuture(null);
//			executionGraphDriver.getTerminationFuture().thenAcceptAsync(this::jobReachedTerminalState, getMainThreadExecutor());
//		} else {
//			suspendExecutionGraphDriver(new FlinkException("ExecutionGraph is being reset in order to be rescheduled."));
//			final ExecutionGraphDriver newExecutionGraphDriver = createAndRestoreExecutionGraphDriver();
//
//			executionGraphAssignedFuture = executionGraphDriver.getTerminationFuture().handleAsync(
//				(JobStatus ignored, Throwable throwable) -> {
//					assignExecutionGraphDriver(newExecutionGraphDriver);
//					return null;
//				},
//				getMainThreadExecutor());
//		}
//
//		executionGraphAssignedFuture.thenRun(this::scheduleExecutionGraph);
//	}

	private ExecutionGraph createAndRestoreExecutionGraph(JobGraph jobGraph, JobManagerJobMetricGroup jobManagerJobMetricGroup) throws Exception {
		ExecutionGraph newExecutionGraph = executionGraphFactory.create(jobGraph, jobManagerJobMetricGroup);

		final CheckpointCoordinator checkpointCoordinator = newExecutionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			// check whether we find a valid checkpoint
			if (!checkpointCoordinator.restoreLatestCheckpointedState(
				newExecutionGraph.getAllVertices(),
				false,
				false)) {

				// check whether we can restore from a savepoint
				tryRestoreExecutionGraphFromSavepoint(newExecutionGraph, jobGraph.getSavepointRestoreSettings());
			}
		}

		return newExecutionGraph;
	}

	/**
	 * Tries to restore the given {@link ExecutionGraph} from the provided {@link SavepointRestoreSettings}.
	 *
	 * @param executionGraphToRestore {@link ExecutionGraph} which is supposed to be restored
	 * @param savepointRestoreSettings {@link SavepointRestoreSettings} containing information about the savepoint to restore from
	 * @throws Exception if the {@link ExecutionGraph} could not be restored
	 */
	private void tryRestoreExecutionGraphFromSavepoint(ExecutionGraph executionGraphToRestore, SavepointRestoreSettings savepointRestoreSettings) throws Exception {
		if (savepointRestoreSettings.restoreSavepoint()) {
			final CheckpointCoordinator checkpointCoordinator = executionGraphToRestore.getCheckpointCoordinator();
			if (checkpointCoordinator != null) {
				checkpointCoordinator.restoreSavepoint(
					savepointRestoreSettings.getRestorePath(),
					savepointRestoreSettings.allowNonRestoredState(),
					executionGraphToRestore.getAllVertices(),
					userCodeClassLoader);
			}
		}
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
	public CompletableFuture<Acknowledge> rescaleOperators(Collection<JobVertexID> operators, int newParallelism, RescalingBehaviour rescalingBehaviour) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException("Not yet supported."));
//		if (newParallelism <= 0) {
//			return FutureUtils.completedExceptionally(
//				new JobModificationException("The target parallelism of a rescaling operation must be larger than 0."));
//		}
//
//		// 1. Check whether we can rescale the job & rescale the respective vertices
//		try {
//			rescaleJobGraph(operators, newParallelism, rescalingBehaviour);
//		} catch (FlinkException e) {
//			final String msg = String.format("Cannot rescale job %s.", jobGraph.getName());
//
//			log.info(msg, e);
//			return FutureUtils.completedExceptionally(new JobModificationException(msg, e));
//		}
//
//		final ExecutionGraphDriver currentExecutionGraphDriver = executionGraphDriver;
//
//		final JobManagerJobMetricGroup newJobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
//		final ExecutionGraph newExecutionGraph;
//
//		try {
//			newExecutionGraph = createExecutionGraph(jobGraph, newJobManagerJobMetricGroup);
//		} catch (JobExecutionException | JobException e) {
//			return FutureUtils.completedExceptionally(
//				new JobModificationException("Could not create rescaled ExecutionGraph.", e));
//		}
//
//		// 3. disable checkpoint coordinator to suppress subsequent checkpoints
//		currentExecutionGraphDriver.stopCheckpointScheduler();
//
//		// 4. take a savepoint
//		final CompletableFuture<String> savepointFuture = getJobModificationSavepoint(timeout);
//
//		final CompletableFuture<ExecutionGraph> executionGraphFuture = restoreExecutionGraphFromRescalingSavepoint(
//			newExecutionGraph,
//			savepointFuture)
//			.handleAsync(
//				(ExecutionGraph executionGraph, Throwable failure) -> {
//					if (failure != null) {
//						// in case that we couldn't take a savepoint or restore from it, let's restart the checkpoint
//						// coordinator and abort the rescaling operation
//						currentExecutionGraphDriver.startCheckpointScheduler();
//
//						throw new CompletionException(ExceptionUtils.stripCompletionException(failure));
//					} else {
//						return executionGraph;
//					}
//				},
//				getMainThreadExecutor());
//
//		// 5. suspend the current job
//		final CompletableFuture<JobStatus> terminationFuture = executionGraphFuture.thenComposeAsync(
//			(ExecutionGraph ignored) -> {
//				suspendExecutionGraphDriver(new FlinkException("Job is being rescaled."));
//				return currentExecutionGraphDriver.getTerminationFuture();
//			},
//			getMainThreadExecutor());
//
//		final CompletableFuture<Void> suspendedFuture = terminationFuture.thenAccept(
//			(JobStatus jobStatus) -> {
//				if (jobStatus != JobStatus.SUSPENDED) {
//					final String msg = String.format("Job %s rescaling failed because we could not suspend the execution graph.", jobGraph.getName());
//					log.info(msg);
//					throw new CompletionException(new JobModificationException(msg));
//				}
//			});
//
//		// 6. resume the new execution graph from the taken savepoint
//		final CompletableFuture<Acknowledge> rescalingFuture = suspendedFuture.thenCombineAsync(
//			executionGraphFuture,
//			(Void ignored, ExecutionGraph restoredExecutionGraph) -> {
//				// check if the ExecutionGraph is still the same
//				if (executionGraphDriver == currentExecutionGraphDriver) {
//					assignExecutionGraphDriver(
//						new DefaultExecutionGraphDriver(jobGraph, this::createExecutionGraph, restoredExecutionGraph, newJobManagerJobMetricGroup, backPressureStatsTracker));
//					scheduleExecutionGraph();
//
//					return Acknowledge.get();
//				} else {
//					throw new CompletionException(new JobModificationException("Detected concurrent modification of ExecutionGraph. Aborting the rescaling."));
//				}
//
//			},
//			getMainThreadExecutor());
//
//		rescalingFuture.whenComplete(
//			(Acknowledge ignored, Throwable throwable) -> {
//				if (throwable != null) {
//					// fail the newly created execution graph
//					newExecutionGraph.failGlobal(
//						new SuppressRestartsException(
//							new FlinkException(
//								String.format("Failed to rescale the job %s.", jobGraph.getJobID()),
//								throwable)));
//				}
//			});
//
//		return rescalingFuture;
	}

//	/**
//	 * Restore the given {@link ExecutionGraph} from the rescaling savepoint. If the {@link ExecutionGraph} could
//	 * be restored, then this savepoint will be recorded as the latest successful modification savepoint. A previous
//	 * savepoint will be disposed. If the rescaling savepoint is empty, the job will be restored from the initially
//	 * provided savepoint.
//	 *
//	 * @param newExecutionGraph to restore
//	 * @param savepointFuture containing the path to the internal modification savepoint
//	 * @return Future which is completed with the restored {@link ExecutionGraph}
//	 */
//	private CompletableFuture<ExecutionGraph> restoreExecutionGraphFromRescalingSavepoint(ExecutionGraph newExecutionGraph, CompletableFuture<String> savepointFuture) {
//		return savepointFuture
//			.thenApplyAsync(
//				(@Nullable String savepointPath) -> {
//					if (savepointPath != null) {
//						try {
//							tryRestoreExecutionGraphFromSavepoint(newExecutionGraph, SavepointRestoreSettings.forPath(savepointPath, false));
//						} catch (Exception e) {
//							final String message = String.format("Could not restore from temporary rescaling savepoint. This might indicate " +
//									"that the savepoint %s got corrupted. Deleting this savepoint as a precaution.",
//								savepointPath);
//
//							log.info(message);
//
//							CompletableFuture
//								.runAsync(
//									() -> {
//										if (savepointPath.equals(lastInternalSavepoint)) {
//											lastInternalSavepoint = null;
//										}
//									},
//									getMainThreadExecutor())
//								.thenRunAsync(
//									() -> disposeSavepoint(savepointPath),
//									scheduledExecutorService);
//
//							throw new CompletionException(new JobModificationException(message, e));
//						}
//					} else {
//						// No rescaling savepoint, restart from the initial savepoint or none
//						try {
//							tryRestoreExecutionGraphFromSavepoint(newExecutionGraph, jobGraph.getSavepointRestoreSettings());
//						} catch (Exception e) {
//							final String message = String.format("Could not restore from initial savepoint. This might indicate " +
//								"that the savepoint %s got corrupted.", jobGraph.getSavepointRestoreSettings().getRestorePath());
//
//							log.info(message);
//
//							throw new CompletionException(new JobModificationException(message, e));
//						}
//					}
//
//					return newExecutionGraph;
//				}, scheduledExecutorService);
//	}
//
//	/**
//	 * Takes an internal savepoint for job modification purposes. If the savepoint was not successful because
//	 * not all tasks were running, it returns the last successful modification savepoint.
//	 *
//	 * @param timeout for the operation
//	 * @return Future which is completed with the savepoint path or the last successful modification savepoint if the
//	 * former was not successful
//	 */
//	private CompletableFuture<String> getJobModificationSavepoint(Time timeout) {
//		return triggerSavepoint(
//			null,
//			false,
//			timeout)
//			.handleAsync(
//				(String savepointPath, Throwable throwable) -> {
//					if (throwable != null) {
//						final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
//						if (strippedThrowable instanceof CheckpointTriggerException) {
//							final CheckpointTriggerException checkpointTriggerException = (CheckpointTriggerException) strippedThrowable;
//
//							if (checkpointTriggerException.getCheckpointDeclineReason() == CheckpointDeclineReason.NOT_ALL_REQUIRED_TASKS_RUNNING) {
//								return lastInternalSavepoint;
//							} else {
//								throw new CompletionException(checkpointTriggerException);
//							}
//						} else {
//							throw new CompletionException(strippedThrowable);
//						}
//					} else {
//						final String savepointToDispose = lastInternalSavepoint;
//						lastInternalSavepoint = savepointPath;
//
//						if (savepointToDispose != null) {
//							// dispose the old savepoint asynchronously
//							CompletableFuture.runAsync(
//								() -> disposeSavepoint(savepointToDispose),
//								scheduledExecutorService);
//						}
//
//						return lastInternalSavepoint;
//					}
//				},
//				getMainThreadExecutor());
//	}
//
//	/**
//	 * Rescales the given operators of the {@link JobGraph} of this {@link JobMaster} with respect to given
//	 * parallelism and {@link RescalingBehaviour}.
//	 *
//	 * @param operators to rescale
//	 * @param newParallelism new parallelism for these operators
//	 * @param rescalingBehaviour of the rescaling operation
//	 * @throws FlinkException if the {@link JobGraph} could not be rescaled
//	 */
//	private void rescaleJobGraph(Collection<JobVertexID> operators, int newParallelism, RescalingBehaviour rescalingBehaviour) throws FlinkException {
//		for (JobVertexID jobVertexId : operators) {
//			final JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);
//
//			// update max parallelism in case that it has not been configured
//			final AccessExecutionJobVertex executionJobVertex = executionGraphDriver.getJobVertex(jobVertexId);
//
//			if (executionJobVertex != null) {
//				jobVertex.setMaxParallelism(executionJobVertex.getMaxParallelism());
//			}
//
//			rescalingBehaviour.accept(jobVertex, newParallelism);
//		}
//	}

//	/**
//	 * Dispose the savepoint stored under the given path.
//	 *
//	 * @param savepointPath path where the savepoint is stored
//	 */
//	private void disposeSavepoint(String savepointPath) {
//		try {
//			// delete the temporary savepoint
//			Checkpoints.disposeSavepoint(
//				savepointPath,
//				jobMasterConfiguration.getConfiguration(),
//				userCodeLoader,
//				log);
//		} catch (FlinkException | IOException e) {
//			log.info("Could not dispose temporary rescaling savepoint under {}.", savepointPath, e);
//		}
//	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		suspend(new FlinkException("Closing ExecutionGraphDriver."));
		return CompletableFuture.completedFuture(null);
	}
}
