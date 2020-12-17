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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Declarative {@link SchedulerNG} implementation which first declares the required resources and
 * then decides on the parallelism with which to execute the given {@link JobGraph}.
 */
public class DeclarativeScheduler implements SchedulerNG {

    private final JobGraph jobGraph;

    private final Logger logger;

    private final DeclarativeSlotPool declarativeSlotPool;

    private JobStatus jobStatus = JobStatus.CREATED;

    @Nullable private JobStatusListener jobStatusListener;

    private ComponentMainThreadExecutor mainThreadExecutor =
            new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
                    "DeclarativeScheduler has not been initialized.");

    public DeclarativeScheduler(
            JobGraph jobGraph, Logger logger, DeclarativeSlotPool declarativeSlotPool) {
        this.jobGraph = jobGraph;
        this.logger = logger;
        this.declarativeSlotPool = declarativeSlotPool;
    }

    @Override
    public void initialize(ComponentMainThreadExecutor mainThreadExecutor) {
        this.mainThreadExecutor = mainThreadExecutor;
    }

    @Override
    public void registerJobStatusListener(JobStatusListener jobStatusListener) {
        Preconditions.checkState(
                this.jobStatusListener == null,
                "DeclarativeScheduler does not support multiple JobStatusListeners.");

        this.jobStatusListener = jobStatusListener;
    }

    @Override
    public void startScheduling() {
        declareResources();
        scheduleJobGraph();
    }

    private void scheduleJobGraph() {}

    private void declareResources() {
        final ResourceCounter requiredResources = calculateRequiredResources();
        declarativeSlotPool.increaseResourceRequirementsBy(requiredResources);
    }

    private ResourceCounter calculateRequiredResources() {
        int counter = 0;

        for (JobVertex vertex : jobGraph.getVertices()) {
            counter += vertex.getParallelism();
        }

        return ResourceCounter.withResource(ResourceProfile.ANY, counter);
    }

    @Override
    public void suspend(Throwable cause) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void cancel() {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void notifyPartitionDataAvailable(ResultPartitionID partitionID) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public ArchivedExecutionGraph requestJob() {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public JobStatus requestJobStatus() {
        return jobStatus;
    }

    @Override
    public JobDetails requestJobDetails() {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public Optional<OperatorBackPressureStats> requestOperatorBackPressureStats(
            JobVertexID jobVertexId) throws FlinkException {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            String targetDirectory, boolean advanceToEndOfEventTime) {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecution, OperatorID operator, OperatorEvent evt)
            throws FlinkException {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {
        throw new UnsupportedOperationException("Not supported by the declarative scheduler.");
    }
}
