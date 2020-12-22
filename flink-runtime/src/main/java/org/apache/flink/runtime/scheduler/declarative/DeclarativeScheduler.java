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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTrackerDeploymentListenerAdapter;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfoWithUtilization;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Declarative {@link SchedulerNG} implementation which first declares the required resources and
 * then decides on the parallelism with which to execute the given {@link JobGraph}.
 */
public class DeclarativeScheduler implements SchedulerNG {

    private final JobGraph jobGraph;

    private final Logger logger;

    private final DeclarativeSlotPool declarativeSlotPool;
    private final Configuration configuration;
    private final ScheduledExecutorService futureExecutor;
    private final Executor ioExecutor;
    private final ClassLoader userCodeClassLoader;
    private final Time rpcTimeout;
    private final BlobWriter blobWriter;
    private final ShuffleMaster<?> shuffleMaster;
    private final JobMasterPartitionTracker partitionTracker;
    private final ExecutionDeploymentTracker executionDeploymentTracker;
    private final long initializationTimestamp;
    private final CheckpointRecoveryFactory checkpointRecoveryFactory;
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    private JobStatus jobStatus = JobStatus.CREATED;

    @Nullable private JobStatusListener jobStatusListener;

    private ComponentMainThreadExecutor mainThreadExecutor =
            new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
                    "DeclarativeScheduler has not been initialized.");

    private SchedulerState state = SchedulerState.CREATED;

    private long schedulingAttempt = 0;

    private ResourceCounter desiredResources = ResourceCounter.empty();

    @Nullable private ExecutionGraph executionGraph = null;

    public DeclarativeScheduler(
            JobGraph jobGraph,
            Configuration configuration,
            Logger logger,
            DeclarativeSlotPool declarativeSlotPool,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            ClassLoader userCodeClassLoader,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Time rpcTimeout,
            BlobWriter blobWriter,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            ExecutionDeploymentTracker executionDeploymentTracker,
            long initializationTimestamp) {
        this.jobGraph = jobGraph;
        this.configuration = configuration;
        this.logger = logger;
        this.declarativeSlotPool = declarativeSlotPool;
        this.futureExecutor = futureExecutor;
        this.ioExecutor = ioExecutor;
        this.userCodeClassLoader = userCodeClassLoader;
        this.checkpointRecoveryFactory = checkpointRecoveryFactory;
        this.rpcTimeout = rpcTimeout;
        this.blobWriter = blobWriter;
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.shuffleMaster = shuffleMaster;
        this.partitionTracker = partitionTracker;
        this.executionDeploymentTracker = executionDeploymentTracker;
        this.initializationTimestamp = initializationTimestamp;
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

        try {
            waitForResources();
        } catch (Exception e) {
            handleFatalFailure(new FlinkException("Failed to wait for resources.", e));
        }
    }

    private void waitForResources() throws Exception {
        state = SchedulerState.WAITING_FOR_RESOURCES;
        schedulingAttempt++;

        if (!scheduleIfEnoughResources()) {
            mainThreadExecutor.schedule(
                    () -> resourceTimeout(schedulingAttempt), 10L, TimeUnit.SECONDS);
        }
    }

    private boolean scheduleIfEnoughResources() throws Exception {
        if (hasEnoughResources()) {
            scheduleExecutionGraphWithExistingResources();
            return true;
        } else {
            return false;
        }
    }

    private boolean hasEnoughResources() {
        final Collection<? extends SlotInfo> allSlots =
                declarativeSlotPool.getFreeSlotsInformation();
        ResourceCounter outstandingResources = desiredResources;

        final Iterator<? extends SlotInfo> slotIterator = allSlots.iterator();

        while (!outstandingResources.isEmpty() && slotIterator.hasNext()) {
            outstandingResources =
                    outstandingResources.subtract(slotIterator.next().getResourceProfile(), 1);
        }

        return outstandingResources.isEmpty();
    }

    private void resourceTimeout(long timeoutForSchedulingAttempt) {
        if (timeoutForSchedulingAttempt == schedulingAttempt) {
            try {
                scheduleExecutionGraphWithExistingResources();
            } catch (Exception e) {
                handleFatalFailure(
                        new FlinkException("Failed to schedule the execution graph.", e));
            }
        }
    }

    private void scheduleExecutionGraphWithExistingResources() throws Exception {
        logger.info("Start scheduling job with existing resources.");
        Preconditions.checkState(
                state == SchedulerState.WAITING_FOR_RESOURCES,
                "Can only schedule the ExecutionGraph after having waited for resources.");
        state = SchedulerState.EXECUTING;

        createExecutionGraphAndAssignResources();
        deployExecutionGraph();
    }

    private void createExecutionGraphAndAssignResources() throws Exception {
        logger.debug("Calculate parallelism of job and assign resources.");

        final ParallelismAndResourceAssignments parallelismAndResourceAssignments =
                determineParallelismAndAssignResources(jobGraph);

        for (JobVertex vertex : jobGraph.getVertices()) {
            vertex.setParallelism(parallelismAndResourceAssignments.getParallelism(vertex.getID()));
        }

        ExecutionDeploymentListener executionDeploymentListener =
                new ExecutionDeploymentTrackerDeploymentListenerAdapter(executionDeploymentTracker);
        ExecutionStateUpdateListener executionStateUpdateListener =
                (execution, newState) -> {
                    if (newState.isTerminal()) {
                        executionDeploymentTracker.stopTrackingDeploymentOf(execution);
                    }
                };

        executionGraph =
                ExecutionGraphBuilder.buildGraph(
                        null,
                        jobGraph,
                        configuration,
                        futureExecutor,
                        ioExecutor,
                        new ThrowingSlotProvider(),
                        userCodeClassLoader,
                        checkpointRecoveryFactory,
                        rpcTimeout,
                        jobManagerJobMetricGroup,
                        blobWriter,
                        Time.milliseconds(0L),
                        logger,
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentListener,
                        executionStateUpdateListener,
                        initializationTimestamp);
        executionGraph.start(mainThreadExecutor);
        //		executionGraph.setInternalTaskFailuresListener(null);

        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            final LogicalSlot assignedSlot =
                    parallelismAndResourceAssignments.getAssignedSlot(executionVertex.getID());
            executionVertex.tryAssignResource(assignedSlot);
        }
    }

    public void handleFatalFailure(Throwable failure) {
        Preconditions.checkState(jobStatusListener != null, "JobStatusListener has not been set.");
        jobStatusListener.jobStatusChanges(
                jobGraph.getJobID(), JobStatus.FAILED, System.currentTimeMillis(), failure);
    }

    private ParallelismAndResourceAssignments determineParallelismAndAssignResources(
            JobGraph jobGraph) {
        final HashMap<ExecutionVertexID, LogicalSlot> assignedSlots = new HashMap<>();
        final HashMap<JobVertexID, Integer> parallelismPerJobVertex = new HashMap<>();

        final Collection<SlotInfoWithUtilization> freeSlots =
                declarativeSlotPool.getFreeSlotsInformation();
        final int slotsPerVertex = freeSlots.size() / jobGraph.getNumberOfVertices();
        final Iterator<SlotInfoWithUtilization> slotIterator = freeSlots.iterator();

        for (JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
            final int parallelism = Math.min(jobVertex.getParallelism(), slotsPerVertex);
            jobVertex.setParallelism(parallelism);

            parallelismPerJobVertex.put(jobVertex.getID(), parallelism);

            for (int i = 0; i < slotsPerVertex; i++) {
                final SlotInfoWithUtilization slotInfo = slotIterator.next();

                final PhysicalSlot physicalSlot =
                        declarativeSlotPool.reserveFreeSlot(
                                slotInfo.getAllocationId(),
                                ResourceProfile.fromResourceSpec(
                                        jobVertex.getMinResources(), MemorySize.ZERO));

                assignedSlots.put(
                        new ExecutionVertexID(jobVertex.getID(), i),
                        new SingleLogicalSlot(
                                new SlotRequestId(),
                                physicalSlot,
                                null,
                                Locality.UNKNOWN,
                                logicalSlot ->
                                        declarativeSlotPool.freeReservedSlot(
                                                physicalSlot.getAllocationId(),
                                                null,
                                                System.currentTimeMillis())));
            }
        }

        return new ParallelismAndResourceAssignments(assignedSlots, parallelismPerJobVertex);
    }

    private static final class ParallelismAndResourceAssignments {
        private final Map<ExecutionVertexID, ? extends LogicalSlot> assignedSlots;

        private final Map<JobVertexID, Integer> parallelismPerJobVertex;

        private ParallelismAndResourceAssignments(
                Map<ExecutionVertexID, ? extends LogicalSlot> assignedSlots,
                Map<JobVertexID, Integer> parallelismPerJobVertex) {
            this.assignedSlots = assignedSlots;
            this.parallelismPerJobVertex = parallelismPerJobVertex;
        }

        public int getParallelism(JobVertexID jobVertexId) {
            Preconditions.checkState(parallelismPerJobVertex.containsKey(jobVertexId));
            return parallelismPerJobVertex.get(jobVertexId);
        }

        public LogicalSlot getAssignedSlot(ExecutionVertexID executionVertexId) {
            Preconditions.checkState(assignedSlots.containsKey(executionVertexId));
            return assignedSlots.get(executionVertexId);
        }
    }

    private void deployExecutionGraph() {
        Preconditions.checkNotNull(
                executionGraph, "The ExecutionGraph must not be null when being deployed.");

        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            deploySafely(executionVertex);
        }
    }

    private void deploySafely(ExecutionVertex executionVertex) {
        try {
            executionVertex.deploy();
        } catch (JobException e) {
            handleDeploymentFailure(executionVertex, e);
        }
    }

    private void handleDeploymentFailure(ExecutionVertex executionVertex, JobException e) {
        executionVertex.markFailed(e);
    }

    private void declareResources() {
        desiredResources = calculateDesiredResources();
        declarativeSlotPool.increaseResourceRequirementsBy(desiredResources);
    }

    private ResourceCounter calculateDesiredResources() {
        int counter = 0;

        for (JobVertex vertex : jobGraph.getVertices()) {
            counter += vertex.getParallelism();
        }

        return ResourceCounter.withResource(ResourceProfile.UNKNOWN, counter);
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
        if (state == SchedulerState.EXECUTING && executionGraph != null) {
            return executionGraph.updateState(taskExecutionState);
        } else {
            return false;
        }
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
        if (state == SchedulerState.EXECUTING && executionGraph != null) {
            executionGraph.updateAccumulators(accumulatorSnapshot);
        }
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

    private enum SchedulerState {
        CREATED,
        WAITING_FOR_RESOURCES,
        EXECUTING,
    }
}
