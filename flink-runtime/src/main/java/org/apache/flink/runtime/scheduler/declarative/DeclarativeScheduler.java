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
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
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
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTrackerDeploymentListenerAdapter;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
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
import org.apache.flink.runtime.scheduler.SchedulerUtils;
import org.apache.flink.runtime.scheduler.UpdateSchedulerNgOnInternalFailuresListener;
import org.apache.flink.runtime.scheduler.declarative.allocator.SlotSharingSlotAllocator;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
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
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    private final CompletedCheckpointStore completedCheckpointStore;
    private final CheckpointIDCounter checkpointIdCounter;
    private final CheckpointsCleaner checkpointsCleaner;

    private JobStatus jobStatus = JobStatus.CREATED;

    @Nullable private JobStatusListener jobStatusListener;

    private ComponentMainThreadExecutor mainThreadExecutor =
            new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
                    "DeclarativeScheduler has not been initialized.");

    private SchedulerState state = SchedulerState.CREATED;

    private long stateVersioner = 0;

    private ResourceCounter desiredResources = ResourceCounter.empty();

    private final SlotSharingSlotAllocator slotAllocator;

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
            long initializationTimestamp)
            throws Exception {
        this.jobGraph = jobGraph;
        this.configuration = configuration;
        this.logger = LoggerFactory.getLogger(DeclarativeScheduler.class);
        this.declarativeSlotPool = declarativeSlotPool;
        this.futureExecutor = futureExecutor;
        this.ioExecutor = ioExecutor;
        this.userCodeClassLoader = userCodeClassLoader;
        this.rpcTimeout = rpcTimeout;
        this.blobWriter = blobWriter;
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.shuffleMaster = shuffleMaster;
        this.partitionTracker = partitionTracker;
        this.executionDeploymentTracker = executionDeploymentTracker;
        this.initializationTimestamp = initializationTimestamp;

        this.completedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                        jobGraph,
                        configuration,
                        userCodeClassLoader,
                        checkpointRecoveryFactory,
                        logger);
        this.checkpointIdCounter =
                SchedulerUtils.createCheckpointIDCounterIfCheckpointingIsEnabled(
                        jobGraph, checkpointRecoveryFactory);
        this.checkpointsCleaner = new CheckpointsCleaner();

        this.declarativeSlotPool.registerNewSlotsListener(ignored -> newResourcesAvailable());
        this.slotAllocator =
                new SlotSharingSlotAllocator(
                        declarativeSlotPool::reserveFreeSlot,
                        declarativeSlotPool::freeReservedSlot);
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

        waitForResourcesSafely();
    }

    private void waitForResourcesSafely() {
        try {
            waitForResources();
        } catch (Exception e) {
            handleFatalFailure(new FlinkException("Failed to wait for resources.", e));
        }
    }

    private void waitForResources() throws Exception {
        state = SchedulerState.WAITING_FOR_RESOURCES;
        stateVersioner++;

        logger.info("Waiting for resources for job {}.", jobGraph.getJobID());

        final long currentStateVersion = stateVersioner;

        if (!scheduleIfEnoughResources()) {
            mainThreadExecutor.schedule(
                    () -> resourceTimeout(currentStateVersion), 10L, TimeUnit.SECONDS);
        }
    }

    private void newResourcesAvailable() {
        if (state == SchedulerState.WAITING_FOR_RESOURCES) {
            try {
                scheduleIfEnoughResources();
            } catch (Exception e) {
                handleFatalFailure(
                        new FlinkException("Failed to react to new resources available."));
            }
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
            final SlotInfo slotInfo = slotIterator.next();
            final ResourceProfile resourceProfile = slotInfo.getResourceProfile();

            if (outstandingResources.containsResource(resourceProfile)) {
                outstandingResources = outstandingResources.subtract(resourceProfile, 1);
            } else {
                outstandingResources = outstandingResources.subtract(ResourceProfile.UNKNOWN, 1);
            }
        }

        return outstandingResources.isEmpty();
    }

    private void resourceTimeout(long timeoutForSchedulingAttempt) {
        if (timeoutForSchedulingAttempt == stateVersioner) {
            logger.info("Resource timeout for job {} reached.", jobGraph.getJobID());
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
        stateVersioner++;

        createExecutionGraphAndAssignResources();
        deployExecutionGraph();
    }

    private void createExecutionGraphAndAssignResources() throws Exception {
        logger.debug("Calculate parallelism of job and assign resources.");

        final Optional<ParallelismAndResourceAssignments>
                parallelismAndResourceAssignmentsOptional =
                        slotAllocator.determineParallelismAndAssignResources(
                                new JobGraphJobInformation(jobGraph),
                                declarativeSlotPool.getFreeSlotsInformation());

        if (!parallelismAndResourceAssignmentsOptional.isPresent()) {
            throw new JobExecutionException(
                    jobGraph.getJobID(), "Not enough resources available for scheduling.");
        }

        final ParallelismAndResourceAssignments parallelismAndResourceAssignments =
                parallelismAndResourceAssignmentsOptional.get();

        for (JobVertex vertex : jobGraph.getVertices()) {
            vertex.setParallelism(parallelismAndResourceAssignments.getParallelism(vertex.getID()));
        }

        executionGraph = createExecutionGraphAndRestoreState();

        executionGraph.start(mainThreadExecutor);
        executionGraph.transitionToRunning();

        final long currentStateVersion = stateVersioner;

        FutureUtils.assertNoException(
                executionGraph
                        .getTerminationFuture()
                        .thenAccept(
                                terminalState -> {
                                    if (currentStateVersion == stateVersioner) {
                                        handleTerminalState(terminalState);
                                    }
                                }));

        executionGraph.setInternalTaskFailuresListener(
                new UpdateSchedulerNgOnInternalFailuresListener(this, jobGraph.getJobID()));

        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            final LogicalSlot assignedSlot =
                    parallelismAndResourceAssignments.getAssignedSlot(executionVertex.getID());
            executionVertex.tryAssignResource(assignedSlot);
        }
    }

    private ExecutionGraph createExecutionGraphAndRestoreState() throws Exception {
        ExecutionDeploymentListener executionDeploymentListener =
                new ExecutionDeploymentTrackerDeploymentListenerAdapter(executionDeploymentTracker);
        ExecutionStateUpdateListener executionStateUpdateListener =
                (execution, newState) -> {
                    if (newState.isTerminal()) {
                        executionDeploymentTracker.stopTrackingDeploymentOf(execution);
                    }
                };

        final ExecutionGraph newExecutionGraph =
                ExecutionGraphBuilder.buildGraph(
                        jobGraph,
                        configuration,
                        futureExecutor,
                        ioExecutor,
                        new ThrowingSlotProvider(),
                        userCodeClassLoader,
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
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

        final CheckpointCoordinator checkpointCoordinator =
                newExecutionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator != null) {
            // check whether we find a valid checkpoint
            if (!checkpointCoordinator.restoreInitialCheckpointIfPresent(
                    new HashSet<>(newExecutionGraph.getAllVertices().values()))) {

                // check whether we can restore from a savepoint
                tryRestoreExecutionGraphFromSavepoint(
                        newExecutionGraph, jobGraph.getSavepointRestoreSettings());
            }
        }

        return newExecutionGraph;
    }

    /**
     * Tries to restore the given {@link ExecutionGraph} from the provided {@link
     * SavepointRestoreSettings}.
     *
     * @param executionGraphToRestore {@link ExecutionGraph} which is supposed to be restored
     * @param savepointRestoreSettings {@link SavepointRestoreSettings} containing information about
     *     the savepoint to restore from
     * @throws Exception if the {@link ExecutionGraph} could not be restored
     */
    private void tryRestoreExecutionGraphFromSavepoint(
            ExecutionGraph executionGraphToRestore,
            SavepointRestoreSettings savepointRestoreSettings)
            throws Exception {
        if (savepointRestoreSettings.restoreSavepoint()) {
            final CheckpointCoordinator checkpointCoordinator =
                    executionGraphToRestore.getCheckpointCoordinator();
            if (checkpointCoordinator != null) {
                checkpointCoordinator.restoreSavepoint(
                        savepointRestoreSettings.getRestorePath(),
                        savepointRestoreSettings.allowNonRestoredState(),
                        executionGraphToRestore.getAllVertices(),
                        userCodeClassLoader);
            }
        }
    }

    private void handleTerminalState(JobStatus terminalState) {
        Preconditions.checkState(state == SchedulerState.EXECUTING, "Scheduler must be executing.");
        Preconditions.checkArgument(terminalState.isTerminalState());
        state = SchedulerState.FINISHED;

        logger.debug("Job {} reached a terminal state {}.", jobGraph.getJobID(), terminalState);

        stopCheckpointServicesSafely(terminalState);

        if (jobStatusListener != null) {
            jobStatusListener.jobStatusChanges(
                    jobGraph.getJobID(),
                    terminalState,
                    executionGraph.getStatusTimestamp(terminalState),
                    executionGraph.getFailureCause());
        }
    }

    private void stopCheckpointServicesSafely(JobStatus terminalState) {
        Exception exception = null;

        try {
            completedCheckpointStore.shutdown(terminalState, checkpointsCleaner, () -> {});
        } catch (Exception e) {
            exception = e;
        }

        try {
            checkpointIdCounter.shutdown(terminalState);
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            logger.warn("Failed to stop checkpoint services.", exception);
        }
    }

    public void handleFatalFailure(Throwable failure) {
        Preconditions.checkState(jobStatusListener != null, "JobStatusListener has not been set.");
        logger.error("Fatal error occurred in declarative scheduler.", failure);
        jobStatusListener.jobStatusChanges(
                jobGraph.getJobID(), JobStatus.FAILED, System.currentTimeMillis(), failure);
    }

    /** Assignment of slots to execution vertices. */
    public static final class ParallelismAndResourceAssignments {
        private final Map<ExecutionVertexID, ? extends LogicalSlot> assignedSlots;

        private final Map<JobVertexID, Integer> parallelismPerJobVertex;

        public ParallelismAndResourceAssignments(
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
        desiredResources = slotAllocator.calculateRequiredSlots(jobGraph.getVertices());
        declarativeSlotPool.increaseResourceRequirementsBy(desiredResources);
    }

    @Override
    public void suspend(Throwable cause) {
        stateVersioner++;
        executionGraph.suspend(cause);
        declarativeSlotPool.decreaseResourceRequirementsBy(desiredResources);
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
        Preconditions.checkState(
                state == SchedulerState.EXECUTING,
                "The scheduler should only handle failures when it is running.");

        logger.info("Global failure for job {} occurred.", jobGraph.getJobID(), cause);

        if (canRestart(cause)) {
            state = SchedulerState.GLOBALLY_RESTARTING;
            stateVersioner++;

            executionGraph.cancel();
            final CompletableFuture<JobStatus> terminationFuture =
                    executionGraph.getTerminationFuture();

            final long currentStateVersion = stateVersioner;
            FutureUtils.assertNoException(
                    terminationFuture.thenRun(
                            () -> {
                                if (currentStateVersion == stateVersioner) {
                                    restartScheduling();
                                }
                            }));
        } else {
            executionGraph.failJob(cause);
        }
    }

    private void restartScheduling() {
        logger.debug("Restart scheduling of job {}.", jobGraph.getJobID());
        executionGraph = null;
        waitForResourcesSafely();
    }

    private boolean canRestart(Throwable cause) {
        return true;
    }

    @Override
    public boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        if ((state == SchedulerState.EXECUTING || state == SchedulerState.GLOBALLY_RESTARTING)
                && executionGraph != null) {
            final boolean updateStateSuccess = executionGraph.updateState(taskExecutionState);

            if (updateStateSuccess) {
                if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
                    handleTaskFailure(
                            taskExecutionState.getID(),
                            taskExecutionState.getError(userCodeClassLoader));
                }

                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private void handleTaskFailure(ExecutionAttemptID executionAttemptId, Throwable cause) {
        handleGlobalFailure(cause);
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
        if (executionGraph != null) {
            return ArchivedExecutionGraph.createFrom(executionGraph);
        } else {
            return ArchivedExecutionGraph.createFromInitializingJob(
                    jobGraph.getJobID(),
                    jobGraph.getName(),
                    jobStatus,
                    null,
                    initializationTimestamp);
        }
    }

    @Override
    public JobStatus requestJobStatus() {
        if (executionGraph != null) {
            return executionGraph.getState();
        } else {
            return jobStatus;
        }
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

        if (executionGraph != null) {
            final CheckpointCoordinator checkpointCoordinator =
                    executionGraph.getCheckpointCoordinator();
            final AcknowledgeCheckpoint ackMessage =
                    new AcknowledgeCheckpoint(
                            jobID,
                            executionAttemptID,
                            checkpointId,
                            checkpointMetrics,
                            checkpointState);

            final String taskManagerLocationInfo =
                    retrieveTaskManagerLocation(executionGraph, executionAttemptID);

            if (checkpointCoordinator != null) {
                ioExecutor.execute(
                        () -> {
                            try {
                                checkpointCoordinator.receiveAcknowledgeMessage(
                                        ackMessage, taskManagerLocationInfo);
                            } catch (Throwable t) {
                                logger.warn(
                                        "Error while processing checkpoint acknowledgement message",
                                        t);
                            }
                        });
            } else {
                String errorMessage =
                        "Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator";
                if (executionGraph.getState() == JobStatus.RUNNING) {
                    logger.error(errorMessage, jobGraph.getJobID());
                } else {
                    logger.debug(errorMessage, jobGraph.getJobID());
                }
            }
        } else {
            logger.warn(
                    "Received AcknowledgeCheckpoint message for job {} while the ExecutionGraph is null.",
                    jobGraph.getJobID());
        }
    }

    private String retrieveTaskManagerLocation(
            ExecutionGraph executionGraph, ExecutionAttemptID executionAttemptID) {
        final Optional<Execution> currentExecution =
                Optional.ofNullable(
                        executionGraph.getRegisteredExecutions().get(executionAttemptID));

        return currentExecution
                .map(Execution::getAssignedResourceLocation)
                .map(TaskManagerLocation::toString)
                .orElse("Unknown location");
    }

    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        if (executionGraph != null) {
            final CheckpointCoordinator checkpointCoordinator =
                    executionGraph.getCheckpointCoordinator();
            final String taskManagerLocationInfo =
                    retrieveTaskManagerLocation(executionGraph, decline.getTaskExecutionId());

            if (checkpointCoordinator != null) {
                ioExecutor.execute(
                        () -> {
                            try {
                                checkpointCoordinator.receiveDeclineMessage(
                                        decline, taskManagerLocationInfo);
                            } catch (Exception e) {
                                logger.error(
                                        "Error in CheckpointCoordinator while processing {}",
                                        decline,
                                        e);
                            }
                        });
            } else {
                String errorMessage =
                        "Received DeclineCheckpoint message for job {} with no CheckpointCoordinator";
                if (executionGraph.getState() == JobStatus.RUNNING) {
                    logger.error(errorMessage, jobGraph.getJobID());
                } else {
                    logger.debug(errorMessage, jobGraph.getJobID());
                }
            }
        } else {
            logger.warn(
                    "Received DeclineCheckpoint message for job {} while the ExecutionGraph is null.",
                    jobGraph.getJobID());
        }
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
        GLOBALLY_RESTARTING,
        FINISHED,
    }
}
