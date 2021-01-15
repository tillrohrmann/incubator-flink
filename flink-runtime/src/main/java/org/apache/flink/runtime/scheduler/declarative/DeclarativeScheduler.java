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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
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
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerUtils;
import org.apache.flink.runtime.scheduler.UpdateSchedulerNgOnInternalFailuresListener;
import org.apache.flink.runtime.scheduler.declarative.allocator.SlotAllocator;
import org.apache.flink.runtime.scheduler.declarative.allocator.SlotSharingSlotAllocator;
import org.apache.flink.runtime.scheduler.declarative.allocator.VertexAssignment;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/** Declarative scheduler. */
public class DeclarativeScheduler
        implements SchedulerNG,
                Created.Context,
                Finished.Context,
                WaitingForResources.Context,
                Executing.Context,
                Restarting.Context,
                Failing.Context {

    private static final Logger LOG = LoggerFactory.getLogger(DeclarativeScheduler.class);

    private final JobGraphJobInformation jobInformation;

    private final DeclarativeSlotPool declarativeSlotPool;

    private final long initializationTimestamp;

    private final Configuration configuration;
    private final ScheduledExecutorService futureExecutor;
    private final Executor ioExecutor;
    private final ClassLoader userCodeClassLoader;
    private final Time rpcTimeout;
    private final BlobWriter blobWriter;
    private final ShuffleMaster<?> shuffleMaster;
    private final JobMasterPartitionTracker partitionTracker;
    private final ExecutionDeploymentTracker executionDeploymentTracker;
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;
    private final BackPressureStatsTracker backPressureStatsTracker;

    private final CompletedCheckpointStore completedCheckpointStore;
    private final CheckpointIDCounter checkpointIdCounter;
    private final CheckpointsCleaner checkpointsCleaner;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
    private final RestartBackoffTimeStrategy restartBackoffTimeStrategy;

    private ComponentMainThreadExecutor componentMainThreadExecutor =
            new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("foobar");

    @Nullable private JobStatusListener jobStatusListener;

    private final SlotAllocator<?> slotAllocator;

    private State state = new Created(this, LOG);

    public DeclarativeScheduler(
            JobGraph jobGraph,
            Configuration configuration,
            Logger log,
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
            RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            ExecutionDeploymentTracker executionDeploymentTracker,
            BackPressureStatsTracker backPressureStatsTracker,
            long initializationTimestamp)
            throws JobExecutionException {

        this.jobInformation = new JobGraphJobInformation(jobGraph);
        this.declarativeSlotPool = declarativeSlotPool;
        this.initializationTimestamp = initializationTimestamp;
        this.configuration = configuration;
        this.futureExecutor = futureExecutor;
        this.ioExecutor = ioExecutor;
        this.userCodeClassLoader = userCodeClassLoader;
        this.rpcTimeout = rpcTimeout;
        this.blobWriter = blobWriter;
        this.shuffleMaster = shuffleMaster;
        this.partitionTracker = partitionTracker;
        this.restartBackoffTimeStrategy = restartBackoffTimeStrategy;
        this.executionDeploymentTracker = executionDeploymentTracker;
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.backPressureStatsTracker = backPressureStatsTracker;
        this.completedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                        jobGraph,
                        configuration,
                        userCodeClassLoader,
                        checkpointRecoveryFactory,
                        LOG);
        this.checkpointIdCounter =
                SchedulerUtils.createCheckpointIDCounterIfCheckpointingIsEnabled(
                        jobGraph, checkpointRecoveryFactory);
        this.checkpointsCleaner = new CheckpointsCleaner();

        this.slotAllocator =
                new SlotSharingSlotAllocator(
                        declarativeSlotPool::reserveFreeSlot,
                        declarativeSlotPool::freeReservedSlot);

        declarativeSlotPool.registerNewSlotsListener(this::newResourcesAvailable);
    }

    private void newResourcesAvailable(Collection<? extends PhysicalSlot> physicalSlots) {
        state.tryRun(
                ResourceConsumer.class,
                ResourceConsumer::notifyNewResourcesAvailable,
                "newResourcesAvailable");
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
            LOG.warn("Failed to stop checkpoint services.", exception);
        }
    }

    private ResourceCounter calculateDesiredResources() {
        return slotAllocator.calculateRequiredSlots(jobInformation.getVertices());
    }

    private ExecutionGraph createExecutionGraphAndRestoreState(JobGraph adjustedJobGraph)
            throws Exception {
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
                        adjustedJobGraph,
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
                        LOG,
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
                        newExecutionGraph, adjustedJobGraph.getSavepointRestoreSettings());
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

    @Override
    public void initialize(ComponentMainThreadExecutor mainThreadExecutor) {
        this.componentMainThreadExecutor = mainThreadExecutor;
    }

    @Override
    public void registerJobStatusListener(JobStatusListener jobStatusListener) {
        this.jobStatusListener = jobStatusListener;
    }

    @Override
    public void startScheduling() {
        state.as(Created.class)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Can only start scheduling when being in Created state."))
                .startScheduling();
    }

    @Override
    public void suspend(Throwable cause) {
        state.suspend(cause);
    }

    @Override
    public void cancel() {
        state.cancel();
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        state.handleGlobalFailure(cause);
    }

    @Override
    public boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        final Optional<Boolean> optionalResult =
                state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.updateTaskExecutionState(
                                        taskExecutionState),
                        "updateTaskExecutionState");

        return optionalResult.orElse(false);
    }

    @Override
    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.requestNextInputSplit(
                                        vertexID, executionAttempt),
                        "requestNextInputSplit")
                .orElseThrow(
                        () -> new IOException("Scheduler is currently not executing the job."));
    }

    @Override
    public ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.requestPartitionState(
                                        intermediateResultId, resultPartitionId),
                        "requestPartitionState")
                .orElseThrow(() -> new PartitionProducerDisposedException(resultPartitionId));
    }

    @Override
    public void notifyPartitionDataAvailable(ResultPartitionID partitionID) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyPartitionDataAvailable(partitionID),
                "notifyPartitionDataAvailable");
    }

    @Override
    public ArchivedExecutionGraph requestJob() {
        return state.getJob();
    }

    @Override
    public JobStatus requestJobStatus() {
        return state.getJobStatus();
    }

    @Override
    public JobDetails requestJobDetails() {
        return JobDetails.createDetailsForJob(state.getJob());
    }

    @Override
    public KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {
        final Optional<StateWithExecutionGraph> asOptional =
                state.as(StateWithExecutionGraph.class);

        if (asOptional.isPresent()) {
            return asOptional.get().requestKvStateLocation(jobId, registrationName);
        } else {
            throw new UnknownKvStateLocation(registrationName);
        }
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
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyKvStateRegistered(
                                jobId,
                                jobVertexId,
                                keyGroupRange,
                                registrationName,
                                kvStateId,
                                kvStateServerAddress),
                "notifyKvStateRegistered");
    }

    @Override
    public void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyKvStateUnregistered(
                                jobId, jobVertexId, keyGroupRange, registrationName),
                "notifyKvStateUnregistered");
    }

    @Override
    public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.updateAccumulators(accumulatorSnapshot),
                "updateAccumulators");
    }

    @Override
    public Optional<OperatorBackPressureStats> requestOperatorBackPressureStats(
            JobVertexID jobVertexId) throws FlinkException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.requestOperatorBackPressureStats(
                                        jobVertexId),
                        "requestOperatorBackPressureStats")
                .flatMap(Function.identity());
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob) {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.triggerSavepoint(
                                        targetDirectory, cancelJob),
                        "triggerSavepoint")
                .orElse(
                        FutureUtils.completedExceptionally(
                                new CheckpointException(
                                        "The Flink job is currently not executing.",
                                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE)));
    }

    @Override
    public void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.acknowledgeCheckpoint(
                                jobID,
                                executionAttemptID,
                                checkpointId,
                                checkpointMetrics,
                                checkpointState),
                "acknowledgeCheckpoint");
    }

    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph -> stateWithExecutionGraph.declineCheckpoint(decline),
                "declineCheckpoint");
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            String targetDirectory, boolean advanceToEndOfEventTime) {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.stopWithSavepoint(
                                        targetDirectory, advanceToEndOfEventTime),
                        "stopWithSavepoint")
                .orElse(
                        FutureUtils.completedExceptionally(
                                new CheckpointException(
                                        "The Flink job is currently not executing.",
                                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE)));
    }

    @Override
    public void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecution, OperatorID operator, OperatorEvent evt)
            throws FlinkException {
        final StateWithExecutionGraph stateWithExecutionGraph =
                state.as(StateWithExecutionGraph.class)
                        .orElseThrow(
                                () ->
                                        new TaskNotRunningException(
                                                "Task is not known or in state running on the JobManager."));

        stateWithExecutionGraph.deliverOperatorEventToCoordinator(taskExecution, operator, evt);
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.deliverCoordinationRequestToCoordinator(
                                        operator, request),
                        "deliverCoordinationRequestToCoordinator")
                .orElseGet(
                        () ->
                                FutureUtils.completedExceptionally(
                                        new FlinkException(
                                                "Coordinator of operator "
                                                        + operator
                                                        + " does not exist")));
    }

    // ----------------------------------------------------------------

    @Override
    public boolean hasEnoughResources(ResourceCounter desiredResources) {
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

    private <T extends VertexAssignment>
            ParallelismAndResourceAssignments determineParallelismAndAssignResources(
                    SlotAllocator<T> slotAllocator) throws JobExecutionException {
        final Optional<T> parallelism =
                slotAllocator.determineParallelism(
                        jobInformation, declarativeSlotPool.getFreeSlotsInformation());

        if (!parallelism.isPresent()) {
            throw new JobExecutionException(
                    jobInformation.getJobID(), "Not enough resources available for scheduling.");
        }

        return slotAllocator.assignResources(
                jobInformation, declarativeSlotPool.getFreeSlotsInformation(), parallelism.get());
    }

    @Override
    public ExecutionGraph createExecutionGraphWithAvailableResources() throws Exception {
        final ParallelismAndResourceAssignments parallelismAndResourceAssignments =
                determineParallelismAndAssignResources(slotAllocator);

        JobGraph adjustedJobGraph = jobInformation.copyJobGraph();
        for (JobVertex vertex : adjustedJobGraph.getVertices()) {
            vertex.setParallelism(parallelismAndResourceAssignments.getParallelism(vertex.getID()));
        }

        final ExecutionGraph executionGraph = createExecutionGraphAndRestoreState(adjustedJobGraph);

        executionGraph.start(componentMainThreadExecutor);
        executionGraph.transitionToRunning();

        executionGraph.setInternalTaskFailuresListener(
                new UpdateSchedulerNgOnInternalFailuresListener(this, jobInformation.getJobID()));

        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            final LogicalSlot assignedSlot =
                    parallelismAndResourceAssignments.getAssignedSlot(executionVertex.getID());
            executionVertex
                    .getCurrentExecutionAttempt()
                    .registerProducedPartitions(assignedSlot.getTaskManagerLocation(), false);
            executionVertex.tryAssignResource(assignedSlot);
        }
        return executionGraph;
    }

    @Override
    public ArchivedExecutionGraph getArchivedExecutionGraph(
            JobStatus jobStatus, @Nullable Throwable cause) {
        return ArchivedExecutionGraph.createFromInitializingJob(
                jobInformation.getJobID(),
                jobInformation.getName(),
                jobStatus,
                cause,
                initializationTimestamp);
    }

    @Override
    public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
        transitionToState(new Finished(this, archivedExecutionGraph, LOG));
    }

    @Override
    public void goToExecuting(ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph, backPressureStatsTracker, LOG, ioExecutor);
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new OperatorCoordinatorHandler(executionGraph, this::handleGlobalFailure);
        operatorCoordinatorHandler.initializeOperatorCoordinators(componentMainThreadExecutor);
        operatorCoordinatorHandler.startAllOperatorCoordinators();

        transitionToState(
                new Executing(
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        this,
                        userCodeClassLoader));
    }

    @Override
    public void goToWaitingForResources() {
        final ResourceCounter desiredResources = calculateDesiredResources();
        // TODO: replace with set resources
        declarativeSlotPool.increaseResourceRequirementsBy(desiredResources);

        transitionToState(new WaitingForResources(this, LOG, desiredResources));
    }

    @Override
    public void goToCanceling(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler) {
        transitionToState(
                new Canceling(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG));
    }

    @Override
    public void goToRestarting(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Duration backoffTime) {
        transitionToState(
                new Restarting(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        backoffTime));
    }

    @Override
    public void goToFailing(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Throwable failureCause) {
        transitionToState(
                new Failing(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        failureCause));
    }

    @Override
    public boolean canScaleUp(ExecutionGraph executionGraph) {
        int availableSlots = declarativeSlotPool.getFreeSlotsInformation().size();

        if (availableSlots > 0) {
            // check if rescaling would increase the parallelism of at least one vertex

            final Optional<? extends VertexAssignment> potentialNewParallelism =
                    slotAllocator.determineParallelism(
                            jobInformation, declarativeSlotPool.getAllSlotsInformation());

            return potentialNewParallelism.isPresent()
                    && isParallelismHigher(potentialNewParallelism.get(), executionGraph);
        }
        return false;
    }

    private boolean isParallelismHigher(
            VertexAssignment vertexAssignment, ExecutionGraph executionGraph) {
        Map<JobVertexID, Integer> perVertex = vertexAssignment.getMaxParallelismForVertices();

        return StreamSupport.stream(executionGraph.getAllExecutionVertices().spliterator(), false)
                .anyMatch(
                        vertex ->
                                perVertex.get(vertex.getJobvertexId())
                                        > vertex.getJobVertex().getParallelism());
    }

    @Override
    public void onFinished(ArchivedExecutionGraph archivedExecutionGraph) {
        stopCheckpointServicesSafely(archivedExecutionGraph.getState());

        if (jobStatusListener != null) {
            jobStatusListener.jobStatusChanges(
                    jobInformation.getJobID(),
                    archivedExecutionGraph.getState(),
                    archivedExecutionGraph.getStatusTimestamp(archivedExecutionGraph.getState()),
                    archivedExecutionGraph.getFailureInfo() != null
                            ? archivedExecutionGraph.getFailureInfo().getException()
                            : null);
        }
    }

    @Override
    public void runIfState(State expectedState, Runnable action) {
        if (state == expectedState) {
            try {
                action.run();
            } catch (Throwable t) {
                // TODO pass a proper FatalExceptionHandler to the scheduler
                FatalExitExceptionHandler.INSTANCE.uncaughtException(Thread.currentThread(), t);
            }
        } else {
            LOG.debug(
                    "Ignoring scheduled action because expected state {} is not the actual state {}.",
                    expectedState,
                    state);
        }
    }

    @Override
    public boolean isState(State expectedState) {
        return expectedState == this.state;
    }

    @Override
    public Executor getMainThreadExecutor() {
        return componentMainThreadExecutor;
    }

    @Override
    public void runIfState(State expectedState, Runnable action, Duration delay) {
        componentMainThreadExecutor.schedule(
                () -> runIfState(expectedState, action), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Executing.FailureResult howToHandleFailure(Throwable failure) {
        if (ExecutionFailureHandler.isUnrecoverableError(failure)) {
            return Executing.FailureResult.canNotRestart(
                    new JobException("The failure is not recoverable", failure));
        }

        restartBackoffTimeStrategy.notifyFailure(failure);
        if (restartBackoffTimeStrategy.canRestart()) {
            return Executing.FailureResult.canRestart(
                    Duration.ofMillis(restartBackoffTimeStrategy.getBackoffTime()));
        } else {
            return Executing.FailureResult.canNotRestart(
                    new JobException(
                            "Recovery is suppressed by " + restartBackoffTimeStrategy, failure));
        }
    }

    // ----------------------------------------------------------------

    private void transitionToState(State newState) {
        if (state != newState) {
            LOG.debug(
                    "Transition from state {} to {}.",
                    state.getClass().getSimpleName(),
                    newState.getClass().getSimpleName());
            final State oldState = state;
            state = newState;

            oldState.onLeave(newState);
            newState.onEnter();
        }
    }
}
