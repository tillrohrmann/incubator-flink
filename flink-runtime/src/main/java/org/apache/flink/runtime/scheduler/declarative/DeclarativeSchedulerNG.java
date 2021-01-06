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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.io.InputSplit;
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
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
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
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
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
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerUtils;
import org.apache.flink.runtime.scheduler.UpdateSchedulerNgOnInternalFailuresListener;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Declarative scheduler ng. */
public class DeclarativeSchedulerNG implements SchedulerNG {

    private static final Logger LOG = LoggerFactory.getLogger(DeclarativeSchedulerNG.class);

    private final JobGraph jobGraph;

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

    private ComponentMainThreadExecutor componentMainThreadExecutor =
            new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("foobar");

    @Nullable private JobStatusListener jobStatusListener;

    private State state = new Created();

    public DeclarativeSchedulerNG(
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
            ExecutionDeploymentTracker executionDeploymentTracker,
            BackPressureStatsTracker backPressureStatsTracker,
            long initializationTimestamp)
            throws JobExecutionException {

        this.jobGraph = jobGraph;
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

        declarativeSlotPool.registerNewSlotsListener(this::newResourcesAvailable);
    }

    private void newResourcesAvailable(Collection<? extends PhysicalSlot> physicalSlots) {
        state.tryRun(
                ResourceConsumer.class,
                ResourceConsumer::newResourcesAvailable,
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

    @Nonnull
    private ArchivedExecutionGraph createArchivedExecutionGraph(
            JobStatus jobStatus, @Nullable Throwable throwable) {
        return ArchivedExecutionGraph.createFromInitializingJob(
                jobGraph.getJobID(),
                jobGraph.getName(),
                jobStatus,
                throwable,
                initializationTimestamp);
    }

    private ResourceCounter calculateDesiredResources() {
        int counter = 0;

        for (JobVertex vertex : jobGraph.getVertices()) {
            counter += vertex.getParallelism();
        }

        return ResourceCounter.withResource(ResourceProfile.UNKNOWN, counter);
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
    public void initialize(ComponentMainThreadExecutor mainThreadExecutor) {
        this.componentMainThreadExecutor = mainThreadExecutor;
    }

    @Override
    public void registerJobStatusListener(JobStatusListener jobStatusListener) {
        this.jobStatusListener = jobStatusListener;
    }

    @Override
    public void startScheduling() {
        transitionToState(
                state.as(Created.class)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Can only start scheduling when being in Created state."))
                        .startScheduling());
    }

    @Override
    public void suspend(Throwable cause) {
        transitionToState(state.suspend(cause));
    }

    @Override
    public void cancel() {
        transitionToState(state.cancel());
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        transitionToState(state.handleGlobalFailure(cause));
    }

    @Override
    public boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        final Optional<StateAndBoolean> optionalResult =
                state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.updateTaskExecutionState(
                                        taskExecutionState),
                        "updateTaskExecutionState");

        if (optionalResult.isPresent()) {
            final StateAndBoolean stateAndBoolean = optionalResult.get();

            transitionToState(stateAndBoolean.getState());
            return stateAndBoolean.getResult();
        } else {
            return false;
        }
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
        state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.deliverOperatorEventToCoordinator(
                                        taskExecution, operator, evt),
                        "deliverOperatorEventToCoordinator")
                .orElseThrow(
                        () ->
                                new TaskNotRunningException(
                                        "Task is not known or in state running on the JobManager."));
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

    private void transitionToState(State newState) {
        if (state != newState) {
            LOG.debug(
                    "Transition from state {} to {}.",
                    state.getClass().getSimpleName(),
                    newState.getClass().getSimpleName());
            state = newState;
        }
    }

    private void runIfState(State expectedState, Runnable action) {
        runIfState(expectedState, action, 0L, TimeUnit.SECONDS);
    }

    private void runIfState(State expectedState, Runnable action, long delay, TimeUnit timeUnit) {
        componentMainThreadExecutor.schedule(
                () -> {
                    if (state == expectedState) {
                        action.run();
                    } else {
                        LOG.debug(
                                "Ignoring scheduled action because expected state {} is not the actual state {}.",
                                expectedState,
                                state);
                    }
                },
                delay,
                timeUnit);
    }

    // ----------------------------------------------------------------

    private final class Created implements State {

        @Override
        public State cancel() {
            return new Finished(createArchivedExecutionGraph(JobStatus.CANCELED, null));
        }

        @Override
        public State suspend(Throwable cause) {
            return new Finished(createArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
        }

        @Override
        public JobStatus getJobStatus() {
            return JobStatus.INITIALIZING;
        }

        @Override
        public ArchivedExecutionGraph getJob() {
            return createArchivedExecutionGraph(getJobStatus(), null);
        }

        @Override
        public State handleGlobalFailure(Throwable cause) {
            return new Finished(createArchivedExecutionGraph(JobStatus.INITIALIZING, cause));
        }

        WaitingForResources startScheduling() {
            final ResourceCounter desiredResources = calculateDesiredResources();
            declarativeSlotPool.increaseResourceRequirementsBy(desiredResources);

            return new WaitingForResources(desiredResources);
        }
    }

    private final class WaitingForResources implements State, ResourceConsumer {

        private final ResourceCounter desiredResources;

        private WaitingForResources(ResourceCounter desiredResources) {
            this.desiredResources = desiredResources;

            runIfState(this, this::newResourcesAvailable);

            // register timeout
            runIfState(this, this::resourceTimeout, 10L, TimeUnit.SECONDS);
        }

        @Override
        public State cancel() {
            return new Finished(createArchivedExecutionGraph(JobStatus.CANCELED, null));
        }

        @Override
        public State suspend(Throwable cause) {
            return new Finished(createArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
        }

        @Override
        public JobStatus getJobStatus() {
            return JobStatus.INITIALIZING;
        }

        @Override
        public ArchivedExecutionGraph getJob() {
            return createArchivedExecutionGraph(JobStatus.INITIALIZING, null);
        }

        @Override
        public State handleGlobalFailure(Throwable cause) {
            return new Finished(createArchivedExecutionGraph(JobStatus.INITIALIZING, cause));
        }

        @Override
        public void newResourcesAvailable() {
            if (hasEnoughResources()) {
                transitionToState(createExecutionGraphWithAvailableResources());
            }
        }

        private void resourceTimeout() {
            transitionToState(createExecutionGraphWithAvailableResources());
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
                    outstandingResources =
                            outstandingResources.subtract(ResourceProfile.UNKNOWN, 1);
                }
            }

            return outstandingResources.isEmpty();
        }

        private State createExecutionGraphWithAvailableResources() {
            try {
                final ExecutionGraph executionGraph = createExecutionGraphAndAssignResources();

                return new Executing(executionGraph);
            } catch (Exception exception) {
                return new Finished(createArchivedExecutionGraph(JobStatus.FAILED, exception));
            }
        }

        private DeclarativeScheduler.ParallelismAndResourceAssignments
                determineParallelismAndAssignResources(JobGraph jobGraph) {
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

                    final SingleLogicalSlot logicalSlot =
                            new SingleLogicalSlot(
                                    new SlotRequestId(),
                                    physicalSlot,
                                    null,
                                    Locality.UNKNOWN,
                                    slot ->
                                            declarativeSlotPool.freeReservedSlot(
                                                    physicalSlot.getAllocationId(),
                                                    null,
                                                    System.currentTimeMillis()));
                    physicalSlot.tryAssignPayload(logicalSlot);

                    assignedSlots.put(new ExecutionVertexID(jobVertex.getID(), i), logicalSlot);
                }
            }

            return new DeclarativeScheduler.ParallelismAndResourceAssignments(
                    assignedSlots, parallelismPerJobVertex);
        }

        @Nonnull
        private ExecutionGraph createExecutionGraphAndAssignResources() throws Exception {
            final DeclarativeScheduler.ParallelismAndResourceAssignments
                    parallelismAndResourceAssignments =
                            determineParallelismAndAssignResources(jobGraph);

            for (JobVertex vertex : jobGraph.getVertices()) {
                vertex.setParallelism(
                        parallelismAndResourceAssignments.getParallelism(vertex.getID()));
            }

            final ExecutionGraph executionGraph = createExecutionGraphAndRestoreState();

            executionGraph.start(componentMainThreadExecutor);
            executionGraph.transitionToRunning();

            executionGraph.setInternalTaskFailuresListener(
                    new UpdateSchedulerNgOnInternalFailuresListener(
                            DeclarativeSchedulerNG.this, jobGraph.getJobID()));

            for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
                final LogicalSlot assignedSlot =
                        parallelismAndResourceAssignments.getAssignedSlot(executionVertex.getID());
                executionVertex.tryAssignResource(assignedSlot);
            }
            return executionGraph;
        }
    }

    private final class Executing extends StateWithExecutionGraph {

        private Executing(ExecutionGraph executionGraph) {
            super(executionGraph);

            runIfState(this, this::deploy);
        }

        @Override
        public State cancel() {
            return new Canceling(executionGraph);
        }

        @Override
        public State handleGlobalFailure(Throwable cause) {
            executionGraph.initFailureCause(cause);
            return new Restarting(executionGraph);
        }

        @Override
        StateAndBoolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
            final boolean successfulUpdate = executionGraph.updateState(taskExecutionState);

            if (successfulUpdate) {
                if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
                    return StateAndBoolean.create(new Restarting(executionGraph), true);
                } else {
                    return StateAndBoolean.create(this, true);
                }
            } else {
                return StateAndBoolean.create(this, false);
            }
        }

        @Override
        void onTerminalState(JobStatus jobStatus) {
            transitionToState(new Finished(ArchivedExecutionGraph.createFrom(executionGraph)));
        }

        private void deploy() {
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
    }

    private final class Restarting extends StateWithExecutionGraph {

        private Restarting(ExecutionGraph executionGraph) {
            super(executionGraph);
            executionGraph.cancel();
        }

        @Override
        public State cancel() {
            return new Canceling(executionGraph);
        }

        @Override
        public State handleGlobalFailure(Throwable cause) {
            return this;
        }

        @Override
        StateAndBoolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
            final boolean successfulUpdate = executionGraph.updateState(taskExecutionState);

            return StateAndBoolean.create(this, successfulUpdate);
        }

        @Override
        void onTerminalState(JobStatus jobStatus) {
            Preconditions.checkArgument(jobStatus == JobStatus.CANCELED);

            transitionToState(new WaitingForResources(calculateDesiredResources()));
        }
    }

    private final class Canceling extends StateWithExecutionGraph {

        private Canceling(ExecutionGraph executionGraph) {
            super(executionGraph);
            executionGraph.cancel();
        }

        @Override
        public State cancel() {
            return this;
        }

        @Override
        public State handleGlobalFailure(Throwable cause) {
            return this;
        }

        @Override
        StateAndBoolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
            final boolean successfulUpdate = executionGraph.updateState(taskExecutionState);

            return StateAndBoolean.create(this, successfulUpdate);
        }

        @Override
        void onTerminalState(JobStatus jobStatus) {
            // not sure whether FAILED can or cannot happen here
            Preconditions.checkArgument(
                    jobStatus == JobStatus.CANCELED || jobStatus == JobStatus.FAILED);

            transitionToState(new Finished(ArchivedExecutionGraph.createFrom(executionGraph)));
        }
    }

    private final class Finished implements State {

        private final ArchivedExecutionGraph archivedExecutionGraph;

        private Finished(ArchivedExecutionGraph archivedExecutionGraph) {
            this.archivedExecutionGraph = archivedExecutionGraph;

            // set declared resources to 0
            // declarativeSlotPool.decreaseResourceRequirementsBy(0);

            runIfState(this, this::processFinishedState);
        }

        private void processFinishedState() {
            stopCheckpointServicesSafely(archivedExecutionGraph.getState());

            if (jobStatusListener != null) {
                jobStatusListener.jobStatusChanges(
                        jobGraph.getJobID(),
                        archivedExecutionGraph.getState(),
                        archivedExecutionGraph.getStatusTimestamp(
                                archivedExecutionGraph.getState()),
                        archivedExecutionGraph.getFailureInfo() != null
                                ? archivedExecutionGraph.getFailureInfo().getException()
                                : null);
            }
        }

        @Override
        public State cancel() {
            return this;
        }

        @Override
        public State suspend(Throwable cause) {
            return this;
        }

        @Override
        public JobStatus getJobStatus() {
            return archivedExecutionGraph.getState();
        }

        @Override
        public ArchivedExecutionGraph getJob() {
            return archivedExecutionGraph;
        }

        @Override
        public State handleGlobalFailure(Throwable cause) {
            return this;
        }
    }

    private static final class StateAndBoolean {
        private final State state;
        private final boolean result;

        private StateAndBoolean(State state, boolean result) {
            this.state = state;
            this.result = result;
        }

        public static StateAndBoolean create(State state, boolean result) {
            return new StateAndBoolean(state, result);
        }

        public State getState() {
            return state;
        }

        public boolean getResult() {
            return result;
        }
    }

    private abstract class StateWithExecutionGraph implements State {
        protected final ExecutionGraph executionGraph;

        private final Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap;

        protected StateWithExecutionGraph(ExecutionGraph executionGraph) {
            this.executionGraph = executionGraph;

            this.coordinatorMap = createCoordinatorMap(executionGraph);

            executionGraph
                    .getTerminationFuture()
                    .thenAcceptAsync(
                            jobStatus -> {
                                if (state == this) {
                                    onTerminalState(jobStatus);
                                }
                            },
                            componentMainThreadExecutor);
        }

        private Map<OperatorID, OperatorCoordinatorHolder> createCoordinatorMap(
                ExecutionGraph executionGraph) {
            Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap = new HashMap<>();
            for (ExecutionJobVertex vertex : executionGraph.getAllVertices().values()) {
                for (OperatorCoordinatorHolder holder : vertex.getOperatorCoordinators()) {
                    coordinatorMap.put(holder.operatorId(), holder);
                }
            }
            return coordinatorMap;
        }

        @Override
        public State suspend(Throwable cause) {
            executionGraph.suspend(cause);
            Preconditions.checkState(executionGraph.getState() == JobStatus.SUSPENDED);
            return new Finished(ArchivedExecutionGraph.createFrom(executionGraph));
        }

        @Override
        public JobStatus getJobStatus() {
            return executionGraph.getState();
        }

        @Override
        public ArchivedExecutionGraph getJob() {
            return ArchivedExecutionGraph.createFrom(executionGraph);
        }

        void notifyPartitionDataAvailable(ResultPartitionID partitionID) {
            executionGraph.notifyPartitionDataAvailable(partitionID);
        }

        SerializedInputSplit requestNextInputSplit(
                JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
            final Execution execution =
                    executionGraph.getRegisteredExecutions().get(executionAttempt);
            if (execution == null) {
                // can happen when JobManager had already unregistered this execution upon on task
                // failure,
                // but TaskManager get some delay to aware of that situation
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Can not find Execution for attempt {}.", executionAttempt);
                }
                // but we should TaskManager be aware of this
                throw new IllegalArgumentException(
                        "Can not find Execution for attempt " + executionAttempt);
            }

            final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
            if (vertex == null) {
                throw new IllegalArgumentException(
                        "Cannot find execution vertex for vertex ID " + vertexID);
            }

            if (vertex.getSplitAssigner() == null) {
                throw new IllegalStateException("No InputSplitAssigner for vertex ID " + vertexID);
            }

            final InputSplit nextInputSplit = execution.getNextInputSplit();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Send next input split {}.", nextInputSplit);
            }

            try {
                final byte[] serializedInputSplit =
                        InstantiationUtil.serializeObject(nextInputSplit);
                return new SerializedInputSplit(serializedInputSplit);
            } catch (Exception ex) {
                IOException reason =
                        new IOException(
                                "Could not serialize the next input split of class "
                                        + nextInputSplit.getClass()
                                        + ".",
                                ex);
                vertex.fail(reason);
                throw reason;
            }
        }

        private ExecutionState requestPartitionState(
                IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
                throws PartitionProducerDisposedException {
            final Execution execution =
                    executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
            if (execution != null) {
                return execution.getState();
            } else {
                final IntermediateResult intermediateResult =
                        executionGraph.getAllIntermediateResults().get(intermediateResultId);

                if (intermediateResult != null) {
                    // Try to find the producing execution
                    Execution producerExecution =
                            intermediateResult
                                    .getPartitionById(resultPartitionId.getPartitionId())
                                    .getProducer()
                                    .getCurrentExecutionAttempt();

                    if (producerExecution
                            .getAttemptId()
                            .equals(resultPartitionId.getProducerId())) {
                        return producerExecution.getState();
                    } else {
                        throw new PartitionProducerDisposedException(resultPartitionId);
                    }
                } else {
                    throw new IllegalArgumentException(
                            "Intermediate data set with ID "
                                    + intermediateResultId
                                    + " not found.");
                }
            }
        }

        private void acknowledgeCheckpoint(
                JobID jobID,
                ExecutionAttemptID executionAttemptID,
                long checkpointId,
                CheckpointMetrics checkpointMetrics,
                TaskStateSnapshot checkpointState) {

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
                                LOG.warn(
                                        "Error while processing checkpoint acknowledgement message",
                                        t);
                            }
                        });
            } else {
                String errorMessage =
                        "Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator";
                if (executionGraph.getState() == JobStatus.RUNNING) {
                    LOG.error(errorMessage, jobGraph.getJobID());
                } else {
                    LOG.debug(errorMessage, jobGraph.getJobID());
                }
            }
        }

        private void declineCheckpoint(DeclineCheckpoint decline) {
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
                                LOG.error(
                                        "Error in CheckpointCoordinator while processing {}",
                                        decline,
                                        e);
                            }
                        });
            } else {
                String errorMessage =
                        "Received DeclineCheckpoint message for job {} with no CheckpointCoordinator";
                if (executionGraph.getState() == JobStatus.RUNNING) {
                    LOG.error(errorMessage, jobGraph.getJobID());
                } else {
                    LOG.debug(errorMessage, jobGraph.getJobID());
                }
            }
        }

        private void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
            executionGraph.updateAccumulators(accumulatorSnapshot);
        }

        private KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
                throws FlinkJobNotFoundException, UnknownKvStateLocation {
            if (jobGraph.getJobID().equals(jobId)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Lookup key-value state for job {} with registration " + "name {}.",
                            jobGraph.getJobID(),
                            registrationName);
                }

                final KvStateLocationRegistry registry =
                        executionGraph.getKvStateLocationRegistry();
                final KvStateLocation location = registry.getKvStateLocation(registrationName);
                if (location != null) {
                    return location;
                } else {
                    throw new UnknownKvStateLocation(registrationName);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Request of key-value state location for unknown job {} received.",
                            jobId);
                }
                throw new FlinkJobNotFoundException(jobId);
            }
        }

        private void notifyKvStateRegistered(
                JobID jobId,
                JobVertexID jobVertexId,
                KeyGroupRange keyGroupRange,
                String registrationName,
                KvStateID kvStateId,
                InetSocketAddress kvStateServerAddress)
                throws FlinkJobNotFoundException {
            if (jobGraph.getJobID().equals(jobId)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Key value state registered for job {} under name {}.",
                            jobGraph.getJobID(),
                            registrationName);
                }

                try {
                    executionGraph
                            .getKvStateLocationRegistry()
                            .notifyKvStateRegistered(
                                    jobVertexId,
                                    keyGroupRange,
                                    registrationName,
                                    kvStateId,
                                    kvStateServerAddress);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new FlinkJobNotFoundException(jobId);
            }
        }

        private void notifyKvStateUnregistered(
                JobID jobId,
                JobVertexID jobVertexId,
                KeyGroupRange keyGroupRange,
                String registrationName)
                throws FlinkJobNotFoundException {
            if (jobGraph.getJobID().equals(jobId)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Key value state unregistered for job {} under name {}.",
                            jobGraph.getJobID(),
                            registrationName);
                }

                try {
                    executionGraph
                            .getKvStateLocationRegistry()
                            .notifyKvStateUnregistered(
                                    jobVertexId, keyGroupRange, registrationName);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new FlinkJobNotFoundException(jobId);
            }
        }

        private Optional<OperatorBackPressureStats> requestOperatorBackPressureStats(
                JobVertexID jobVertexId) throws FlinkException {
            final ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexId);

            if (jobVertex == null) {
                throw new FlinkException("JobVertexID not found " + jobVertexId);
            }

            return backPressureStatsTracker.getOperatorBackPressureStats(jobVertex);
        }

        private CompletableFuture<String> triggerSavepoint(
                String targetDirectory, boolean cancelJob) {
            final CheckpointCoordinator checkpointCoordinator =
                    executionGraph.getCheckpointCoordinator();
            if (checkpointCoordinator == null) {
                throw new IllegalStateException(
                        String.format("Job %s is not a streaming job.", jobGraph.getJobID()));
            } else if (targetDirectory == null
                    && !checkpointCoordinator
                            .getCheckpointStorage()
                            .hasDefaultSavepointLocation()) {
                LOG.info(
                        "Trying to cancel job {} with savepoint, but no savepoint directory configured.",
                        jobGraph.getJobID());

                throw new IllegalStateException(
                        "No savepoint directory configured. You can either specify a directory "
                                + "while cancelling via -s :targetDirectory or configure a cluster-wide "
                                + "default via key '"
                                + CheckpointingOptions.SAVEPOINT_DIRECTORY.key()
                                + "'.");
            }

            LOG.info(
                    "Triggering {}savepoint for job {}.",
                    cancelJob ? "cancel-with-" : "",
                    jobGraph.getJobID());

            if (cancelJob) {
                checkpointCoordinator.stopCheckpointScheduler();
            }

            return checkpointCoordinator
                    .triggerSavepoint(targetDirectory)
                    .thenApply(CompletedCheckpoint::getExternalPointer)
                    .handleAsync(
                            (path, throwable) -> {
                                if (throwable != null) {
                                    if (cancelJob && state == this) {
                                        startCheckpointScheduler(checkpointCoordinator);
                                    }
                                    throw new CompletionException(throwable);
                                } else if (cancelJob && state == this) {
                                    LOG.info(
                                            "Savepoint stored in {}. Now cancelling {}.",
                                            path,
                                            jobGraph.getJobID());
                                    cancel();
                                }
                                return path;
                            },
                            componentMainThreadExecutor);
        }

        private CompletableFuture<String> stopWithSavepoint(
                String targetDirectory, boolean advanceToEndOfEventTime) {
            final CheckpointCoordinator checkpointCoordinator =
                    executionGraph.getCheckpointCoordinator();

            if (checkpointCoordinator == null) {
                return FutureUtils.completedExceptionally(
                        new IllegalStateException(
                                String.format(
                                        "Job %s is not a streaming job.", jobGraph.getJobID())));
            }

            if (targetDirectory == null
                    && !checkpointCoordinator
                            .getCheckpointStorage()
                            .hasDefaultSavepointLocation()) {
                LOG.info(
                        "Trying to cancel job {} with savepoint, but no savepoint directory configured.",
                        jobGraph.getJobID());

                return FutureUtils.completedExceptionally(
                        new IllegalStateException(
                                "No savepoint directory configured. You can either specify a directory "
                                        + "while cancelling via -s :targetDirectory or configure a cluster-wide "
                                        + "default via key '"
                                        + CheckpointingOptions.SAVEPOINT_DIRECTORY.key()
                                        + "'."));
            }

            LOG.info("Triggering stop-with-savepoint for job {}.", jobGraph.getJobID());

            // we stop the checkpoint coordinator so that we are guaranteed
            // to have only the data of the synchronous savepoint committed.
            // in case of failure, and if the job restarts, the coordinator
            // will be restarted by the CheckpointCoordinatorDeActivator.
            checkpointCoordinator.stopCheckpointScheduler();

            final CompletableFuture<String> savepointFuture =
                    checkpointCoordinator
                            .triggerSynchronousSavepoint(advanceToEndOfEventTime, targetDirectory)
                            .thenApply(CompletedCheckpoint::getExternalPointer);

            final CompletableFuture<JobStatus> terminationFuture =
                    executionGraph
                            .getTerminationFuture()
                            .handle(
                                    (jobstatus, throwable) -> {
                                        if (throwable != null) {
                                            LOG.info(
                                                    "Failed during stopping job {} with a savepoint. Reason: {}",
                                                    jobGraph.getJobID(),
                                                    throwable.getMessage());
                                            throw new CompletionException(throwable);
                                        } else if (jobstatus != JobStatus.FINISHED) {
                                            LOG.info(
                                                    "Failed during stopping job {} with a savepoint. Reason: Reached state {} instead of FINISHED.",
                                                    jobGraph.getJobID(),
                                                    jobstatus);
                                            throw new CompletionException(
                                                    new FlinkException(
                                                            "Reached state "
                                                                    + jobstatus
                                                                    + " instead of FINISHED."));
                                        }
                                        return jobstatus;
                                    });

            return savepointFuture
                    .thenCompose((path) -> terminationFuture.thenApply((jobStatus -> path)))
                    .handleAsync(
                            (path, throwable) -> {
                                if (throwable != null) {
                                    if (state == this) {
                                        // restart the checkpoint coordinator if stopWithSavepoint
                                        // failed.
                                        startCheckpointScheduler(checkpointCoordinator);
                                    }
                                    throw new CompletionException(throwable);
                                }

                                return path;
                            },
                            componentMainThreadExecutor);
        }

        private void startCheckpointScheduler(final CheckpointCoordinator checkpointCoordinator) {
            if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
                try {
                    checkpointCoordinator.startCheckpointScheduler();
                } catch (IllegalStateException ignored) {
                    // Concurrent shut down of the coordinator
                }
            }
        }

        private Void deliverOperatorEventToCoordinator(
                ExecutionAttemptID taskExecutionId, OperatorID operatorId, OperatorEvent evt)
                throws FlinkException {
            // Failure semantics (as per the javadocs of the method):
            // If the task manager sends an event for a non-running task or an non-existing operator
            // coordinator, then respond with an exception to the call. If task and coordinator
            // exist,
            // then we assume that the call from the TaskManager was valid, and any bubbling
            // exception
            // needs to cause a job failure.

            final Execution exec = executionGraph.getRegisteredExecutions().get(taskExecutionId);
            if (exec == null || exec.getState() != ExecutionState.RUNNING) {
                // This situation is common when cancellation happens, or when the task failed while
                // the
                // event was just being dispatched asynchronously on the TM side.
                // It should be fine in those expected situations to just ignore this event, but, to
                // be
                // on the safe, we notify the TM that the event could not be delivered.
                throw new TaskNotRunningException(
                        "Task is not known or in state running on the JobManager.");
            }

            final OperatorCoordinatorHolder coordinator = coordinatorMap.get(operatorId);
            if (coordinator == null) {
                throw new FlinkException("No coordinator registered for operator " + operatorId);
            }

            try {
                coordinator.handleEventFromOperator(exec.getParallelSubtaskIndex(), evt);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                DeclarativeSchedulerNG.this.handleGlobalFailure(t);
            }

            return null;
        }

        private CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
                OperatorID operator, CoordinationRequest request) throws FlinkException {
            final OperatorCoordinatorHolder coordinatorHolder = coordinatorMap.get(operator);
            if (coordinatorHolder == null) {
                throw new FlinkException("Coordinator of operator " + operator + " does not exist");
            }

            final OperatorCoordinator coordinator = coordinatorHolder.coordinator();
            if (coordinator instanceof CoordinationRequestHandler) {
                return ((CoordinationRequestHandler) coordinator)
                        .handleCoordinationRequest(request);
            } else {
                throw new FlinkException(
                        "Coordinator of operator " + operator + " cannot handle client event");
            }
        }

        abstract StateAndBoolean updateTaskExecutionState(
                TaskExecutionStateTransition taskExecutionState);

        abstract void onTerminalState(JobStatus jobStatus);
    }

    // ----------------------------------------------------------------

    interface ResourceConsumer {
        void newResourcesAvailable();
    }

    interface State {
        State cancel();

        State suspend(Throwable cause);

        JobStatus getJobStatus();

        ArchivedExecutionGraph getJob();

        State handleGlobalFailure(Throwable cause);

        default <T> Optional<T> as(Class<? extends T> clazz) {
            if (clazz.isAssignableFrom(this.getClass())) {
                return Optional.of(clazz.cast(this));
            } else {
                return Optional.empty();
            }
        }

        default <T, E extends Exception> void tryRun(
                Class<? extends T> clazz, ThrowingConsumer<T, E> action, String debugMessage)
                throws E {
            final Optional<? extends T> asOptional = as(clazz);

            if (asOptional.isPresent()) {
                action.accept(asOptional.get());
            } else {
                LOG.debug(
                        "Cannot run '{}' because the actual state is {} and not {}.",
                        debugMessage,
                        this.getClass().getSimpleName(),
                        clazz.getSimpleName());
            }
        }

        default <T, V, E extends Exception> Optional<V> tryCall(
                Class<? extends T> clazz,
                FunctionWithException<T, V, E> action,
                String debugMessage)
                throws E {
            final Optional<? extends T> asOptional = as(clazz);

            if (asOptional.isPresent()) {
                return Optional.of(action.apply(asOptional.get()));
            } else {
                LOG.debug(
                        "Cannot run '{}' because the actual state is {} and not {}.",
                        debugMessage,
                        this.getClass().getSimpleName(),
                        clazz.getSimpleName());
                return Optional.empty();
            }
        }
    }
}
