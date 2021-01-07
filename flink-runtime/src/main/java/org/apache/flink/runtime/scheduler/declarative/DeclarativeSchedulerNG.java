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
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.io.InputSplit;
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
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTrackerDeploymentListenerAdapter;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
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
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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

    private final CompletedCheckpointStore completedCheckpointStore;
    private final CheckpointIDCounter checkpointIdCounter;
    private final CheckpointsCleaner checkpointsCleaner;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private ComponentMainThreadExecutor componentMainThreadExecutor =
            new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("foobar");

    @Nullable private JobStatusListener jobStatusListener;

    private final RequirementsCalculator requirementsCalculator =
            new SlotSharingRequirementsCalculator();
    private final MappingCalculator mappingCalculator = new SlotSharingMappingCalculator();

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
        return requirementsCalculator.calculateRequiredSlots(jobGraph.getVertices());
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
    public void handleGlobalFailure(Throwable cause) {}

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
            transitionToState(optionalResult.get().getState());
            return optionalResult.get().getResult();
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
        return null;
    }

    @Override
    public void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {}

    @Override
    public void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException {}

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
        return Optional.empty();
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob) {
        return null;
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
        return null;
    }

    @Override
    public void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecution, OperatorID operator, OperatorEvent evt)
            throws FlinkException {}

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {
        return null;
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
                determineParallelismAndAssignResources(JobGraph jobGraph)
                        throws JobExecutionException {
            final HashMap<ExecutionVertexID, LogicalSlot> assignedSlots = new HashMap<>();

            final Optional<SlotSharingAssignments> slotSharingSlotAssignmentsOptional =
                    mappingCalculator.determineParallelismAndAssignResources(
                            new JobGraphJobInformation(jobGraph),
                            declarativeSlotPool.getFreeSlotsInformation());

            if (!slotSharingSlotAssignmentsOptional.isPresent()) {
                throw new JobExecutionException(
                        jobGraph.getJobID(), "Not enough resources available for scheduling.");
            }

            final SlotSharingAssignments slotSharingSlotAssignments =
                    slotSharingSlotAssignmentsOptional.get();

            for (ExecutionSlotSharingGroupAndSlot executionSlotSharingGroup :
                    slotSharingSlotAssignments.getAssignments()) {
                final SharedSlot sharedSlot =
                        reserveSharedSlot(executionSlotSharingGroup.getSlotInfo());

                for (ExecutionVertexID executionVertexId :
                        executionSlotSharingGroup
                                .getExecutionSlotSharingGroup()
                                .getContainedExecutionVertices()) {
                    final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();
                    assignedSlots.put(executionVertexId, logicalSlot);
                }
            }

            final Map<JobVertexID, Integer> parallelismPerJobVertex =
                    slotSharingSlotAssignments.getMaxParallelismForVertices();

            return new DeclarativeScheduler.ParallelismAndResourceAssignments(
                    assignedSlots, parallelismPerJobVertex);
        }

        private SharedSlot reserveSharedSlot(SlotInfo slotInfo) {
            final PhysicalSlot physicalSlot =
                    declarativeSlotPool.reserveFreeSlot(
                            slotInfo.getAllocationId(),
                            ResourceProfile.fromResourceSpec(
                                    ResourceSpec.DEFAULT, MemorySize.ZERO));

            final SharedSlot sharedSlot =
                    new SharedSlot(
                            new SlotRequestId(),
                            physicalSlot,
                            slotInfo.willBeOccupiedIndefinitely(),
                            () ->
                                    declarativeSlotPool.freeReservedSlot(
                                            slotInfo.getAllocationId(),
                                            null,
                                            System.currentTimeMillis()));
            physicalSlot.tryAssignPayload(sharedSlot);

            return sharedSlot;
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
        StateAndBoolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
            final boolean successfulUpdate = executionGraph.updateState(taskExecutionState);

            if (successfulUpdate) {
                return StateAndBoolean.create(this, true);
            } else {
                return StateAndBoolean.create(this, false);
            }
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
        StateAndBoolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
            final boolean successfulUpdate = executionGraph.updateState(taskExecutionState);

            if (successfulUpdate) {
                return StateAndBoolean.create(this, true);
            } else {
                return StateAndBoolean.create(this, false);
            }
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
    }

    private static class StateAndBoolean {
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

        protected StateWithExecutionGraph(ExecutionGraph executionGraph) {
            this.executionGraph = executionGraph;

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

        abstract StateAndBoolean updateTaskExecutionState(
                TaskExecutionStateTransition taskExecutionState);

        abstract void onTerminalState(JobStatus jobStatus);

        public ExecutionState requestPartitionState(
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

        public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
            executionGraph.updateAccumulators(accumulatorSnapshot);
        }
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

        default <T> Optional<T> as(Class<? extends T> clazz) {
            if (clazz.isAssignableFrom(this.getClass())) {
                return Optional.of(clazz.cast(this));
            } else {
                return Optional.empty();
            }
        }

        default <T> void tryRun(Class<? extends T> clazz, Consumer<T> action, String debugMessage) {
            OptionalConsumer.of(as(clazz))
                    .ifPresent(action::accept)
                    .ifNotPresent(
                            () ->
                                    LOG.debug(
                                            "Cannot run '{}' because the actual state is {} and not {}.",
                                            debugMessage,
                                            this.getClass().getSimpleName(),
                                            clazz.getSimpleName()));
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
