/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StopWithSavepointOperations;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerUtils;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationHandlerImpl;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationManager;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * When a "stop with savepoint" operation (wait until savepoint has been created, then cancel job)
 * is triggered on the {@link Executing} state, we transition into this state. This state is
 * delegating the tracking of the stop with savepoint operation to the {@link
 * StopWithSavepointTerminationManager}, which is shared with {@link SchedulerBase}. What remains
 * for this state is reacting to signals from the termination manager, and tracking the
 * "operationCompletionFuture" (notify the user if we got cancelled, suspended etc. in the meantime)
 */
public class StopWithSavepoint extends StateWithExecutionGraph
        implements StopWithSavepointOperations {

    private final CompletableFuture<String> operationCompletionFuture;
    private final Context context;
    private final ClassLoader userCodeClassLoader;

    StopWithSavepoint(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger,
            ClassLoader userCodeClassLoader,
            String targetDirectory,
            boolean terminate) {
        super(context, executionGraph, executionGraphHandler, operatorCoordinatorHandler, logger);
        this.context = context;
        this.userCodeClassLoader = userCodeClassLoader;

        // to ensure that all disjoint subgraphs of a job finish successfully on savepoint creation,
        // we track the job termination via all execution termination futures (FLINK-21030).
        final CompletableFuture<Collection<ExecutionState>> executionTerminationsFuture =
                SchedulerUtils.getCombinedExecutionTerminationFuture(executionGraph);

        // do not trigger checkpoints while creating the final savepoint
        stopCheckpointScheduler();

        // trigger savepoint. This operation will also terminate/suspend the job once the savepoint
        // has been created.
        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                triggerSynchronousSavepoint(terminate, targetDirectory);

        final StopWithSavepointTerminationManager stopWithSavepointTerminationManager =
                new StopWithSavepointTerminationManager(
                        new StopWithSavepointTerminationHandlerImpl(
                                executionGraph.getJobID(), context, this, logger));

        this.operationCompletionFuture =
                stopWithSavepointTerminationManager.trackStopWithSavepoint(
                        savepointFuture,
                        executionTerminationsFuture,
                        context.getMainThreadExecutor());
    }

    @Override
    public void cancel() {
        // the canceling state will cancel the execution graph, which will eventually lead to a call
        // of handleAnyFailure(), completing the operationCompletionFuture.
        context.goToCanceling(
                getExecutionGraph(), getExecutionGraphHandler(), getOperatorCoordinatorHandler());
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.RUNNING;
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        handleAnyFailure(cause);
    }

    @Override
    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionStateTransition) {
        final boolean successfulUpdate =
                getExecutionGraph().updateState(taskExecutionStateTransition);

        if (successfulUpdate) {
            if (taskExecutionStateTransition.getExecutionState() == ExecutionState.FAILED) {
                Throwable cause = taskExecutionStateTransition.getError(userCodeClassLoader);
                handleAnyFailure(cause);
            }
        }

        return successfulUpdate;
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        context.goToFinished(ArchivedExecutionGraph.createFrom(getExecutionGraph()));
    }

    private void handleAnyFailure(Throwable cause) {
        operationCompletionFuture.completeExceptionally(
                new FlinkException("Failure while stopping with savepoint", cause));
        final Executing.FailureResult failureResult = context.howToHandleFailure(cause);

        if (failureResult.canRestart()) {
            startCheckpointScheduler();
            context.goToRestarting(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    failureResult.getBackoffTime());
        } else {
            context.goToFailing(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    failureResult.getFailureCause());
        }
    }

    CompletableFuture<String> getOperationCompletionFuture() {
        return operationCompletionFuture;
    }

    @Override
    public void startCheckpointScheduler() {
        final CheckpointCoordinator coordinator = getExecutionGraph().getCheckpointCoordinator();
        if (coordinator == null) {
            if (!getExecutionGraph().getState().isTerminalState()) {
                // for a streaming job, the checkpoint coordinator is always set (even if periodic
                // checkpoints are disabled). The only situation where it can be null is when the
                // job reached a terminal state.
                throw new IllegalStateException(
                        "Coordinator is only allowed to be null if we are in a terminal state.");
            }
            return;
        }
        if (coordinator.isPeriodicCheckpointingConfigured()) {
            try {
                coordinator.startCheckpointScheduler();
            } catch (IllegalStateException ignored) {
                // Concurrent shut down of the coordinator
            }
        }
    }

    @Override
    public void stopCheckpointScheduler() {
        final CheckpointCoordinator coordinator = getExecutionGraph().getCheckpointCoordinator();
        if (coordinator != null) {
            coordinator.stopCheckpointScheduler();
        }
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
            boolean terminate, @Nullable String targetLocation) {
        return getExecutionGraph()
                .getCheckpointCoordinator()
                .triggerSynchronousSavepoint(terminate, targetLocation);
    }

    interface Context extends StateWithExecutionGraph.Context, SchedulerFailureHandler {
        /**
         * Asks how to handle the failure.
         *
         * @param failure failure describing the failure cause
         * @return {@link Executing.FailureResult} which describes how to handle the failure
         */
        Executing.FailureResult howToHandleFailure(Throwable failure);

        /**
         * Transitions into the {@link Canceling} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Canceling} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Canceling} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Canceling} state
         */
        void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);

        /**
         * Transitions into the {@link Restarting} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Restarting} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Restarting}
         *     state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pas to the {@link
         *     Restarting} state
         * @param backoffTime backoffTime to wait before transitioning to the {@link Restarting}
         *     state
         */
        void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime);

        /**
         * Transitions into the {@link Failing} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Failing} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Failing} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Failing} state
         * @param failureCause failureCause describing why the job execution failed
         */
        void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause);
    }

    static class Factory implements StateFactory<StopWithSavepoint> {
        private final Context context;

        private final ExecutionGraph executionGraph;

        private final ExecutionGraphHandler executionGraphHandler;

        private final OperatorCoordinatorHandler operatorCoordinatorHandler;

        private final Logger logger;

        private final ClassLoader userCodeClassLoader;

        private final String targetDirectory;

        private final boolean terminate;

        public Factory(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Logger logger,
                ClassLoader userCodeClassLoader,
                String targetDirectory,
                boolean terminate) {
            this.context = context;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.logger = logger;
            this.userCodeClassLoader = userCodeClassLoader;
            this.targetDirectory = targetDirectory;
            this.terminate = terminate;
        }

        @Override
        public Class<StopWithSavepoint> getStateClass() {
            return StopWithSavepoint.class;
        }

        @Override
        public StopWithSavepoint getState() {
            return new StopWithSavepoint(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    logger,
                    userCodeClassLoader,
                    targetDirectory,
                    terminate);
        }
    }
}
