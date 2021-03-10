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
import org.apache.flink.runtime.checkpoint.StopWithSavepointOperations;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointOperationHandler;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointOperationManager;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * When a "stop with savepoint" operation (wait until savepoint has been created, then cancel job)
 * is triggered on the {@link Executing} state, we transition into this state. This state is
 * delegating the tracking of the stop with savepoint operation to the {@link
 * StopWithSavepointOperationManagerForAdaptiveScheduler} which tracks the operation through the
 * {@link StopWithSavepointOperationHandler}. This allows us to share the operation tracking logic
 * across all scheduler implementations.
 */
class StopWithSavepoint extends StateWithExecutionGraph {

    private final Context context;
    private final ClassLoader userCodeClassLoader;

    private final StopWithSavepointOperationManagerForAdaptiveScheduler
            stopWithSavepointOperationManager;

    StopWithSavepoint(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            StopWithSavepointOperations stopWithSavepointOperations,
            Logger logger,
            ClassLoader userCodeClassLoader,
            @Nullable String targetDirectory,
            boolean terminate) {
        super(context, executionGraph, executionGraphHandler, operatorCoordinatorHandler, logger);
        this.context = context;
        this.userCodeClassLoader = userCodeClassLoader;

        stopWithSavepointOperationManager =
                new StopWithSavepointOperationManagerForAdaptiveScheduler(
                        executionGraph,
                        stopWithSavepointOperations,
                        terminate,
                        targetDirectory,
                        context.getMainThreadExecutor(),
                        logger);
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        stopWithSavepointOperationManager.onLeave();

        super.onLeave(newState);
    }

    @Override
    public void cancel() {
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

    /**
     * The {@code executionTerminationsFuture} will complete if a task reached a terminal state, and
     * {@link StopWithSavepointOperationManager} will act accordingly.
     */
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
        stopWithSavepointOperationManager.onError(cause);

        final Executing.FailureResult failureResult = context.howToHandleFailure(cause);

        if (failureResult.canRestart()) {
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
        return stopWithSavepointOperationManager.getSavepointPathFuture();
    }

    interface Context extends StateWithExecutionGraph.Context {
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

        private final StopWithSavepointOperations stopWithSavepointOperations;

        private final Logger logger;

        private final ClassLoader userCodeClassLoader;

        @Nullable private final String targetDirectory;

        private final boolean terminate;

        Factory(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                StopWithSavepointOperations stopWithSavepointOperations,
                Logger logger,
                ClassLoader userCodeClassLoader,
                @Nullable String targetDirectory,
                boolean terminate) {
            this.context = context;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.stopWithSavepointOperations = stopWithSavepointOperations;
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
                    stopWithSavepointOperations,
                    logger,
                    userCodeClassLoader,
                    targetDirectory,
                    terminate);
        }
    }
}
