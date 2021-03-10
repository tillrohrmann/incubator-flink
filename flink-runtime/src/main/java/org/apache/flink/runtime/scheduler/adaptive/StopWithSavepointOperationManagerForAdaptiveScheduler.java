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

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StopWithSavepointOperations;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointOperationHandler;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointOperationHandlerImpl;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * A thin wrapper around the StopWithSavepointOperationHandler to handle global failures according
 * to the needs of AdaptiveScheduler. AdaptiveScheduler currently doesn't support local failover,
 * hence, any reported failure will lead to a transition to Restarting or Failing state.
 */
class StopWithSavepointOperationManagerForAdaptiveScheduler implements GlobalFailureHandler {
    private final StopWithSavepointOperationHandler handler;
    private final ExecutionGraph executionGraph;
    private final StopWithSavepointOperations stopWithSavepointOperations;
    private boolean operationFinished = false;

    StopWithSavepointOperationManagerForAdaptiveScheduler(
            ExecutionGraph executionGraph,
            StopWithSavepointOperations stopWithSavepointOperations,
            boolean terminate,
            @Nullable String targetDirectory,
            Executor mainThreadExecutor,
            Logger logger) {

        this.stopWithSavepointOperations = stopWithSavepointOperations;

        // do not trigger checkpoints while creating the final savepoint. We will start the
        // scheduler onLeave() again.
        stopWithSavepointOperations.stopCheckpointScheduler();

        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                stopWithSavepointOperations.triggerSynchronousSavepoint(terminate, targetDirectory);

        this.handler =
                new StopWithSavepointOperationHandlerImpl(
                        executionGraph.getJobID(), this, stopWithSavepointOperations, logger);

        FutureUtils.assertNoException(
                savepointFuture
                        // the completedSavepointFuture could also be completed by
                        // CheckpointCanceller which doesn't run in the mainThreadExecutor
                        .handleAsync(
                        (completedSavepoint, throwable) -> {
                            handler.handleSavepointCreation(completedSavepoint, throwable);
                            return null;
                        },
                        mainThreadExecutor));

        this.executionGraph = executionGraph;
    }

    public CompletableFuture<String> getSavepointPathFuture() {
        return handler.getSavepointPathFuture();
    }

    public void onError(Throwable cause) {
        operationFinished = true;
        handler.abortOperation(cause);
    }

    public void onLeave() {
        // if we are leaving to Finished state (on success), all executions will be in final
        // state, the handler will complete the stop with savepoint future
        // if we are in any error state, the StopWithSavepoint state will decide what to do.
        operationFinished = true;
        Collection<ExecutionState> executionsState =
                executionGraph.getRegisteredExecutions().values().stream()
                        .map(Execution::getState)
                        .collect(Collectors.toList());
        handler.handleExecutionsTermination(executionsState);

        // make sure we are reactivating the scheduler in all cases.
        stopWithSavepointOperations.startCheckpointScheduler();
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        // ignore failures reported from the StopWithSavepointOperationHandlerImpl.
        Preconditions.checkState(
                operationFinished,
                "Unexpected failure. Assuming failures only after operation has finished.");
    }
}
