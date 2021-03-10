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

package org.apache.flink.runtime.scheduler.stopwithsavepoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StopWithSavepointOperations;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@code StopWithSavepointOperationManager} fulfills the contract given by {@link
 * StopWithSavepointOperationHandler} to run the stop-with-savepoint steps in a specific order.
 */
public class StopWithSavepointOperationManager {

    private final StopWithSavepointOperationHandler stopWithSavepointOperationHandler;
    private final StopWithSavepointOperations stopWithSavepointOperations;

    public StopWithSavepointOperationManager(
            StopWithSavepointOperations stopWithSavepointOperations,
            StopWithSavepointOperationHandler stopWithSavepointOperationHandler) {
        this.stopWithSavepointOperations = stopWithSavepointOperations;
        this.stopWithSavepointOperationHandler =
                Preconditions.checkNotNull(stopWithSavepointOperationHandler);
    }

    /**
     * Enforces the correct completion order of the passed {@code CompletableFuture} instances in
     * accordance to the contract of {@link StopWithSavepointOperationHandler}.
     *
     * @param terminate Flag indicating whether to terminate or suspend the job.
     * @param targetDirectory Target for the savepoint.
     * @param terminatedExecutionStatesFuture The {@code CompletableFuture} of the termination step.
     * @param mainThreadExecutor The executor the {@code StopWithSavepointOperationHandler}
     *     operations run on.
     * @return A {@code CompletableFuture} containing the path to the created savepoint.
     */
    public CompletableFuture<String> trackStopWithSavepointWithTerminationFutures(
            boolean terminate,
            @Nullable String targetDirectory,
            CompletableFuture<Collection<ExecutionState>> terminatedExecutionStatesFuture,
            Executor mainThreadExecutor) {

        // do not trigger checkpoints while creating the final savepoint
        stopWithSavepointOperations.stopCheckpointScheduler();

        // trigger savepoint. This operation will also terminate/suspend the job once the savepoint
        // has been created.
        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                stopWithSavepointOperations.triggerSynchronousSavepoint(terminate, targetDirectory);
        FutureUtils.assertNoException(
                savepointFuture
                        // the completedSavepointFuture could also be completed by
                        // CheckpointCanceller which doesn't run in the mainThreadExecutor
                        .handleAsync(
                                (completedSavepoint, throwable) -> {
                                    stopWithSavepointOperationHandler.handleSavepointCreation(
                                            completedSavepoint, throwable);
                                    return null;
                                },
                                mainThreadExecutor)
                        .thenRun(
                                () ->
                                        FutureUtils.assertNoException(
                                                // the execution termination has to run in a
                                                // separate Runnable to disconnect it from any
                                                // previous task failure handling
                                                terminatedExecutionStatesFuture.thenAcceptAsync(
                                                        stopWithSavepointOperationHandler
                                                                ::handleExecutionsTermination,
                                                        mainThreadExecutor))));

        return stopWithSavepointOperationHandler.getSavepointPathFuture();
    }

    public static void checkStopWithSavepointPreconditions(
            CheckpointCoordinator checkpointCoordinator,
            @Nullable String targetDirectory,
            JobID jobId,
            Logger logger) {
        if (checkpointCoordinator == null) {
            throw new IllegalStateException(String.format("Job %s is not a streaming job.", jobId));
        }

        if (targetDirectory == null
                && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
            logger.info(
                    "Trying to cancel job {} with savepoint, but no savepoint directory configured.",
                    jobId);

            throw new IllegalStateException(
                    "No savepoint directory configured. You can either specify a directory "
                            + "while cancelling via -s :targetDirectory or configure a cluster-wide "
                            + "default via key '"
                            + CheckpointingOptions.SAVEPOINT_DIRECTORY.key()
                            + "'.");
        }
    }
}
