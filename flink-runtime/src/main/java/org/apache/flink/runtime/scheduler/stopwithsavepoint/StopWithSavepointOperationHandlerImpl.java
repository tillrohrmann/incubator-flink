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
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StopWithSavepointOperations;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.scheduler.adaptive.GlobalFailureHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@code StopWithSavepointOperationHandlerImpl} implements {@link
 * StopWithSavepointOperationHandler}.
 *
 * <p>The operation only succeeds if both steps, the savepoint creation and the successful
 * termination of the job, succeed. If the former step fails, the operation fails exceptionally
 * without any further actions. If the latter one fails, a global fail-over is triggered before
 * failing the operation.
 *
 * <p>The implementation expects the savepoint creation being completed before the executions
 * terminate.
 *
 * @see StopWithSavepointOperationManager
 */
public class StopWithSavepointOperationHandlerImpl implements StopWithSavepointOperationHandler {

    private final Logger log;

    private final GlobalFailureHandler globalFailureHandler;
    private final StopWithSavepointOperations stopWithSavepointOperations;
    private final JobID jobId;

    private final CompletableFuture<String> result = new CompletableFuture<>();

    private State state = new WaitingForSavepoint();

    private boolean inStateTransition = false;

    public <H extends GlobalFailureHandler & StopWithSavepointOperations>
            StopWithSavepointOperationHandlerImpl(
                    JobID jobId, H failureHandlerAndOperations, Logger log) {
        this(jobId, failureHandlerAndOperations, failureHandlerAndOperations, log);
    }

    public StopWithSavepointOperationHandlerImpl(
            JobID jobId,
            GlobalFailureHandler globalFailureHandler,
            StopWithSavepointOperations stopWithSavepointOperations,
            Logger log) {
        this.jobId = checkNotNull(jobId);
        this.globalFailureHandler = checkNotNull(globalFailureHandler);
        this.stopWithSavepointOperations = checkNotNull(stopWithSavepointOperations);
        this.log = checkNotNull(log);
    }

    @Override
    public CompletableFuture<String> getSavepointPath() {
        return result;
    }

    @Override
    public void handleSavepointCreation(
            CompletedCheckpoint completedSavepoint, Throwable throwable) {
        if (throwable != null) {
            checkArgument(
                    completedSavepoint == null,
                    "No savepoint should be provided if a throwable is passed.");
            handleSavepointCreationFailure(throwable);
        } else {
            handleSavepointCreationSuccess(checkNotNull(completedSavepoint));
        }
    }

    @Override
    public void handleExecutionsTermination(Collection<ExecutionState> terminatedExecutionStates) {
        final Set<ExecutionState> notFinishedExecutionStates =
                checkNotNull(terminatedExecutionStates).stream()
                        .filter(state -> state != ExecutionState.FINISHED)
                        .collect(Collectors.toSet());

        if (notFinishedExecutionStates.isEmpty()) {
            handleExecutionsFinished();
        } else {
            handleAnyExecutionNotFinished(notFinishedExecutionStates);
        }
    }

    @Override
    public void abortOperation(Throwable cause) {
        transitionToState(() -> state.onAbort(cause), String.format("on abort for job %s.", jobId));
    }

    private void handleSavepointCreationSuccess(CompletedCheckpoint completedCheckpoint) {
        transitionToState(
                () -> state.onSavepointCreation(completedCheckpoint),
                String.format("savepoint creation handling for job %s.", jobId));
    }

    private void handleSavepointCreationFailure(Throwable throwable) {
        transitionToState(
                () -> state.onSavepointCreationFailure(throwable),
                String.format("savepoint creation failure handling for job %s.", jobId));
    }

    private void handleExecutionsFinished() {
        transitionToState(
                () -> state.onExecutionsFinished(),
                String.format(
                        "execution termination handling with all executions being finished for job %s.",
                        jobId));
    }

    private void handleAnyExecutionNotFinished(Set<ExecutionState> notFinishedExecutionStates) {
        transitionToState(
                () -> state.onAnyExecutionNotFinished(notFinishedExecutionStates),
                String.format(
                        "execution termination handling for job %s with some executions being in an not-finished state: %s",
                        jobId, notFinishedExecutionStates));
    }

    private void transitionToState(Supplier<? extends State> targetState, String message) {
        Preconditions.checkState(
                !inStateTransition,
                "Can not transition to new state because we are already in a state transition");
        final State oldState = state;
        inStateTransition = true;
        try {
            state = targetState.get();
        } finally {
            inStateTransition = false;
        }

        log.debug(
                String.format(
                        "Stop-with-savepoint transitioned from %s to %s on %s",
                        oldState.getName(), state.getName(), message));
    }

    /**
     * Handles the termination of the {@code StopWithSavepointOperationHandlerImpl} exceptionally
     * after triggering a global job fail-over.
     *
     * @param unfinishedExecutionStates the unfinished states that caused the failure.
     * @param savepointPath the path to the successfully created savepoint.
     */
    private void terminateExceptionallyWithGlobalFailoverOnUnfinishedExecution(
            Iterable<ExecutionState> unfinishedExecutionStates, String savepointPath) {
        String errorMessage =
                String.format(
                        "Inconsistent execution state after stopping with savepoint. At least one execution is still in one of the following states: %s. A global fail-over is triggered to recover the job %s.",
                        StringUtils.join(unfinishedExecutionStates, ", "), jobId);
        FlinkException inconsistentFinalStateException = new FlinkException(errorMessage);

        terminateExceptionallyWithGlobalFailover(inconsistentFinalStateException, savepointPath);
    }

    private void terminateExceptionallyWithGlobalFailover(Throwable cause, String savepointPath) {
        log.warn(
                "A savepoint was created at {} but the corresponding job {} didn't terminate successfully.",
                savepointPath,
                jobId,
                cause);

        globalFailureHandler.handleGlobalFailure(cause);
        result.completeExceptionally(cause);
    }

    /**
     * Handles the termination of the {@code StopWithSavepointOperationHandlerImpl} exceptionally
     * without triggering a global job fail-over but restarting the checkpointing. It does restart
     * the checkpoint scheduling.
     *
     * @param throwable the error that caused the exceptional termination.
     */
    private void terminateExceptionally(Throwable throwable) {
        stopWithSavepointOperations.startCheckpointScheduler();
        result.completeExceptionally(throwable);
    }

    /**
     * Handles the successful termination of the {@code StopWithSavepointOperationHandlerImpl}.
     *
     * @param completedSavepoint the completed savepoint
     */
    private void terminateSuccessfully(CompletedCheckpoint completedSavepoint) {
        result.complete(completedSavepoint.getExternalPointer());
    }

    private final class WaitingForSavepoint implements State {

        @Override
        public State onSavepointCreation(CompletedCheckpoint completedSavepoint) {
            return new SavepointCreated(completedSavepoint);
        }

        @Override
        public State onSavepointCreationFailure(Throwable throwable) {
            terminateExceptionally(throwable);
            return new FinalState();
        }

        @Override
        public State onAbort(Throwable cause) {
            terminateExceptionally(cause);
            return new FinalState();
        }
    }

    private final class SavepointCreated implements State {

        private final CompletedCheckpoint completedSavepoint;

        private SavepointCreated(CompletedCheckpoint completedSavepoint) {
            this.completedSavepoint = completedSavepoint;
        }

        @Override
        public State onExecutionsFinished() {
            terminateSuccessfully(completedSavepoint);
            return new FinalState();
        }

        @Override
        public State onAnyExecutionNotFinished(
                Iterable<ExecutionState> notFinishedExecutionStates) {
            terminateExceptionallyWithGlobalFailoverOnUnfinishedExecution(
                    notFinishedExecutionStates, completedSavepoint.getExternalPointer());
            return new FinalState();
        }

        @Override
        public State onAbort(Throwable cause) {
            terminateExceptionallyWithGlobalFailover(
                    cause, completedSavepoint.getExternalPointer());
            return new FinalState();
        }
    }

    private static final class FinalState implements State {

        @Override
        public State onExecutionsFinished() {
            return this;
        }

        @Override
        public State onAnyExecutionNotFinished(
                Iterable<ExecutionState> notFinishedExecutionStates) {
            return this;
        }

        @Override
        public State onSavepointCreationFailure(Throwable throwable) {
            return this;
        }

        @Override
        public State onAbort(Throwable cause) {
            return this;
        }
    }

    private interface State {

        default State onSavepointCreation(CompletedCheckpoint completedSavepoint) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onSavepointCreation.");
        }

        default State onSavepointCreationFailure(Throwable throwable) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onSavepointCreationFailure.");
        }

        default State onExecutionsFinished() {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onExecutionsFinished.");
        }

        default State onAnyExecutionNotFinished(
                Iterable<ExecutionState> notFinishedExecutionStates) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onAnyExecutionNotFinished.");
        }

        default String getName() {
            return this.getClass().getSimpleName();
        }

        default State onAbort(Throwable cause) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName() + " state does not support onAbort.");
        }
    }
}
