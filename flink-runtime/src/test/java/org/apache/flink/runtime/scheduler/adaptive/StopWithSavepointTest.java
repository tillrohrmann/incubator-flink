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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StopWithSavepointOperations;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.runtime.scheduler.adaptive.ExecutingTest.createFailingStateTransition;
import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link StopWithSavepoint} state. */
public class StopWithSavepointTest extends TestLogger {

    @Test
    public void testCancel() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setStopWithSavepoint(sws);
            ctx.setExpectCancelling(assertNonNull());

            sws.cancel();
        }
    }

    @Test
    public void testSuspend() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                    });

            sws.suspend(new RuntimeException());
        }
    }

    @Test
    public void testRestartOnGlobalFailureIfRestartConfigured() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(
                    (ignore) -> Executing.FailureResult.canRestart(Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            sws.handleGlobalFailure(new RuntimeException());
        }
    }

    @Test
    public void testFailingOnGlobalFailureIfNoRestartConfigured() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {

            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(
                                failingArguments.getFailureCause(),
                                containsCause(RuntimeException.class));
                    });

            sws.handleGlobalFailure(new RuntimeException());
        }
    }

    @Test
    public void testFailingOnUpdateTaskExecutionStateWithNoRestart() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {

            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, new StateTrackingMockExecutionGraph());
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(
                                failingArguments.getFailureCause(),
                                containsCause(RuntimeException.class));
                    });

            assertThat(sws.updateTaskExecutionState(createFailingStateTransition()), is(true));
        }
    }

    @Test
    public void testRestartingOnUpdateTaskExecutionStateWithRestart() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {

            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, new StateTrackingMockExecutionGraph());
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(
                    (ignore) -> Executing.FailureResult.canRestart(Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            assertThat(sws.updateTaskExecutionState(createFailingStateTransition()), is(true));
        }
    }

    @Test
    public void testExceptionalFutureCompletionOnLeaveWhileWaitingOnSavepointCompletion()
            throws Exception {
        MockStopWithSavepointContext ctx = new MockStopWithSavepointContext();
        StopWithSavepoint sws = createStopWithSavepoint(ctx);
        ctx.setStopWithSavepoint(sws);

        sws.onLeave(Canceling.class);

        ctx.close();
        assertThat(sws.getOperationCompletionFuture().isCompletedExceptionally(), is(true));
    }

    @Test
    public void testExceptionalSavepointCompletionLeadsToExceptionalOperationFutureCompletion()
            throws Exception {
        MockStopWithSavepointContext ctx = new MockStopWithSavepointContext();
        MockStopWithSavepointOperations mockStopWithSavepointOperations =
                new MockStopWithSavepointOperations();
        StopWithSavepoint sws = createStopWithSavepoint(ctx, mockStopWithSavepointOperations);

        mockStopWithSavepointOperations
                .getCheckpointCoordinatorSavepointFuture()
                .completeExceptionally(new RuntimeException("Test error"));

        ctx.close();
        assertThat(sws.getOperationCompletionFuture().isCompletedExceptionally(), is(true));
    }

    @Test
    public void testRestartOnTaskFailureAfterSavepointCompletion() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            MockStopWithSavepointOperations mockStopWithSavepointOperations =
                    new MockStopWithSavepointOperations();
            StopWithSavepoint sws = createStopWithSavepoint(ctx, mockStopWithSavepointOperations);
            ctx.setStopWithSavepoint(sws);

            ctx.setHowToHandleFailure(
                    (ignore) -> Executing.FailureResult.canRestart(Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            // 1. complete savepoint future
            mockStopWithSavepointOperations
                    .getCheckpointCoordinatorSavepointFuture()
                    .complete(createCompletedSavepoint());
            ctx.triggerScheduledExecutors();

            // 2. fail task
            assertThat(sws.updateTaskExecutionState(createFailingStateTransition()), is(true));
        }
    }

    @Test
    public void testEnsureCheckpointSchedulerLifecycle() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            MockStopWithSavepointOperations mockStopWithSavepointOperations =
                    new MockStopWithSavepointOperations();

            assertThat(mockStopWithSavepointOperations.isCheckpointSchedulerStarted(), is(true));

            // this should stop the scheduler
            createStopWithSavepoint(ctx, mockStopWithSavepointOperations);

            assertThat(mockStopWithSavepointOperations.isCheckpointSchedulerStarted(), is(false));

            // this should start the scheduler
            mockStopWithSavepointOperations
                    .getCheckpointCoordinatorSavepointFuture()
                    .completeExceptionally(new RuntimeException("Test error"));

            ctx.close();
            assertThat(mockStopWithSavepointOperations.isCheckpointSchedulerStarted(), is(true));
        }
    }

    private StopWithSavepoint createStopWithSavepoint(MockStopWithSavepointContext ctx) {
        return createStopWithSavepoint(
                ctx, new MockStopWithSavepointOperations(), new StateTrackingMockExecutionGraph());
    }

    private StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx, ExecutionGraph executionGraph) {
        return createStopWithSavepoint(ctx, new MockStopWithSavepointOperations(), executionGraph);
    }

    private StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx,
            StopWithSavepointOperations stopWithSavepointOperations) {
        return createStopWithSavepoint(
                ctx, stopWithSavepointOperations, new StateTrackingMockExecutionGraph());
    }

    private StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx,
            StopWithSavepointOperations stopWithSavepointOperations,
            ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        OperatorCoordinatorHandler operatorCoordinatorHandler =
                new OperatorCoordinatorHandler(
                        executionGraph,
                        (throwable) -> {
                            throw new RuntimeException("Error in test", throwable);
                        });

        executionGraph.transitionToRunning();

        return new StopWithSavepoint(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                stopWithSavepointOperations,
                log,
                ClassLoader.getSystemClassLoader(),
                "",
                true);
    }

    private static class MockStopWithSavepointContext extends MockStateWithExecutionGraphContext
            implements StopWithSavepoint.Context {

        private Function<Throwable, Executing.FailureResult> howToHandleFailure;

        private final StateValidator<ExecutingTest.FailingArguments> failingStateValidator =
                new StateValidator<>("failing");
        private final StateValidator<ExecutingTest.RestartingArguments> restartingStateValidator =
                new StateValidator<>("restarting");
        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("cancelling");

        private StopWithSavepoint state;

        public void setStopWithSavepoint(StopWithSavepoint sws) {
            this.state = sws;
        }

        public void setExpectFailing(Consumer<ExecutingTest.FailingArguments> asserter) {
            failingStateValidator.expectInput(asserter);
        }

        public void setExpectRestarting(Consumer<ExecutingTest.RestartingArguments> asserter) {
            restartingStateValidator.expectInput(asserter);
        }

        public void setExpectCancelling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setHowToHandleFailure(Function<Throwable, Executing.FailureResult> function) {
            this.howToHandleFailure = function;
        }

        @Override
        public Executing.FailureResult howToHandleFailure(Throwable failure) {
            return howToHandleFailure.apply(failure);
        }

        private void simulateTransitionToState(Class<? extends State> target) {
            checkNotNull(
                    state,
                    "StopWithSavepoint state must be set via setStopWithSavepoint() to call onLeave() on leaving the state");
            state.onLeave(target);
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
            simulateTransitionToState(Canceling.class);

            cancellingStateValidator.validateInput(
                    new ExecutingTest.CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime) {
            simulateTransitionToState(Restarting.class);
            restartingStateValidator.validateInput(
                    new ExecutingTest.RestartingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            backoffTime));
            hadStateTransition = true;
        }

        @Override
        public void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause) {
            simulateTransitionToState(Failing.class);
            failingStateValidator.validateInput(
                    new ExecutingTest.FailingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            failureCause));
            hadStateTransition = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            failingStateValidator.close();
            restartingStateValidator.close();
            cancellingStateValidator.close();
        }
    }

    private static CompletedCheckpoint createCompletedSavepoint() {
        return new CompletedCheckpoint(
                new JobID(),
                0,
                0L,
                0L,
                new HashMap<>(),
                null,
                CheckpointProperties.forSavepoint(true),
                new TestCompletedCheckpointStorageLocation(
                        new TestingStreamStateHandle(), "savepoint-path"));
    }

    private static class MockStopWithSavepointOperations implements StopWithSavepointOperations {
        private boolean checkpointSchedulerStarted = true;
        private CompletableFuture<CompletedCheckpoint> savepointFuture = new CompletableFuture<>();

        @Override
        public void startCheckpointScheduler() {
            checkpointSchedulerStarted = true;
        }

        @Override
        public void stopCheckpointScheduler() {
            checkpointSchedulerStarted = false;
        }

        @Override
        public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
                boolean terminate, @Nullable String targetLocation) {
            return savepointFuture;
        }

        boolean isCheckpointSchedulerStarted() {
            return checkpointSchedulerStarted;
        }

        CompletableFuture<CompletedCheckpoint> getCheckpointCoordinatorSavepointFuture() {
            return savepointFuture;
        }
    }
}
