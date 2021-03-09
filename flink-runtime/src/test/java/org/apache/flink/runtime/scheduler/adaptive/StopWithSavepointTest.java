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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/** Tests for the {@link StopWithSavepoint} state. */
public class StopWithSavepointTest extends TestLogger {

    @Test
    public void testCancel() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            TestingStopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setExpectCancelling(assertNonNull());

            sws.cancel();
        }
    }

    public void testSuspend() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            TestingStopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setExpectFinished(assertNonNull());

            sws.suspend(new RuntimeException());
        }
    }

    @Test
    public void testExceptionalFutureCompletionOnLeaveWhileWaitingOnSavepointCompletion()
            throws Exception {
        MockStopWithSavepointContext ctx = new MockStopWithSavepointContext();
        TestingStopWithSavepoint sws = createStopWithSavepoint(ctx);

        sws.onLeave(Canceling.class);

        ctx.close();
        assertThat(sws.getOperationCompletionFuture().isCompletedExceptionally(), is(true));
    }

    @Test
    public void testExceptionalFutureCompletionAndStateTransitionOnLeaveAfterSavepointCompletion()
            throws Exception {
        MockStopWithSavepointContext ctx = new MockStopWithSavepointContext();
        TestingStopWithSavepoint sws = createStopWithSavepoint(ctx);
        ctx.setState(sws);
        ctx.setHowToHandleFailure((ignore) -> Executing.FailureResult.canRestart(Duration.ZERO));
        ctx.setExpectRestarting(assertNonNull());
        sws.getSavepointFuture().complete(createCompletedSavepoint());

        sws.onLeave(Canceling.class);

        ctx.close();
        assertThat(sws.getOperationCompletionFuture().isCompletedExceptionally(), is(true));
    }

    @Test
    public void testExceptionalSavepointCompletion() throws Exception {
        MockStopWithSavepointContext ctx = new MockStopWithSavepointContext();
        TestingStopWithSavepoint sws = createStopWithSavepoint(ctx);

        sws.getSavepointFuture().completeExceptionally(new RuntimeException("Test error"));

        ctx.close();
        assertThat(sws.getOperationCompletionFuture().isCompletedExceptionally(), is(true));
        assertThat(ctx.hadStateTransition, is(false));
    }

    @Test
    public void testEnsureCheckpointSchedulerLifecycle() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            TestingStopWithSavepoint sws = createStopWithSavepoint(ctx);
            assertThat(sws.isCheckpointSchedulerStarted(), is(false));

            sws.getSavepointFuture().completeExceptionally(new RuntimeException("Test error"));

            ctx.close();
            assertThat(sws.isCheckpointSchedulerStarted(), is(true));
        }
    }

    private TestingStopWithSavepoint createStopWithSavepoint(MockStopWithSavepointContext ctx)
            throws JobException, JobExecutionException {
        ExecutionGraph executionGraph = TestingDefaultExecutionGraphBuilder.newBuilder().build();
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

        return new TestingStopWithSavepoint(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                ClassLoader.getSystemClassLoader(),
                "",
                true);
    }

    private static class TestingStopWithSavepoint extends StopWithSavepoint {

        private CompletableFuture<CompletedCheckpoint> savepointFuture;
        private boolean checkpointSchedulerStarted = false;

        TestingStopWithSavepoint(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Logger logger,
                ClassLoader userCodeClassLoader,
                String targetDirectory,
                boolean terminate) {
            super(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    logger,
                    userCodeClassLoader,
                    targetDirectory,
                    terminate);
        }

        public boolean isCheckpointSchedulerStarted() {
            return checkpointSchedulerStarted;
        }

        public CompletableFuture<CompletedCheckpoint> getSavepointFuture() {
            // since triggerSynchronousSavepoint() gets called in the StopWithSavepoint constructor,
            // we initialize the field lazily.
            if (savepointFuture == null) {
                savepointFuture = new CompletableFuture<>();
            }
            return savepointFuture;
        }

        @Override
        public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
                boolean terminate, @Nullable String targetLocation) {
            return getSavepointFuture();
        }

        @Override
        public void startCheckpointScheduler() {
            checkpointSchedulerStarted = true;
        }

        @Override
        public void stopCheckpointScheduler() {
            checkpointSchedulerStarted = false;
        }
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

        private StopWithSavepoint state = null;

        public void setState(StopWithSavepoint setState) {
            this.state = setState;
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
        public void handleGlobalFailure(Throwable cause) {
            assertThat(state, is(notNullValue()));
            state.handleGlobalFailure(cause);
        }

        @Override
        public Executing.FailureResult howToHandleFailure(Throwable failure) {
            return howToHandleFailure.apply(failure);
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
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
}
