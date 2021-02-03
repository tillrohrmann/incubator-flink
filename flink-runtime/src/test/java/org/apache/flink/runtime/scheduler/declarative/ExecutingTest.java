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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertexDeploymentTest;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.runtime.scheduler.declarative.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/** Tests for declarative scheduler's {@link Executing} state. */
public class ExecutingTest extends TestLogger {
    @Test
    public void testTransitionToFailing() throws Exception {
        final String failureMsg = "test exception";
        MockExecutingContext ctx = new MockExecutingContext();

        ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);
        Executing exec = getExecutingState(ctx);
        exec.onEnter();
        exec.handleGlobalFailure(new RuntimeException(failureMsg));
        ctx.assertFailing(
                (failingArguments -> {
                    assertThat(failingArguments.getExecutionGraph(), notNullValue());
                    assertThat(failingArguments.getFailureCause().getMessage(), is(failureMsg));
                }));
    }

    @Test
    public void testTransitionToRestarting() throws Exception {
        final Duration duration = Duration.ZERO;
        MockExecutingContext ctx = new MockExecutingContext();

        ctx.setHowToHandleFailure((t) -> Executing.FailureResult.canRestart(duration));
        Executing exec = getExecutingState(ctx);
        exec.onEnter();
        exec.handleGlobalFailure(new RuntimeException("Recoverable error"));
        ctx.assertRestarting(
                (restartingArguments ->
                        assertThat(restartingArguments.getBackoffTime(), is(duration))));
    }

    @Test
    public void testTransitionToCancelling() throws Exception {
        MockExecutingContext ctx = new MockExecutingContext();
        Executing exec = getExecutingState(ctx);
        exec.onEnter();
        exec.cancel();
        ctx.assertCancelling(assertNonNull());
    }

    @Test
    public void testTransitionToFinishedOnTerminalState() throws Exception {
        ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        MockExecutingContext ctx = new MockExecutingContext();

        ctx.setGetMainThreadExecutor(() -> executor);
        Executing exec = getExecutingState(ctx);
        ctx.setExpectedStateChecker((state) -> state == exec);
        exec.onEnter();
        // transition EG into terminal state, which will notify the Executing state about the
        // failure (async via the supplied executor)
        exec.getExecutionGraph().failJob(new RuntimeException("test failure"));
        executor.triggerAll();
        ctx.assertFinished(
                archivedExecutionGraph ->
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));
    }

    @Test
    public void testTransitionToFinishedOnSuspend() throws Exception {
        MockExecutingContext ctx = new MockExecutingContext();

        Executing exec = getExecutingState(ctx);
        exec.onEnter();
        exec.suspend(new RuntimeException("suspend"));
        ctx.assertFinished(
                archivedExecutionGraph -> {
                    assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                });
    }

    @Test
    public void testScaleUp() throws Exception {
        MockExecutingContext ctx = new MockExecutingContext();

        ctx.setCanScaleUp(() -> true);
        Executing exec = getExecutingState(ctx);
        exec.onEnter();
        exec.notifyNewResourcesAvailable();
        ctx.assertRestarting(
                restartingArguments -> {
                    // expect immediate restart on scale up
                    assertThat(restartingArguments.getBackoffTime(), is(Duration.ZERO));
                });
    }

    @Test
    public void testNoScaleUp() throws Exception {
        MockExecutingContext ctx = new MockExecutingContext();
        ctx.setCanScaleUp(() -> false);
        Executing exec = getExecutingState(ctx);
        exec.onEnter();
        exec.notifyNewResourcesAvailable();
        ctx.assertNoStateTransition();
    }

    @Test
    public void testFailingOnDeploymentFailure() throws Exception {
        MockExecutingContext ctx = new MockExecutingContext();
        ctx.setCanScaleUp(() -> false);
        ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);

        // create ExecutionGraph with one ExecutionVertex, which fails during deployment.
        JobGraph jobGraph = new JobGraph(new JobVertex("test"));
        Executing exec = getExecutingState(ctx, jobGraph);
        TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();
        TaskManagerGateway taskManagerGateway =
                new ExecutionVertexDeploymentTest.SubmitFailingSimpleAckingTaskManagerGateway();
        slotBuilder.setTaskManagerGateway(taskManagerGateway);
        LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
        exec.getExecutionGraph()
                .getAllExecutionVertices()
                .forEach(executionVertex -> executionVertex.tryAssignResource(slot));

        // trigger deployment
        exec.onEnter();

        ctx.assertFailing(assertNonNull());
    }

    @Test
    public void testJobInformationMethods() throws Exception {
        MockExecutingContext ctx = new MockExecutingContext();
        Executing exec = getExecutingState(ctx);
        final JobID jobId = exec.getExecutionGraph().getJobID();
        exec.onEnter();
        assertThat(exec.getJob(), instanceOf(ArchivedExecutionGraph.class));
        assertThat(exec.getJob().getJobID(), is(jobId));
        assertThat(exec.getJobStatus(), is(JobStatus.CREATED));
    }

    public Executing getExecutingState(MockExecutingContext ctx)
            throws JobException, JobExecutionException {
        return getExecutingState(ctx, null);
    }

    public Executing getExecutingState(MockExecutingContext ctx, @Nullable JobGraph jobGraph)
            throws JobException, JobExecutionException {
        ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
        if (jobGraph != null) {
            executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());
        }
        ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(executionGraph, log, ForkJoinPool.commonPool());
        OperatorCoordinatorHandler operatorCoordinatorHandler =
                new OperatorCoordinatorHandler(
                        executionGraph,
                        (throwable) -> {
                            throw new RuntimeException("Error in test", throwable);
                        });
        return new Executing(
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                ctx,
                ClassLoader.getSystemClassLoader());
    }

    private static class MockExecutingContext implements Executing.Context {

        private ArchivedExecutionGraph finishedState = null;
        private FailingArguments failingState = null;
        private RestartingArguments restartingState = null;
        private CancellingArguments cancellingState = null;
        private final Object[] stateFields =
                new Object[] {finishedState, failingState, restartingState, cancellingState};

        private Function<Throwable, Executing.FailureResult> howToHandleFailure;
        private Supplier<Boolean> canScaleUp;
        private Supplier<Executor> getMainThreadExecutor = ForkJoinPool::commonPool;
        private Function<State, Boolean> expectedStateChecker;

        public void setHowToHandleFailure(Function<Throwable, Executing.FailureResult> function) {
            this.howToHandleFailure = function;
        }

        public void setCanScaleUp(Supplier<Boolean> supplier) {
            this.canScaleUp = supplier;
        }

        public void setGetMainThreadExecutor(Supplier<Executor> supplier) {
            this.getMainThreadExecutor = supplier;
        }

        public void setExpectedStateChecker(Function<State, Boolean> function) {
            this.expectedStateChecker = function;
        }

        public void assertFinished(Consumer<ArchivedExecutionGraph> asserter) {
            assertState(finishedState, "finished", asserter);
        }

        public void assertFailing(Consumer<FailingArguments> asserter) {
            assertState(failingState, "failing", asserter);
        }

        public void assertRestarting(Consumer<RestartingArguments> asserter) {
            assertState(restartingState, "restarting", asserter);
        }

        public void assertCancelling(Consumer<CancellingArguments> asserter) {
            assertState(cancellingState, "cancelling", asserter);
        }

        private <T> void assertState(T state, String name, Consumer<T> asserter) {
            if (state == null) {
                throw new AssertionError(name + " state not reached");
            }
            asserter.accept(state);
            if (Arrays.stream(stateFields).filter(Objects::nonNull).count() > 1) {
                throw new AssertionError("More than one state transition occurred");
            }
        }

        private void assertNoStateTransition() {
            if (Arrays.stream(stateFields).anyMatch(Objects::nonNull)) {
                throw new AssertionError("State transition occurred");
            }
        }

        // --------- Interface Implementations ------- //

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
            cancellingState =
                    new CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler);
        }

        @Override
        public Executing.FailureResult howToHandleFailure(Throwable failure) {
            return howToHandleFailure.apply(failure);
        }

        @Override
        public boolean canScaleUp(ExecutionGraph executionGraph) {
            return canScaleUp.get();
        }

        @Override
        public void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime) {
            restartingState =
                    new RestartingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            backoffTime);
        }

        @Override
        public void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause) {
            failingState =
                    new FailingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            failureCause);
        }

        @Override
        public void runIfState(State expectedState, Runnable action) {
            if (expectedStateChecker.apply(expectedState)) {
                action.run();
            }
        }

        @Override
        public boolean isState(State expectedState) {
            throw new UnsupportedOperationException("Not covered by this test at the moment");
        }

        @Override
        public Executor getMainThreadExecutor() {
            return getMainThreadExecutor.get();
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            finishedState = archivedExecutionGraph;
        }
    }

    private static class CancellingArguments {
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandle;

        public CancellingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandle) {
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandle = operatorCoordinatorHandle;
        }

        public ExecutionGraph getExecutionGraph() {
            return executionGraph;
        }

        public ExecutionGraphHandler getExecutionGraphHandler() {
            return executionGraphHandler;
        }

        public OperatorCoordinatorHandler getOperatorCoordinatorHandle() {
            return operatorCoordinatorHandle;
        }
    }

    private static class RestartingArguments extends CancellingArguments {
        private final Duration backoffTime;

        public RestartingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime) {
            super(executionGraph, executionGraphHandler, operatorCoordinatorHandler);
            this.backoffTime = backoffTime;
        }

        public Duration getBackoffTime() {
            return backoffTime;
        }
    }

    private static class FailingArguments extends CancellingArguments {
        private final Throwable failureCause;

        public FailingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause) {
            super(executionGraph, executionGraphHandler, operatorCoordinatorHandler);
            this.failureCause = failureCause;
        }

        public Throwable getFailureCause() {
            return failureCause;
        }
    }
}
