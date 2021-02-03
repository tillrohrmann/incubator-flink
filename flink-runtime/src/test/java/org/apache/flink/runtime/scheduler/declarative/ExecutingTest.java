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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.runtime.scheduler.declarative.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/** Tests for declarative scheduler's {@link Executing} state. */
public class ExecutingTest extends TestLogger {
    @Test
    public void testTransitionToFailing() throws Exception {
        final String failureMsg = "test exception";
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setExpectFailing(
                    (failingArguments -> {
                        assertThat(failingArguments.getExecutionGraph(), notNullValue());
                        assertThat(failingArguments.getFailureCause().getMessage(), is(failureMsg));
                    }));
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            exec.handleGlobalFailure(new RuntimeException(failureMsg));
        }
    }

    @Test
    public void testTransitionToRestarting() throws Exception {
        final Duration duration = Duration.ZERO;
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setExpectRestarting(
                    (restartingArguments ->
                            assertThat(restartingArguments.getBackoffTime(), is(duration))));
            ctx.setHowToHandleFailure((t) -> Executing.FailureResult.canRestart(duration));
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            exec.handleGlobalFailure(new RuntimeException("Recoverable error"));
        }
    }

    @Test
    public void testTransitionToCancelling() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setExpectCancelling(assertNonNull());
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            exec.cancel();
        }
    }

    @Test
    public void testTransitionToFinishedOnTerminalState() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));
            ctx.setGetMainThreadExecutor(() -> executor);
            Executing exec = getExecutingState(ctx);
            ctx.setExpectedStateChecker((state) -> state == exec);
            exec.onEnter();
            exec.getExecutionGraph().failJob(new RuntimeException("test failure"));
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                    });
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            exec.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    public void testScaleUp() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setExpectRestarting(
                    restartingArguments -> {
                        assertThat(restartingArguments.getBackoffTime(), is(Duration.ZERO));
                    });
            ctx.setCanScaleUp(() -> true);
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            exec.notifyNewResourcesAvailable();
        }
    }

    @Test
    public void testNoScaleUp() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setCanScaleUp(() -> false);
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            exec.notifyNewResourcesAvailable();
        }
    }

    @Test
    public void testFailingOnDeploymentFailure() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setCanScaleUp(() -> false);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);
            ctx.setExpectFailing(assertNonNull());

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
        }
    }

    public Executing getExecutingState(MockExecutingContext ctx)
            throws JobException, JobExecutionException {
        return getExecutingState(ctx, null);
    }

    public Executing getExecutingState(MockExecutingContext ctx, @Nullable JobGraph jobGraph)
            throws JobException, JobExecutionException {
        ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
        if (jobGraph != null)
            executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());
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

    private class MockExecutingContext implements Executing.Context, AutoCloseable {

        private final StateValidator<FailingArguments> failingStateValidator =
                new StateValidator<>("failing");
        private final StateValidator<RestartingArguments> restartingStateValidator =
                new StateValidator<>("restarting");
        private final StateValidator<CancellingArguments> cancellingStateValidator =
                new StateValidator<>("cancelling");
        private StateValidator<ArchivedExecutionGraph> finishedStateValidator =
                new StateValidator<>("finished");

        private Function<Throwable, Executing.FailureResult> howToHandleFailure;
        private Supplier<Boolean> canScaleUp;
        private Supplier<Executor> getMainThreadExecutor = () -> ForkJoinPool.commonPool();
        private Function<State, Boolean> expectedStateChecker;

        public void setExpectFailing(Consumer<FailingArguments> asserter) {
            failingStateValidator.activate(asserter);
        }

        public void setExpectRestarting(Consumer<RestartingArguments> asserter) {
            restartingStateValidator.activate(asserter);
        }

        public void setExpectCancelling(Consumer<CancellingArguments> asserter) {
            cancellingStateValidator.activate(asserter);
        }

        public void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
            finishedStateValidator.activate(asserter);
        }

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

        // --------- Interface Implementations ------- //

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
            cancellingStateValidator.validateInput(
                    new CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
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
            restartingStateValidator.validateInput(
                    new RestartingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            backoffTime));
        }

        @Override
        public void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause) {
            failingStateValidator.validateInput(
                    new FailingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            failureCause));
        }

        @Override
        public void runIfState(State expectedState, Runnable action) {
            if (expectedStateChecker.apply(expectedState)) {
                action.run();
            }
        }

        @Override
        public boolean isState(State expectedState) {
            return false;
        }

        @Override
        public Executor getMainThreadExecutor() {
            return getMainThreadExecutor.get();
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            finishedStateValidator.validateInput(archivedExecutionGraph);
        }

        @Override
        public void close() throws Exception {
            failingStateValidator.close();
            restartingStateValidator.close();
            cancellingStateValidator.close();
            finishedStateValidator.close();
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

    private static class RestartingArguments {
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandler;
        private final Duration backoffTime;

        public RestartingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime) {
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.backoffTime = backoffTime;
        }

        public ExecutionGraph getExecutionGraph() {
            return executionGraph;
        }

        public ExecutionGraphHandler getExecutionGraphHandler() {
            return executionGraphHandler;
        }

        public OperatorCoordinatorHandler getOperatorCoordinatorHandler() {
            return operatorCoordinatorHandler;
        }

        public Duration getBackoffTime() {
            return backoffTime;
        }
    }

    private static class FailingArguments {
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandler;
        private final Throwable failureCause;

        public FailingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause) {
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.failureCause = failureCause;
        }

        public ExecutionGraph getExecutionGraph() {
            return executionGraph;
        }

        public ExecutionGraphHandler getExecutionGraphHandler() {
            return executionGraphHandler;
        }

        public OperatorCoordinatorHandler getOperatorCoordinatorHandler() {
            return operatorCoordinatorHandler;
        }

        public Throwable getFailureCause() {
            return failureCause;
        }
    }
}
