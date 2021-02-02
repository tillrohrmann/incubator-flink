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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

/** Tests for the WaitingForResources state. */
public class WaitingForResourcesTest extends TestLogger {

    /** WaitingForResources is transitioning to Executing if there are enough resources */
    @Test
    public void testTransitionToExecuting() {
        StateTestContext testContext =
                new StateTestContext(ctx -> ctx.setHasEnoughResources(() -> true));

        testContext.assertStateTransitionTo(Executing.class);
    }

    @Test
    public void testTransitionToFinishedOnFailure() {
        StateTestContext testContext =
                new StateTestContext(
                        ctx -> {
                            ctx.setHasEnoughResources(() -> true);
                            ctx.setCreateExecutionGraphWithAvailableResources(
                                    () -> {
                                        throw new RuntimeException("Test");
                                    });
                        });

        testContext.assertStateTransitionTo(Finished.class);
    }

    @Test
    public void testDelayedResourceAvailability() {
        StateTestContext testContext =
                new StateTestContext(
                        ctx -> {
                            ctx.setHasEnoughResources(() -> false);
                        });

        // no resources --> no state transition
        testContext.getWaitingForResources().notifyNewResourcesAvailable();
        testContext.assertNoStateTransition();

        // make resources available
        testContext.getMockContext().setHasEnoughResources(() -> true);
        testContext.getWaitingForResources().notifyNewResourcesAvailable();

        testContext.assertStateTransitionTo(Executing.class);
    }

    /*
    @Test
    public void testResourceTimeout() {
        MockContext ctx = new MockContext();
        ctx.setHasEnoughResources(() -> false);

        WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());
        wfr.onEnter();
        assertThat(ctx.getActions(), hasSize(1));
        assertThat(ctx.getActions().get(0).f0, is(wfr));

        // trigger delayed action (in this case resource timeout)
        ctx.getActions().get(0).f1.run();

        assertThat(
                ctx.getStateTransitions(),
                contains(Tuple2.of(MockContext.States.EXECUTING, JobStatus.RUNNING)));
    }

    @Test
    public void testTransitionToFinishedOnGlobalFailure() {
        MockContext ctx = new MockContext();
        ctx.setHasEnoughResources(() -> false);

        WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());
        wfr.onEnter();
        wfr.handleGlobalFailure(new RuntimeException("mock failure"));
        assertThat(
                ctx.getStateTransitions(),
                contains(Tuple2.of(MockContext.States.FINISHED, JobStatus.INITIALIZING)));
    }

    @Test
    public void testCancel() {
        MockContext ctx = new MockContext();
        ctx.setHasEnoughResources(() -> false);

        WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());
        wfr.onEnter();
        wfr.cancel();
        assertThat(
                ctx.getStateTransitions(),
                contains(Tuple2.of(MockContext.States.FINISHED, JobStatus.CANCELED)));
    }

    @Test
    public void testSuspend() {
        MockContext ctx = new MockContext();
        ctx.setHasEnoughResources(() -> false);

        WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());
        wfr.onEnter();
        wfr.suspend(new Exception("test"));
        assertThat(
                ctx.getStateTransitions(),
                contains(Tuple2.of(MockContext.States.FINISHED, JobStatus.SUSPENDED)));
    } */

    private class StateTestContext {
        private final MockContext mockContext;
        private WaitingForResources waitingForResources;

        public StateTestContext(Consumer<MockContext> modifyContext) {
            this.mockContext = new MockContext();
            modifyContext.accept(mockContext);
            this.waitingForResources =
                    new WaitingForResources(mockContext, log, ResourceCounter.empty());
            this.mockContext.setWaitingForResources(waitingForResources);
            waitingForResources.onEnter();
        }

        public MockContext getMockContext() {
            return mockContext;
        }

        public void assertStateTransitionTo(Class<? extends State> expected) {
            Assert.assertThat(mockContext.getTransitionTo(), instanceOf(expected));
        }

        public void assertNoStateTransition() {
            Assert.assertThat(mockContext.getTransitionTo(), nullValue());
        }

        public WaitingForResources getWaitingForResources() {
            return waitingForResources;
        }
    }

    private static class MockContext implements WaitingForResources.Context {

        private static final Logger LOG = LoggerFactory.getLogger(MockContext.class);

        private WaitingForResources waitingForResources;

        private State transitionTo = null;
        private Supplier<Boolean> hasEnoughResourcesSupplier = () -> false;
        private Supplier<ExecutionGraph> createExecutionGraphWithAvailableResources =
                () -> {
                    try {
                        return TestingExecutionGraphBuilder.newBuilder().build();
                    } catch (Throwable e) {
                        throw new RuntimeException("Error", e);
                    }
                };
        private final List<Tuple3<State, Runnable, Duration>> actions = new ArrayList<>();

        private void transitionToState(State target) {
            if (transitionTo != null) {
                Assert.fail("More than one state transition initiated");
            }
            transitionTo = target;
            // waitingForResources.onLeave(target);
        }

        private State getTransitionTo() {
            return transitionTo;
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            transitionToState(new Finished(null, archivedExecutionGraph, LOG));
        }

        @Override
        public void goToExecuting(ExecutionGraph executionGraph) {
            Executing.Context mockExecutingContext = new MockExecutingContext();
            transitionToState(
                    new Executing(
                            executionGraph,
                            new ExecutionGraphHandler(
                                    executionGraph, LOG, ForkJoinPool.commonPool()),
                            new OperatorCoordinatorHandler(
                                    executionGraph,
                                    (throwable -> {
                                        throw new RuntimeException("Error in test", throwable);
                                    })),
                            LOG,
                            mockExecutingContext,
                            ClassLoader.getSystemClassLoader()));
        }

        @Override
        public ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause) {
            return new ArchivedExecutionGraphBuilder()
                    .setState(jobStatus)
                    .setFailureCause(cause == null ? null : new ErrorInfo(cause, 1337))
                    .build();
        }

        @Override
        public boolean hasEnoughResources(ResourceCounter desiredResources) {
            return hasEnoughResourcesSupplier.get();
        }

        @Override
        public ExecutionGraph createExecutionGraphWithAvailableResources() throws Exception {
            return createExecutionGraphWithAvailableResources.get();
        }

        @Override
        public void runIfState(State expectedState, Runnable action, Duration delay) {
            actions.add(Tuple3.of(expectedState, action, delay));
        }

        // ---- Testing extensions ------

        public void setWaitingForResources(WaitingForResources wfr) {
            this.waitingForResources = wfr;
        }

        public List<Tuple3<State, Runnable, Duration>> getActions() {
            return actions;
        }

        public void setHasEnoughResources(Supplier<Boolean> sup) {
            hasEnoughResourcesSupplier = sup;
        }

        public void setCreateExecutionGraphWithAvailableResources(Supplier<ExecutionGraph> sup) {
            this.createExecutionGraphWithAvailableResources = sup;
        }

        private class MockExecutingContext implements Executing.Context {
            @Override
            public void goToCanceling(
                    ExecutionGraph executionGraph,
                    ExecutionGraphHandler executionGraphHandler,
                    OperatorCoordinatorHandler operatorCoordinatorHandler) {}

            @Override
            public Executing.FailureResult howToHandleFailure(Throwable failure) {
                return null;
            }

            @Override
            public boolean canScaleUp(ExecutionGraph executionGraph) {
                return false;
            }

            @Override
            public void goToRestarting(
                    ExecutionGraph executionGraph,
                    ExecutionGraphHandler executionGraphHandler,
                    OperatorCoordinatorHandler operatorCoordinatorHandler,
                    Duration backoffTime) {}

            @Override
            public void goToFailing(
                    ExecutionGraph executionGraph,
                    ExecutionGraphHandler executionGraphHandler,
                    OperatorCoordinatorHandler operatorCoordinatorHandler,
                    Throwable failureCause) {}

            @Override
            public void runIfState(State expectedState, Runnable action) {}

            @Override
            public boolean isState(State expectedState) {
                return false;
            }

            @Override
            public Executor getMainThreadExecutor() {
                return ForkJoinPool.commonPool();
            }

            @Override
            public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {}
        }
    }
}
