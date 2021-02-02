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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Tests for the WaitingForResources state. */
public class ChesnayWaitingForResourcesTest extends TestLogger {

    /** WaitingForResources is transitioning to Executing if there are enough resources */
    @Test
    public void testTransitionToExecuting() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> true);
            ctx.setExpectExecuting(
                    (eg) -> {
                        log.info("Received eg {}", eg);
                    });
            WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());

            try (TestStateContext tsc = new TestStateContext(ctx, wfr)) {}
        }
    }

    @Test
    public void testTransitionToFinishedOnFailure() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> true);
            ctx.setCreateExecutionGraphWithAvailableResources(
                    () -> {
                        throw new RuntimeException("Test");
                    });
            ctx.setExpectFinished(
                    (eg) -> {
                        log.info("Received eg {}", eg);
                    });
            WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());

            try (TestStateContext tsc = new TestStateContext(ctx, wfr)) {}
        }
    }

    private static class TestStateContext implements AutoCloseable {
        final MockContext context;
        final WaitingForResources wfr;

        public TestStateContext(MockContext context, WaitingForResources wfr) {
            this.context = context;
            this.wfr = wfr;
            wfr.onEnter();
        }

        @Override
        public void close() {
            if (this.context.didTransition) {
                try {
                    this.context.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class MockContext implements WaitingForResources.Context, AutoCloseable {

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
        private Runnable noFinishTrap = () -> {};
        private Runnable noExecutingTrap = () -> {};
        private boolean didTransition = false;
        private Consumer<ExecutionGraph> goToExecutingConsumer;
        private Consumer<ArchivedExecutionGraph> goToFinishingConsumer;

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

        public List<Tuple3<State, Runnable, Duration>> getActions() {
            return actions;
        }

        public void setHasEnoughResources(Supplier<Boolean> sup) {
            hasEnoughResourcesSupplier = sup;
        }

        public void setCreateExecutionGraphWithAvailableResources(Supplier<ExecutionGraph> sup) {
            this.createExecutionGraphWithAvailableResources = sup;
        }

        void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
            this.noFinishTrap =
                    () -> {
                        throw new AssertionError("no transition to finished");
                    };
            this.setGoToFinishedConsumer(asserter);
        }

        void setExpectExecuting(Consumer<ExecutionGraph> asserter) {
            this.noExecutingTrap =
                    () -> {
                        throw new AssertionError("no transition to executing");
                    };
            this.setGoToExecutingConsumer(asserter);
        }

        private void setGoToExecutingConsumer(Consumer<ExecutionGraph> goToExecutingConsumer) {
            this.goToExecutingConsumer = goToExecutingConsumer;
        }

        private void setGoToFinishedConsumer(Consumer<ArchivedExecutionGraph> asserter) {
            this.goToFinishingConsumer = asserter;
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            this.noFinishTrap = () -> {};
            this.didTransition = true;
            this.goToFinishingConsumer.accept(archivedExecutionGraph);
        }

        @Override
        public void goToExecuting(ExecutionGraph eg) {
            this.noExecutingTrap = () -> {};
            this.didTransition = true;
            // assert arguments
            this.goToExecutingConsumer.accept(eg);
        }

        @Override
        public void close() throws Exception {
            noFinishTrap.run();
            noExecutingTrap.run();
        }
    }
}
