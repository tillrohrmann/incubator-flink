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
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExecutingTest extends TestLogger {
    @Test
    public void testTransitionToFailing() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {

            ctx.setExpectFailing(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED));
                    }));
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            exec.handleGlobalFailure(new RuntimeException("Horrible test exception"));
        }
    }

    public Executing getExecutingState(MockExecutingContext ctx)
            throws JobException, JobExecutionException {
        ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
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

        private Runnable noFailingTrap;
        private Function4<
                        ExecutionGraph,
                        ExecutionGraphHandler,
                        OperatorCoordinatorHandler,
                        Throwable>
                goToFailingConsumer;

        public void setExpectFailing(
                Function4<
                                ExecutionGraph,
                                ExecutionGraphHandler,
                                OperatorCoordinatorHandler,
                                Throwable>
                        asserter) {
            this.noFailingTrap =
                    () -> {
                        throw new AssertionError("no transition to failing");
                    };
            this.goToFailingConsumer = asserter;
        }

        // --------- Interface Implementations ------- //

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
            return null;
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {}

        @Override
        public void close() throws Exception {}
    }

    @FunctionalInterface
    interface Function4<T1, T2, T3, T4> {
        void apply(T1 a1, T2 a2, T3 a3, T4 a4);
    }
}
