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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/** Tests for the WaitingForResources state. */
public class WaitingForResourcesTest extends TestLogger {

    /** WaitingForResources is transitioning to Executing if resources are stable. */
    @Test
    public void testTransitionToExecuting() {
        MockContext ctx = new MockContext();
        ctx.setHasEnoughResources(() -> true);
        WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());
        wfr.onEnter();
        assertThat(
                ctx.getStateTransitions(),
                contains(Tuple2.of(MockContext.States.EXECUTING, JobStatus.RUNNING)));
    }

    @Test
    public void testTransitionToFinishedOnFailure() {
        MockContext ctx = new MockContext();
        ctx.setHasEnoughResources(() -> true);
        ctx.setCreateExecutionGraphWithAvailableResources(
                () -> {
                    throw new RuntimeException("Test");
                });
        WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());
        wfr.onEnter();
        assertThat(
                ctx.getStateTransitions(),
                contains(Tuple2.of(MockContext.States.FINISHED, JobStatus.FAILED)));
    }

    @Test
    public void testDelayedResourceAvailability() {
        MockContext ctx = new MockContext();
        ctx.setHasEnoughResources(() -> false);

        WaitingForResources wfr = new WaitingForResources(ctx, log, ResourceCounter.empty());
        wfr.onEnter();
        wfr.notifyNewResourcesAvailable();

        assertThat(ctx.getStateTransitions(), hasSize(0));

        ctx.setHasEnoughResources(() -> true);
        wfr.notifyNewResourcesAvailable();
        assertThat(
                ctx.getStateTransitions(),
                contains(Tuple2.of(MockContext.States.EXECUTING, JobStatus.RUNNING)));
    }

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
    }

    private static class MockContext implements WaitingForResources.Context {

        private enum States {
            EXECUTING,
            FINISHED
        }

        private final List<Tuple2<States, JobStatus>> stateTransitions = new ArrayList<>();
        private Supplier<Boolean> hasEnoughResourcesSupplier = () -> false;
        private Supplier<ExecutionGraph> createExecutionGraphWithAvailableResources = () -> null;
        private final List<Tuple3<State, Runnable, Duration>> actions = new ArrayList<>();

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            stateTransitions.add(Tuple2.of(States.FINISHED, archivedExecutionGraph.getState()));
        }

        @Override
        public void goToExecuting(ExecutionGraph executionGraph) {
            stateTransitions.add(Tuple2.of(States.EXECUTING, JobStatus.RUNNING));
        }

        @Override
        public ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause) {
            return new MockArchivedExecutionGraph(jobStatus, cause);
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

        public List<Tuple2<States, JobStatus>> getStateTransitions() {
            return stateTransitions;
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
    }

    private static class MockArchivedExecutionGraph extends ArchivedExecutionGraph {

        public MockArchivedExecutionGraph(JobStatus state, Throwable failureCause) {
            super(
                    new JobID(),
                    "mock",
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    new long[] {},
                    state,
                    failureCause == null ? null : new ErrorInfo(failureCause, 1337),
                    "jsonPlan",
                    new StringifiedAccumulatorResult[0],
                    Collections.emptyMap(),
                    new ExecutionConfig().archive(),
                    false,
                    null,
                    null,
                    null);
        }
    }
}
