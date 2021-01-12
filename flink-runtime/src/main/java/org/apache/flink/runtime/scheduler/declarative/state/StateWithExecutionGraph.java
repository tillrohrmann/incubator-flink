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

package org.apache.flink.runtime.scheduler.declarative.state;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import java.util.concurrent.Executor;

public abstract class StateWithExecutionGraph implements State {
    private final Context context;

    protected final ExecutionGraph executionGraph;

    private final Logger logger;

    protected StateWithExecutionGraph(
            Context context, ExecutionGraph executionGraph, Logger logger) {
        this.context = context;
        this.executionGraph = executionGraph;
        this.logger = logger;

        executionGraph
                .getTerminationFuture()
                .thenAcceptAsync(
                        jobStatus -> context.runIfState(this, () -> onTerminalState(jobStatus)),
                        context.getMainThreadExecutor());
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return ArchivedExecutionGraph.createFrom(executionGraph);
    }

    @Override
    public JobStatus getJobStatus() {
        return executionGraph.getState();
    }

    @Override
    public void suspend(Throwable cause) {
        executionGraph.suspend(cause);
        Preconditions.checkState(executionGraph.getState() == JobStatus.SUSPENDED);
        context.goToFinished(ArchivedExecutionGraph.createFrom(executionGraph));
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    abstract boolean updateTaskExecutionState(
            TaskExecutionStateTransition taskExecutionStateTransition);

    abstract void onTerminalState(JobStatus terminalState);

    protected interface Context {
        void runIfState(State state, Runnable action);

        Executor getMainThreadExecutor();

        void goToFinished(ArchivedExecutionGraph archivedExecutionGraph);
    }
}
