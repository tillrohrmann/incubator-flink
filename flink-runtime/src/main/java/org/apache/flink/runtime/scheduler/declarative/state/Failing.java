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
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

/** State which describes a failing job which is currently being canceled. */
public class Failing extends StateWithExecutionGraph {
    private final Context context;

    private final Throwable failureCause;

    public Failing(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger,
            Throwable failureCause) {
        super(context, executionGraph, executionGraphHandler, operatorCoordinatorHandler, logger);
        this.context = context;
        this.failureCause = failureCause;
    }

    @Override
    public void onEnter() {
        executionGraph.failJob(failureCause);
    }

    @Override
    public void cancel() {
        context.goToCanceling(executionGraph, executionGraphHandler, operatorCoordinatorHandler);
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        // nothing to do since we are already failing
    }

    @Override
    public boolean updateTaskExecutionState(
            TaskExecutionStateTransition taskExecutionStateTransition) {
        return executionGraph.updateState(taskExecutionStateTransition);
    }

    @Override
    void onTerminalState(JobStatus terminalState) {
        Preconditions.checkState(terminalState == JobStatus.FAILED);
        context.goToFinished(ArchivedExecutionGraph.createFrom(executionGraph));
    }

    /** Context of the {@link Failing} state. */
    public interface Context extends StateWithExecutionGraph.Context {

        /**
         * Transitions into the {@link Canceling} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Canceling} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Canceling} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Canceling} state
         */
        void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);
    }
}
