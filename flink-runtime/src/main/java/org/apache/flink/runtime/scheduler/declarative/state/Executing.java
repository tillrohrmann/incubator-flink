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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;

import org.slf4j.Logger;

public class Executing extends StateWithExecutionGraph {

    private final Context context;

    private final ClassLoader userCodeClassLoader;

    public Executing(
            ExecutionGraph executionGraph,
            Logger logger,
            Context context,
            ClassLoader userCodeClassLoader) {
        super(context, executionGraph, logger);
        this.context = context;
        this.userCodeClassLoader = userCodeClassLoader;
    }

    @Override
    public void onEnter() {
        deploy();
    }

    @Override
    public void cancel() {
        context.goToCanceling(executionGraph);
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        context.handleExecutingFailure(cause);
    }

    @Override
    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        final boolean successfulUpdate = executionGraph.updateState(taskExecutionState);

        if (successfulUpdate) {
            if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
                Throwable cause = taskExecutionState.getError(userCodeClassLoader);
                context.handleExecutingFailure(cause);
            }
        }

        return successfulUpdate;
    }

    @Override
    void onTerminalState(JobStatus jobStatus) {
        context.goToFinished(ArchivedExecutionGraph.createFrom(executionGraph));
    }

    private void deploy() {
        for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
            for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
                deploySafely(executionVertex);
            }
        }
    }

    private void deploySafely(ExecutionVertex executionVertex) {
        try {
            executionVertex.deploy();
        } catch (JobException e) {
            handleDeploymentFailure(executionVertex, e);
        }
    }

    private void handleDeploymentFailure(ExecutionVertex executionVertex, JobException e) {
        executionVertex.markFailed(e);
    }

    interface Context extends StateWithExecutionGraph.Context {

        void goToCanceling(ExecutionGraph executionGraph);

        void handleExecutingFailure(Throwable failure);
    }
}
