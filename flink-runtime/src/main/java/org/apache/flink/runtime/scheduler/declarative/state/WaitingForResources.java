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
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;

public class WaitingForResources implements State, ResourceConsumer {

    private final Context context;

    private final Logger logger;

    private final ResourceCounter desiredResources;

    private WaitingForResources(Context context, Logger logger, ResourceCounter desiredResources) {
        this.context = context;
        this.logger = logger;
        this.desiredResources = desiredResources;
    }

    @Override
    public void onEnter() {
        context.runIfState(this, this::resourceTimeout, Duration.ofSeconds(10L));
        newResourcesAvailable();
    }

    @Override
    public void cancel() {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.CANCELED, null));
    }

    @Override
    public void suspend(Throwable cause) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.INITIALIZING;
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return context.getArchivedExecutionGraph(getJobStatus(), null);
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.INITIALIZING, cause));
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public void newResourcesAvailable() {
        if (context.hasEnoughResources(desiredResources)) {
            createExecutionGraphWithAvailableResources();
        }
    }

    private void resourceTimeout() {
        createExecutionGraphWithAvailableResources();
    }

    private void createExecutionGraphWithAvailableResources() {
        try {
            final ExecutionGraph executionGraph =
                    context.createExecutionGraphWithAvailableResources();

            context.goToExecuting(executionGraph);
        } catch (Exception exception) {
            context.goToFinished(context.getArchivedExecutionGraph(JobStatus.FAILED, exception));
        }
    }

    interface Context {
        void goToFinished(ArchivedExecutionGraph archivedExecutionGraph);

        void goToExecuting(ExecutionGraph executionGraph);

        ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause);

        boolean hasEnoughResources(ResourceCounter desiredResources);

        ExecutionGraph createExecutionGraphWithAvailableResources() throws Exception;

        void runIfState(State state, Runnable action, Duration delay);
    }
}
