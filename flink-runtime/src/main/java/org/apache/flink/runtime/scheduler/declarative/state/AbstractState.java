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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;

public abstract class AbstractState implements State {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Context context;

    protected AbstractState(Context context) {
        this.context = context;
    }

    protected Context getContext() {
        return context;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    protected ArchivedExecutionGraph createArchivedExecutionGraph(
            JobGraph jobGraph,
            JobStatus jobStatus,
            long initializationTimestamp,
            @Nullable Throwable throwable) {
        return ArchivedExecutionGraph.createFromInitializingJob(
                jobGraph.getJobID(),
                jobGraph.getName(),
                jobStatus,
                throwable,
                initializationTimestamp);
    }

    interface Context {
        void transitionToState(State state);

        void runIfState(State expectedState, Runnable action);

        void runIfState(State expectedState, Runnable action, long delay, TimeUnit timeUnit);

        JobGraph getJobGraph();

        Finished createFinished(ArchivedExecutionGraph archivedExecutionGraph);

        WaitingForResources createWaitingForResources(ResourceCounter desiredResources);
    }
}
