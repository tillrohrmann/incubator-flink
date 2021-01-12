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
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.scheduler.declarative.allocator.SlotAllocator;

import javax.annotation.Nullable;

public class Created extends AbstractState {

    private final DeclarativeSlotPool declarativeSlotPool;

    private final SlotAllocator slotAllocator;

    private final JobGraph jobGraph;

    private final long initializationTimestamp;

    protected Created(
            Context context,
            DeclarativeSlotPool declarativeSlotPool,
            SlotAllocator slotAllocator,
            JobGraph jobGraph,
            long initializationTimestamp) {
        super(context);
        this.declarativeSlotPool = declarativeSlotPool;
        this.slotAllocator = slotAllocator;
        this.jobGraph = jobGraph;
        this.initializationTimestamp = initializationTimestamp;
    }

    @Override
    public State cancel() {
        return getContext().createFinished(getArchivedExecutionGraph(JobStatus.CANCELED, null));
    }

    @Override
    public State suspend(Throwable cause) {
        return getContext().createFinished(getArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.INITIALIZING;
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return getArchivedExecutionGraph(getJobStatus(), null);
    }

    @Override
    public State handleGlobalFailure(Throwable cause) {
        return getContext()
                .createFinished(getArchivedExecutionGraph(JobStatus.INITIALIZING, cause));
    }

    private ArchivedExecutionGraph getArchivedExecutionGraph(
            JobStatus initializing, Throwable cause) {
        return createArchivedExecutionGraph(jobGraph, initializing, initializationTimestamp, cause);
    }

    WaitingForResources startScheduling() {
        final ResourceCounter desiredResources =
                slotAllocator.calculateRequiredSlots(jobGraph.getVertices());
        declarativeSlotPool.increaseResourceRequirementsBy(desiredResources);

        return getContext().createWaitingForResources(desiredResources);
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
}
