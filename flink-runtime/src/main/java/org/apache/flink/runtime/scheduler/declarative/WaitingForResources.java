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
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.declarative.allocator.SlotAllocator;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class WaitingForResources extends AbstractState {
    private final ResourceCounter desiredResources;

    private final DeclarativeSlotPool declarativeSlotPool;

    private final SlotAllocator slotAllocator;

    private final InternalFailuresListener internalFailuresListener;

    private final ComponentMainThreadExecutor componentMainThreadExecutor;

    private WaitingForResources(
            Context context,
            ResourceCounter desiredResources,
            DeclarativeSlotPool declarativeSlotPool,
            SlotAllocator slotAllocator,
            InternalFailuresListener internalFailuresListener,
            ComponentMainThreadExecutor componentMainThreadExecutor) {
        super(context);
        this.desiredResources = desiredResources;
        this.declarativeSlotPool = declarativeSlotPool;
        this.slotAllocator = slotAllocator;
        this.internalFailuresListener = internalFailuresListener;
        this.componentMainThreadExecutor = componentMainThreadExecutor;

        // register timeout
        context.runIfState(this, this::resourceTimeout, 10L, TimeUnit.SECONDS);
    }

    @Override
    public void onEnter() {
        newResourcesAvailable();
    }

    @Override
    public State cancel() {
        return getContext().createFinished(createArchivedExecutionGraph(JobStatus.CANCELED, null));
    }

    @Override
    public State suspend(Throwable cause) {
        return getContext()
                .createFinished(createArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.INITIALIZING;
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return createArchivedExecutionGraph(JobStatus.INITIALIZING, null);
    }

    @Override
    public State handleGlobalFailure(Throwable cause) {
        return getContext()
                .createFinished(createArchivedExecutionGraph(JobStatus.INITIALIZING, cause));
    }

    @Override
    public void newResourcesAvailable() {
        if (hasEnoughResources()) {
            transitionToState(createExecutionGraphWithAvailableResources());
        }
    }

    private void resourceTimeout() {
        transitionToState(createExecutionGraphWithAvailableResources());
    }

    private boolean hasEnoughResources() {
        final Collection<? extends SlotInfo> allSlots =
                declarativeSlotPool.getFreeSlotsInformation();
        ResourceCounter outstandingResources = desiredResources;

        final Iterator<? extends SlotInfo> slotIterator = allSlots.iterator();

        while (!outstandingResources.isEmpty() && slotIterator.hasNext()) {
            final SlotInfo slotInfo = slotIterator.next();
            final ResourceProfile resourceProfile = slotInfo.getResourceProfile();

            if (outstandingResources.containsResource(resourceProfile)) {
                outstandingResources = outstandingResources.subtract(resourceProfile, 1);
            } else {
                outstandingResources = outstandingResources.subtract(ResourceProfile.UNKNOWN, 1);
            }
        }

        return outstandingResources.isEmpty();
    }

    private State createExecutionGraphWithAvailableResources() {
        try {
            final ExecutionGraph executionGraph = createExecutionGraphAndAssignResources();

            return new Executing(executionGraph);
        } catch (Exception exception) {
            return getContext()
                    .createFinished(createArchivedExecutionGraph(JobStatus.FAILED, exception));
        }
    }

    @Nonnull
    private ExecutionGraph createExecutionGraphAndAssignResources() throws Exception {
        final JobGraph jobGraph = getContext().getJobGraph();

        final Optional<DeclarativeScheduler.ParallelismAndResourceAssignments>
                parallelismAndResourceAssignmentsOptional =
                        slotAllocator.determineParallelismAndAssignResources(
                                new JobGraphJobInformation(jobGraph),
                                declarativeSlotPool.getFreeSlotsInformation());

        if (!parallelismAndResourceAssignmentsOptional.isPresent()) {
            throw new JobExecutionException(
                    jobGraph.getJobID(), "Not enough resources available for scheduling.");
        }

        final DeclarativeScheduler.ParallelismAndResourceAssignments
                parallelismAndResourceAssignments = parallelismAndResourceAssignmentsOptional.get();

        for (JobVertex vertex : jobGraph.getVertices()) {
            vertex.setParallelism(parallelismAndResourceAssignments.getParallelism(vertex.getID()));
        }

        final ExecutionGraph executionGraph = createExecutionGraphAndRestoreState();

        executionGraph.start(componentMainThreadExecutor);
        executionGraph.transitionToRunning();

        executionGraph.setInternalTaskFailuresListener(internalFailuresListener);

        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            final LogicalSlot assignedSlot =
                    parallelismAndResourceAssignments.getAssignedSlot(executionVertex.getID());
            executionVertex
                    .getCurrentExecutionAttempt()
                    .registerProducedPartitions(assignedSlot.getTaskManagerLocation(), false);
            executionVertex.tryAssignResource(assignedSlot);
        }
        return executionGraph;
    }
}
