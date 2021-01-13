/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.declarative.allocator;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfoWithUtilization;
import org.apache.flink.runtime.scheduler.declarative.ParallelismAndResourceAssignments;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.function.TriConsumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/** {@link SlotAllocator} implementation that supports slot sharing. */
public class SlotSharingSlotAllocator implements SlotAllocator<SlotSharingAssignments> {

    private final RequirementsCalculator requirementsCalculator;
    private final MappingCalculator mappingCalculator;
    // TODO: introduce nicer interfaces for these operations
    private final BiFunction<AllocationID, ResourceProfile, PhysicalSlot> reserveSlot;
    private final TriConsumer<AllocationID, Throwable, Long> freeSlot;

    public SlotSharingSlotAllocator(
            BiFunction<AllocationID, ResourceProfile, PhysicalSlot> reserveSlot,
            TriConsumer<AllocationID, Throwable, Long> freeSlot) {
        this.reserveSlot = reserveSlot;
        this.freeSlot = freeSlot;

        this.requirementsCalculator = new SlotSharingRequirementsCalculator();
        this.mappingCalculator = new SlotSharingMappingCalculator();
    }

    @Override
    public ResourceCounter calculateRequiredSlots(Iterable<JobVertex> vertices) {
        return requirementsCalculator.calculateRequiredSlots(vertices);
    }

    @Override
    public Optional<SlotSharingAssignments> determineParallelism(
            JobInformation jobInformation, Collection<SlotInfoWithUtilization> freeSlots) {
        return mappingCalculator.determineParallelismAndAssignResources(jobInformation, freeSlots);
    }

    @Override
    public ParallelismAndResourceAssignments assignResources(
            JobInformation jobInformation,
            Collection<SlotInfoWithUtilization> freeSlots,
            SlotSharingAssignments slotSharingSlotAssignments) {

        final HashMap<ExecutionVertexID, LogicalSlot> assignedSlots = new HashMap<>();

        for (ExecutionSlotSharingGroupAndSlot executionSlotSharingGroup :
                slotSharingSlotAssignments.getAssignments()) {
            final SharedSlot sharedSlot =
                    reserveSharedSlot(executionSlotSharingGroup.getSlotInfo());

            for (ExecutionVertexID executionVertexId :
                    executionSlotSharingGroup
                            .getExecutionSlotSharingGroup()
                            .getContainedExecutionVertices()) {
                final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();
                assignedSlots.put(executionVertexId, logicalSlot);
            }
        }

        final Map<JobVertexID, Integer> parallelismPerJobVertex =
                slotSharingSlotAssignments.getMaxParallelismForVertices();

        return new ParallelismAndResourceAssignments(assignedSlots, parallelismPerJobVertex);
    }

    private SharedSlot reserveSharedSlot(SlotInfo slotInfo) {
        final PhysicalSlot physicalSlot =
                reserveSlot.apply(
                        slotInfo.getAllocationId(),
                        ResourceProfile.fromResourceSpec(ResourceSpec.DEFAULT, MemorySize.ZERO));

        final SharedSlot sharedSlot =
                new SharedSlot(
                        new SlotRequestId(),
                        physicalSlot,
                        slotInfo.willBeOccupiedIndefinitely(),
                        () ->
                                freeSlot.accept(
                                        slotInfo.getAllocationId(),
                                        null,
                                        System.currentTimeMillis()));
        physicalSlot.tryAssignPayload(sharedSlot);

        return sharedSlot;
    }
}
