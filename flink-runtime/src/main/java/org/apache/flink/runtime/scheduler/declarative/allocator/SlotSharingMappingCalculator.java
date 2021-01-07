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

import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfoWithUtilization;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link MappingCalculator} that supports slot sharing. */
class SlotSharingMappingCalculator implements MappingCalculator {

    @Override
    public Optional<SlotSharingAssignments> determineParallelismAndAssignResources(
            JobInformation jobInformation, Collection<SlotInfoWithUtilization> freeSlots) {

        // TODO: This can waste slots if the max parallelism for slot sharing groups is not equal
        final int slotsPerSlotSharingGroup =
                freeSlots.size() / jobInformation.getSlotSharingGroups().size();
        final Iterator<SlotInfoWithUtilization> slotIterator = freeSlots.iterator();

        final Collection<ExecutionSlotSharingGroupAndSlot> assignments = new ArrayList<>();

        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            final List<JobInformation.VertexInformation> containedJobVertices =
                    slotSharingGroup.getJobVertexIds().stream()
                            .map(jobInformation::getVertexInformation)
                            .collect(Collectors.toList());

            final Iterable<ExecutionSlotSharingGroup> sharedSlotToVertexAssignment =
                    determineParallelismAndAssignToFutureSlotIndex(
                            containedJobVertices, slotsPerSlotSharingGroup);

            for (ExecutionSlotSharingGroup executionSlotSharingGroup :
                    sharedSlotToVertexAssignment) {
                final SlotInfoWithUtilization slotInfo = slotIterator.next();

                assignments.add(
                        new ExecutionSlotSharingGroupAndSlot(executionSlotSharingGroup, slotInfo));
            }
        }

        return Optional.of(new SlotSharingAssignments(assignments));
    }

    private Iterable<ExecutionSlotSharingGroup> determineParallelismAndAssignToFutureSlotIndex(
            Collection<JobInformation.VertexInformation> containedJobVertices, int availableSlots) {
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();
        for (JobInformation.VertexInformation jobVertex : containedJobVertices) {
            final int parallelism = Math.min(jobVertex.getParallelism(), availableSlots);

            for (int i = 0; i < parallelism; i++) {
                sharedSlotToVertexAssignment
                        .computeIfAbsent(i, ignored -> new HashSet<>())
                        .add(new ExecutionVertexID(jobVertex.getJobVertexID(), i));
            }
        }

        return sharedSlotToVertexAssignment.values().stream()
                .map(ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }
}
