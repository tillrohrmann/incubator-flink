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

import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfoWithUtilization;
import org.apache.flink.runtime.scheduler.declarative.ParallelismAndResourceAssignments;

import java.util.Collection;
import java.util.Optional;

/** Component for calculating the slot requirements and mapping of vertices to slots. */
public interface SlotAllocator<T extends VertexAssignment> extends RequirementsCalculator {

    /**
     * Determines the parallelism at which the vertices could given the collection of slots.
     *
     * @param jobInformation information about the job graph
     * @param slots Slots to consider for determining the parallelism
     * @return parallelism of each vertex along with implementation specific information, if the job
     *     could be run with the given slots
     */
    Optional<T> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> slots);

    /**
     * Assigns vertices to the given slots.
     *
     * @param jobInformation information about the job graph
     * @param freeSlots currently free slots
     * @param assignment information on how slots should be assigned to the slots
     * @return parallelism of each vertex and mapping slots to vertices
     */
    ParallelismAndResourceAssignments assignResources(
            JobInformation jobInformation,
            Collection<SlotInfoWithUtilization> freeSlots,
            T assignment);
}
