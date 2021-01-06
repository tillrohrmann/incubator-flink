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

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfoWithUtilization;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/** Calculates a potential mapping between slots & * vertices. */
public interface MappingCalculator {

    /**
     * Attempts to create a mapping between vertices and slots.
     *
     * @param jobInformation information about the job graph
     * @param freeSlots currently free slots
     * @return mapping of slots and the vertices that should be deployed into them, if a mapping
     *     could be found
     */
    // TODO: add a new data structure for the return value that is easier to understand
    Optional<Map<SlotSharingGroupId, Map<SlotInfo, ExecutionSlotSharingGroup>>>
            determineParallelismAndAssignResources(
                    JobInformation jobInformation, Collection<SlotInfoWithUtilization> freeSlots);
}
