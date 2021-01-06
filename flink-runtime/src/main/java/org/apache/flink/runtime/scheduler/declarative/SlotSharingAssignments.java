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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/** Mapping of slots to execution slot sharing groups. */
public class SlotSharingAssignments {

    private final Collection<ExecutionSlotSharingGroupAndSlot> assignments;

    public SlotSharingAssignments(Collection<ExecutionSlotSharingGroupAndSlot> assignments) {
        this.assignments = assignments;
    }

    public Iterable<ExecutionSlotSharingGroupAndSlot> getAssignments() {
        return assignments;
    }

    public Map<JobVertexID, Integer> getMaxParallelismForVertices() {
        return assignments.stream()
                .map(ExecutionSlotSharingGroupAndSlot::getExecutionSlotSharingGroup)
                .flatMap(x -> x.getContainedExecutionVertices().stream())
                .collect(
                        Collectors.toMap(
                                ExecutionVertexID::getJobVertexId,
                                v -> v.getSubtaskIndex() + 1,
                                Math::max));
    }
}
