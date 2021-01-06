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

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;

/** Calculates resource requirements for a set of vertices. */
public interface RequirementsCalculator {

    /**
     * Calculates the total resources required for scheduling the given vertices.
     *
     * @param vertices vertices to schedule
     * @return required resources
     */
    // TODO: replace JobVertex with VertexInformation
    ResourceCounter calculateRequiredSlots(Iterable<JobVertex> vertices);
}
