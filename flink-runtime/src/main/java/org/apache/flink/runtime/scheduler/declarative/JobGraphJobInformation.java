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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.declarative.allocator.JobInformation;

import java.util.Collection;

/** {@link JobInformation} backed by a {@link JobGraph}. */
public class JobGraphJobInformation implements JobInformation {

    private final JobGraph jobGraph;

    public JobGraphJobInformation(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
    }

    @Override
    public Collection<SlotSharingGroup> getSlotSharingGroups() {
        return jobGraph.getSlotSharingGroups();
    }

    @Override
    public VertexInformation getVertexInformation(JobVertexID jobVertexId) {
        return new JobVertexInformation(jobGraph.findVertexByID(jobVertexId));
    }

    private static class JobVertexInformation implements VertexInformation {

        private final JobVertex jobVertex;

        private JobVertexInformation(JobVertex jobVertex) {
            this.jobVertex = jobVertex;
        }

        @Override
        public JobVertexID getJobVertexID() {
            return jobVertex.getID();
        }

        @Override
        public int getParallelism() {
            return jobVertex.getParallelism();
        }
    }
}
