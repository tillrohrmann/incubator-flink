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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Information about the parallelism of job vertices. */
public class JobVertexParallelism implements Serializable {
    private static final long serialVersionUID = 5764302211446393341L;

    private static final JobVertexParallelism EMPTY =
            new JobVertexParallelism(Collections.emptyMap());

    private final Map<JobVertexID, Integer> parallelismPerJobVertex;

    private JobVertexParallelism(Map<JobVertexID, Integer> parallelismPerJobVertex) {
        this.parallelismPerJobVertex = parallelismPerJobVertex;
    }

    public Optional<Integer> getParallelismForJobVertex(JobVertexID jobVertexId) {
        return Optional.ofNullable(parallelismPerJobVertex.get(jobVertexId));
    }

    public Set<JobVertexID> getJobVertices() {
        return parallelismPerJobVertex.keySet();
    }

    public Set<Map.Entry<JobVertexID, Integer>> getJobVertexParallelisms() {
        return parallelismPerJobVertex.entrySet();
    }

    public static JobVertexParallelism empty() {
        return JobVertexParallelism.EMPTY;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<JobVertexID, Integer> parallelismPerJobVertex;

        public Builder() {
            parallelismPerJobVertex = new HashMap<>();
        }

        public Builder setParallelismForJobVertex(JobVertexID jobVertexId, int newParallelism) {
            parallelismPerJobVertex.put(jobVertexId, newParallelism);
            return this;
        }

        public JobVertexParallelism build() {
            return new JobVertexParallelism(new HashMap<>(parallelismPerJobVertex));
        }
    }
}
