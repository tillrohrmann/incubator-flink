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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.jobmaster.JobVertexParallelism;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexParallelismDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexParallelismSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

/** Body for change job requests. */
public class ChangeJobRequestBody implements RequestBody {
    private static final String FIELD_NAME_JOB_VERTEX_PARALLELISM = "jobVertexParallelism";

    @JsonProperty(FIELD_NAME_JOB_VERTEX_PARALLELISM)
    @JsonSerialize(using = JobVertexParallelismSerializer.class)
    @Nullable
    private final JobVertexParallelism jobVertexParallelism;

    @JsonCreator
    public ChangeJobRequestBody(
            @JsonDeserialize(using = JobVertexParallelismDeserializer.class)
                    @JsonProperty(FIELD_NAME_JOB_VERTEX_PARALLELISM)
                    @Nullable
                    JobVertexParallelism jobVertexParallelism) {
        this.jobVertexParallelism = jobVertexParallelism;
    }

    public JobVertexParallelism getJobVertexParallelism() {
        if (jobVertexParallelism == null) {
            return JobVertexParallelism.empty();
        } else {
            return jobVertexParallelism;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the {@link ChangeJobRequestBody}. */
    public static final class Builder {

        private JobVertexParallelism jobVertexParallelism = JobVertexParallelism.empty();

        public Builder() {}

        public Builder changeParallelism(JobVertexParallelism newJobVertexParallelism) {
            this.jobVertexParallelism = newJobVertexParallelism;
            return this;
        }

        public ChangeJobRequestBody build() {
            return new ChangeJobRequestBody(jobVertexParallelism);
        }
    }
}
