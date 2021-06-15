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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobVertexParallelism;
import org.apache.flink.runtime.rest.messages.RestRequestMarshallingTestBase;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for the marshalling of {@link ChangeJobRequestBody}. */
public class ChangeJobRequestBodyTest extends RestRequestMarshallingTestBase<ChangeJobRequestBody> {
    @Override
    protected Class<ChangeJobRequestBody> getTestRequestClass() {
        return ChangeJobRequestBody.class;
    }

    @Override
    protected ChangeJobRequestBody getTestRequestInstance() throws Exception {
        return ChangeJobRequestBody.newBuilder()
                .changeParallelism(
                        JobVertexParallelism.newBuilder()
                                .setParallelismForJobVertex(new JobVertexID(), 42)
                                .setParallelismForJobVertex(new JobVertexID(), 1337)
                                .build())
                .build();
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            ChangeJobRequestBody expected, ChangeJobRequestBody actual) {
        assertThat(expected, equalsChangeJobRequestBody(actual));
    }

    private EqualityChangeJobRequestBodyMatcher equalsChangeJobRequestBody(
            ChangeJobRequestBody actual) {
        return new EqualityChangeJobRequestBodyMatcher(actual);
    }

    private static final class EqualityChangeJobRequestBodyMatcher
            extends TypeSafeMatcher<ChangeJobRequestBody> {

        private final ChangeJobRequestBody actualChangeJobRequestBody;

        private EqualityChangeJobRequestBodyMatcher(
                ChangeJobRequestBody actualChangeJobRequestBody) {
            this.actualChangeJobRequestBody = actualChangeJobRequestBody;
        }

        @Override
        protected boolean matchesSafely(ChangeJobRequestBody changeJobRequestBody) {
            final JobVertexParallelism jobVertexParallelism =
                    changeJobRequestBody.getJobVertexParallelism();
            final JobVertexParallelism actualJobVertexParallelism =
                    actualChangeJobRequestBody.getJobVertexParallelism();

            return jobVertexParallelism
                    .getJobVertexParallelisms()
                    .equals(actualJobVertexParallelism.getJobVertexParallelisms());
        }

        @Override
        public void describeTo(Description description) {}
    }
}
