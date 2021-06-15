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

package org.apache.flink.runtime.rest.messages.json;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobVertexParallelism;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Iterator;

/** JSON Deserializer for the {@link JobVertexParallelism}. */
public class JobVertexParallelismDeserializer extends StdDeserializer<JobVertexParallelism> {

    public JobVertexParallelismDeserializer() {
        super(JobVertexParallelism.class);
    }

    @Override
    public JobVertexParallelism deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        final JsonNode rootNode = jsonParser.readValueAsTree();

        final Iterator<String> fieldIterator = rootNode.fieldNames();
        final JobVertexParallelism.Builder builder = JobVertexParallelism.newBuilder();

        while (fieldIterator.hasNext()) {
            final String key = fieldIterator.next();
            final int value = rootNode.get(key).asInt();

            builder.setParallelismForJobVertex(JobVertexID.fromHexString(key), value);
        }

        return builder.build();
    }
}
