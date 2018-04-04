/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rescaling;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nonnull;

/**
 * New parallelism and the {@link JobVertexID} of the corresponding operator.
 */
public class OperatorParallelism {

	@Nonnull
	private final JobVertexID jobVertexId;

	private final int newParallelism;

	public OperatorParallelism(@Nonnull JobVertexID jobVertexId, int newParallelism) {
		this.jobVertexId = jobVertexId;
		this.newParallelism = newParallelism;
	}

	@Nonnull
	public JobVertexID getJobVertexId() {
		return jobVertexId;
	}

	public int getNewParallelism() {
		return newParallelism;
	}
}
