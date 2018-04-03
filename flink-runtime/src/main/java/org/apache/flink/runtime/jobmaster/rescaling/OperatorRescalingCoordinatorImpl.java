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

package org.apache.flink.runtime.jobmaster.rescaling;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescaling.OperatorParallelism;
import org.apache.flink.runtime.rescaling.OperatorRescalingContextImpl;
import org.apache.flink.runtime.rescaling.OperatorRescalingPolicy;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Class which is responsible for coordinating operator rescaling requests coming
 * from a collection of {@link OperatorRescalingPolicy}.
 */
public class OperatorRescalingCoordinatorImpl implements OperatorRescalingCoordinator {

	private final Map<JobVertexID, OperatorRescalingPolicy> operatorRescalingPolicies;

	private final CompletableFuture<Void> terminationFuture;

	private boolean running;

	public OperatorRescalingCoordinatorImpl(@Nonnull Map<JobVertexID, OperatorRescalingPolicy> operatorRescalingPolicies) {
		terminationFuture = new CompletableFuture<>();
		running = true;

		this.operatorRescalingPolicies = new HashMap<>(operatorRescalingPolicies);
	}

	@Override
	public JobRescalingTarget checkRescalingPolicies(AccessExecutionGraph accessExecutionGraph) {
		final List<OperatorParallelism> operatorRescalingResults = operatorRescalingPolicies
			.entrySet()
			.stream()
			.map((Map.Entry<JobVertexID, OperatorRescalingPolicy> mapEntry) -> {
				final JobVertexID jobVertexId = mapEntry.getKey();
				final AccessExecutionJobVertex jobVertex = accessExecutionGraph.getJobVertex(jobVertexId);
				return checkRescalingPolicy(jobVertex, mapEntry.getValue());
			})
			.collect(Collectors.toList());

		return new JobRescalingTarget(operatorRescalingResults);
	}

	private OperatorParallelism checkRescalingPolicy(AccessExecutionJobVertex jobVertex, OperatorRescalingPolicy operatorRescalingPolicy) {
		final JobVertexID jobVertexId = jobVertex.getJobVertexId();
		final OperatorRescalingContextImpl rescalingContext = new OperatorRescalingContextImpl(jobVertex);
		return new OperatorParallelism(jobVertexId, operatorRescalingPolicy.rescaleTo(rescalingContext));
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		if (running) {
			running = false;

			final Collection<CompletableFuture<Void>> operatorRescalingPolicyTerminationFutures = operatorRescalingPolicies
				.values()
				.stream()
				.map(OperatorRescalingPolicy::closeAsync)
				.collect(Collectors.toList());

			final CompletableFuture<Void> operatorRescalingPoliciesTerminationFuture = FutureUtils.waitForAll(operatorRescalingPolicyTerminationFutures);

			operatorRescalingPoliciesTerminationFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					if (throwable != null) {
						terminationFuture.completeExceptionally(throwable);
					} else {
						terminationFuture.complete(null);
					}
				});
		}

		return terminationFuture;
	}
}
