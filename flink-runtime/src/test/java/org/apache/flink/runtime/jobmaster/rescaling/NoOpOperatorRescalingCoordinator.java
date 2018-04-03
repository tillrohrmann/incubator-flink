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

import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Simple no-op implementation of {@link OperatorRescalingCoordinator}.
 */
public enum NoOpOperatorRescalingCoordinator implements OperatorRescalingCoordinator {
	INSTANCE {
		@Override
		public CompletableFuture<Void> closeAsync() {
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public JobRescalingTarget checkRescalingPolicies(AccessExecutionGraph accessExecutionGraph) {
			return new JobRescalingTarget(Collections.emptyList());
		}
	}
}
