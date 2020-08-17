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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.slotsbro.ResourceRequirements;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Component which is responsible to manage the connection to the ResourceManager for
 * the {@link FutureSlotPool}.
 */
class DefaultResourceManagerConnectionManager implements ResourceManagerConnectionManager {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultResourceManagerConnectionManager.class);

	private final JobID jobId;

	private final JobMasterId jobMasterId;

	private final String jobManagerAddress;

	private final ComponentMainThreadExecutor componentMainThreadExecutor;

	private final Time rpcTimeout;

	@Nullable
	private ResourceManagerGateway resourceManagerGateway;

	@Nullable
	private ResourceRequirements currentResourceRequirements;

	DefaultResourceManagerConnectionManager(
			JobID jobId,
			JobMasterId jobMasterId,
			String jobManagerAddress,
			ComponentMainThreadExecutor componentMainThreadExecutor,
			Time rpcTimeout) {
		this.jobId = jobId;
		this.jobMasterId = jobMasterId;
		this.jobManagerAddress = jobManagerAddress;
		this.componentMainThreadExecutor = componentMainThreadExecutor;
		this.rpcTimeout = rpcTimeout;
	}

	@Override
	public void connect(ResourceManagerGateway resourceManagerGateway) {
		componentMainThreadExecutor.assertRunningInMainThread();
		this.resourceManagerGateway = resourceManagerGateway;
	}

	@Override
	public void disconnect() {
		componentMainThreadExecutor.assertRunningInMainThread();
		this.resourceManagerGateway = null;
	}

	@Override
	public void declareResourceRequirements(Collection<ResourceRequirement> resourceRequirements) {
		componentMainThreadExecutor.assertRunningInMainThread();

		if (resourceManagerGateway != null) {
			currentResourceRequirements = new ResourceRequirements(
				jobId,
				jobManagerAddress,
				resourceRequirements);

			sendResourceRequirements(Duration.ofMillis(1L), Duration.ofMillis(10000L), currentResourceRequirements);
		}
	}

	private void sendResourceRequirements(Duration sleepOnError, Duration maxSleepOnError, ResourceRequirements resourceRequirementsToSend) {
		componentMainThreadExecutor.assertRunningInMainThread();

		if (resourceManagerGateway != null) {
			if (resourceRequirementsToSend == currentResourceRequirements) {
				final CompletableFuture<Acknowledge> declareResourcesFuture = resourceManagerGateway.declareRequiredResources(
					jobMasterId,
					resourceRequirementsToSend,
					rpcTimeout);

				declareResourcesFuture.whenComplete(
					(acknowledge, throwable) -> {
						if (throwable != null) {
							componentMainThreadExecutor.schedule(
								() -> sendResourceRequirements(
									ObjectUtils.min(sleepOnError.multipliedBy(2), maxSleepOnError),
									maxSleepOnError,
									resourceRequirementsToSend),
								sleepOnError.toMillis(),
								TimeUnit.MILLISECONDS);
						}
					});
			} else {
				LOG.debug("Newer resource requirements found. Stop sending old requirements.");
			}
		} else {
			LOG.debug("Stop sending resource requirements to ResourceManager because it is not connected.");
		}
	}

	@Override
	public void close() {
		componentMainThreadExecutor.assertRunningInMainThread();
		resourceManagerGateway = null;
		currentResourceRequirements = null;
	}

	public static ResourceManagerConnectionManager create(
			JobID jobId,
			JobMasterId jobMasterId,
			String jobManagerAddress,
			ComponentMainThreadExecutor componentMainThreadExecutor,
			Time rpcTimeout) {
		return new DefaultResourceManagerConnectionManager(
			jobId,
			jobMasterId,
			jobManagerAddress,
			componentMainThreadExecutor,
			rpcTimeout);
	}
}
