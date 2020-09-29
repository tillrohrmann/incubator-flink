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

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slots.ResourceRequirements;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link ResourceManagerConnectionManager}.
 *
 * <p>The resource manager connection manager is responsible for sending new
 * resource requirements to the resource manager. In case of faults it continues
 * retrying to send the latest resource requirements to the resource manager with
 * an exponential backoff strategy.
 */
class DefaultResourceManagerConnectionManager implements ResourceManagerConnectionManager {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultResourceManagerConnectionManager.class);

	private final Object lock = new Object();

	private final ScheduledExecutor scheduledExecutor;

	@Nullable
	@GuardedBy("lock")
	private DeclareResourceRequirementsService declareResourceRequirementsService;

	@Nullable
	@GuardedBy("lock")
	private ResourceRequirements currentResourceRequirements;

	DefaultResourceManagerConnectionManager(ScheduledExecutor scheduledExecutor) {
		this.scheduledExecutor = scheduledExecutor;
	}

	@Override
	public void connect(DeclareResourceRequirementsService declareResourceRequirementsService) {
		synchronized (lock) {
			this.declareResourceRequirementsService = declareResourceRequirementsService;
		}
	}

	@Override
	public void disconnect() {
		synchronized (lock) {
			this.declareResourceRequirementsService = null;
		}
	}

	@Override
	public void declareResourceRequirements(ResourceRequirements resourceRequirements) {
		synchronized (lock) {
			if (declareResourceRequirementsService != null) {
				currentResourceRequirements = resourceRequirements;

				sendResourceRequirements(Duration.ofMillis(1L), Duration.ofMillis(10000L), currentResourceRequirements);
			}
		}
	}

	@GuardedBy("lock")
	private void sendResourceRequirements(Duration sleepOnError, Duration maxSleepOnError, ResourceRequirements resourceRequirementsToSend) {

		if (declareResourceRequirementsService != null) {
			if (resourceRequirementsToSend == currentResourceRequirements) {
				final CompletableFuture<Acknowledge> declareResourcesFuture = declareResourceRequirementsService.declareResourceRequirements(resourceRequirementsToSend);

				declareResourcesFuture.whenComplete(
					(acknowledge, throwable) -> {
						if (throwable != null) {
							scheduledExecutor.schedule(
								() -> {
									synchronized (lock) {
										sendResourceRequirements(
											ObjectUtils.min(sleepOnError.multipliedBy(2), maxSleepOnError),
											maxSleepOnError,
											resourceRequirementsToSend);
									}
								},
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
		synchronized (lock) {
			declareResourceRequirementsService = null;
			currentResourceRequirements = null;
		}
	}

	public static ResourceManagerConnectionManager create(ScheduledExecutor scheduledExecutor) {
		return new DefaultResourceManagerConnectionManager(scheduledExecutor);
	}
}
