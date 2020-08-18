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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.slotsbro.ResourceRequirements;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link DefaultResourceManagerConnectionManager}.
 */
public class DefaultResourceManagerConnectionManagerTest extends TestLogger {

	private final ManuallyTriggeredScheduledExecutorService scheduledExecutor = new ManuallyTriggeredScheduledExecutorService();
	private final JobID jobId = new JobID();

	@Test
	public void testIgnoreDeclareResourceRequirementsIfNotConnected() {
		final ResourceManagerConnectionManager resourceManagerConnectionManager = createResourceManagerConnectionManager();

		resourceManagerConnectionManager.declareResourceRequirements(createResourceRequirements());
	}

	@Test
	public void testDeclareResourceRequirementsSendsRequirementsIfConnected() {
		final ResourceManagerConnectionManager resourceManagerConnectionManager = createResourceManagerConnectionManager();

		final CompletableFuture<ResourceRequirements> declareResourceRequirementsFuture = new CompletableFuture<>();
		resourceManagerConnectionManager.connect(resourceRequirements -> {
			declareResourceRequirementsFuture.complete(resourceRequirements);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

		final ResourceRequirements resourceRequirements = createResourceRequirements();
		resourceManagerConnectionManager.declareResourceRequirements(resourceRequirements);

		assertThat(declareResourceRequirementsFuture.join(), is(resourceRequirements));
	}

	@Test
	public void testRetryDeclareResourceRequirementsIfTransmissionFailed() throws InterruptedException {
		final ResourceManagerConnectionManager resourceManagerConnectionManager = createResourceManagerConnectionManager();

		final FailingDeclareResourceRequirementsService failingDeclareResourceRequirementsService = new FailingDeclareResourceRequirementsService(4);
		resourceManagerConnectionManager.connect(failingDeclareResourceRequirementsService);

		final ResourceRequirements resourceRequirements = createResourceRequirements();

		resourceManagerConnectionManager.declareResourceRequirements(resourceRequirements);

		scheduledExecutor.drainNonPeriodicScheduledTasks();

		assertThat(failingDeclareResourceRequirementsService.nextResourceRequirements(), is(resourceRequirements));
		assertThat(failingDeclareResourceRequirementsService.hasResourceRequirements(), is(false));
	}

	@Nonnull
	private ResourceRequirements createResourceRequirements() {
		return createResourceRequirements(Arrays.asList(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2)));
	}

	@Test
	public void testDisconnectStopsSendingResourceRequirements() throws InterruptedException {
		runStopSendingResourceRequirementsTest(ResourceManagerConnectionManager::disconnect);
	}

	private void runStopSendingResourceRequirementsTest(Consumer<ResourceManagerConnectionManager> testAction) throws InterruptedException {
		final ResourceManagerConnectionManager resourceManagerConnectionManager = createResourceManagerConnectionManager();

		final FailingDeclareResourceRequirementsService declareResourceRequirementsService = new FailingDeclareResourceRequirementsService(1);
		resourceManagerConnectionManager.connect(declareResourceRequirementsService);

		final ResourceRequirements resourceRequirements = createResourceRequirements();
		resourceManagerConnectionManager.declareResourceRequirements(resourceRequirements);

		declareResourceRequirementsService.waitForResourceRequirementsDeclaration();

		testAction.accept(resourceManagerConnectionManager);
		scheduledExecutor.drainNonPeriodicScheduledTasks();

		assertThat(declareResourceRequirementsService.hasResourceRequirements(), is(false));
	}

	@Test
	public void testCloseStopsSendingResourceRequirements() throws InterruptedException {
		runStopSendingResourceRequirementsTest(ResourceManagerConnectionManager::close);
	}

	@Test
	public void testNewResourceRequirementsOverrideOldRequirements() throws InterruptedException {
		final ResourceManagerConnectionManager resourceManagerConnectionManager = createResourceManagerConnectionManager();
		final ResourceRequirements resourceRequirements1 = createResourceRequirements(Arrays.asList(ResourceRequirement.create(ResourceProfile.UNKNOWN, 1)));
		final ResourceRequirements resourceRequirements2 = createResourceRequirements(Arrays.asList(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2)));

		final FailingDeclareResourceRequirementsService failingDeclareResourceRequirementsService = new FailingDeclareResourceRequirementsService(1);
		resourceManagerConnectionManager.connect(failingDeclareResourceRequirementsService);

		resourceManagerConnectionManager.declareResourceRequirements(resourceRequirements1);

		failingDeclareResourceRequirementsService.waitForResourceRequirementsDeclaration();

		resourceManagerConnectionManager.declareResourceRequirements(resourceRequirements2);

		scheduledExecutor.drainNonPeriodicScheduledTasks();

		assertThat(failingDeclareResourceRequirementsService.nextResourceRequirements(), is(resourceRequirements2));
		assertThat(failingDeclareResourceRequirementsService.hasResourceRequirements(), is(false));
	}

	private static final class FailingDeclareResourceRequirementsService implements ResourceManagerConnectionManager.DeclareResourceRequirementsService {

		private final BlockingQueue<ResourceRequirements> resourceRequirements = new ArrayBlockingQueue<>(2);

		private final OneShotLatch declareResourceRequirementsLatch = new OneShotLatch();

		private int failureCounter;

		private FailingDeclareResourceRequirementsService(int failureCounter) {
			this.failureCounter = failureCounter;
		}

		@Override
		public CompletableFuture<Acknowledge> declareResourceRequirements(ResourceRequirements resourceRequirements) {
			if (failureCounter > 0) {
				failureCounter--;
				declareResourceRequirementsLatch.trigger();
				return FutureUtils.completedExceptionally(new FlinkException("Test exception"));
			} else {
				this.resourceRequirements.offer(resourceRequirements);
				return CompletableFuture.completedFuture(Acknowledge.get());
			}
		}

		private boolean hasResourceRequirements() {
			return !resourceRequirements.isEmpty();
		}

		private ResourceRequirements nextResourceRequirements() throws InterruptedException {
			return resourceRequirements.take();
		}

		public void waitForResourceRequirementsDeclaration() throws InterruptedException {
			declareResourceRequirementsLatch.await();
		}
	}

	private ResourceRequirements createResourceRequirements(List<ResourceRequirement> requestedResourceRequirements) {
		return new ResourceRequirements(
			jobId,
			"localhost",
			requestedResourceRequirements);
	}

	@Nonnull
	private ResourceManagerConnectionManager createResourceManagerConnectionManager() {
		return DefaultResourceManagerConnectionManager.create(scheduledExecutor);
	}

}
