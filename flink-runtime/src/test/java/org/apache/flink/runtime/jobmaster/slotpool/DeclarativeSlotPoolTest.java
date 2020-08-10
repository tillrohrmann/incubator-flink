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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.slotsbro.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link DeclarativeSlotPool}.
 */
public class DeclarativeSlotPoolTest extends TestLogger {

	private final JobID jobId = new JobID();

	private static final String jobMasterAddress = "localhost";

	private final ResourceProfile resourceProfile = ResourceProfile.newBuilder().build();

	private final ManualClock clock = new ManualClock();

	private final Time rpcTimeout = Time.seconds(10);

	private final Time slotAllocationTimeout = Time.seconds(10);

	private final Time idleSlotTimeout = Time.seconds(20);

	private final Time batchSlotTimeout = Time.seconds(20);

	private final ComponentMainThreadExecutor componentMainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();

	private final JobMasterId jobMasterId = JobMasterId.generate();

	/**
	 * Tests that requesting new allocated resources will increase the
	 * resource requirements.
	 */
	@Test
	public void testRequestNewAllocatedSlotIncreasesResourceRequirements() throws Exception {
		try (final DeclarativeSlotPool declarativeSlotPool = createAndStartDeclarativeSlotPool()) {
			final BlockingQueue<ResourceRequirements> receivedResourceRequirements = new ArrayBlockingQueue<>(2);
			final TestingResourceManagerGateway resourceManagerGateway = createRequirementRecordingResourceManagerGateway(receivedResourceRequirements);

			declarativeSlotPool.connectToResourceManager(resourceManagerGateway);

			// initial resource declaration
			receivedResourceRequirements.take();

			declarativeSlotPool.requestNewAllocatedSlot(new SlotRequestId(), resourceProfile, slotAllocationTimeout);

			final ResourceRequirements resourceRequirements = receivedResourceRequirements.take();

			assertThat(resourceRequirements.getJobId(), is(jobId));
			assertThat(resourceRequirements.getTargetAddress(), is(jobMasterAddress));
			assertThat(resourceRequirements.getResourceRequirements(), contains(ResourceRequirement.create(resourceProfile, 1)));
		}
	}

	private DeclarativeSlotPool createAndStartDeclarativeSlotPool() throws Exception {
		final DeclarativeSlotPool declarativeSlotPool = new DeclarativeSlotPool(jobId, clock, rpcTimeout, idleSlotTimeout, batchSlotTimeout);
		declarativeSlotPool.start(jobMasterId, jobMasterAddress, componentMainThreadExecutor);
		return declarativeSlotPool;
	}

	/**
	 * Tests that releasing a pending slot will decrease the resource requirements.
	 */
	@Test
	public void testReleasePendingSlotWillDecreaseResourceRequirements() throws Exception {
		try (final DeclarativeSlotPool declarativeSlotPool = createAndStartDeclarativeSlotPool()) {
			final BlockingQueue<ResourceRequirements> receivedResourceRequirements = new ArrayBlockingQueue<>(2);
			final TestingResourceManagerGateway resourceManagerGateway = createRequirementRecordingResourceManagerGateway(receivedResourceRequirements);
			declarativeSlotPool.connectToResourceManager(resourceManagerGateway);

			// initial resource declaration
			receivedResourceRequirements.take();

			final SlotRequestId slotRequestId = new SlotRequestId();
			declarativeSlotPool.requestNewAllocatedSlot(slotRequestId, resourceProfile, slotAllocationTimeout);

			receivedResourceRequirements.take();

			declarativeSlotPool.releaseSlot(slotRequestId, null);

			final ResourceRequirements resourceRequirements = receivedResourceRequirements.take();

			assertThat(resourceRequirements.getResourceRequirements(), is(empty()));
		}
	}

	private TestingResourceManagerGateway createRequirementRecordingResourceManagerGateway(BlockingQueue<? super ResourceRequirements> receivedResourceRequirements) {
		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		resourceManagerGateway.setDeclareRequiredResourcesFunction(
			(jobMasterId, resourceRequirements) -> {
				receivedResourceRequirements.offer(resourceRequirements);
				return CompletableFuture.completedFuture(Acknowledge.get());
			}
		);
		return resourceManagerGateway;
	}

	/**
	 * Tests that offered slots are matched with pending slot requests.
	 */
	@Test
	public void testMatchingBetweenPendingSlotRequestsAndSlotOffers() throws Exception {
		try (final DeclarativeSlotPool declarativeSlotPool = createAndStartDeclarativeSlotPool()) {
			final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

			declarativeSlotPool.connectToResourceManager(resourceManagerGateway);

			final SlotRequestId slotRequestId1 = new SlotRequestId();
			final ResourceProfile resourceProfile1 = ResourceProfile.newBuilder()
				.setCpuCores(2.0)
				.setManagedMemoryMB(1024)
				.build();

			final SlotRequestId slotRequestId2 = new SlotRequestId();
			final ResourceProfile resourceProfile2 = ResourceProfile.newBuilder()
				.setCpuCores(1.0)
				.setManagedMemoryMB(2048)
				.build();

			final CompletableFuture<PhysicalSlot> slotFuture1 = declarativeSlotPool.requestNewAllocatedSlot(slotRequestId1, resourceProfile1, slotAllocationTimeout);
			final CompletableFuture<PhysicalSlot> slotFuture2 = declarativeSlotPool.requestNewAllocatedSlot(slotRequestId2, resourceProfile2, slotAllocationTimeout);

			final LocalTaskManagerLocation localTaskManagerLocation = new LocalTaskManagerLocation();

			final AllocationID allocationId1 = new AllocationID();
			final SlotOffer slotOffer1 = new SlotOffer(allocationId1, 0, resourceProfile2);
			final AllocationID allocationId2 = new AllocationID();
			final SlotOffer slotOffer2 = new SlotOffer(allocationId2, 1, resourceProfile1);
			final Collection<SlotOffer> slotOffers = Arrays.asList(slotOffer1, slotOffer2);

			final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
			final TaskManagerGateway taskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, jobMasterId);

			declarativeSlotPool.registerTaskManager(localTaskManagerLocation.getResourceID());
			declarativeSlotPool.offerSlots(localTaskManagerLocation, taskManagerGateway, slotOffers);

			assertThat(slotFuture1.get().getAllocationId(), is(allocationId2));
			assertThat(slotFuture2.get().getAllocationId(), is(allocationId1));
		}
	}

	/**
	 * Tests that freeing a slot will also decrease the resource requirements.
	 */
	@Test
	public void testFreeSlotWillDecreaseResourceRequirements() throws Exception {
		try (final DeclarativeSlotPool declarativeSlotPool = createAndStartDeclarativeSlotPool()) {
			final BlockingQueue<ResourceRequirements> receivedResourceRequirements = new ArrayBlockingQueue<>(2);
			final TestingResourceManagerGateway resourceManagerGateway = createRequirementRecordingResourceManagerGateway(receivedResourceRequirements);

			final SlotRequestId slotRequestId = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> slotFuture = declarativeSlotPool.requestNewAllocatedSlot(slotRequestId, resourceProfile, slotAllocationTimeout);

			declarativeSlotPool.connectToResourceManager(resourceManagerGateway);

			// first requirement increase can be ignored
			receivedResourceRequirements.take();

			final CompletableFuture<AllocationID> freedSlotFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setFreeSlotFunction((allocationID, throwable) -> {
					freedSlotFuture.complete(allocationID);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.createTestingTaskExecutorGateway();
			final TaskManagerGateway taskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, jobMasterId);
			final LocalTaskManagerLocation localTaskManagerLocation = new LocalTaskManagerLocation();

			final AllocationID allocationId = new AllocationID();
			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, resourceProfile);
			final Collection<SlotOffer> slotOffers = Arrays.asList(slotOffer);

			declarativeSlotPool.registerTaskManager(localTaskManagerLocation.getResourceID());

			declarativeSlotPool.offerSlots(localTaskManagerLocation, taskManagerGateway, slotOffers);

			// check that the requested slot is fulfilled
			slotFuture.get();

			declarativeSlotPool.releaseSlot(slotRequestId, null);

			clock.advanceTime(idleSlotTimeout.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
			declarativeSlotPool.checkIdleSlot();

			final ResourceRequirements resourceRequirements = receivedResourceRequirements.take();
			assertThat(resourceRequirements.getResourceRequirements(), is(empty()));
			assertThat(freedSlotFuture.get(), is(allocationId));
		}
	}
}
