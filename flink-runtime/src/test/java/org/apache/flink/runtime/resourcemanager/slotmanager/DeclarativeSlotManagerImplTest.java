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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.slotsbro.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link DeclarativeSlotManagerImpl}.
 */
public class DeclarativeSlotManagerImplTest extends TestLogger {

	private static final FlinkException TEST_EXCEPTION = new FlinkException("Test exception");

	private static final WorkerResourceSpec WORKER_RESOURCE_SPEC = new WorkerResourceSpec.Builder()
		.setCpuCores(100.0)
		.setTaskHeapMemoryMB(10000)
		.setTaskOffHeapMemoryMB(10000)
		.setNetworkMemoryMB(10000)
		.setManagedMemoryMB(10000)
		.build();

	/**
	 * Tests that we can register task manager and their slots at the slot manager.
	 */
	@Test
	public void testTaskManagerRegistration() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		final ResourceID resourceId = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			assertTrue("The number registered slots does not equal the expected number.", 2 == slotManager.getNumberRegisteredSlots());

			assertNotNull(slotManager.getSlot(slotId1));
			assertNotNull(slotManager.getSlot(slotId2));
		}
	}

	/**
	 * Tests that un-registration of task managers will free and remove all registered slots.
	 */
	@Test
	public void testTaskManagerUnregistration() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();
		final JobID jobId = new JobID();

		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setRequestSlotFunction(tuple6 -> {
				assertThat(tuple6.f5, is(equalTo(resourceManagerId)));
				return new CompletableFuture<>();
			})
			.createTestingTaskExecutorGateway();

		final ResourceID resourceId = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final AllocationID allocationId1 = new AllocationID();
		final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile, jobId, allocationId1);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			assertEquals("The number registered slots does not equal the expected number.", 2, slotManager.getNumberRegisteredSlots());

			DeclarativeTaskManagerSlot slot1 = slotManager.getSlot(slotId1);
			DeclarativeTaskManagerSlot slot2 = slotManager.getSlot(slotId2);

			assertSame(DeclarativeTaskManagerSlot.State.ALLOCATED, slot1.getState());
			assertSame(DeclarativeTaskManagerSlot.State.FREE, slot2.getState());

			slotManager.processResourceRequirements(resourceRequirements);

			assertSame(DeclarativeTaskManagerSlot.State.PENDING, slot2.getState());

			slotManager.unregisterTaskManager(taskManagerConnection.getInstanceID(), TEST_EXCEPTION);

			assertEquals(0, slotManager.getNumberRegisteredSlots());
		}
	}

	/**
	 * Tests that a slot request with no free slots will trigger the resource allocation.
	 */
	@Test
	public void testSlotRequestWithoutFreeSlots() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

		final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

		CompletableFuture<WorkerResourceSpec> allocateResourceFuture = new CompletableFuture<>();
		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setAllocateResourceConsumer(allocateResourceFuture::complete)
			.build();

		try (SlotManager slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {

			slotManager.processResourceRequirements(resourceRequirements);

			allocateResourceFuture.get();
		}
	}

	/**
	 * Tests that resources continue to be considered missing if we cannot allocate more resources.
	 */
	@Test
	public void testResourceDeclarationWithResourceAllocationFailure() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setAllocateResourceFunction(value -> false)
			.build();

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {

			slotManager.processResourceRequirements(resourceRequirements);

			assertThat(slotManager.getNumResources(resourceRequirements.getJobId(), JobResourceState.MISSING), is(1));
		}
	}

	/**
	 * Tests that a slot request which can be fulfilled will trigger a slot allocation.
	 */
	@Test
	public void testSlotRequestWithFreeSlot() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId = new SlotID(resourceID, 0);
		final String targetAddress = "localhost";
		final AllocationID allocationId = DeclarativeSlotManagerImpl.DUMMY_ALLOCATION_ID;
		final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			final CompletableFuture<Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>> requestFuture = new CompletableFuture<>();
			// accept an incoming slot request
			final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setRequestSlotFunction(tuple6 -> {
					requestFuture.complete(Tuple6.of(tuple6.f0, tuple6.f1, tuple6.f2, tuple6.f3, tuple6.f4, tuple6.f5));
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.createTestingTaskExecutorGateway();

			final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);

			final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
			final SlotReport slotReport = new SlotReport(slotStatus);

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			ResourceRequirements requirements = new ResourceRequirements(
				jobId,
				targetAddress,
				Collections.singleton(new ResourceRequirement(resourceProfile, 1)));
			slotManager.processResourceRequirements(requirements);

			assertThat(requestFuture.get(), is(equalTo(Tuple6.of(slotId, jobId, requestFuture.get().f2, resourceProfile, targetAddress, resourceManagerId))));

			DeclarativeTaskManagerSlot slot = slotManager.getSlot(slotId);

			assertEquals("The slot has not been allocated to the expected allocation id.", jobId, slot.getJobId());
		}
	}

	/**
	 * Checks that reducing resource requirements while a slot allocation is in-progress results in the slot being freed
	 * one the allocation completes.
	 */
	@Test
	@Ignore
	public void testResourceRequirementReductionDuringAllocation() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();
		final ResourceID resourceID = ResourceID.generate();
		final SlotID slotId = new SlotID(resourceID, 0);

		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

		final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
		final SlotReport slotReport = new SlotReport(slotStatus);

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			DeclarativeTaskManagerSlot slot = slotManager.getSlot(slotId);

			final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();
			slotManager.processResourceRequirements(resourceRequirements);

			assertSame(DeclarativeTaskManagerSlot.State.PENDING, slot.getState());

			slotManager.processResourceRequirements(new ResourceRequirements(resourceRequirements.getJobId(), "foobar", Collections.emptyList()));

			slot = slotManager.getSlot(slotId);

			assertSame(DeclarativeTaskManagerSlot.State.FREE, slot.getState());
		}
	}

	/**
	 * Tests that pending slot requests are tried to be fulfilled upon new slot registrations.
	 */
	@Test
	public void testFulfillingPendingSlotRequest() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId = new SlotID(resourceID, 0);
		final String targetAddress = "localhost";
		final AllocationID allocationId = DeclarativeSlotManagerImpl.DUMMY_ALLOCATION_ID;
		final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

		final AtomicInteger numberAllocateResourceCalls = new AtomicInteger(0);
		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setAllocateResourceConsumer(ignored -> numberAllocateResourceCalls.incrementAndGet())
			.build();

		final CompletableFuture<Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>> requestFuture = new CompletableFuture<>();
		// accept an incoming slot request
		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setRequestSlotFunction(tuple6 -> {
				requestFuture.complete(Tuple6.of(tuple6.f0, tuple6.f1, tuple6.f2, tuple6.f3, tuple6.f4, tuple6.f5));
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.createTestingTaskExecutorGateway();

		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);

		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {

			final ResourceRequirements resourceRequirements = new ResourceRequirements(
				jobId,
				targetAddress,
				Collections.singleton(new ResourceRequirement(resourceProfile, 1)));
			slotManager.processResourceRequirements(resourceRequirements);

			assertThat(numberAllocateResourceCalls.get(), is(1));

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			assertThat(requestFuture.get(), is(equalTo(Tuple6.of(slotId, jobId, requestFuture.get().f2, resourceProfile, targetAddress, resourceManagerId))));

			DeclarativeTaskManagerSlot slot = slotManager.getSlot(slotId);

			assertEquals("The slot has not been allocated to the expected job id.", jobId, slot.getJobId());
		}
	}

	/**
	 * Tests that freeing a slot will correctly reset the slot and mark it as a free slot.
	 */
	@Test
	public void testFreeSlot() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final JobID jobId = new JobID();

		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
		final ResourceID resourceID = taskExecutorConnection.getResourceID();
		final SlotID slotId = new SlotID(resourceID, 0);
		final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile, jobId, new AllocationID());
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			DeclarativeTaskManagerSlot slot = slotManager.getSlot(slotId);

			assertSame(DeclarativeTaskManagerSlot.State.ALLOCATED, slot.getState());

			slotManager.freeSlot(slotId, new AllocationID());

			assertSame(DeclarativeTaskManagerSlot.State.FREE, slot.getState());

			assertEquals(1, slotManager.getNumberFreeSlots());
		}
	}

	/**
	 * Tests that duplicate resource requirement declaration do not result in additional slots being allocated after a
	 * pending slot request has been fulfilled but not yet freed.
	 */
	@Test
	public void testDuplicateResourceRequirementDeclarationAfterSuccessfulAllocation() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final AtomicInteger allocateResourceCalls = new AtomicInteger(0);
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setAllocateResourceConsumer(ignored -> allocateResourceCalls.incrementAndGet())
			.build();
		ResourceRequirements requirements = createResourceRequirementsForSingleSlot();

		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

		final ResourceID resourceID = ResourceID.generate();

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);

		final SlotID slotId = new SlotID(resourceID, 0);
		final SlotStatus slotStatus = new SlotStatus(slotId, ResourceProfile.ANY);
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			slotManager.processResourceRequirements(requirements);

			DeclarativeTaskManagerSlot slot = slotManager.getSlot(slotId);

			assertThat(slot.getState(), is(DeclarativeTaskManagerSlot.State.ALLOCATED));

			slotManager.processResourceRequirements(requirements);
		}

		// check that we have only called the resource allocation only for the first slot request,
		// since the second request is a duplicate
		assertThat(allocateResourceCalls.get(), is(0));
	}

	/**
	 * Tests that a slot allocated for one job can be allocated for another job after being freed.
	 */
	@Test
	public void testSlotCanBeAllocatedForDifferentJobAfterFree() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final AllocationID allocationId = new AllocationID();
		final ResourceRequirements resourceRequirements1 = createResourceRequirementsForSingleSlot();
		final ResourceRequirements resourceRequirements2 = createResourceRequirementsForSingleSlot();

		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

		final ResourceID resourceID = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);

		final SlotID slotId = new SlotID(resourceID, 0);
		final SlotStatus slotStatus = new SlotStatus(slotId, ResourceProfile.fromResources(2.0, 2));
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, new TestingResourceActionsBuilder().build())) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			slotManager.processResourceRequirements(resourceRequirements1);

			DeclarativeTaskManagerSlot slot = slotManager.getSlot(slotId);

			assertEquals("The slot has not been allocated to the expected job id.", resourceRequirements1.getJobId(), slot.getJobId());

			// clear resource requirements, so that the slot isn't immediately re-assigned
			slotManager.processResourceRequirements(new ResourceRequirements(resourceRequirements1.getJobId(), resourceRequirements1.getTargetAddress(), Collections.emptyList()));
			slotManager.freeSlot(slotId, allocationId);

			// check that the slot has been freed
			assertSame(DeclarativeTaskManagerSlot.State.FREE, slot.getState());

			slotManager.processResourceRequirements(resourceRequirements2);

			assertEquals("The slot has not been allocated to the expected job id.", resourceRequirements2.getJobId(), slot.getJobId());
		}
	}

	/**
	 * Tests that the slot manager ignores slot reports of unknown origin (not registered
	 * task managers).
	 */
	@Test
	public void testReceivingUnknownSlotReport() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final InstanceID unknownInstanceID = new InstanceID();
		final SlotID unknownSlotId = new SlotID(ResourceID.generate(), 0);
		final ResourceProfile unknownResourceProfile = ResourceProfile.fromResources(1.0, 1);
		final SlotStatus unknownSlotStatus = new SlotStatus(unknownSlotId, unknownResourceProfile);
		final SlotReport unknownSlotReport = new SlotReport(unknownSlotStatus);

		try (SlotManager slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			// check that we don't have any slots registered
			assertTrue(0 == slotManager.getNumberRegisteredSlots());

			// this should not update anything since the instance id is not known to the slot manager
			assertFalse(slotManager.reportSlotStatus(unknownInstanceID, unknownSlotReport));

			assertTrue(0 == slotManager.getNumberRegisteredSlots());
		}
	}

	/**
	 * Tests that slots are updated with respect to the latest incoming slot report. This means that
	 * slots for which a report was received are updated accordingly.
	 */
	@Test
	public void testUpdateSlotReport() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final JobID jobId = new JobID();

		final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection();
		final ResourceID resourceId = taskManagerConnection.getResourceID();

		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);

		final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);

		final SlotStatus newSlotStatus2 = new SlotStatus(slotId2, resourceProfile, jobId, new AllocationID());

		final SlotReport slotReport1 = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));
		final SlotReport slotReport2 = new SlotReport(Arrays.asList(newSlotStatus2, slotStatus1));

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			// check that we don't have any slots registered
			assertEquals(0, slotManager.getNumberRegisteredSlots());

			slotManager.registerTaskManager(taskManagerConnection, slotReport1);

			DeclarativeTaskManagerSlot slot1 = slotManager.getSlot(slotId1);
			DeclarativeTaskManagerSlot slot2 = slotManager.getSlot(slotId2);

			assertEquals(2, slotManager.getNumberRegisteredSlots());

			assertSame(DeclarativeTaskManagerSlot.State.FREE, slot1.getState());
			assertSame(DeclarativeTaskManagerSlot.State.FREE, slot2.getState());

			assertTrue(slotManager.reportSlotStatus(taskManagerConnection.getInstanceID(), slotReport2));

			assertEquals(2, slotManager.getNumberRegisteredSlots());

			assertNotNull(slotManager.getSlot(slotId1));
			assertNotNull(slotManager.getSlot(slotId2));

			// slotId2 should have been allocated for jobiD
			assertEquals(jobId, slotManager.getSlot(slotId2).getJobId());
		}
	}

	/**
	 * Tests that slot allocation time out after the specified request timeout. If a slot allocation
	 * times out, then a pending resource is declared missing.
	 */
	@Test
	public void testSlotAllocationTimeout() throws Exception {
		final long allocationTimeout = 50L;

		final JobID jobId = new JobID();

		final CompletableFuture<Void> secondSlotRequestFuture = new CompletableFuture<>();
		final AtomicInteger slotRequestsCount = new AtomicInteger();
		final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection(new TestingTaskExecutorGatewayBuilder()
			.setRequestSlotFunction(ignored -> {
				if (slotRequestsCount.getAndAdd(1) == 1) {
					secondSlotRequestFuture.complete(null);
				}
				return new CompletableFuture<>();
			})
			.createTestingTaskExecutorGateway());
		final SlotReport slotReport = createSlotReport(taskManagerConnection.getResourceID(), 2);

		final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

		try (DeclarativeSlotManagerImpl slotManager = createDeclarativeSlotManagerBuilder()
			.setSlotRequestTimeout(Time.milliseconds(allocationTimeout))
			.build()) {

			slotManager.start(ResourceManagerId.generate(), mainThreadExecutor, new TestingResourceActionsBuilder().build());

			CompletableFuture.runAsync(() -> slotManager.registerTaskManager(taskManagerConnection, slotReport), mainThreadExecutor)
				.thenRun(() -> slotManager.processResourceRequirements(createResourceRequirementsForSingleSlot(jobId)))
				.get(5, TimeUnit.SECONDS);

			// a second request is only sent if the first request timed out
			secondSlotRequestFuture.get();
		}
	}

	/**
	 * Tests that a slot allocation is retried if it times out on the task manager side.
	 */
	@Test
	public void testTaskManagerSlotAllocationTimeoutHandling() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final JobID jobId = new JobID();
		final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot(jobId);
		final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();
		final CompletableFuture<Acknowledge> slotRequestFuture2 = new CompletableFuture<>();
		final Iterator<CompletableFuture<Acknowledge>> slotRequestFutureIterator = Arrays.asList(slotRequestFuture1, slotRequestFuture2).iterator();
		final ArrayBlockingQueue<SlotID> slotIds = new ArrayBlockingQueue<>(2);

		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setRequestSlotFunction(FunctionUtils.uncheckedFunction(
				requestSlotParameters -> {
					slotIds.put(requestSlotParameters.f0);
					return slotRequestFutureIterator.next();
				}))
			.createTestingTaskExecutorGateway();

		final ResourceID resourceId = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, ResourceProfile.ANY);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, ResourceProfile.ANY);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		try (DeclarativeSlotManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {

			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			slotManager.processResourceRequirements(resourceRequirements);

			final SlotID firstSlotId = slotIds.take();
			assertThat(slotIds, is(empty()));

			DeclarativeTaskManagerSlot failedSlot = slotManager.getSlot(firstSlotId);

			// let the first attempt fail --> this should trigger a second attempt
			slotRequestFuture1.completeExceptionally(new SlotAllocationException("Test exception."));

			assertThat(slotManager.getNumResources(jobId, JobResourceState.ACQUIRED), is(1));

			// the second attempt succeeds
			slotRequestFuture2.complete(Acknowledge.get());

			final SlotID secondSlotId = slotIds.take();
			assertThat(slotIds, is(empty()));

			DeclarativeTaskManagerSlot slot = slotManager.getSlot(secondSlotId);

			assertTrue(slot.getState() == DeclarativeTaskManagerSlot.State.ALLOCATED);
			assertEquals(jobId, slot.getJobId());

			if (!failedSlot.getSlotId().equals(slot.getSlotId())) {
				assertTrue(failedSlot.getState() == DeclarativeTaskManagerSlot.State.FREE);
			}
		}
	}

	/**
	 * Tests that pending resource are rejected if a slot report with a different job ID is received.
	 */
	@Test
	public void testSlotReportWhileActiveSlotAllocation() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final JobID jobId = new JobID();
		final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot(jobId);
		final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();

		final Iterator<CompletableFuture<Acknowledge>> slotRequestFutureIterator = Arrays.asList(
			slotRequestFuture1,
			CompletableFuture.completedFuture(Acknowledge.get())).iterator();
		final ArrayBlockingQueue<SlotID> slotIds = new ArrayBlockingQueue<>(2);

		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setRequestSlotFunction(FunctionUtils.uncheckedFunction(
				requestSlotParameters -> {
					slotIds.put(requestSlotParameters.f0);
					return slotRequestFutureIterator.next();
				}))
			.createTestingTaskExecutorGateway();

		final ResourceID resourceId = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, ResourceProfile.ANY);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, ResourceProfile.ANY);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		final ScheduledExecutor mainThreadExecutor = TestingUtils.defaultScheduledExecutor();

		final DeclarativeSlotManagerImpl slotManager = createDeclarativeSlotManagerBuilder()
			.setScheduledExecutor(mainThreadExecutor)
			.build();

		try {

			slotManager.start(resourceManagerId, mainThreadExecutor, resourceManagerActions);

			CompletableFuture
				.runAsync(() -> slotManager.registerTaskManager(taskManagerConnection, slotReport), mainThreadExecutor)
				.thenRun(() -> slotManager.processResourceRequirements(resourceRequirements))
				.get(5, TimeUnit.SECONDS);

			final SlotID requestedSlotId = slotIds.take();
			final SlotID freeSlotId = requestedSlotId.equals(slotId1) ? slotId2 : slotId1;

			final SlotStatus newSlotStatus1 = new SlotStatus(requestedSlotId, ResourceProfile.ANY, new JobID(), new AllocationID());
			final SlotStatus newSlotStatus2 = new SlotStatus(freeSlotId, ResourceProfile.ANY);
			final SlotReport newSlotReport = new SlotReport(Arrays.asList(newSlotStatus1, newSlotStatus2));

			CompletableFuture<Boolean> reportSlotStatusFuture = CompletableFuture.supplyAsync(
				// this should update the slot with the pending slot request triggering the reassignment of it
				() -> slotManager.reportSlotStatus(taskManagerConnection.getInstanceID(), newSlotReport),
				mainThreadExecutor);

			assertTrue(reportSlotStatusFuture.get());

			final SlotID requestedSlotId2 = slotIds.take();

			assertEquals(freeSlotId, requestedSlotId2);
		} finally {
			CompletableFuture.runAsync(
				ThrowingRunnable.unchecked(slotManager::close),
				mainThreadExecutor);
		}
	}

	/**
	 * Tests that formerly used task managers can again timeout after all of their slots have
	 * been freed.
	 */
	@Test
	public void testTimeoutForUnusedTaskManager() throws Exception {
		final long taskManagerTimeout = 50L;

		final CompletableFuture<InstanceID> releasedResourceFuture = new CompletableFuture<>();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setReleaseResourceConsumer((instanceID, e) -> releasedResourceFuture.complete(instanceID))
			.build();
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

		final ResourceID resourceId = ResourceID.generate();

		final JobID jobId = new JobID();

		final CompletableFuture<SlotID> requestedSlotFuture = new CompletableFuture<>();
		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setRequestSlotFunction(tuple6 -> {
				requestedSlotFuture.complete(tuple6.f0);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.createTestingTaskExecutorGateway();

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, ResourceProfile.ANY);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, ResourceProfile.ANY);
		final SlotReport initialSlotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

		try (final DeclarativeSlotManagerImpl slotManager = createDeclarativeSlotManagerBuilder()
			.setTaskManagerTimeout(Time.of(taskManagerTimeout, TimeUnit.MILLISECONDS))
			.setRedundantTaskManagerNum(0)
			.build()) {

			slotManager.start(resourceManagerId, mainThreadExecutor, resourceManagerActions);

			CompletableFuture.runAsync(
				() -> slotManager.processResourceRequirements(createResourceRequirementsForSingleSlot(jobId)), mainThreadExecutor)
				.thenRun(() -> slotManager.registerTaskManager(taskManagerConnection, initialSlotReport))
				.get();

			final SlotID slotId = requestedSlotFuture.get();

			CompletableFuture<Boolean> idleFuture = CompletableFuture.supplyAsync(
				() -> slotManager.isTaskManagerIdle(taskManagerConnection.getInstanceID()),
				mainThreadExecutor);

			// check that the TaskManager is not idle
			assertFalse(idleFuture.get());

			CompletableFuture<DeclarativeTaskManagerSlot> slotFuture = CompletableFuture.supplyAsync(
				() -> slotManager.getSlot(slotId),
				mainThreadExecutor);

			DeclarativeTaskManagerSlot slot = slotFuture.get();

			assertSame(DeclarativeTaskManagerSlot.State.ALLOCATED, slot.getState());
			assertEquals(jobId, slot.getJobId());

			CompletableFuture<Boolean> idleFuture2 = CompletableFuture.runAsync(
				() -> slotManager.processResourceRequirements(createEmptyResourceRequirements(jobId)), mainThreadExecutor)
				.thenRun(() -> slotManager.freeSlot(slotId, DeclarativeSlotManagerImpl.DUMMY_ALLOCATION_ID))
				.thenApply((Object value) -> slotManager.isTaskManagerIdle(taskManagerConnection.getInstanceID()));

			assertTrue(idleFuture2.get());

			assertThat(releasedResourceFuture.get(), is(equalTo(taskManagerConnection.getInstanceID())));
		}
	}

	/**
	 * Tests that a task manager timeout does not remove the slots from the SlotManager.
	 * A timeout should only trigger the {@link ResourceActions#releaseResource(InstanceID, Exception)}
	 * callback. The receiver of the callback can then decide what to do with the TaskManager.
	 *
	 * <p>See FLINK-7793
	 */
	@Test
	public void testTaskManagerTimeoutDoesNotRemoveSlots() throws Exception {
		final Time taskManagerTimeout = Time.milliseconds(10L);
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID resourceID = ResourceID.generate();
		final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();
		final ResourceActions resourceActions = new TestingResourceActionsBuilder()
			.setReleaseResourceConsumer((instanceId, ignored) -> releaseResourceFuture.complete(instanceId))
			.build();
		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);
		final SlotStatus slotStatus = createEmptySlotStatus(new SlotID(resourceID, 0), ResourceProfile.fromResources(1.0, 1));
		final SlotReport initialSlotReport = new SlotReport(slotStatus);

		try (final SlotManager slotManager = createDeclarativeSlotManagerBuilder()
			.setTaskManagerTimeout(taskManagerTimeout)
			.setRedundantTaskManagerNum(0)
			.buildAndStartWithDirectExec(resourceManagerId, resourceActions)) {

			slotManager.registerTaskManager(taskExecutorConnection, initialSlotReport);

			assertEquals(1, slotManager.getNumberRegisteredSlots());

			// wait for the timeout call to happen
			assertThat(releaseResourceFuture.get(), is(taskExecutorConnection.getInstanceID()));

			assertEquals(1, slotManager.getNumberRegisteredSlots());

			slotManager.unregisterTaskManager(taskExecutorConnection.getInstanceID(), TEST_EXCEPTION);

			assertEquals(0, slotManager.getNumberRegisteredSlots());
		}
	}

	/**
	 * Tests that free slots which are reported as allocated won't be considered for fulfilling
	 * other pending slot requests.
	 *
	 * <p>See: FLINK-8505
	 */
	@Test
	public void testReportAllocatedSlot() throws Exception {
		final ResourceID taskManagerId = ResourceID.generate();
		final ResourceActions resourceActions = new TestingResourceActionsBuilder().build();
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(taskManagerId, taskExecutorGateway);

		try (final DeclarativeSlotManagerImpl slotManager = createSlotManager(ResourceManagerId.generate(), resourceActions)) {

			// initially report a single slot as free
			final SlotID slotId = new SlotID(taskManagerId, 0);
			final SlotStatus initialSlotStatus = new SlotStatus(
				slotId,
				ResourceProfile.ANY);
			final SlotReport initialSlotReport = new SlotReport(initialSlotStatus);

			slotManager.registerTaskManager(taskExecutorConnection, initialSlotReport);

			assertThat(slotManager.getNumberRegisteredSlots(), is(equalTo(1)));

			// Now report this slot as allocated
			final SlotStatus slotStatus = new SlotStatus(
				slotId,
				ResourceProfile.ANY,
				new JobID(),
				new AllocationID());
			final SlotReport slotReport = new SlotReport(
				slotStatus);

			slotManager.reportSlotStatus(
				taskExecutorConnection.getInstanceID(),
				slotReport);

			final JobID jobId = new JobID();
			// this resource requirement should not be fulfilled
			ResourceRequirements requirements = createResourceRequirementsForSingleSlot(jobId);

			slotManager.processResourceRequirements(requirements);

			assertThat(slotManager.getSlot(slotId).getJobId(), is(slotStatus.getJobID()));
			assertThat(slotManager.getNumResources(jobId, JobResourceState.MISSING), is(1));
		}
	}

	/**
	 * Tests that the SlotManager retries allocating a slot if the TaskExecutor#requestSlot call
	 * fails.
	 */
	@Test
	public void testSlotRequestFailure() throws Exception {
		try (final DeclarativeSlotManagerImpl slotManager = createSlotManager(ResourceManagerId.generate(),
			new TestingResourceActionsBuilder().build())) {

			ResourceRequirements requirements = createResourceRequirementsForSingleSlot();
			slotManager.processResourceRequirements(requirements);

			final BlockingQueue<Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>> requestSlotQueue = new ArrayBlockingQueue<>(1);
			final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue = new ArrayBlockingQueue<>(1);

			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setRequestSlotFunction(slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
					requestSlotQueue.offer(slotIDJobIDAllocationIDStringResourceManagerIdTuple6);
					try {
						return responseQueue.take();
					} catch (InterruptedException ignored) {
						return FutureUtils.completedExceptionally(new FlinkException("Response queue was interrupted."));
					}
				})
				.createTestingTaskExecutorGateway();

			final ResourceID taskExecutorResourceId = ResourceID.generate();
			final TaskExecutorConnection taskExecutionConnection = new TaskExecutorConnection(taskExecutorResourceId, testingTaskExecutorGateway);
			final SlotReport slotReport = new SlotReport(createEmptySlotStatus(new SlotID(taskExecutorResourceId, 0), ResourceProfile.ANY));

			final CompletableFuture<Acknowledge> firstManualSlotRequestResponse = new CompletableFuture<>();
			responseQueue.offer(firstManualSlotRequestResponse);

			slotManager.registerTaskManager(taskExecutionConnection, slotReport);

			final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId> firstRequest = requestSlotQueue.take();

			final CompletableFuture<Acknowledge> secondManualSlotRequestResponse = new CompletableFuture<>();
			responseQueue.offer(secondManualSlotRequestResponse);

			// fail first request
			firstManualSlotRequestResponse.completeExceptionally(new SlotAllocationException("Test exception"));

			final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId> secondRequest = requestSlotQueue.take();

			assertThat(secondRequest.f1, equalTo(firstRequest.f1));
			assertThat(secondRequest.f0, equalTo(firstRequest.f0));

			secondManualSlotRequestResponse.complete(Acknowledge.get());

			final DeclarativeTaskManagerSlot slot = slotManager.getSlot(secondRequest.f0);
			assertThat(slot.getState(), equalTo(DeclarativeTaskManagerSlot.State.ALLOCATED));
			assertThat(slot.getJobId(), equalTo(secondRequest.f1));
		}
	}

	/**
	 * Tests that pending request is removed if task executor reports a slot with the same job id.
	 */
	@Test
	public void testSlotRequestRemovedIfTMReportAllocation() throws Exception {
		try (final DeclarativeSlotManagerImpl slotManager = createSlotManager(ResourceManagerId.generate(),
				new TestingResourceActionsBuilder().build())) {

			final JobID jobID = new JobID();
			slotManager.processResourceRequirements(createResourceRequirementsForSingleSlot(jobID));

			final BlockingQueue<Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>> requestSlotQueue = new ArrayBlockingQueue<>(1);
			final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue = new ArrayBlockingQueue<>(1);

			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
					.setRequestSlotFunction(slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
						requestSlotQueue.offer(slotIDJobIDAllocationIDStringResourceManagerIdTuple6);
						try {
							return responseQueue.take();
						} catch (InterruptedException ignored) {
							return FutureUtils.completedExceptionally(new FlinkException("Response queue was interrupted."));
						}
					})
					.createTestingTaskExecutorGateway();

			final ResourceID taskExecutorResourceId = ResourceID.generate();
			final TaskExecutorConnection taskExecutionConnection = new TaskExecutorConnection(taskExecutorResourceId, testingTaskExecutorGateway);
			final SlotReport slotReport = new SlotReport(createEmptySlotStatus(new SlotID(taskExecutorResourceId, 0), ResourceProfile.ANY));

			final CompletableFuture<Acknowledge> firstManualSlotRequestResponse = new CompletableFuture<>();
			responseQueue.offer(firstManualSlotRequestResponse);

			slotManager.registerTaskManager(taskExecutionConnection, slotReport);

			final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId> firstRequest = requestSlotQueue.take();

			final CompletableFuture<Acknowledge> secondManualSlotRequestResponse = new CompletableFuture<>();
			responseQueue.offer(secondManualSlotRequestResponse);

			// fail first request
			firstManualSlotRequestResponse.completeExceptionally(new TimeoutException("Test exception to fail first allocation"));

			final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId> secondRequest = requestSlotQueue.take();

			// fail second request
			secondManualSlotRequestResponse.completeExceptionally(new SlotOccupiedException("Test exception", new AllocationID(), jobID));

			assertThat(firstRequest.f1, equalTo(jobID));
			assertThat(secondRequest.f1, equalTo(jobID));
			assertThat(secondRequest.f0, equalTo(firstRequest.f0));

			final DeclarativeTaskManagerSlot slot = slotManager.getSlot(secondRequest.f0);
			assertThat(slot.getState(), equalTo(DeclarativeTaskManagerSlot.State.ALLOCATED));
			assertThat(slot.getJobId(), equalTo(firstRequest.f1));

			assertThat(slotManager.getNumberRegisteredSlots(), is(1));
			assertThat(slotManager.getNumResources(jobID, JobResourceState.ACQUIRED), is(1));
		}
	}

	/**
	 * Tests notify the job manager of the allocations when the task manager is failed/killed.
	 */
	@Test
	public void testNotifyFailedAllocationWhenTaskManagerTerminated() throws Exception {

		try (final DeclarativeSlotManagerImpl slotManager = createSlotManager(
			ResourceManagerId.generate(),
			new TestingResourceActionsBuilder().build())) {

			// register slot request for job1.
			JobID jobId1 = new JobID();
			slotManager.processResourceRequirements(createResourceRequirements(jobId1, 2));

			// create task-manager-1 with 2 slots.
			final ResourceID taskExecutorResourceId1 = ResourceID.generate();
			final TestingTaskExecutorGateway testingTaskExecutorGateway1 = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
			final TaskExecutorConnection taskExecutionConnection1 = new TaskExecutorConnection(taskExecutorResourceId1, testingTaskExecutorGateway1);
			final SlotReport slotReport1 = createSlotReport(taskExecutorResourceId1, 2);

			// register the task-manager-1 to the slot manager, this will trigger the slot allocation for job1.
			slotManager.registerTaskManager(taskExecutionConnection1, slotReport1);

			// register slot request for job2.
			JobID jobId2 = new JobID();
			slotManager.processResourceRequirements(createResourceRequirements(jobId2, 2));

			// register slot request for job3.
			JobID jobId3 = new JobID();
			slotManager.processResourceRequirements(createResourceRequirements(jobId3, 1));

			// create task-manager-2 with 3 slots.
			final ResourceID taskExecutorResourceId2 = ResourceID.generate();
			final TestingTaskExecutorGateway testingTaskExecutorGateway2 = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
			final TaskExecutorConnection taskExecutionConnection2 = new TaskExecutorConnection(taskExecutorResourceId2, testingTaskExecutorGateway2);
			final SlotReport slotReport2 = createSlotReport(taskExecutorResourceId2, 3);

			// register the task-manager-2 to the slot manager, this will trigger the slot allocation for job2 and job3.
			slotManager.registerTaskManager(taskExecutionConnection2, slotReport2);

			// validate for job1.
			slotManager.unregisterTaskManager(taskExecutionConnection1.getInstanceID(), TEST_EXCEPTION);

			assertThat(slotManager.getNumResources(jobId1, JobResourceState.MISSING), is(2));

			// validate the result for job2 and job3.
			slotManager.unregisterTaskManager(taskExecutionConnection2.getInstanceID(), TEST_EXCEPTION);

			assertThat(slotManager.getNumResources(jobId2, JobResourceState.MISSING), is(2));
			assertThat(slotManager.getNumResources(jobId3, JobResourceState.MISSING), is(1));
		}
	}

	@Nonnull
	private SlotReport createSlotReport(ResourceID taskExecutorResourceId, int numberSlots) {
		return createSlotReport(taskExecutorResourceId, numberSlots, ResourceProfile.ANY, DeclarativeSlotManagerImplTest::createEmptySlotStatus);
	}

	@Nonnull
	private SlotReport createSlotReport(
			ResourceID taskExecutorResourceId,
			int numberSlots,
			ResourceProfile resourceProfile,
			BiFunction<SlotID, ResourceProfile, SlotStatus> slotStatusFactory) {
		final Set<SlotStatus> slotStatusSet = new HashSet<>(numberSlots);
		for (int i = 0; i < numberSlots; i++) {
			slotStatusSet.add(slotStatusFactory.apply(new SlotID(taskExecutorResourceId, i), resourceProfile));
		}

		return new SlotReport(slotStatusSet);
	}

	private static SlotStatus createEmptySlotStatus(SlotID slotId, ResourceProfile resourceProfile) {
		return new SlotStatus(slotId, resourceProfile);
	}

	@Nonnull
	private SlotRequest createSlotRequest(JobID jobId) {
		return createSlotRequest(jobId, ResourceProfile.UNKNOWN);
	}

	@Nonnull
	private SlotRequest createSlotRequest(JobID jobId, ResourceProfile resourceProfile) {
		return new SlotRequest(jobId, new AllocationID(), resourceProfile, "foobar1");
	}

	private DeclarativeSlotManagerImpl createSlotManager(ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions) {
		return createSlotManager(resourceManagerId, resourceManagerActions, 1);
	}

	private DeclarativeSlotManagerImpl createSlotManager(ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions, int numSlotsPerWorker) {
		DeclarativeSlotManagerImpl slotManager = createDeclarativeSlotManagerBuilder()
			.setNumSlotsPerWorker(numSlotsPerWorker)
			.setRedundantTaskManagerNum(0)
			.buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions);
		return slotManager;
	}

	private DeclarativeSlotManagerBuilder createDeclarativeSlotManagerBuilder() {
		return DeclarativeSlotManagerBuilder.newBuilder().setDefaultWorkerResourceSpec(WORKER_RESOURCE_SPEC);
	}

	/**
	 * Tests that we only request new resources/containers once we have assigned
	 * all pending task manager slots.
	 */
	@Test
	public void testRequestNewResources() throws Exception {
		final int numberSlots = 2;
		final AtomicInteger resourceRequests = new AtomicInteger(0);
		final TestingResourceActions testingResourceActions = new TestingResourceActionsBuilder()
			.setAllocateResourceFunction(
				ignored -> {
					resourceRequests.incrementAndGet();
					return true;
				})
			.build();

		try (final DeclarativeSlotManagerImpl slotManager = createSlotManager(
			ResourceManagerId.generate(),
			testingResourceActions,
			numberSlots)) {

			final JobID jobId = new JobID();

			// we currently do not take pending slots into account for matching resource requirements, so we
			// requests new resources for every missing resource every time we check the requirements

			slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));
			assertThat(resourceRequests.get(), is(1));

			slotManager.processResourceRequirements(createResourceRequirements(jobId, 2));
			assertThat(resourceRequests.get(), is(3));

			slotManager.processResourceRequirements(createResourceRequirements(jobId, 3));
			assertThat(resourceRequests.get(), is(6));
		}
	}

	private TaskExecutorConnection createTaskExecutorConnection() {
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		return createTaskExecutorConnection(taskExecutorGateway);
	}

	private TaskExecutorConnection createTaskExecutorConnection(TaskExecutorGateway taskExecutorGateway) {
		return new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
	}

	/**
	 * Tests that a different slot can fulfill a pending slot request. If the
	 * pending slot request has a pending task manager slot assigned, it should
	 * be freed.
	 */
	@Test
	public void testRegistrationOfDifferentSlot() throws Exception {
		final int numberSlots = 1;
		final TestingResourceActions resourceActions = new TestingResourceActionsBuilder().build();

		try (final DeclarativeSlotManagerImpl slotManager = createSlotManager(ResourceManagerId.generate(), resourceActions, numberSlots)) {

			ResourceRequirements requirements = createResourceRequirementsForSingleSlot();
			slotManager.processResourceRequirements(requirements);

			assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));

			final TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
			final int numberOfferedSlots = 1;
			final ResourceProfile offeredSlotProfile = ResourceProfile.fromResources(2.0, 2);
			final SlotReport slotReport = createSlotReport(
				taskExecutorConnection.getResourceID(),
				numberOfferedSlots,
				offeredSlotProfile,
				DeclarativeSlotManagerImplTest::createEmptySlotStatus);

			slotManager.registerTaskManager(taskExecutorConnection, slotReport);

			assertThat(slotManager.getNumberRegisteredSlots(), is(numberOfferedSlots));
			assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));
		}
	}

	/**
	 * Tests that only free slots can fulfill/complete a pending task manager slot.
	 */
	@Test
	public void testOnlyFreeSlotsCanFulfillPendingTaskManagerSlot() throws Exception {
		final int numberSlots = 1;
		final TestingResourceActions resourceActions = new TestingResourceActionsBuilder().build();

		try (final DeclarativeSlotManagerImpl slotManager = createSlotManager(ResourceManagerId.generate(), resourceActions, numberSlots)) {
			final JobID jobId = new JobID();
			final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot(jobId);
			slotManager.processResourceRequirements(resourceRequirements);

			final TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
			final SlotID slotId = new SlotID(taskExecutorConnection.getResourceID(), 0);
			final SlotStatus slotStatus = new SlotStatus(slotId, ResourceProfile.ANY, jobId, new AllocationID());
			final SlotReport slotReport = new SlotReport(slotStatus);

			slotManager.registerTaskManager(taskExecutorConnection, slotReport);

			assertThat(slotManager.getNumberRegisteredSlots(), is(1));
			assertThat(slotManager.getNumberPendingTaskManagerSlots(), is(numberSlots));
		}
	}

	private SlotReport createSingleAllocatedSlotReport(ResourceID resourceID, JobID jobId) {
		return createSlotReport(
			resourceID,
			1,
			ResourceProfile.ANY,
			(slotId, resourceProfile) -> new SlotStatus(slotId, resourceProfile, jobId, new AllocationID()));
	}

	/**
	 * The spread out slot allocation strategy should spread out the allocated
	 * slots across all available TaskExecutors. See FLINK-12122.
	 */
	@Test
	public void testSpreadOutSlotAllocationStrategy() throws Exception {
		try (DeclarativeSlotManagerImpl slotManager = createDeclarativeSlotManagerBuilder()
			.setSlotMatchingStrategy(LeastUtilizationSlotMatchingStrategy.INSTANCE)
			.buildAndStartWithDirectExec(ResourceManagerId.generate(), new TestingResourceActionsBuilder().build())) {

			final List<CompletableFuture<JobID>> requestSlotFutures = new ArrayList<>();

			final int numberTaskExecutors = 5;

			// register n TaskExecutors with 2 slots each
			for (int i = 0; i < numberTaskExecutors; i++) {
				final CompletableFuture<JobID> requestSlotFuture = new CompletableFuture<>();
				requestSlotFutures.add(requestSlotFuture);
				registerTaskExecutorWithTwoSlots(slotManager, requestSlotFuture);
			}

			final JobID jobId = new JobID();

			final ResourceRequirements resourceRequirements = createResourceRequirements(jobId, numberTaskExecutors);
			slotManager.processResourceRequirements(resourceRequirements);

			// check that every TaskExecutor has received a slot request
			final Set<JobID> jobIds = new HashSet<>(FutureUtils.combineAll(requestSlotFutures).get(10L, TimeUnit.SECONDS));
			assertThat(jobIds, hasSize(1));
			assertThat(jobIds, containsInAnyOrder(jobId));
		}
	}

	/**
	 * Test that the slot manager respect the max limitation of the number of slots when allocate new resource.
	 */
	@Test
	public void testMaxSlotLimitAllocateResource() throws Exception {
		final int numberSlots = 1;
		final int maxSlotNum = 1;

		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

		final AtomicInteger resourceRequests = new AtomicInteger(0);
		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setAllocateResourceFunction(
				ignored -> {
					resourceRequests.incrementAndGet();
					return true;
				})
			.build();

		try (DeclarativeSlotManagerImpl slotManager = createDeclarativeSlotManagerBuilder()
			.setNumSlotsPerWorker(numberSlots)
			.setMaxSlotNum(maxSlotNum)
			.buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions)) {

			slotManager.processResourceRequirements(createResourceRequirementsForSingleSlot());
			assertThat(resourceRequests.get(), is(1));

			slotManager.processResourceRequirements(createResourceRequirementsForSingleSlot());
			assertThat(resourceRequests.get(), is(1));
		}
	}

	/**
	 * Test that the slot manager release resource when the number of slots exceed max limit when new TaskExecutor registered.
	 */
	@Test
	public void testMaxSlotLimitRegisterResource() throws Exception {
		final int numberSlots = 1;
		final int maxSlotNum = 1;
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

		final CompletableFuture<InstanceID> releasedResourceFuture = new CompletableFuture<>();
		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setReleaseResourceConsumer((instanceID, e) -> releasedResourceFuture.complete(instanceID))
			.build();

		final TaskExecutorGateway taskExecutorGateway1 = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		final TaskExecutorGateway taskExecutorGateway2 = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		final ResourceID resourceId1 = ResourceID.generate();
		final ResourceID resourceId2 = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection1 = new TaskExecutorConnection(resourceId1, taskExecutorGateway1);
		final TaskExecutorConnection taskManagerConnection2 = new TaskExecutorConnection(resourceId2, taskExecutorGateway2);

		final SlotID slotId1 = new SlotID(resourceId1, 0);
		final SlotID slotId2 = new SlotID(resourceId1, 0);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, ResourceProfile.UNKNOWN);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, ResourceProfile.UNKNOWN);
		final SlotReport slotReport1 = new SlotReport(Collections.singletonList(slotStatus1));
		final SlotReport slotReport2 = new SlotReport(Collections.singletonList(slotStatus2));

		try (DeclarativeSlotManagerImpl slotManager = createDeclarativeSlotManagerBuilder()
			.setNumSlotsPerWorker(numberSlots)
			.setMaxSlotNum(maxSlotNum)
			.setRedundantTaskManagerNum(0)
			.buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection1, slotReport1);
			slotManager.registerTaskManager(taskManagerConnection2, slotReport2);

			assertThat("The number registered slots does not equal the expected number.", slotManager.getNumberRegisteredSlots(), is(1));
			assertNotNull(slotManager.getSlot(slotId1));

			// The second registered task manager should be released.
			assertThat(releasedResourceFuture.get(), is(equalTo(taskManagerConnection2.getInstanceID())));
		}
	}

	private void registerTaskExecutorWithTwoSlots(DeclarativeSlotManagerImpl slotManager, CompletableFuture<JobID> firstRequestSlotFuture) {
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setRequestSlotFunction(slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
				firstRequestSlotFuture.complete(slotIDJobIDAllocationIDStringResourceManagerIdTuple6.f1);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.createTestingTaskExecutorGateway();
		final TaskExecutorConnection firstTaskExecutorConnection = createTaskExecutorConnection(taskExecutorGateway);
		final SlotReport firstSlotReport = createSlotReport(firstTaskExecutorConnection.getResourceID(), 2);
		slotManager.registerTaskManager(firstTaskExecutorConnection, firstSlotReport);
	}

	@Test
	public void testGenerateDefaultSlotProfile() {
		final int numSlots = 5;
		final ResourceProfile resourceProfile = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(1)
			.setTaskOffHeapMemoryMB(2)
			.setNetworkMemoryMB(3)
			.setManagedMemoryMB(4)
			.build();
		final WorkerResourceSpec workerResourceSpec = new WorkerResourceSpec.Builder()
			.setCpuCores(1.0 * numSlots)
			.setTaskHeapMemoryMB(1 * numSlots)
			.setTaskOffHeapMemoryMB(2 * numSlots)
			.setNetworkMemoryMB(3 * numSlots)
			.setManagedMemoryMB(4 * numSlots)
			.build();

		assertThat(
			SlotManagerImpl.generateDefaultSlotResourceProfile(workerResourceSpec, numSlots),
			is(resourceProfile));
	}

	@Test
	public void testNotificationAboutNotEnoughResources() throws Exception {
		final JobID jobId = new JobID();

		CompletableFuture<Tuple2<JobID, Collection<ResourceRequirement>>> notEnoughResourceNotification = new CompletableFuture<>();
		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setAllocateResourceFunction(ignored -> false)
			.setNotEnoughResourcesConsumer((jobId1, acquiredResources) -> notEnoughResourceNotification.complete(Tuple2.of(jobId1, acquiredResources)))
			.build();

		try (DeclarativeSlotManagerImpl slotManager = createDeclarativeSlotManagerBuilder()
			.buildAndStartWithDirectExec(ResourceManagerId.generate(), resourceManagerActions)) {

			final ResourceID taskExecutorResourceId = ResourceID.generate();
			final TaskExecutorConnection taskExecutionConnection = new TaskExecutorConnection(taskExecutorResourceId, new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());
			final SlotReport slotReport = createSlotReport(taskExecutorResourceId, 1);
			slotManager.registerTaskManager(taskExecutionConnection, slotReport);

			ResourceRequirements resourceRequirements = createResourceRequirements(jobId, 3);
			slotManager.processResourceRequirements(resourceRequirements);

			Tuple2<JobID, Collection<ResourceRequirement>> jobIDCollectionTuple2 = notEnoughResourceNotification.get();
			assertThat(jobIDCollectionTuple2.f0, is(jobId));
			assertThat(jobIDCollectionTuple2.f1, hasItem(new ResourceRequirement(ResourceProfile.ANY, 1)));
		}
	}

	private static ResourceRequirements createResourceRequirementsForSingleSlot() {
		return createResourceRequirementsForSingleSlot(new JobID());
	}

	private static ResourceRequirements createResourceRequirementsForSingleSlot(JobID jobId) {
		return createResourceRequirements(jobId, 1);
	}

	private static ResourceRequirements createResourceRequirements(JobID jobId, int numRequiredSlots) {
		return new ResourceRequirements(
			jobId,
			"foobar",
			Collections.singleton(new ResourceRequirement(ResourceProfile.UNKNOWN, numRequiredSlots)));
	}

	private static ResourceRequirements createEmptyResourceRequirements(JobID jobId) {
		return new ResourceRequirements(
			jobId,
			"foobar",
			Collections.emptyList());
	}
}
