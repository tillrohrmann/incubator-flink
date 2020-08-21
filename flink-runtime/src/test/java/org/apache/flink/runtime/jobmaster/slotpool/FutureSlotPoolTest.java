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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.SystemClock;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link FutureSlotPool}.
 */
public class FutureSlotPoolTest extends TestLogger {

	private final Time rpcTimeout = Time.seconds(20);
	private final Time idleSlotTimeout = Time.seconds(20);
	private final Time batchSlotTimeout = Time.seconds(20);
	private final JobID jobId = new JobID();
	private final JobMasterId jobMasterId = JobMasterId.generate();
	private final ComponentMainThreadExecutor mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();

	@Test
	public void testReleasingAllocatedSlot() throws Exception {
		final CompletableFuture<AllocationID> releaseSlotFuture = new CompletableFuture<>();
		final AllocationID expectedAllocationId = new AllocationID();
		final PhysicalSlot allocatedSlot = createAllocatedSlot(expectedAllocationId);

		final TestingDeclarativeSlotPoolNgBuilder builder = TestingDeclarativeSlotPoolNg
			.builder()
			.setAllocateFreeSlotFunction(allocationId -> {
				assertThat(allocationId, is(expectedAllocationId));
				return allocatedSlot;
			})
			.setReleaseSlotConsumer((allocationID, throwable, aLong) -> releaseSlotFuture.complete(allocationID));

		try (FutureSlotPool futureSlotPool = new FutureSlotPool(
				jobId,
				new TestingDeclarativeSlotPoolFactory(builder),
				SystemClock.getInstance(),
				rpcTimeout,
				idleSlotTimeout,
				batchSlotTimeout)) {
			futureSlotPool.start(jobMasterId, "localhost", mainThreadExecutor);

			final SlotRequestId slotRequestId = new SlotRequestId();

			futureSlotPool.allocateAvailableSlot(slotRequestId, expectedAllocationId);
			futureSlotPool.releaseSlot(slotRequestId, null);

			assertThat(releaseSlotFuture.join(), is(expectedAllocationId));
		}
	}

	@Test
	public void testNoConcurrentModificationWhenSuspendingAndReleasingSlot() throws Exception {
		try (FutureSlotPool futureSlotPool = new FutureSlotPool(
			jobId,
			new DefaultDeclarativeSlotPoolNgFactory(),
			SystemClock.getInstance(),
			rpcTimeout,
			idleSlotTimeout,
			batchSlotTimeout)) {

			futureSlotPool.start(jobMasterId, "localhost", mainThreadExecutor);

			final List<SlotRequestId> slotRequestIds = Arrays.asList(new SlotRequestId(), new SlotRequestId());

			final List<CompletableFuture<PhysicalSlot>> slotFutures = slotRequestIds.stream()
				.map(slotRequestId -> {
					final CompletableFuture<PhysicalSlot> slotFuture = futureSlotPool.requestNewAllocatedSlot(slotRequestId, ResourceProfile.UNKNOWN, rpcTimeout);
					slotFuture.whenComplete((physicalSlot, throwable) -> {
						if (throwable != null) {
							futureSlotPool.releaseSlot(slotRequestId, throwable);
						}
					});
					return slotFuture;
				})
				.collect(Collectors.toList());

			futureSlotPool.suspend();

			try {
				FutureUtils.waitForAll(slotFutures).get();
				fail("The slot futures should be completed exceptionally.");
			} catch (ExecutionException expected) {
				// expected
			}
		}
	}

	private PhysicalSlot createAllocatedSlot(AllocationID allocationID) {
		return new AllocatedSlot(
			allocationID,
			new LocalTaskManagerLocation(),
			0,
			ResourceProfile.UNKNOWN,
			new RpcTaskManagerGateway(new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway(), JobMasterId.generate()));
	}

	private static final class TestingDeclarativeSlotPoolFactory implements DeclarativeSlotPoolNgFactory {

		final TestingDeclarativeSlotPoolNgBuilder builder;

		private TestingDeclarativeSlotPoolFactory(TestingDeclarativeSlotPoolNgBuilder builder) {
			this.builder = builder;
		}

		@Override
		public DeclarativeSlotPoolNg create(Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements, Consumer<? super Collection<? extends PhysicalSlot>> notifyNewSlots, Time idleSlotTimeout, Time rpcTimeout) {
			return builder.build();
		}
	}

}
