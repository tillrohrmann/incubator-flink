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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link DefaultDeclarativeSlotPoolNg}.
 */
public class DefaultDeclarativeSlotPoolNgTest extends TestLogger {

	private final ResourceProfile resourceProfile1 = ResourceProfile.newBuilder().setCpuCores(1.7).build();
	private final ResourceProfile resourceProfile2 = ResourceProfile.newBuilder().setManagedMemoryMB(100).build();

	@Test
	public void testIncreasingResourceRequirementsWillSendResourceRequirementNotification() throws InterruptedException {
		final NewResourceRequirementsService notifyNewResourceRequirements = new NewResourceRequirementsService();

		final DeclarativeSlotPoolNg slotPool = createDefaultDeclarativeSlotPool(notifyNewResourceRequirements);

		final ResourceCounter increment1 = ResourceCounter.withResource(resourceProfile1, 1);
		final ResourceCounter increment2 = createResourceRequirements();
		slotPool.increaseResourceRequirementsBy(increment1);
		slotPool.increaseResourceRequirementsBy(increment2);

		assertThat(notifyNewResourceRequirements.takeResourceRequirements(), is(toResourceRequirements(increment1)));

		final ResourceCounter totalResources = increment1.add(increment2);
		totalResources.add(increment2);
		assertThat(notifyNewResourceRequirements.takeResourceRequirements(), is(toResourceRequirements(totalResources)));
		assertThat(notifyNewResourceRequirements.hasNextResourceRequirements(), is(false));
	}

	@Nonnull
	private ResourceCounter createResourceRequirements() {
		final Map<ResourceProfile, Integer> resources2 = new HashMap<>();
		resources2.put(resourceProfile1, 2);
		resources2.put(resourceProfile2, 1);

		final ResourceCounter increment2 = ResourceCounter.withResources(resources2);
		return increment2;
	}

	@Nonnull
	private DefaultDeclarativeSlotPoolNg createDefaultDeclarativeSlotPool(NewResourceRequirementsService notifyNewResourceRequirements) {
		return DefaultDeclarativeSlotPoolBuilder.builder()
			.setNotifyNewResourceRequirements(notifyNewResourceRequirements)
			.build();
	}

	private Collection<ResourceRequirement> toResourceRequirements(ResourceCounter resourceCounter) {
		return resourceCounter.getResourcesWithCount().stream()
			.map(resourceCount -> ResourceRequirement.create(resourceCount.getKey(), resourceCount.getValue()))
			.collect(Collectors.toList());
	}

	@Test
	public void testDecreasingResourceRequirementsWillSendResourceRequirementNotification() throws InterruptedException {
		final NewResourceRequirementsService notifyNewResourceRequirements = new NewResourceRequirementsService();

		final DefaultDeclarativeSlotPoolNg slotPool = createDefaultDeclarativeSlotPool(notifyNewResourceRequirements);

		final ResourceCounter increment = ResourceCounter.withResource(resourceProfile1, 3);

		slotPool.increaseResourceRequirementsBy(increment);

		notifyNewResourceRequirements.takeResourceRequirements();

		final ResourceCounter decrement = ResourceCounter.withResource(resourceProfile1, 2);
		slotPool.decreaseResourceRequirementsBy(decrement);

		assertThat(notifyNewResourceRequirements.takeResourceRequirements(), is(toResourceRequirements(ResourceCounter.withResource(resourceProfile1, 1))));
	}

	@Test
	public void testGetResourceRequirements() {
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder().build();

		final ResourceCounter resourceRequirements = createResourceRequirements();

		slotPool.increaseResourceRequirementsBy(resourceRequirements);

		assertThat(slotPool.getResourceRequirements(), is(toResourceRequirements(resourceRequirements)));
	}

	@Test
	public void testOfferSlots() throws InterruptedException {
		final NewSlotsService notifyNewSlots = new NewSlotsService();
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setNotifyNewSlots(notifyNewSlots)
			.build();

		final ResourceCounter resourceRequirements = createResourceRequirements();

		slotPool.increaseResourceRequirementsBy(resourceRequirements);

		Collection<SlotOffer> slotOffers = createSlotOffersForResourceRequirements(resourceRequirements);

		final Collection<SlotOffer> acceptedSlots = slotPool.offerSlots(slotOffers, new LocalTaskManagerLocation(), createTaskManagerGateway(null), 0);

		assertThat(acceptedSlots, containsInAnyOrder(slotOffers.toArray()));

		final Collection<PhysicalSlot> newSlots = drainNewSlotService(notifyNewSlots);

		assertThat(newSlots, containsInAnyOrder(slotOffers.stream().map(this::matchesSlotOffer).collect(Collectors.toList())));
		assertThat(slotPool.getAllSlotsInformation(), containsInAnyOrder(newSlots.stream().map(DefaultAllocatedSlotPoolTest::matchesPhysicalSlot).collect(Collectors.toList())));
	}

	@Nonnull
	private Collection<SlotOffer> createSlotOffersForResourceRequirements(ResourceCounter resourceRequirements) {
		Collection<SlotOffer> slotOffers = new ArrayList<>();
		int slotIndex = 0;

		for (Map.Entry<ResourceProfile, Integer> resourceWithCount : resourceRequirements.getResourcesWithCount()) {
			for (int i = 0; i < resourceWithCount.getValue(); i++) {
				slotOffers.add(new SlotOffer(new AllocationID(), slotIndex++, resourceWithCount.getKey()));
			}
		}
		return slotOffers;
	}

	@Test
	public void testDuplicateSlotOfferings() throws InterruptedException {
		final NewSlotsService notifyNewSlots = new NewSlotsService();
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setNotifyNewSlots(notifyNewSlots)
			.build();

		final ResourceCounter resourceRequirements = createResourceRequirements();

		slotPool.increaseResourceRequirementsBy(resourceRequirements);

		final Collection<SlotOffer> slotOffers = createSlotOffersForResourceRequirements(resourceRequirements);

		final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		final TaskManagerGateway taskManagerGateway = createTaskManagerGateway(null);
		slotPool.offerSlots(slotOffers, taskManagerLocation, taskManagerGateway, 0);

		drainNewSlotService(notifyNewSlots);

		final Collection<SlotOffer> acceptedSlots = slotPool.offerSlots(slotOffers, taskManagerLocation, taskManagerGateway, 0);

		assertThat(acceptedSlots, containsInAnyOrder(slotOffers.toArray()));
		// duplicate slots should not trigger notify new slots
		assertFalse(notifyNewSlots.hasNextNewSlots());
	}

	@Test
	public void testOfferingTooManySlots() {
		final NewSlotsService notifyNewSlots = new NewSlotsService();
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setNotifyNewSlots(notifyNewSlots)
			.build();

		final ResourceCounter resourceRequirements = createResourceRequirements();

		slotPool.increaseResourceRequirementsBy(resourceRequirements);

		final ResourceCounter increasedRequirements = resourceRequirements.add(resourceProfile1, 2);

		final Collection<SlotOffer> slotOffers = createSlotOffersForResourceRequirements(increasedRequirements);

		final Collection<SlotOffer> acceptedSlots = slotPool.offerSlots(slotOffers, new LocalTaskManagerLocation(), createTaskManagerGateway(null), 0);

		final Map<ResourceProfile, Long> resourceProfileCount = acceptedSlots.stream().map(SlotOffer::getResourceProfile).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

		for (Map.Entry<ResourceProfile, Integer> resourceCount : resourceRequirements.getResourcesWithCount()) {
			assertThat(resourceProfileCount.getOrDefault(resourceCount.getKey(), 0L), is((long) resourceCount.getValue()));
		}
	}

	@Test
	public void testFailSlotsDecreasesResourceRequirements() throws InterruptedException {
		final NewResourceRequirementsService notifyNewResourceRequirements = new NewResourceRequirementsService();
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setNotifyNewResourceRequirements(notifyNewResourceRequirements)
			.build();

		final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		increaseRequirementsAndOfferSlotsToSlotPool(slotPool, createResourceRequirements(), taskManagerLocation);

		notifyNewResourceRequirements.takeResourceRequirements();

		slotPool.failSlots(taskManagerLocation.getResourceID(), new FlinkException("Test failure"));
		assertThat(notifyNewResourceRequirements.takeResourceRequirements(), is(empty()));
		assertNoAvailableAndRequiredResources(slotPool);
	}

	@Test
	public void testFailSlotsReturnsSlot() {
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.build();

		final ResourceCounter resourceRequirements = createResourceRequirements();

		final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		final FreeSlotConsumer freeSlotConsumer = new FreeSlotConsumer();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setFreeSlotFunction(freeSlotConsumer)
			.createTestingTaskExecutorGateway();

		final Collection<SlotOffer> slotOffers = increaseRequirementsAndOfferSlotsToSlotPool(
			slotPool,
			resourceRequirements,
			taskManagerLocation,
			testingTaskExecutorGateway);

		slotPool.failSlots(taskManagerLocation.getResourceID(), new FlinkException("Test failure"));

		final Collection<AllocationID> freedSlots = freeSlotConsumer.drainFreedSlots();

		assertThat(freedSlots, containsInAnyOrder(slotOffers.stream().map(SlotOffer::getAllocationId).toArray()));
	}

	@Nonnull
	private Collection<SlotOffer> increaseRequirementsAndOfferSlotsToSlotPool(DefaultDeclarativeSlotPoolNg slotPool, ResourceCounter resourceRequirements, @Nullable LocalTaskManagerLocation taskManagerLocation) {
		return increaseRequirementsAndOfferSlotsToSlotPool(slotPool, resourceRequirements, taskManagerLocation, null);
	}

	@Nonnull
	private Collection<SlotOffer> increaseRequirementsAndOfferSlotsToSlotPool(DefaultDeclarativeSlotPoolNg slotPool, ResourceCounter resourceRequirements, @Nullable LocalTaskManagerLocation taskManagerLocation, @Nullable TaskExecutorGateway taskExecutorGateway) {
		final Collection<SlotOffer> slotOffers = createSlotOffersForResourceRequirements(resourceRequirements);

		slotPool.increaseResourceRequirementsBy(resourceRequirements);

		return slotPool.offerSlots(slotOffers, taskManagerLocation == null ? new LocalTaskManagerLocation() : taskManagerLocation, createTaskManagerGateway(taskExecutorGateway), 0);
	}

	@Test
	public void testFailSlotDecreasesResourceRequirements() throws InterruptedException {
		final NewResourceRequirementsService notifyNewResourceRequirements = new NewResourceRequirementsService();
		final NewSlotsService notifyNewSlots = new NewSlotsService();
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setNotifyNewSlots(notifyNewSlots)
			.setNotifyNewResourceRequirements(notifyNewResourceRequirements)
			.build();

		final ResourceCounter resourceRequirements = createResourceRequirements();
		increaseRequirementsAndOfferSlotsToSlotPool(slotPool, resourceRequirements, null);

		final Collection<? extends PhysicalSlot> physicalSlots = notifyNewSlots.takeNewSlots();

		final PhysicalSlot physicalSlot = physicalSlots.iterator().next();

		notifyNewResourceRequirements.takeResourceRequirements();

		slotPool.failSlot(physicalSlot.getAllocationId(), new FlinkException("Test failure"));

		final ResourceCounter finalResourceRequirements = resourceRequirements.subtract(physicalSlot.getResourceProfile(), 1);
		final Collection<ResourceRequirement> expectedResourceRequirements = toResourceRequirements(finalResourceRequirements);
		assertThat(notifyNewResourceRequirements.takeResourceRequirements(), is(expectedResourceRequirements));
		assertThat(slotPool.getResourceRequirements(), is(expectedResourceRequirements));
		assertThat(slotPool.getAvailableResources(), is(finalResourceRequirements));
	}

	@Test
	public void testFailSlotReturnsSlot() throws InterruptedException {
		final NewSlotsService notifyNewSlots = new NewSlotsService();
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setNotifyNewSlots(notifyNewSlots)
			.build();

		final ResourceCounter resourceRequirements = createResourceRequirements();
		final FreeSlotConsumer freeSlotConsumer = new FreeSlotConsumer();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setFreeSlotFunction(freeSlotConsumer)
			.createTestingTaskExecutorGateway();

		increaseRequirementsAndOfferSlotsToSlotPool(
			slotPool,
			resourceRequirements,
			new LocalTaskManagerLocation(),
			testingTaskExecutorGateway);

		final Collection<? extends PhysicalSlot> physicalSlots = notifyNewSlots.takeNewSlots();

		final PhysicalSlot physicalSlot = physicalSlots.iterator().next();

		slotPool.failSlot(physicalSlot.getAllocationId(), new FlinkException("Test failure"));

		final AllocationID freedSlot = Iterables.getOnlyElement(freeSlotConsumer.drainFreedSlots());

		assertThat(freedSlot, is(physicalSlot.getAllocationId()));
	}

	@Test
	public void testReturnIdleSlotsAfterTimeout() {
		final Time idleSlotTimeout = Time.seconds(10);
		final long offerTime = 0;
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setIdleSlotTimeout(idleSlotTimeout)
			.build();

		final ResourceCounter resourceRequirements = createResourceRequirements();
		final FreeSlotConsumer freeSlotConsumer = new FreeSlotConsumer();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setFreeSlotFunction(freeSlotConsumer)
			.createTestingTaskExecutorGateway();

		final Collection<SlotOffer> acceptedSlots = increaseRequirementsAndOfferSlotsToSlotPool(
			slotPool,
			resourceRequirements,
			new LocalTaskManagerLocation(),
			testingTaskExecutorGateway);

		// decrease the resource requirements so that slots are no longer needed
		slotPool.decreaseResourceRequirementsBy(resourceRequirements);

		slotPool.returnIdleSlots(offerTime + idleSlotTimeout.toMilliseconds());

		final Collection<AllocationID> freedSlots = freeSlotConsumer.drainFreedSlots();

		assertThat(acceptedSlots, is(not(empty())));
		assertThat(freedSlots, containsInAnyOrder(acceptedSlots.stream().map(SlotOffer::getAllocationId).toArray()));
		assertNoAvailableAndRequiredResources(slotPool);
	}

	private void assertNoAvailableAndRequiredResources(DefaultDeclarativeSlotPoolNg slotPool) {
		assertTrue(slotPool.getAvailableResources().isEmpty());
		assertTrue(slotPool.getResourceRequirements().isEmpty());
		assertThat(slotPool.getAllSlotsInformation(), is(empty()));
	}

	@Test
	public void testOnlyReturnExcessIdleSlots() {
		final Time idleSlotTimeout = Time.seconds(10);
		final long offerTime = 0;
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setIdleSlotTimeout(idleSlotTimeout)
			.build();

		final ResourceCounter resourceRequirements = createResourceRequirements();
		final Collection<SlotOffer> slotOffers = createSlotOffersForResourceRequirements(resourceRequirements);

		slotPool.increaseResourceRequirementsBy(resourceRequirements);
		final Collection<SlotOffer> acceptedSlots = slotPool.offerSlots(slotOffers, new LocalTaskManagerLocation(), createTaskManagerGateway(null), offerTime);

		final ResourceCounter requiredResources = ResourceCounter.withResource(resourceProfile1, 1);
		final ResourceCounter excessRequirements = resourceRequirements.subtract(requiredResources);
		slotPool.decreaseResourceRequirementsBy(excessRequirements);

		slotPool.returnIdleSlots(offerTime + idleSlotTimeout.toMilliseconds());

		assertThat(acceptedSlots, is(not(empty())));
		assertThat(slotPool.getAvailableResources(), is(requiredResources));
	}

	@Test
	public void testReleasedSlotWillBeUsedToFulfillOutstandingResourceRequirements() throws InterruptedException {
		final NewSlotsService notifyNewSlots = new NewSlotsService();
		final DefaultDeclarativeSlotPoolNg slotPool = DefaultDeclarativeSlotPoolBuilder.builder()
			.setNotifyNewSlots(notifyNewSlots)
			.build();

		final ResourceProfile smallResourceProfile = resourceProfile1;
		final ResourceProfile largeResourceProfile = smallResourceProfile.multiply(2);
		final ResourceCounter initialRequirements = ResourceCounter.withResource(largeResourceProfile, 1);

		increaseRequirementsAndOfferSlotsToSlotPool(slotPool, initialRequirements, null);

		final ResourceCounter newRequirements = ResourceCounter.withResource(smallResourceProfile, 1);
		slotPool.increaseResourceRequirementsBy(newRequirements);

		final Collection<PhysicalSlot> newSlots = drainNewSlotService(notifyNewSlots);
		final PhysicalSlot newSlot = Iterables.getOnlyElement(newSlots);

		slotPool.allocateFreeSlot(newSlot.getAllocationId());
		slotPool.releaseSlot(newSlot.getAllocationId(), null, 0);

		final Collection<PhysicalSlot> recycledSlots = drainNewSlotService(notifyNewSlots);

		assertThat(Iterables.getOnlyElement(recycledSlots), sameInstance(newSlot));

		final Collection<SlotOffer> newSlotOffers = createSlotOffersForResourceRequirements(initialRequirements);

		// the pending requirement should be fulfilled by the released slot --> rejecting new slot offers
		final Collection<SlotOffer> acceptedSlots = slotPool.offerSlots(newSlotOffers, new LocalTaskManagerLocation(), createTaskManagerGateway(null), 0);

		assertThat(acceptedSlots, is(empty()));
		assertTrue(slotPool.calculateUnfulfilledResources().isEmpty());
	}

	@Nonnull
	private Collection<PhysicalSlot> drainNewSlotService(NewSlotsService notifyNewSlots) throws InterruptedException {
		final Collection<PhysicalSlot> newSlots = new ArrayList<>();

		while (notifyNewSlots.hasNextNewSlots()) {
			newSlots.addAll(notifyNewSlots.takeNewSlots());
		}
		return newSlots;
	}

	private TypeSafeMatcher<PhysicalSlot> matchesSlotOffer(SlotOffer slotOffer) {
		return new PhysicalSlotSlotOfferMatcher(slotOffer);
	}

	private TaskManagerGateway createTaskManagerGateway(@Nullable TaskExecutorGateway taskExecutorGateway) {
		return new RpcTaskManagerGateway(
			taskExecutorGateway == null ? new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway() : taskExecutorGateway,
			JobMasterId.generate());
	}

	private static final class NewResourceRequirementsService implements Consumer<Collection<ResourceRequirement>> {

		private final BlockingQueue<Collection<ResourceRequirement>> resourceRequirementsQueue = new ArrayBlockingQueue<>(2);

		@Override
		public void accept(Collection<ResourceRequirement> resourceRequirements) {
			resourceRequirementsQueue.offer(resourceRequirements);
		}

		private Collection<ResourceRequirement> takeResourceRequirements() throws InterruptedException {
			return resourceRequirementsQueue.take();
		}

		public boolean hasNextResourceRequirements() {
			return !resourceRequirementsQueue.isEmpty();
		}
	}

	private static final class NewSlotsService implements Consumer<Collection<? extends PhysicalSlot>> {

		private final BlockingQueue<Collection<? extends PhysicalSlot>> physicalSlotsQueue = new ArrayBlockingQueue<>(2);

		@Override
		public void accept(Collection<? extends PhysicalSlot> physicalSlots) {
			physicalSlotsQueue.offer(physicalSlots);
		}

		private Collection<? extends PhysicalSlot> takeNewSlots() throws InterruptedException {
			return physicalSlotsQueue.take();
		}

		private boolean hasNextNewSlots() {
			return !physicalSlotsQueue.isEmpty();
		}
	}

	private static final class DefaultDeclarativeSlotPoolBuilder {

		private AllocatedSlotPool allocatedSlotPool = new DefaultAllocatedSlotPool();
		private Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements = ignored -> {};
		private Consumer<? super Collection<? extends PhysicalSlot>> notifyNewSlots = ignored -> {};
		private Time idleSlotTimeout = Time.seconds(20);
		private Time rpcTimeout = Time.seconds(20);

		public DefaultDeclarativeSlotPoolBuilder setAllocatedSlotPool(AllocatedSlotPool allocatedSlotPool) {
			this.allocatedSlotPool = allocatedSlotPool;
			return this;
		}

		public DefaultDeclarativeSlotPoolBuilder setNotifyNewResourceRequirements(Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements) {
			this.notifyNewResourceRequirements = notifyNewResourceRequirements;
			return this;
		}

		public DefaultDeclarativeSlotPoolBuilder setNotifyNewSlots(Consumer<? super Collection<? extends PhysicalSlot>> notifyNewSlots) {
			this.notifyNewSlots = notifyNewSlots;
			return this;
		}

		public DefaultDeclarativeSlotPoolBuilder setIdleSlotTimeout(Time idleSlotTimeout) {
			this.idleSlotTimeout = idleSlotTimeout;
			return this;
		}

		DefaultDeclarativeSlotPoolNg build() {
			return new DefaultDeclarativeSlotPoolNg(
				allocatedSlotPool,
				notifyNewResourceRequirements,
				notifyNewSlots,
				idleSlotTimeout,
				rpcTimeout);
		}

		static DefaultDeclarativeSlotPoolBuilder builder() {
			return new DefaultDeclarativeSlotPoolBuilder();
		}
	}

	private static class PhysicalSlotSlotOfferMatcher extends TypeSafeMatcher<PhysicalSlot> {
		private final SlotOffer slotOffer;

		public PhysicalSlotSlotOfferMatcher(SlotOffer slotOffer) {
			this.slotOffer = slotOffer;
		}

		@Override
		protected boolean matchesSafely(PhysicalSlot item) {
			return item.getAllocationId().equals(slotOffer.getAllocationId()) &&
				item.getResourceProfile().equals(slotOffer.getResourceProfile()) &&
				item.getPhysicalSlotNumber() == slotOffer.getSlotIndex();
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("SlotOffer: ");
			description.appendValueList("{", ",", "}",
				slotOffer.getAllocationId(),
				slotOffer.getResourceProfile(),
				slotOffer.getSlotIndex());
		}
	}

	private static class FreeSlotConsumer implements BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> {

		final BlockingQueue<AllocationID> freedSlots = new ArrayBlockingQueue<>(10);

		@Override
		public CompletableFuture<Acknowledge> apply(AllocationID allocationID, Throwable throwable) {
			freedSlots.offer(allocationID);
			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		private Collection<AllocationID> drainFreedSlots() {
			final Collection<AllocationID> result = new ArrayList<>();

			freedSlots.drainTo(result);

			return result;
		}
	}
}
