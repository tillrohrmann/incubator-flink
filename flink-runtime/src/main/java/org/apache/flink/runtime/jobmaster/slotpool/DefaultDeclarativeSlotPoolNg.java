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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Default {@link DeclarativeSlotPoolNg} implementation.
 */
public class DefaultDeclarativeSlotPoolNg implements DeclarativeSlotPoolNg {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultDeclarativeSlotPoolNg.class);

	private final PoolService slotPool;

	private final ResourceCounter resourceRequirements;

	private final ResourceCounter availableResources;

	private final Map<AllocationID, ResourceProfile> slotToResourceProfileMappings;

	private final Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements;

	private final Consumer<? super Collection<? extends PhysicalSlot>> notifyNewSlots;

	private final Time idleSlotTimeout;
	private final Time rpcTimeout;

	public DefaultDeclarativeSlotPoolNg(
			PoolService slotPool,
			Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements,
			Consumer<? super Collection<? extends PhysicalSlot>> notifyNewSlots,
			Time idleSlotTimeout,
			Time rpcTimeout) {
		this.slotPool = slotPool;
		this.notifyNewResourceRequirements = notifyNewResourceRequirements;
		this.notifyNewSlots = notifyNewSlots;
		this.idleSlotTimeout = idleSlotTimeout;
		this.rpcTimeout = rpcTimeout;
		this.resourceRequirements = ResourceCounter.empty();
		this.availableResources = ResourceCounter.empty();
		this.slotToResourceProfileMappings = new HashMap<>();
	}

	@Override
	public void increaseResourceRequirementsBy(ResourceCounter increment) {
		resourceRequirements.add(increment);

		declareResourceRequirements();
	}

	@Override
	public void decreaseResourceRequirementsBy(ResourceCounter decrement) {
		resourceRequirements.subtract(decrement);

		declareResourceRequirements();
	}

	private void increaseAvailableResources(ResourceCounter acceptedResources) {
		availableResources.add(acceptedResources);
	}

	private void declareResourceRequirements() {
		notifyNewResourceRequirements.accept(getResourceRequirements());
	}

	@Override
	public Collection<ResourceRequirement> getResourceRequirements() {
		final Collection<ResourceRequirement> currentResourceRequirements = new ArrayList<>();

		for (Map.Entry<ResourceProfile, Integer> resourceRequirement : resourceRequirements.entrySet()) {
			currentResourceRequirements.add(new ResourceRequirement(resourceRequirement.getKey(), resourceRequirement.getValue()));
		}

		return currentResourceRequirements;
	}

	@Override
	public Collection<SlotOffer> offerSlots(
			Collection<SlotOffer> offers,
			TaskManagerLocation taskManagerLocation,
			TaskManagerGateway taskManagerGateway,
			long currentTime) {
		final Collection<SlotOffer> acceptedSlotOffers = new ArrayList<>();
		final Collection<SlotOffer> candidates = new ArrayList<>();

		for (SlotOffer offer : offers) {
			final AllocationID allocationId = offer.getAllocationId();
			if (slotPool.containsSlot(allocationId)) {
				acceptedSlotOffers.add(offer);
			} else {
				candidates.add(offer);
			}
		}

		final Collection<SlotOfferMatching> matchings = matchOffersWithOutstandingRequirements(candidates);

		final Collection<AllocatedSlot> acceptedSlots = new ArrayList<>();
		final ResourceCounter acceptedResources = ResourceCounter.empty();

		for (SlotOfferMatching matching : matchings) {
			if (matching.getMatching().isPresent()) {
				final ResourceProfile matchedResourceProfile = matching.getMatching().get();

				final AllocatedSlot allocatedSlot = createAllocatedSlot(
					matching.getSlotOffer(),
					taskManagerLocation,
					taskManagerGateway);

				acceptedSlots.add(allocatedSlot);
				acceptedSlotOffers.add(matching.getSlotOffer());

				acceptedResources.add(matchedResourceProfile, 1);

				slotToResourceProfileMappings.put(allocatedSlot.getAllocationId(), matchedResourceProfile);
			}
		}

		slotPool.addSlots(acceptedSlots, currentTime);
		increaseAvailableResources(acceptedResources);
		notifyNewSlots.accept(acceptedSlots);

		return acceptedSlotOffers;
	}

	private AllocatedSlot createAllocatedSlot(
			SlotOffer slotOffer,
			TaskManagerLocation taskManagerLocation,
			TaskManagerGateway taskManagerGateway) {
		return new AllocatedSlot(
			slotOffer.getAllocationId(),
			taskManagerLocation,
			slotOffer.getSlotIndex(),
			slotOffer.getResourceProfile(),
			taskManagerGateway);
	}

	private Collection<SlotOfferMatching> matchOffersWithOutstandingRequirements(Collection<SlotOffer> slotOffers) {
		final ResourceCounter unfulfilledResources = calculateUnfulfilledResources();

		final Collection<SlotOfferMatching> matching = new ArrayList<>();

		for (SlotOffer slotOffer : slotOffers) {
			ResourceProfile matchingResourceProfile = null;

			if (unfulfilledResources.containsResource(slotOffer.getResourceProfile())) {
				unfulfilledResources.subtract(slotOffer.getResourceProfile(), 1);

				matchingResourceProfile = slotOffer.getResourceProfile();
			} else {
				for (ResourceProfile unfulfilledResource : unfulfilledResources.getResources()) {
					if (slotOffer.getResourceProfile().isMatching(unfulfilledResource)) {
						matchingResourceProfile = unfulfilledResource;
						break;
					}
				}
			}

			final SlotOfferMatching slotOfferMatching;
			if (matchingResourceProfile != null) {
				slotOfferMatching = SlotOfferMatching.createMatching(slotOffer, matchingResourceProfile);
			} else {
				slotOfferMatching = SlotOfferMatching.createMismatch(slotOffer);
			}

			matching.add(slotOfferMatching);
		}

		return matching;
	}

	private ResourceCounter calculateUnfulfilledResources() {
		final ResourceCounter unfulfilledResources = ResourceCounter.withResources(resourceRequirements);
		unfulfilledResources.subtract(availableResources);

		return unfulfilledResources;
	}

	@Override
	public Collection<SlotInfoWithUtilization> getFreeSlotsInformation() {
		return slotPool.getFreeSlotsInformation().stream()
			.map(Pool.FreeSlotInfo::asSlotInfo)
			.collect(Collectors.toList());
	}

	@Override
	public Collection<? extends SlotInfo> getAllSlotsInformation() {
		return slotPool.getAllSlotsInformation();
	}

	@Override
	public void failSlots(ResourceID resourceId, Exception cause) {
		final Collection<AllocatedSlot> removedSlots = slotPool.removeSlots(resourceId);

		releasePayloadAndDecreaseResourceRequirement(removedSlots, cause);
	}

	@Override
	public void failSlot(AllocationID allocationID, Exception cause) {
		final Optional<AllocatedSlot> removedSlot = slotPool.removeSlot(allocationID);

		removedSlot.ifPresent(allocatedSlot -> releasePayloadAndDecreaseResourceRequirement(Collections.singleton(allocatedSlot), cause));
	}

	private void releasePayloadAndDecreaseResourceRequirement(Collection<? extends AllocatedSlot> allocatedSlots, Throwable cause) {
		final ResourceCounter resourceDecrement = ResourceCounter.empty();

		for (AllocatedSlot allocatedSlot : allocatedSlots) {
			allocatedSlot.releasePayload(cause);
			resourceDecrement.add(allocatedSlot.getResourceProfile(), 1);
		}

		decreaseResourceRequirementsBy(resourceDecrement);
	}

	@Override
	public PhysicalSlot allocateFreeSlot(AllocationID allocationID) {
		return slotPool.allocateFreeSlot(allocationID);
	}

	@Override
	public void releaseSlot(AllocationID allocationId, Throwable cause, long currentTime) {
		final Optional<AllocatedSlot> releasedSlot = slotPool.releaseAllocatedSlot(allocationId, currentTime);

		releasedSlot.ifPresent(allocatedSlot -> {
			releasePayloadAndDecreaseResourceRequirement(Collections.singleton(allocatedSlot), cause);
			notifyNewSlots.accept(Collections.singletonList(allocatedSlot));
		});
	}

	@Override
	public boolean containsSlots(ResourceID resourceId) {
		return slotPool.containsSlots(resourceId);
	}

	@Override
	public void returnIdleSlots(long currentTimeMillis) {
		final Collection<Pool.FreeSlotInfo> freeSlotsInformation = slotPool.getFreeSlotsInformation();

		final ResourceCounter excessResources = ResourceCounter.withResources(availableResources);
		excessResources.subtract(resourceRequirements);

		final Iterator<Pool.FreeSlotInfo> freeSlotIterator = freeSlotsInformation.iterator();

		final Collection<AllocatedSlot> slotsToReturnToOwner = new ArrayList<>();

		while (!excessResources.isEmpty() && freeSlotIterator.hasNext()) {
			final Pool.FreeSlotInfo idleSlot = freeSlotIterator.next();

			if (currentTimeMillis > idleSlot.getIdleDuration() + idleSlotTimeout.toMilliseconds()) {
				final ResourceProfile matchingProfile = slotToResourceProfileMappings.get(idleSlot.getAllocationId());

				if (excessResources.containsResource(matchingProfile)) {
					excessResources.subtract(matchingProfile, 1);
					availableResources.subtract(matchingProfile, 1);
					slotToResourceProfileMappings.remove(idleSlot.getAllocationId());
					final Optional<AllocatedSlot> removedSlot = slotPool.removeSlot(idleSlot.getAllocationId());

					if (removedSlot.isPresent()) {
						slotsToReturnToOwner.add(removedSlot.get());
					}
				}
			}
		}

		returnSlotsToOwner(slotsToReturnToOwner);
	}

	private void returnSlotsToOwner(Collection<AllocatedSlot> slotsToReturnToOwner) {
		final FlinkException cause = new FlinkException("");
		for (AllocatedSlot expiredSlot : slotsToReturnToOwner) {
			Preconditions.checkState(!expiredSlot.isUsed(), "Free slot must not be used.");

			LOG.info("Releasing idle slot [{}].", expiredSlot.getAllocationId());
			final CompletableFuture<Acknowledge> freeSlotFuture = expiredSlot.getTaskManagerGateway().freeSlot(
				expiredSlot.getAllocationId(),
				cause,
				rpcTimeout);

			freeSlotFuture.whenComplete((Acknowledge ignored, Throwable throwable) -> {
				if (throwable != null) {
					// The slot status will be synced to task manager in next heartbeat.
					LOG.debug("Releasing slot [{}] of registered TaskExecutor {} failed. Discarding slot.",
						expiredSlot.getAllocationId(), expiredSlot.getTaskManagerId(), throwable);
				}
			});
		}
	}

	private static final class SlotOfferMatching {
		private final SlotOffer slotOffer;

		@Nullable
		private final ResourceProfile matching;

		private SlotOfferMatching(SlotOffer slotOffer, @Nullable ResourceProfile matching) {
			this.slotOffer = slotOffer;
			this.matching = matching;
		}

		private SlotOffer getSlotOffer() {
			return slotOffer;
		}

		private Optional<ResourceProfile> getMatching() {
			return Optional.ofNullable(matching);
		}

		private static SlotOfferMatching createMatching(SlotOffer slotOffer, ResourceProfile matching) {
			return new SlotOfferMatching(slotOffer, matching);
		}

		private static SlotOfferMatching createMismatch(SlotOffer slotOffer) {
			return new SlotOfferMatching(slotOffer, null);
		}
	}
}
