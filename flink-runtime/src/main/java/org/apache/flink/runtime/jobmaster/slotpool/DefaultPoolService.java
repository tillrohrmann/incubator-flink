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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Default {@link PoolService} implementation.
 */
public class DefaultPoolService implements PoolService {

	private final Map<AllocationID, AllocatedSlot> registeredSlots;

	private final Map<AllocationID, Long> freeSlotsSince;

	private final Map<ResourceID, Set<AllocationID>> slotsPerTaskExecutor;

	public DefaultPoolService() {
		this.registeredSlots = new HashMap<>();
		this.slotsPerTaskExecutor = new HashMap<>();
		this.freeSlotsSince = new HashMap<>();
	}

	@Override
	public Collection<? extends SlotInfo> getAllSlotsInformation() {
		return registeredSlots.values();
	}

	@Override
	public void addSlots(Collection<AllocatedSlot> slots, long currentTime) {
		for (AllocatedSlot slot : slots) {
			addSlot(slot, currentTime);
		}
	}

	private void addSlot(AllocatedSlot slot, long currentTime) {
		Preconditions.checkState(!registeredSlots.containsKey(slot.getAllocationId()), "The slot pool already contains a slot with id %s", slot.getAllocationId());
		addSlotInternal(slot, currentTime);

		slotsPerTaskExecutor
			.computeIfAbsent(slot.getTaskManagerId(), resourceID -> new HashSet<>())
			.add(slot.getAllocationId());
	}

	private void addSlotInternal(AllocatedSlot slot, long currentTime) {
		registeredSlots.put(slot.getAllocationId(), slot);
		freeSlotsSince.put(slot.getAllocationId(), currentTime);
	}

	@Override
	public Optional<AllocatedSlot> removeSlot(AllocationID allocationId) {
		final AllocatedSlot removedSlot = removeSlotInternal(allocationId);

		if (removedSlot != null) {
			final ResourceID owner = removedSlot.getTaskManagerId();

			slotsPerTaskExecutor.computeIfPresent(owner, (resourceID, allocationIds) -> {
				allocationIds.remove(allocationId);

				if (allocationIds.isEmpty()) {
					return null;
				}

				return allocationIds;
			});

			return Optional.of(removedSlot);
		} else {
			return Optional.empty();
		}
	}

	private AllocatedSlot removeSlotInternal(AllocationID allocationId) {
		final AllocatedSlot removedSlot = registeredSlots.remove(allocationId);
		freeSlotsSince.remove(allocationId);
		return removedSlot;
	}

	@Override
	public Collection<AllocatedSlot> removeSlots(ResourceID owner) {
		final Set<AllocationID> slotsOfTaskExecutor = slotsPerTaskExecutor.remove(owner);

		final Collection<AllocatedSlot> removedSlots = new ArrayList<>();

		for (AllocationID allocationId : slotsOfTaskExecutor) {
			removedSlots.add(Preconditions.checkNotNull(removeSlotInternal(allocationId)));
		}

		return removedSlots;
	}

	@Override
	public boolean containsSlots(ResourceID owner) {
		return slotsPerTaskExecutor.containsKey(owner);
	}

	@Override
	public boolean containsSlot(AllocationID allocationId) {
		return registeredSlots.containsKey(allocationId);
	}

	@Override
	public Collection<FreeSlotInfo> getFreeSlotsInformation() {
		final Map<ResourceID, Integer> freeSlotsPerTaskExecutor = new HashMap<>();

		for (AllocationID allocationId : freeSlotsSince.keySet()) {
			final ResourceID owner = Preconditions.checkNotNull(registeredSlots.get(allocationId)).getTaskManagerId();
			final int newValue = freeSlotsPerTaskExecutor.getOrDefault(owner, 0) + 1;
			freeSlotsPerTaskExecutor.put(owner, newValue);
		}

		final Collection<FreeSlotInfo> freeSlotInfos = new ArrayList<>();

		for (Map.Entry<AllocationID, Long> freeSlot : freeSlotsSince.entrySet()) {
			final AllocatedSlot allocatedSlot = Preconditions.checkNotNull(registeredSlots.get(freeSlot.getKey()));

			final ResourceID owner = allocatedSlot.getTaskManagerId();
			final int numberOfSlotsOnOwner = slotsPerTaskExecutor.get(owner).size();
			final int numberOfFreeSlotsOnOwner = freeSlotsPerTaskExecutor.get(owner);
			final double taskExecutorUtilization = (double) (numberOfSlotsOnOwner - numberOfFreeSlotsOnOwner) / numberOfSlotsOnOwner;

			final SlotInfoWithUtilization slotInfoWithUtilization = SlotInfoWithUtilization.from(allocatedSlot, taskExecutorUtilization);

			freeSlotInfos.add(DefaultFreeSlotInfo.create(slotInfoWithUtilization, freeSlot.getValue()));
		}

		return freeSlotInfos;
	}

	@Override
	public AllocatedSlot allocateFreeSlot(AllocationID allocationId) {
		Preconditions.checkState(freeSlotsSince.remove(allocationId) != null, "The slot with id %s was not free.", allocationId);
		return registeredSlots.get(allocationId);
	}

	@Override
	public Optional<AllocatedSlot> releaseAllocatedSlot(AllocationID slotId, long currentTime) {
		final AllocatedSlot allocatedSlot = registeredSlots.get(slotId);

		if (allocatedSlot != null) {
			freeSlotsSince.put(slotId, currentTime);
			return Optional.of(allocatedSlot);
		} else {
			return Optional.empty();
		}
	}

	private static final class DefaultFreeSlotInfo implements Pool.FreeSlotInfo {

		private final SlotInfoWithUtilization slotInfoWithUtilization;

		private final long idleSince;

		private DefaultFreeSlotInfo(SlotInfoWithUtilization slotInfoWithUtilization, long idleSince) {
			this.slotInfoWithUtilization = slotInfoWithUtilization;
			this.idleSince = idleSince;
		}

		@Override
		public SlotInfoWithUtilization asSlotInfo() {
			return slotInfoWithUtilization;
		}

		@Override
		public long getIdleDuration() {
			return idleSince;
		}

		@Override
		public AllocationID getAllocationId() {
			return slotInfoWithUtilization.getAllocationId();
		}

		private static DefaultFreeSlotInfo create(SlotInfoWithUtilization slotInfoWithUtilization, long idleSince) {
			return new DefaultFreeSlotInfo(slotInfoWithUtilization, idleSince);
		}
	}
}
