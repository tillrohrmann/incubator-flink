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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * {@link SlotCollection} which tries to evenly spread out task across the available set of
 * registered TaskManagers.
 */
public class EvenlySpreadOutSlotCollection implements SlotCollection {

	private static final Logger LOG = LoggerFactory.getLogger(EvenlySpreadOutSlotCollection.class);

	private final PriorityQueue<SlotsPerTaskManager> orderedSlots;

	EvenlySpreadOutSlotCollection() {
		this.orderedSlots = new PriorityQueue<>(16, new SlotsPerTaskManagerComparator());
	}

	@Override
	public int size() {
		return orderedSlots.stream().mapToInt(SlotsPerTaskManager::getNumberAvailableSlots).sum();
	}

	@Nullable
	@Override
	public TaskManagerSlot findMatchingSlot(ResourceProfile requestResourceProfile) {
		final Iterator<SlotsPerTaskManager> iterator = orderedSlots.iterator();

		while (iterator.hasNext()) {
			final SlotsPerTaskManager slotsPerTaskManager = iterator.next();

			final TaskManagerSlot taskManagerSlot = slotsPerTaskManager.findMatchingSlot(requestResourceProfile);

			if (taskManagerSlot != null) {
				iterator.remove();
				slotsPerTaskManager.removeSlot(taskManagerSlot.getSlotId());
				orderedSlots.offer(slotsPerTaskManager);

				return taskManagerSlot;
			}
		}

		return null;
	}

	@Override
	public void remove(SlotID slotId) {
		final SlotsPerTaskManager slotsPerTaskManager = removeSlotsPerTaskManager(slotId.getResourceID());

		if (slotsPerTaskManager != null) {
			slotsPerTaskManager.removeSlot(slotId);

			orderedSlots.offer(slotsPerTaskManager);
		}
	}

	@Nullable
	private SlotsPerTaskManager removeSlotsPerTaskManager(ResourceID resourceID) {
		final Iterator<SlotsPerTaskManager> iterator = orderedSlots.iterator();

		while (iterator.hasNext()) {
			final SlotsPerTaskManager slotsPerTaskManager = iterator.next();

			if (slotsPerTaskManager.getTaskManagerResourceId().equals(resourceID)) {
				iterator.remove();
				return slotsPerTaskManager;
			}
		}

		return null;
	}

	@Override
	public void put(SlotID slotId, TaskManagerSlot freeSlot) {
		final ResourceID taskManagerResourceId = freeSlot.getSlotId().getResourceID();
		SlotsPerTaskManager slotsPerTaskManager = removeSlotsPerTaskManager(taskManagerResourceId);

		if (slotsPerTaskManager == null) {
			throw new IllegalStateException(String.format("No task manager registered under %s.", taskManagerResourceId));
		} else {
			slotsPerTaskManager.putSlot(slotId, freeSlot);
			orderedSlots.offer(slotsPerTaskManager);
		}
	}

	@Override
	public void unregisterTaskManager(ResourceID resourceID) {
		orderedSlots.removeIf(slotsPerTaskManager -> slotsPerTaskManager.getTaskManagerResourceId().equals(resourceID));
	}

	@Override
	public void registerTaskManager(ResourceID resourceID, int numberSlots) {
		orderedSlots.offer(new SlotsPerTaskManager(resourceID, numberSlots));
	}

	private static final class SlotsPerTaskManager {
		private final ResourceID taskManagerResourceId;
		private final int numberSlots;
		private final Map<SlotID, TaskManagerSlot> slots;

		private SlotsPerTaskManager(ResourceID taskManagerResourceId, int numberSlots) {
			this.taskManagerResourceId = taskManagerResourceId;
			this.numberSlots = numberSlots;
			this.slots = new HashMap<>(16);
		}

		private int getNumberAvailableSlots() {
			return slots.size();
		}

		private int getNumberSlots() {
			return numberSlots;
		}

		private ResourceID getTaskManagerResourceId() {
			return taskManagerResourceId;
		}

		void removeSlot(SlotID slotId) {
			slots.remove(slotId);
		}

		void putSlot(SlotID slotId, TaskManagerSlot freeSlot) {
			slots.put(slotId, freeSlot);
		}

		@Nullable
		TaskManagerSlot findMatchingSlot(ResourceProfile requestResourceProfile) {
			final Iterator<TaskManagerSlot> iterator = slots.values().iterator();

			while (iterator.hasNext()) {
				final TaskManagerSlot taskManagerSlot = iterator.next();

				if (taskManagerSlot.isMatchingRequirement(requestResourceProfile)) {
					iterator.remove();
					return taskManagerSlot;
				}
			}

			return null;
		}
	}

	private static final class SlotsPerTaskManagerComparator implements Comparator<SlotsPerTaskManager>, Serializable {
		private static final long serialVersionUID = 276433884059695446L;

		@Override
		public int compare(SlotsPerTaskManager a, SlotsPerTaskManager b) {
			return a.getNumberSlots() * b.getNumberAvailableSlots() - a.getNumberAvailableSlots() * b.getNumberSlots();
		}
	}
}
