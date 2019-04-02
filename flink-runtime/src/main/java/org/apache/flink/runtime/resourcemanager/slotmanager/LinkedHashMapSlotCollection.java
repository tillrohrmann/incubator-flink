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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Default implementation of {@link SlotCollection}.
 */
public class LinkedHashMapSlotCollection implements SlotCollection {
	private final LinkedHashMap<SlotID, TaskManagerSlot> slots;

	public LinkedHashMapSlotCollection() {
		this.slots = new LinkedHashMap<>(16);
	}

	@Override
	public int size() {
		return slots.size();
	}

	@Nullable
	@Override
	public TaskManagerSlot findMatchingSlot(ResourceProfile requestResourceProfile) {
		Iterator<Map.Entry<SlotID, TaskManagerSlot>> iterator = slots.entrySet().iterator();

		while (iterator.hasNext()) {
			TaskManagerSlot taskManagerSlot = iterator.next().getValue();

			// sanity check
			Preconditions.checkState(
				taskManagerSlot.getState() == TaskManagerSlot.State.FREE,
				"TaskManagerSlot %s is not in state FREE but %s.",
				taskManagerSlot.getSlotId(), taskManagerSlot.getState());

			if (taskManagerSlot.getResourceProfile().isMatching(requestResourceProfile)) {
				iterator.remove();
				return taskManagerSlot;
			}
		}

		return null;
	}

	@Override
	public void remove(SlotID slotId) {
		slots.remove(slotId);
	}

	@Override
	public void put(SlotID slotId, TaskManagerSlot freeSlot) {
		slots.put(slotId, freeSlot);
	}

	@Override
	public void unregisterTaskManager(ResourceID resourceID) {
		// no op
	}

	@Override
	public void registerTaskManager(ResourceID resourceID, int numberSlots) {
		// no op
	}
}
