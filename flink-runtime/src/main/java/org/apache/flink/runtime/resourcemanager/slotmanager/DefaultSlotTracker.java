/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * Default SlotTracker implementation.
 */
public class DefaultSlotTracker {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultSlotTracker.class);

	/**
	 * Map for all registered slots.
	 */
	private final HashMap<SlotID, DeclarativeTaskManagerSlot> slots = new HashMap<>();

	/**
	 * Index of all currently free slots.
	 */
	private final LinkedHashMap<SlotID, DeclarativeTaskManagerSlot> freeSlots = new LinkedHashMap<>();

	private final TaskManagerAgnosticSlotMatchingStrategy slotMatchingStrategy;

	private final SlotStatusUpdateListener slotStatusUpdateListener;

	public DefaultSlotTracker(TaskManagerAgnosticSlotMatchingStrategy slotMatchingStrategy, SlotStatusUpdateListener slotStatusUpdateListener) {
		this.slotMatchingStrategy = slotMatchingStrategy;
		this.slotStatusUpdateListener = slotStatusUpdateListener;
	}

	public void addSlot(
		SlotID slotId,
		JobID jobId,
		ResourceProfile resourceProfile,
		TaskExecutorConnection taskManagerConnection) {

		if (slots.containsKey(slotId)) {
			// remove the old slot first
			removeSlot(slotId);
		}

		createAndRegisterTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);
		notifySlotAllocation(slotId, jobId);
	}

	public void notifyAllocationStart(SlotID slotId, JobID jobId) {
		transitionSlotToPending(slots.get(slotId), jobId);
	}

	private void createAndRegisterTaskManagerSlot(SlotID slotId, ResourceProfile resourceProfile, TaskExecutorConnection taskManagerConnection) {
		final DeclarativeTaskManagerSlot slot = new DeclarativeTaskManagerSlot(
			slotId,
			resourceProfile,
			taskManagerConnection);
		slots.put(slotId, slot);
	}

	public boolean freeSlot(SlotID slotId) {
		DeclarativeTaskManagerSlot slot = slots.get(slotId);

		if (null != slot) {
			transitionSlotToFree(slot, false);
		} else {
			LOG.debug("Trying to free a slot {} which has not been registered. Ignoring this message.", slotId);
		}
		return false;
	}

	/**
	 * Removes the given set of slots from the slot manager.
	 *
	 * @param slotsToRemove identifying the slots to remove from the slot manager
	 */
	public void removeSlots(Iterable<SlotID> slotsToRemove) {
		for (SlotID slotId : slotsToRemove) {
			removeSlot(slotId);
		}
	}

	/**
	 * Removes the given slot from the slot manager.
	 *
	 * @param slotId identifying the slot to remove
	 */
	private void removeSlot(SlotID slotId) {
		DeclarativeTaskManagerSlot slot = slots.remove(slotId);

		if (null != slot) {
			transitionSlotToFree(slot, false);
			freeSlots.remove(slotId);
		} else {
			LOG.debug("There was no slot registered with slot id {}.", slotId);
		}
	}

	public boolean notifySlotAllocation(SlotID slotId, JobID jobId) {
		LOG.debug("Notify slot allocation {} for job {}.", slotId, jobId);
		return internalUpdateSlot(slotId, jobId, false);
	}

	public void notifySlotReport(SlotReport slotReport) {
		for (SlotStatus slotStatus : slotReport) {
			internalUpdateSlot(slotStatus.getSlotID(), slotStatus.getJobID(), true);
		}
	}

	private boolean internalUpdateSlot(SlotID slotId, JobID jobId, boolean ignoreTransitionFromPending) {
		final DeclarativeTaskManagerSlot slot = slots.get(slotId);

		if (slot != null) {
			if (jobId == null) {
				transitionSlotToFree(slot, ignoreTransitionFromPending);
			} else {
				transitionsSlotToAllocated(slot, jobId);
			}

			return true;
		} else {
			LOG.debug("Trying to update unknown slot with slot id {}.", slotId);

			return false;
		}
	}

	private void transitionSlotToPending(DeclarativeTaskManagerSlot slot, JobID jobId) {
		switch (slot.getState()) {
			case FREE:
				// the slot is currently free --> it is stored in freeSlots
				freeSlots.remove(slot.getSlotId());
				slot.startAllocation(jobId);
				slotStatusUpdateListener.notifySlotStatusChange(slot, DeclarativeTaskManagerSlot.State.FREE, DeclarativeTaskManagerSlot.State.PENDING, jobId, slot.getResourceProfile());
				break;
			case PENDING:
				break;
			case ALLOCATED:
				transitionSlotToFree(slot, true);
				transitionSlotToPending(slot, jobId);
				break;
		}
	}

	private void transitionsSlotToAllocated(DeclarativeTaskManagerSlot slot, JobID jobId) {
		switch (slot.getState()) {
			case PENDING:
				if (!jobId.equals(slot.getJobId())) {
					transitionSlotToFree(slot, false);
					transitionSlotToPending(slot, jobId);
					transitionsSlotToAllocated(slot, jobId);
				} else {
					slotStatusUpdateListener.notifySlotStatusChange(slot, DeclarativeTaskManagerSlot.State.PENDING, DeclarativeTaskManagerSlot.State.ALLOCATED, jobId, slot.getResourceProfile());
					slot.completeAllocation();
				}
				break;
			case ALLOCATED:
				break;
			case FREE:
				transitionSlotToPending(slot, jobId);
				transitionsSlotToAllocated(slot, jobId);
				break;
		}
	}

	private void transitionSlotToFree(DeclarativeTaskManagerSlot slot, boolean ignoreTransitionFromPending) {
		switch (slot.getState()) {
			case FREE:
				handleFreeSlot(slot);
				break;
			case PENDING:
				if (!ignoreTransitionFromPending) {
					internalFreeSlot(slot);
				}
				// don't do anything because we expect the slot to be allocated soon
				break;
			case ALLOCATED:
				internalFreeSlot(slot);
				break;
		}
	}

	private void internalFreeSlot(DeclarativeTaskManagerSlot slot) {
		slotStatusUpdateListener.notifySlotStatusChange(slot, slot.getState(), DeclarativeTaskManagerSlot.State.FREE, slot.getJobId(), slot.getResourceProfile());
		switch (slot.getState()) {
			case FREE:
				break;
			case PENDING:
				slot.cancelAllocation();
				handleFreeSlot(slot);
				break;
			case ALLOCATED:
				slot.freeSlot();
				handleFreeSlot(slot);
		}
	}

	/**
	 * Handles a free slot. It first tries to find a pending slot request which can be fulfilled.
	 * If there is no such request, then it will add the slot to the set of free slots.
	 *
	 * @param freeSlot to find a new slot request for
	 */
	private void handleFreeSlot(DeclarativeTaskManagerSlot freeSlot) {
		Preconditions.checkState(freeSlot.getState() == DeclarativeTaskManagerSlot.State.FREE);

		freeSlots.put(freeSlot.getSlotId(), freeSlot);
	}

	public Optional<DeclarativeTaskManagerSlot> findAndReserveMatchingSlot(ResourceProfile requestResourceProfile) {
		final Optional<DeclarativeTaskManagerSlot> optionalMatchingSlot = slotMatchingStrategy.findMatchingSlot(
			requestResourceProfile,
			freeSlots.values());

		optionalMatchingSlot.ifPresent(taskManagerSlot -> {
			// sanity check
			Preconditions.checkState(
				taskManagerSlot.getState() == DeclarativeTaskManagerSlot.State.FREE,
				"TaskManagerSlot %s is not in state FREE but %s.",
				taskManagerSlot.getSlotId(), taskManagerSlot.getState());

			freeSlots.remove(taskManagerSlot.getSlotId());
		});

		return optionalMatchingSlot;
	}

	public int getNumberOfFreeSlots() {
		return freeSlots.size();
	}

	@VisibleForTesting
	public DeclarativeTaskManagerSlot getSlot(SlotID slotId) {
		return slots.get(slotId);
	}
}
