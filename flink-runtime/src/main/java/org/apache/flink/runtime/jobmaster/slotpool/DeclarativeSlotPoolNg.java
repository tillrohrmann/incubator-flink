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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Slot pool interface which uses Flink's declarative resource management
 * protocol to acquire resources.
 *
 * <p>In order to acquire new resources, users need to increase the required
 * resources. Once they no longer need the resources, users need to decrease
 * the required resources so that superfluous resources can be returned.
 */
public interface DeclarativeSlotPoolNg {

	/**
	 * Increases the resource requirements by increment.
	 *
	 * @param increment increment by which to increase the resource requirements
	 */
	void increaseResourceRequirementsBy(ResourceCounter increment);

	/**
	 * Decreases the resource requirements by decrement.
	 *
	 * @param decrement decrement by which to decrease the resource requirements
	 */
	void decreaseResourceRequirementsBy(ResourceCounter decrement);

	/**
	 * Gets the current resource requirements.
	 *
	 * @return current resource requirements
	 */
	Collection<ResourceRequirement> getResourceRequirements();

	/**
	 * Offers slots to this slot pool. The slot pool is free to accept as many slots as it
	 * needs.
	 *
	 * @param offers offers containing the list of slots offered to this slot pool
	 * @param taskManagerLocation taskManagerLocation is the location of the offering TaskExecutor
	 * @param taskManagerGateway taskManagerGateway is the gateway to talk to the offering TaskExecutor
	 * @param currentTime currentTime is the time the slots are being offered
	 * @return collection of accepted slots; the other slot offers are implicitly rejected
	 */
	Collection<SlotOffer> offerSlots(Collection<? extends SlotOffer> offers, TaskManagerLocation taskManagerLocation, TaskManagerGateway taskManagerGateway, long currentTime);

	/**
	 * Gets the slot information for all free slots (slots which can be allocated from the slot pool).
	 *
	 * @return collection of free slot information
	 */
	Collection<SlotInfoWithUtilization> getFreeSlotsInformation();

	/**
	 * Gets the slot information for all slots (free and allocated slots).
	 *
	 * @return collection of slot information
	 */
	Collection<? extends SlotInfo> getAllSlotsInformation();

	/**
	 * Fails all slots belonging to the owning TaskExecutor if it has been registered.
	 * This will also decrease the required resources by the set of failed slots.
	 *
	 * @param owner owner identifying the owning TaskExecutor
	 * @param cause cause for failing the slots
	 */
	// TODO: Don't decrease resource requirements here, move the responsibility to another component (user of this class)
	void failSlots(ResourceID owner, Exception cause);

	/**
	 * Fails the slot specified by allocationId if one exists. This will also decrease
	 * the required resources by the {@link ResourceProfile} of the failed
	 * slot.
	 *
	 * @param allocationId allocationId identifying the slot to fail
	 * @param cause cause for failing the slot
	 */
	// TODO: Don't decrease resource requirements here, move the responsibility to another component (user of this class)
	void failSlot(AllocationID allocationId, Exception cause);

	/**
	 * Allocates a free slot identified by the given allocationId.
	 *
	 * @param allocationId allocationId identifies the free slot to allocate
	 * @return a PhysicalSlot representing the allocated slot
	 * @throw IllegalStateException if no free slot with the given allocationId exists
	 */
	PhysicalSlot allocateFreeSlot(AllocationID allocationId);

	/**
	 * Releases a slot identified by the given allocationId. If no allocated slot
	 * with allocationId exists, then the call is ignored. If a slot exists, then
	 * the resource requirements will be reduced by the slot's matched {@link ResourceProfile}.
	 *
	 * <p>Whether the freed slot is returned to the owning TaskExecutor is implementation
	 * dependent.
	 *
	 * @param allocationId allocationId identifying the slot to release
	 * @param cause cause for releasing the slot; can be {@code null}
	 * @param currentTime currentTime when the slot was released
	 */
	// TODO: Don't decrease resource requirements here, move the responsibility to another component (user of this class)
	void releaseSlot(AllocationID allocationId, @Nullable Throwable cause, long currentTime);

	/**
	 * True if the slot pool has a slot registered which is owned by the given
	 * TaskExecutor.
	 *
	 * @param owner owner identifying the TaskExecutor for which to check whether the
	 *                 slot pool has some slots registered
	 * @return true if the given TaskExecutor has a slot registered at the slot pool
	 */
	boolean containsSlots(ResourceID owner);

	/**
	 * Returns idle slots, which have exceeded the idle slot timeout and are no longer
	 * needed to fulfill the resource requirements, to their owners.
	 *
	 * @param relativeTimeMillis relativeTimeMillis denotes the current time
	 */
	void returnIdleSlots(long relativeTimeMillis);

	/**
	 * Allocates a free slot identified by the given allocationId and maps it to
	 * the given requiredSlotProfile. Moreover, this method increase the required
	 * resources by the given requiredSlotProfile
	 *
	 * @param allocationId allocationId identifies the free slot to allocate
	 * @param requiredSlotProfile requiredSlotProfile specifying the resource requirement
	 * @return a PhysicalSlot representing the allocated slot
	 * @throw IllegalStateException if no free slot with the given allocationId exists or if
	 * the specified slot cannot fulfill the requiredSlotProfile
	 */
	// TODO: Try to get rid of this method; a prerequisite is probably a non static matching between the resource requirements
	//  and slots, meaning that a slot can match against different resource requirements at different points in time
	PhysicalSlot allocateFreeSlotForResource(AllocationID allocationId, ResourceProfile requiredSlotProfile);
}
