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
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;

/**
 *
 */
public interface DeclarativeSlotPoolNg {
	void increaseResourceRequirementsBy(ResourceCounter increment);

	void decreaseResourceRequirementsBy(ResourceCounter decrement);

	Collection<ResourceRequirement> getResourceRequirements();

	Collection<SlotOffer> offerSlots(Collection<SlotOffer> offers, TaskManagerLocation taskManagerLocation, TaskManagerGateway taskManagerGateway, long currentTime);

	Collection<SlotInfoWithUtilization> getFreeSlotsInformation();

	Collection<? extends SlotInfo> getAllSlotsInformation();

	void failSlots(ResourceID resourceId, Exception cause);

	void failSlot(AllocationID allocationID, Exception cause);

	PhysicalSlot allocateFreeSlot(AllocationID allocationID);

	void releaseSlot(AllocationID allocationId, Throwable cause, long currentTime);

	boolean containsSlots(ResourceID resourceId);

	void returnIdleSlots(long relativeTimeMillis);
}
