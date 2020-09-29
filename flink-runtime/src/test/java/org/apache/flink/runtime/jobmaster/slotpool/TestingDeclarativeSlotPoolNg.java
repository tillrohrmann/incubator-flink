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
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.function.QuadFunction;
import org.apache.flink.util.function.TriConsumer;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Testing {@link DeclarativeSlotPoolNg} implementation.
 */
final class TestingDeclarativeSlotPoolNg implements DeclarativeSlotPoolNg {

	private final Consumer<ResourceCounter> increaseResourceRequirementsByConsumer;

	private final Consumer<ResourceCounter> decraseResourceRequirementsByConsumer;

	private final Supplier<Collection<ResourceRequirement>> getResourceRequirementsSupplier;

	private final QuadFunction<Collection<? extends SlotOffer>, TaskManagerLocation, TaskManagerGateway, Long, Collection<SlotOffer>> offerSlotsFunction;

	private final Supplier<Collection<SlotInfoWithUtilization>> getFreeSlotsInformationSupplier;

	private final Supplier<Collection<? extends SlotInfo>> getAllSlotsInformationSupplier;

	private final BiConsumer<ResourceID, Exception> failSlotsConsumer;

	private final BiConsumer<AllocationID, Exception> failSlotConsumer;

	private final Function<AllocationID, PhysicalSlot> allocateFreeSlotFunction;

	private final BiFunction<AllocationID, ResourceProfile, PhysicalSlot> allocateFreeSlotForResourceFunction;

	private final TriConsumer<AllocationID, Throwable, Long> releaseSlotConsumer;

	private final Function<ResourceID, Boolean> containsSlotsFunction;

	private final LongConsumer returnIdleSlotsConsumer;

	TestingDeclarativeSlotPoolNg(
		Consumer<ResourceCounter> increaseResourceRequirementsByConsumer,
		Consumer<ResourceCounter> decraseResourceRequirementsByConsumer,
		Supplier<Collection<ResourceRequirement>> getResourceRequirementsSupplier,
		QuadFunction<Collection<? extends SlotOffer>, TaskManagerLocation, TaskManagerGateway, Long, Collection<SlotOffer>> offerSlotsFunction,
		Supplier<Collection<SlotInfoWithUtilization>> getFreeSlotsInformationSupplier,
		Supplier<Collection<? extends SlotInfo>> getAllSlotsInformationSupplier,
		BiConsumer<ResourceID, Exception> failSlotsConsumer,
		BiConsumer<AllocationID, Exception> failSlotConsumer,
		Function<AllocationID, PhysicalSlot> allocateFreeSlotFunction,
		BiFunction<AllocationID, ResourceProfile, PhysicalSlot> allocateFreeSlotForResourceFunction,
		TriConsumer<AllocationID, Throwable, Long> releaseSlotConsumer,
		Function<ResourceID, Boolean> containsSlotsFunction,
		LongConsumer returnIdleSlotsConsumer) {
		this.increaseResourceRequirementsByConsumer = increaseResourceRequirementsByConsumer;
		this.decraseResourceRequirementsByConsumer = decraseResourceRequirementsByConsumer;
		this.getResourceRequirementsSupplier = getResourceRequirementsSupplier;
		this.offerSlotsFunction = offerSlotsFunction;
		this.getFreeSlotsInformationSupplier = getFreeSlotsInformationSupplier;
		this.getAllSlotsInformationSupplier = getAllSlotsInformationSupplier;
		this.failSlotsConsumer = failSlotsConsumer;
		this.failSlotConsumer = failSlotConsumer;
		this.allocateFreeSlotFunction = allocateFreeSlotFunction;
		this.allocateFreeSlotForResourceFunction = allocateFreeSlotForResourceFunction;
		this.releaseSlotConsumer = releaseSlotConsumer;
		this.containsSlotsFunction = containsSlotsFunction;
		this.returnIdleSlotsConsumer = returnIdleSlotsConsumer;
	}

	@Override
	public void increaseResourceRequirementsBy(ResourceCounter increment) {
		increaseResourceRequirementsByConsumer.accept(increment);
	}

	@Override
	public void decreaseResourceRequirementsBy(ResourceCounter decrement) {
		decraseResourceRequirementsByConsumer.accept(decrement);
	}

	@Override
	public Collection<ResourceRequirement> getResourceRequirements() {
		return getResourceRequirementsSupplier.get();
	}

	@Override
	public Collection<SlotOffer> offerSlots(Collection<? extends SlotOffer> offers, TaskManagerLocation taskManagerLocation, TaskManagerGateway taskManagerGateway, long currentTime) {
		return offerSlotsFunction.apply(offers, taskManagerLocation, taskManagerGateway, currentTime);
	}

	@Override
	public Collection<SlotInfoWithUtilization> getFreeSlotsInformation() {
		return getFreeSlotsInformationSupplier.get();
	}

	@Override
	public Collection<? extends SlotInfo> getAllSlotsInformation() {
		return getAllSlotsInformationSupplier.get();
	}

	@Override
	public void failSlots(ResourceID owner, Exception cause) {
		failSlotsConsumer.accept(owner, cause);
	}

	@Override
	public void failSlot(AllocationID allocationId, Exception cause) {
		failSlotConsumer.accept(allocationId, cause);
	}

	@Override
	public PhysicalSlot allocateFreeSlot(AllocationID allocationId) {
		return allocateFreeSlotFunction.apply(allocationId);
	}

	@Override
	public PhysicalSlot allocateFreeSlotForResource(AllocationID allocationId, ResourceProfile requiredSlotProfile) {
		return allocateFreeSlotForResourceFunction.apply(allocationId, requiredSlotProfile);
	}

	@Override
	public void releaseSlot(AllocationID allocationId, @Nullable Throwable cause, long currentTime) {
		releaseSlotConsumer.accept(allocationId, cause, currentTime);
	}

	@Override
	public boolean containsSlots(ResourceID owner) {
		return containsSlotsFunction.apply(owner);
	}

	@Override
	public void returnIdleSlots(long relativeTimeMillis) {
		returnIdleSlotsConsumer.accept(relativeTimeMillis);
	}

	public static TestingDeclarativeSlotPoolNgBuilder builder() {
		return new TestingDeclarativeSlotPoolNgBuilder();
	}
}
