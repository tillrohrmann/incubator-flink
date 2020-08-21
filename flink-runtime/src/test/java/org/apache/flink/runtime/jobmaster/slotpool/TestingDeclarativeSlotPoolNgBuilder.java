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
import org.apache.flink.util.function.QuadFunction;
import org.apache.flink.util.function.TriConsumer;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Builder for {@link TestingDeclarativeSlotPoolNg}.
 */
public class TestingDeclarativeSlotPoolNgBuilder {

	private Consumer<ResourceCounter> increaseResourceRequirementsByConsumer = ignored -> {};
	private Consumer<ResourceCounter> decraseResourceRequirementsByConsumer = ignored -> {};
	private Supplier<Collection<ResourceRequirement>> getResourceRequirementsSupplier = Collections::emptyList;
	private QuadFunction<Collection<? extends SlotOffer>, TaskManagerLocation, TaskManagerGateway, Long, Collection<SlotOffer>> offerSlotsFunction = (ignoredA, ignoredB, ignoredC, ignoredD) -> Collections.emptyList();
	private Supplier<Collection<SlotInfoWithUtilization>> getFreeSlotsInformationSupplier = Collections::emptyList;
	private Supplier<Collection<? extends SlotInfo>> getAllSlotsInformationSupplier = Collections::emptyList;
	private BiConsumer<ResourceID, Exception> failSlotsConsumer = (ignoredA, ignoredB) -> {};
	private BiConsumer<AllocationID, Exception> failSlotConsumer = (ignoredA, ignoredB) -> {};
	private Function<AllocationID, PhysicalSlot> allocateFreeSlotFunction = ignored -> null;
	private TriConsumer<AllocationID, Throwable, Long> releaseSlotConsumer = (ignoredA, ignoredB, ignoredC) -> {};
	private Function<ResourceID, Boolean> containsSlotsFunction = ignored -> false;
	private LongConsumer returnIdleSlotsConsumer = ignored -> {};
	private BiFunction<AllocationID, ResourceProfile, PhysicalSlot> allocateFreeSlotForResourceFunction = (ignoredA, ignoredB) -> null;

	public TestingDeclarativeSlotPoolNgBuilder setIncreaseResourceRequirementsByConsumer(Consumer<ResourceCounter> increaseResourceRequirementsByConsumer) {
		this.increaseResourceRequirementsByConsumer = increaseResourceRequirementsByConsumer;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setDecraseResourceRequirementsByConsumer(Consumer<ResourceCounter> decraseResourceRequirementsByConsumer) {
		this.decraseResourceRequirementsByConsumer = decraseResourceRequirementsByConsumer;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setGetResourceRequirementsSupplier(Supplier<Collection<ResourceRequirement>> getResourceRequirementsSupplier) {
		this.getResourceRequirementsSupplier = getResourceRequirementsSupplier;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setOfferSlotsFunction(QuadFunction<Collection<? extends SlotOffer>, TaskManagerLocation, TaskManagerGateway, Long, Collection<SlotOffer>> offerSlotsFunction) {
		this.offerSlotsFunction = offerSlotsFunction;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setGetFreeSlotsInformationSupplier(Supplier<Collection<SlotInfoWithUtilization>> getFreeSlotsInformationSupplier) {
		this.getFreeSlotsInformationSupplier = getFreeSlotsInformationSupplier;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setGetAllSlotsInformationSupplier(Supplier<Collection<? extends SlotInfo>> getAllSlotsInformationSupplier) {
		this.getAllSlotsInformationSupplier = getAllSlotsInformationSupplier;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setFailSlotsConsumer(BiConsumer<ResourceID, Exception> failSlotsConsumer) {
		this.failSlotsConsumer = failSlotsConsumer;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setFailSlotConsumer(BiConsumer<AllocationID, Exception> failSlotConsumer) {
		this.failSlotConsumer = failSlotConsumer;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setAllocateFreeSlotFunction(Function<AllocationID, PhysicalSlot> allocateFreeSlotFunction) {
		this.allocateFreeSlotFunction = allocateFreeSlotFunction;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setAllocateFreeSlotForResourceFunction(BiFunction<AllocationID, ResourceProfile, PhysicalSlot> allocateFreeSlotForResourceFunction) {
		this.allocateFreeSlotForResourceFunction = allocateFreeSlotForResourceFunction;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setReleaseSlotConsumer(TriConsumer<AllocationID, Throwable, Long> releaseSlotConsumer) {
		this.releaseSlotConsumer = releaseSlotConsumer;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setContainsSlotsFunction(Function<ResourceID, Boolean> containsSlotsFunction) {
		this.containsSlotsFunction = containsSlotsFunction;
		return this;
	}

	public TestingDeclarativeSlotPoolNgBuilder setReturnIdleSlotsConsumer(LongConsumer returnIdleSlotsConsumer) {
		this.returnIdleSlotsConsumer = returnIdleSlotsConsumer;
		return this;
	}

	public TestingDeclarativeSlotPoolNg build() {
		return new TestingDeclarativeSlotPoolNg(
			increaseResourceRequirementsByConsumer,
			decraseResourceRequirementsByConsumer,
			getResourceRequirementsSupplier,
			offerSlotsFunction,
			getFreeSlotsInformationSupplier,
			getAllSlotsInformationSupplier,
			failSlotsConsumer,
			failSlotConsumer,
			allocateFreeSlotFunction,
			allocateFreeSlotForResourceFunction,
			releaseSlotConsumer,
			containsSlotsFunction,
			returnIdleSlotsConsumer);
	}
}
