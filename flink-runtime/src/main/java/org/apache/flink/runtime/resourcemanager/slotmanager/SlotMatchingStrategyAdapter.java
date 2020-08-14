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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

/**
 * An adapter to use a {@link SlotMatchingStrategy} as a {@link TaskManagerAgnosticSlotMatchingStrategy}.
 */
public class SlotMatchingStrategyAdapter implements TaskManagerAgnosticSlotMatchingStrategy {

	private final SlotMatchingStrategy slotMatchingStrategy;
	private final Function<InstanceID, Integer> numberRegisteredSlotsLookup;

	public SlotMatchingStrategyAdapter(SlotMatchingStrategy slotMatchingStrategy, Function<InstanceID, Integer> numberRegisteredSlotsLookup) {
		this.slotMatchingStrategy = slotMatchingStrategy;
		this.numberRegisteredSlotsLookup = numberRegisteredSlotsLookup;
	}

	@Override
	public <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(ResourceProfile requestedProfile, Collection<T> freeSlots) {
		return slotMatchingStrategy.findMatchingSlot(requestedProfile, freeSlots, numberRegisteredSlotsLookup);
	}
}
