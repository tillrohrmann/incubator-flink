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

import java.util.Collection;
import java.util.Optional;

/**
 * A {@link SlotMatchingStrategy} variant that is not aware of task executors.
 */
@FunctionalInterface
public interface TaskManagerAgnosticSlotMatchingStrategy {
	/**
	 * Finds a matching slot for the requested {@link ResourceProfile} given the
	 * collection of free slots.
	 *
	 * @param requestedProfile to find a matching slot for
	 * @param freeSlots        collection of free slots
	 * @return Returns a matching slots or {@link Optional#empty()} if there is none
	 */
	<T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
		ResourceProfile requestedProfile,
		Collection<T> freeSlots);

}
