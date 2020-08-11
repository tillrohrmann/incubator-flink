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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;

import java.util.Collection;

/**
 * Interface for a component that can allocate slots.
 */
interface SlotAllocator {

	/**
	 * Requests the allocation of slots for the given job, matching the given resource profiles.
	 *
	 * <p>This method communicates a desire for more slots, and gives no guarantees for the actual allocation.
	 * Slots may or may not be allocated, immediately or at some point in the future, fulfilling some or all of the
	 * requirements.
	 * It is expected that components using this interface have another way to be informed about occurring allocations.
	 *
	 * @param jobId job to allacate the slots for
	 * @param targetAddress address of the jobmaster for the job
	 * @param requirements required slots
	 */
	void requestSlotAllocations(JobID jobId, String targetAddress, Collection<ResourceRequirement> requirements);
}
