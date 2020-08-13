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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.slotsbro.ResourceRequirements;

import java.util.Collection;

/**
 * A tracker for {@link ResourceRequirements}.
 */
public interface RequirementsTracker {

	/**
	 * Notifies the tracker about a new or updated {@link ResourceRequirements}.
	 *
	 * @param resourceRequirements new or updated resource requirements
	 */
	void notifyResourceRequirements(ResourceRequirements resourceRequirements);

	/**
	 * Notifies the tracker about a change in the state of a slot.
	 *
	 * @param previous previous state of the slot
	 * @param current new state of the slot
	 * @param jobId job the slot is allocated for
	 * @param resourceProfile resource profile of the slot
	 */
	void notifySlotStatusChange(DeclarativeTaskManagerSlot.State previous, DeclarativeTaskManagerSlot.State current, JobID jobId, ResourceProfile resourceProfile);

	/**
	 * Returns a collection of {@link ResourceRequirements} that describe which resources the corresponding job is
	 * in need/excess of.
	 *
	 * @return required/exceeding resources for each jobs
	 */
	Collection<ResourceRequirements> getExceedingOrRequiredResources();

	/**
	 * Returns a collection of {@link ResourceRequirement}s that describe which resources have been assigned to a job.
	 *
	 * @param jobId job ID
	 * @return required/exceeding resources for each jobs
	 */
	Collection<ResourceRequirement> getAcquiredResources(JobID jobId);

	/**
	 * Removes all state from the tracker.
	 */
	void clear();
}
