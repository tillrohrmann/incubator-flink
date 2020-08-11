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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.slotsbro.ResourceRequirements;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Default {@link RequirementsTracker} implementation.
 */
public class DefaultRequirementsTracker implements RequirementsTracker {

	private final Map<JobID, JobResources> jobResources = new LinkedHashMap<>();

	private final Map<JobID, ResourceRequirements> resourceRequirementsByJob = new LinkedHashMap<>();

	private final SlotAllocator slotAllocator;

	public DefaultRequirementsTracker(SlotAllocator slotAllocator) {
		this.slotAllocator = slotAllocator;
	}

	@Override
	public void clear() {
		jobResources.clear();
		resourceRequirementsByJob.clear();
	}

	// ---------------------------------------------------------------------------------------------
	// Event triggers
	// ---------------------------------------------------------------------------------------------

	@Override
	public void notifySlotStatusChange(DeclarativeTaskManagerSlot.State previous, DeclarativeTaskManagerSlot.State current, JobID jobId, ResourceProfile resourceProfile) {
		if (previous == current) {
			return;
		}
		findAndRemoveMatchingResource(jobId, resourceProfile, mapSlotState(previous));
		addResource(jobId, resourceProfile, mapSlotState(current));
	}

	// ---------------------------------------------------------------------------------------------
	// Requirement processing
	// ---------------------------------------------------------------------------------------------

	@Override
	public void processResourceRequirements(ResourceRequirements resourceRequirements) {
		if (internalProcessResourceRequirements(resourceRequirements)) {
			checkResourceRequirements();
		}
	}

	private boolean internalProcessResourceRequirements(ResourceRequirements resourceRequirements) {
		if (resourceRequirements.getResourceRequirements().isEmpty()) {
			jobResources.remove(resourceRequirements.getJobId());
			resourceRequirementsByJob.remove(resourceRequirements.getJobId());
			return true;
		}

		ResourceRequirements previousResourceRequirements = this.resourceRequirementsByJob.put(resourceRequirements.getJobId(), resourceRequirements);
		if (previousResourceRequirements != null) {
			Optional<ResourceRequirements> newlyRequiredResources = computeNewlyRequiredResources(previousResourceRequirements, resourceRequirements);
			if (newlyRequiredResources.isPresent()) {
				addMissingResourceEntriesFor(newlyRequiredResources.get());
				return true;
			}
		} else {
			addMissingResourceEntriesFor(resourceRequirements);
			return true;
		}
		return false;
	}

	private void addMissingResourceEntriesFor(ResourceRequirements requirements) {
		final JobResources jobResources = this.jobResources.computeIfAbsent(
			requirements.getJobId(),
			ignored -> new JobResources());

		for (ResourceRequirement resourceRequirement : requirements.getResourceRequirements()) {
			for (int x = 0; x < resourceRequirement.getNumberOfRequiredSlots(); x++) {
				jobResources.addResource(resourceRequirement.getResourceProfile(), JobResourceState.MISSING);
			}
		}
	}

	private Optional<ResourceRequirements> computeNewlyRequiredResources(ResourceRequirements previousResourceRequirements, ResourceRequirements currentResourceRequirements) {
		final Collection<ResourceRequirement> newlyRequiredResources = new ArrayList<>();

		final Map<ResourceProfile, ResourceRequirement> previousByProfile = previousResourceRequirements.getResourceRequirements().stream().collect(Collectors.toMap(ResourceRequirement::getResourceProfile, x -> x));
		for (ResourceRequirement current : currentResourceRequirements.getResourceRequirements()) {
			ResourceRequirement previous = previousByProfile.get(current.getResourceProfile());
			int numSlotsDifference = current.getNumberOfRequiredSlots() - previous.getNumberOfRequiredSlots();
			if (numSlotsDifference > 0) {
				newlyRequiredResources.add(new ResourceRequirement(current.getResourceProfile(), numSlotsDifference));
			}
		}

		return newlyRequiredResources.isEmpty()
			? Optional.empty()
			: Optional.of(new ResourceRequirements(currentResourceRequirements.getJobId(), currentResourceRequirements.getTargetAddress(), newlyRequiredResources));
	}

	@Override
	public void checkResourceRequirements() {
		checkWhetherAnyResourceRequirementsAreOverBudget();
		checkWhetherAnyResourceRequirementsAreUnderBudget();
		checkWhetherAnyResourceRequirementsCanBeFulfilled();
	}

	private void checkWhetherAnyResourceRequirementsAreOverBudget() {
		// TODO
	}

	private void checkWhetherAnyResourceRequirementsAreUnderBudget() {
		// TODO
	}

	private void checkWhetherAnyResourceRequirementsCanBeFulfilled() {
		// only process resource for which we have requirements, as there are edge-cases where slots can be assigned
		// to a job without has having a requirement for the corresponding job
		for (Map.Entry<JobID, ResourceRequirements> jobRequirements : resourceRequirementsByJob.entrySet()) {
			final JobID jobId = jobRequirements.getKey();
			final JobResources resources = jobResources.get(jobId);
			final Collection<ResourceRequirement> missingResources = resources.getMissingResources();

			if (!missingResources.isEmpty()) {
				slotAllocator.requestSlotAllocations(jobId, resourceRequirementsByJob.get(jobId).getTargetAddress(), missingResources);
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Data-structure utilities
	// ---------------------------------------------------------------------------------------------

	private JobResources getJobResources(JobID jobId) {
		return jobResources.computeIfAbsent(jobId, ignored -> new JobResources());
	}

	private void addResource(JobID jobId, ResourceProfile resourceProfile, JobResourceState resourceState) {
		getJobResources(jobId).addResource(resourceProfile, resourceState);
	}

	private void findAndRemoveMatchingResource(JobID jobId, ResourceProfile profile, JobResourceState resourceState) {
		getJobResources(jobId).findAndRemoveMatchingResource(profile, resourceState);
	}

	// ---------------------------------------------------------------------------------------------
	// Utilities
	// ---------------------------------------------------------------------------------------------

	private static JobResourceState mapSlotState(DeclarativeTaskManagerSlot.State state) {
		switch (state) {
			case FREE:
				return JobResourceState.MISSING;
			case PENDING:
				return JobResourceState.PENDING;
			case ALLOCATED:
				return JobResourceState.ALLOCATED;
		}
		throw new IllegalStateException("Unknown TaskManagerSlot state :" + state);
	}

	// ---------------------------------------------------------------------------------------------
	// Testing
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	int getNumResources(JobID jobId, JobResourceState state) {
		return getJobResources(jobId).getNumResources(state);
	}
}
