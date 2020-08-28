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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Default {@link RequirementsTracker} implementation.
 * TODO: add more logging, tests, package-private test method for checking that maps are empty to prevent leaks
 */
public class DefaultRequirementsTracker implements RequirementsTracker {

	private final Map<JobID, JobResources> jobResources = new LinkedHashMap<>();

	private final Map<JobID, ResourceRequirements> resourceRequirementsByJob = new LinkedHashMap<>();

	@Override
	public void clear() {
		jobResources.clear();
		resourceRequirementsByJob.clear();
	}

	// ---------------------------------------------------------------------------------------------
	// Event triggers
	// ---------------------------------------------------------------------------------------------

	public void notifyAcquiredResource(JobID jobId, ResourceProfile resourceProfile) {
		findAndRemoveMatchingResource(jobId, resourceProfile, JobResourceState.MISSING);
		addResource(jobId, resourceProfile, JobResourceState.ACQUIRED);
	}

	public void notifyLostResource(JobID jobId, ResourceProfile resourceProfile) {
		findAndRemoveMatchingResource(jobId, resourceProfile, JobResourceState.ACQUIRED);

		// TODO: a lookup would be useful for this
		final Optional<ResourceRequirement> correspondingRequirement = Optional.ofNullable(resourceRequirementsByJob.get(jobId))
			.map(ResourceRequirements::getResourceRequirements)
			.orElse(Collections.emptyList())
			.stream()
			// the comparison order is important; the requirement must not come first since it contains UNKNOWN which doesn't match anything
			.filter(req -> resourceProfile.isMatching(req.getResourceProfile()))
			.findAny();

		final int numberOfRequiredSlots = correspondingRequirement.map(ResourceRequirement::getNumberOfRequiredSlots).orElse(0);
		if (numberOfRequiredSlots > 0) {

			final JobResources jobResources = getJobResources(jobId);
			// TODO: the underlying problem here could be the explicit notion of missing resources
			// TODO: if they were implicitly defined via requirements-acquiredResources we wouldn't have to deal with this
			final int numberOfRequestedSlots = jobResources.getNumResources(JobResourceState.MISSING) + jobResources.getNumResources(JobResourceState.ACQUIRED);

			if (numberOfRequestedSlots < numberOfRequiredSlots) {
				addResource(jobId, resourceProfile, JobResourceState.MISSING);
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Requirement processing
	// ---------------------------------------------------------------------------------------------

	@Override
	public void notifyResourceRequirements(ResourceRequirements resourceRequirements) {
		if (resourceRequirements.getResourceRequirements().isEmpty()) {
			resourceRequirementsByJob.remove(resourceRequirements.getJobId());
			return;
		}

		// TODO: remove excess missing resources
		ResourceRequirements previousResourceRequirements = this.resourceRequirementsByJob.put(resourceRequirements.getJobId(), resourceRequirements);
		if (previousResourceRequirements != null) {
			Optional<ResourceRequirements> newlyRequiredResources = computeNewlyRequiredResources(previousResourceRequirements, resourceRequirements);
			if (newlyRequiredResources.isPresent()) {
				addMissingResourceEntriesFor(newlyRequiredResources.get());
			}
		} else {
			addMissingResourceEntriesFor(resourceRequirements);
		}
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
	public Collection<ResourceRequirements> getExceedingOrRequiredResources() {
		return checkWhetherAnyResourceRequirementsAreUnderBudget();
	}

	@Override
	public Collection<ResourceRequirement> getAcquiredResources(JobID jobId) {
		JobResources jobResources = this.jobResources.get(jobId);
		if (jobResources == null) {
			return Collections.emptyList();
		}
		return jobResources.getPendingAndAllocatedResources();
	}

	private Collection<ResourceRequirements> checkWhetherAnyResourceRequirementsAreUnderBudget() {
		final Collection<ResourceRequirements> requiredResources = new ArrayList<>();

		// only process resource for which we have requirements, as there are edge-cases where slots can be assigned
		// to a job without us having a requirement for the corresponding job
		for (Map.Entry<JobID, ResourceRequirements> jobRequirements : resourceRequirementsByJob.entrySet()) {
			final JobID jobId = jobRequirements.getKey();
			final JobResources resources = getJobResources(jobId);
			final Collection<ResourceRequirement> missingResources = resources.getMissingResources();

			if (!missingResources.isEmpty()) {
				requiredResources.add(new ResourceRequirements(jobId, jobRequirements.getValue().getTargetAddress(), missingResources));
			}
		}
		return requiredResources;
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
		JobResources jobResources = getJobResources(jobId);
		jobResources.findAndRemoveMatchingResource(profile, resourceState);
		// TODO: add a more convenient way ot checking this, or built this removal again into the iterator?
		if (jobResources.getNumResources(JobResourceState.MISSING) == 0 && jobResources.getNumResources(JobResourceState.ACQUIRED) == 0) {
			this.jobResources.remove(jobId);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Testing
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	int getNumResources(JobID jobId, JobResourceState state) {
		return getJobResources(jobId).getNumResources(state);
	}
}
