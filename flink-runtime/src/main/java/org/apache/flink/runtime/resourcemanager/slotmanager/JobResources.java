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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * Data-structure for tracking missing/pending/allocated resources.
 */
class JobResources {

	private static final Logger LOG = LoggerFactory.getLogger(JobResources.class);

	private final Map<ResourceProfile, Integer> missingResources = new LinkedHashMap<>();
	private final Map<ResourceProfile, Integer> acquiredResources = new HashMap<>();

	public Collection<ResourceRequirement> getMissingResources() {
		return missingResources.entrySet().stream().map(entry -> ResourceRequirement.create(entry.getKey(), entry.getValue())).collect(Collectors.toList());
	}

	public Collection<ResourceRequirement> getPendingAndAllocatedResources() {
		return acquiredResources
			.entrySet()
			.stream()
			.map(entry -> ResourceRequirement.create(entry.getKey(), entry.getValue()))
			.collect(Collectors.toList());
	}

	public void addResource(ResourceProfile resourceProfile, JobResourceState state) {
		LOG.debug("Add resource {} with state {}.", resourceProfile, state);
		LOG.debug("State before operation: {}", this);
		switch (state) {
			case MISSING:
				incrementCount(missingResources, resourceProfile);
				break;
			case ACQUIRED:
				incrementCount(acquiredResources, resourceProfile);
				break;
		}
		LOG.debug("State after operation: {}", this);
	}

	private void incrementCount(Map<ResourceProfile, Integer> resources, ResourceProfile resourceProfile) {
		resources.compute(resourceProfile, (ignored, currentCount) -> currentCount == null
			? 1
			: currentCount + 1);
	}

	public void findAndRemoveMatchingResource(ResourceProfile profile, JobResourceState resourceState) {
		LOG.debug("Find and remove matching resource {} with state {}.", profile, resourceState);
		LOG.debug("State before operation: {}", this);
		findAndRemoveResource(getResources(resourceState), profile);
		LOG.debug("State after operation: {}", this);
	}

	private Iterator<ResourceProfile> getResources(JobResourceState state) {
		switch (state) {
			case MISSING:
				return new ResourceIterator(missingResources);
			case ACQUIRED:
				return new ResourceIterator(acquiredResources);
		}
		throw new IllegalStateException("Unknown resource state:" + state);
	}

	private void findAndRemoveResource(Iterator<ResourceProfile> resources, ResourceProfile profile) {
		while (resources.hasNext()) {
			ResourceProfile candidate = resources.next();
			if (profile.isMatching(candidate)) {
				resources.remove();
				return;
			}
		}
	}

	@Override
	public String toString() {
		return "JobResources{" +
			"missingResources=" + missingResources +
			", acquiredResources=" + acquiredResources +
			'}';
	}

	// ---------------------------------------------------------------------------------------------
	// Testing
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	int getNumResources(JobResourceState state) {
		return count(getResources(state));
	}

	private int count(Iterator<?> iterator) {
		int count = 0;
		while (iterator.hasNext()) {
			iterator.next();
			count++;
		}
		return count;
	}

	private static class ResourceIterator implements Iterator<ResourceProfile> {

		private final Iterator<Map.Entry<ResourceProfile, Integer>> profilesIterator;
		private Map.Entry<ResourceProfile, Integer> currentProfile;
		private int currentIndex = 0;

		private ResourceIterator(Map<ResourceProfile, Integer> resources) {
			this.profilesIterator = resources.entrySet().iterator();
			this.currentProfile = profilesIterator.hasNext()
				? profilesIterator.next()
				: null;
		}

		@Override
		public boolean hasNext() {
			return profilesIterator.hasNext() || (currentProfile != null && currentIndex < currentProfile.getValue());
		}

		@Override
		public ResourceProfile next() {
			if (currentIndex < currentProfile.getValue()) {
				currentIndex++;
				return currentProfile.getKey();
			}
			if (profilesIterator.hasNext()) {
				currentProfile = profilesIterator.next();
				currentIndex = 0;
				return currentProfile.getKey();
			}
			throw new NoSuchElementException("iterator exhausted");
		}

		@Override
		public void remove() {
			int currentCount = currentProfile.getValue();
			if (currentCount == 1) {
				profilesIterator.remove();
			} else {
				currentProfile.setValue(currentCount - 1);
				currentIndex--;
			}
		}
	}
}
