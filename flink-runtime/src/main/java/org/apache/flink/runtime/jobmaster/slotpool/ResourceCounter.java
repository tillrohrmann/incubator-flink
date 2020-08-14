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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Counter for {@link ResourceProfile}.
 */
public class ResourceCounter {

	private final Map<ResourceProfile, Integer> resources;

	private ResourceCounter(Map<ResourceProfile, Integer> resources) {
		this.resources = new HashMap<>(resources);
	}

	public int getResourceCount(ResourceProfile resourceProfile) {
		return resources.getOrDefault(resourceProfile, 0);
	}

	public void add(ResourceCounter increment) {
		internalAdd(increment.entrySet());
	}

	public void add(Map<ResourceProfile, Integer> increment) {
		internalAdd(increment.entrySet());
	}

	private void internalAdd(Iterable<? extends Map.Entry<ResourceProfile, Integer>> entries) {
		for (Map.Entry<ResourceProfile, Integer> resourceIncrement : entries) {
			add(resourceIncrement.getKey(), resourceIncrement.getValue());
		}
	}

	public void add(ResourceProfile resourceProfile, int increment) {
		final int newValue = resources.getOrDefault(resourceProfile, 0) + increment;

		updateNewValue(resourceProfile, newValue);
	}

	private void updateNewValue(ResourceProfile resourceProfile, int newValue) {
		if (newValue > 0) {
			resources.put(resourceProfile, newValue);
		} else {
			resources.remove(resourceProfile);
		}
	}

	public void subtract(ResourceCounter decrement) {
		internalSubtract(decrement.entrySet());
	}

	public void subtract(Map<ResourceProfile, Integer> decrement) {
		internalSubtract(decrement.entrySet());
	}

	private void internalSubtract(Iterable<? extends Map.Entry<ResourceProfile, Integer>> entries) {
		for (Map.Entry<ResourceProfile, Integer> resourceDecrement : entries) {
			subtract(resourceDecrement.getKey(), resourceDecrement.getValue());
		}
	}

	public void subtract(ResourceProfile resourceProfile, int decrement) {
		final int newValue = resources.getOrDefault(resourceProfile, 0) - decrement;

		updateNewValue(resourceProfile, newValue);
	}

	public Collection<Map.Entry<ResourceProfile, Integer>> entrySet() {
		return resources.entrySet();
	}

	public boolean containsResource(ResourceProfile resourceProfile) {
		return resources.containsKey(resourceProfile);
	}

	public Set<ResourceProfile> getResources() {
		return resources.keySet();
	}

	public boolean isEmpty() {
		return resources.isEmpty();
	}

	public static ResourceCounter empty() {
		return new ResourceCounter(new HashMap<>());
	}

	public static ResourceCounter withResources(Map<ResourceProfile, Integer> initialResources) {
		return new ResourceCounter(initialResources);
	}

	public static ResourceCounter withResources(ResourceCounter initialResources) {
		return new ResourceCounter(initialResources.resources);
	}

	@Override
	public String toString() {
		return "ResourceCounter{" +
			"resources=" + resources +
			'}';
	}
}
