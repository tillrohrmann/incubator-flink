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
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * TODO: Add javadoc.
 */
class DeclarativeTaskManagerSlot implements TaskManagerSlotInformation {

	/**
	 * The unique identification of this slot.
	 */
	private final SlotID slotId;

	/**
	 * The resource profile of this slot.
	 */
	private final ResourceProfile resourceProfile;

	/**
	 * Gateway to the TaskExecutor which owns the slot.
	 */
	private final TaskExecutorConnection taskManagerConnection;

	/**
	 * Job id for which this slot has been allocated.
	 */
	@Nullable
	private JobID jobId;

	private State state = State.FREE;

	@Nullable
	private CompletableFuture<Acknowledge> allocationFuture;

	private long allocationStartTimeStamp;

	public DeclarativeTaskManagerSlot(SlotID slotId, ResourceProfile resourceProfile, TaskExecutorConnection taskManagerConnection) {
		this.slotId = slotId;
		this.resourceProfile = resourceProfile;
		this.taskManagerConnection = taskManagerConnection;
	}

	public State getState() {
		return state;
	}

	@Override
	public SlotID getSlotId() {
		return slotId;
	}

	@Override
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public TaskExecutorConnection getTaskManagerConnection() {
		return taskManagerConnection;
	}

	@Nullable
	public JobID getJobId() {
		return jobId;
	}

	@Override
	public InstanceID getInstanceId() {
		return taskManagerConnection.getInstanceID();
	}

	public long getAllocationStartTimestamp() {
		return allocationStartTimeStamp;
	}

	public void startAllocation(JobID jobId, CompletableFuture<Acknowledge> allocationFuture) {
		Preconditions.checkState(state == State.FREE, "Slot must be free to be assigned a slot request.");

		this.jobId = jobId;
		this.state = State.PENDING;
		this.allocationStartTimeStamp = System.currentTimeMillis();
		this.allocationFuture = Preconditions.checkNotNull(allocationFuture);
	}

	public void cancelAllocation() {
		Preconditions.checkState(state == State.PENDING, "In order to cancel an allocation, the slot has to be pending.");

		this.jobId = null;
		this.state = State.FREE;
		this.allocationStartTimeStamp = 0;
		this.allocationFuture.cancel(false);
		this.allocationFuture = null;
	}

	public void completeAllocation() {
		Preconditions.checkState(state == State.PENDING, "In order to complete an allocation, the slot has to be allocated.");

		this.state = State.ALLOCATED;
		this.allocationFuture.cancel(false);
		this.allocationFuture = null;
	}

	public void freeSlot() {
		Preconditions.checkState(state == State.ALLOCATED, "Slot must be allocated before freeing it.");

		this.jobId = null;
		this.state = State.FREE;
		this.allocationStartTimeStamp = 0;
	}

	/**
	 * Check whether required resource profile can be matched by this slot.
	 *
	 * @param required The required resource profile
	 * @return true if requirement can be matched
	 */
	@Override
	public boolean isMatchingRequirement(ResourceProfile required) {
		return resourceProfile.isMatching(required);
	}

	/**
	 * State of the {@link DeclarativeTaskManagerSlot}.
	 */
	public enum State {
		FREE,
		PENDING,
		ALLOCATED
	}
}
