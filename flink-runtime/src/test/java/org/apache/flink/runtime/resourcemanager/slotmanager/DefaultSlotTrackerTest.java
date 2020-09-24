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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link DefaultSlotTracker}.
 */
public class DefaultSlotTrackerTest extends TestLogger {

	private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION = new TaskExecutorConnection(
		ResourceID.generate(),
		new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

	private static final JobID jobId = new JobID();

	@Test
	public void testInitialBehavior() {
		SlotTracker tracker = new DefaultSlotTracker((slot, previous, current, jobId) -> {});

		assertThat(tracker.getFreeSlots(), empty());
	}

	@Test
	public void testSlotAddition() {
		SlotTracker tracker = new DefaultSlotTracker((slot, previous, current, jobId) -> {});

		SlotID slotId1 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);
		SlotID slotId2 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);

		tracker.addSlot(slotId1, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
		tracker.addSlot(slotId2, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

		assertThat(tracker.getFreeSlots(), containsInAnyOrder(Arrays.asList(infoWithSlotId(slotId1), infoWithSlotId(slotId2))));
	}

	@Test
	public void testSlotRemoval() {
		Queue<SlotStateTransition> stateTransitions = new ArrayDeque<>();
		DefaultSlotTracker tracker = new DefaultSlotTracker((slot, previous, current, jobId) ->
			stateTransitions.add(new SlotStateTransition(slot.getSlotId(), previous, current, jobId)));

		SlotID slotId1 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);
		SlotID slotId2 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);
		SlotID slotId3 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 2);

		tracker.addSlot(slotId1, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
		tracker.addSlot(slotId2, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
		tracker.addSlot(slotId3, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

		tracker.notifyAllocationStart(slotId2, jobId);
		tracker.notifyAllocationStart(slotId3, jobId);
		tracker.notifyAllocationComplete(slotId3, jobId);

		// the transitions to this point are not relevant for this test
		stateTransitions.clear();
		// we now have 1 slot in each slot state (free, pending, allocated)
		// it should be possible to remove slots regardless of their state
		tracker.removeSlots(Arrays.asList(slotId1, slotId2, slotId3));

		assertThat(tracker.getFreeSlots(), empty());
		assertThat(tracker.areMapsEmpty(), is(true));

		assertThat(stateTransitions, containsInAnyOrder(
			new SlotStateTransition(slotId2, SlotState.PENDING, SlotState.FREE, jobId),
			new SlotStateTransition(slotId3, SlotState.ALLOCATED, SlotState.FREE, jobId)
		));
	}

	@Test
	public void testAllocationCompletion() {
		Queue<SlotStateTransition> stateTransitions = new ArrayDeque<>();
		SlotTracker tracker = new DefaultSlotTracker((slot, previous, current, jobId) ->
			stateTransitions.add(new SlotStateTransition(slot.getSlotId(), previous, current, jobId)));

		SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);

		tracker.addSlot(slotId, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

		tracker.notifyAllocationStart(slotId, jobId);
		assertThat(tracker.getFreeSlots(), empty());
		assertThat(stateTransitions.remove(), is(new SlotStateTransition(slotId, SlotState.FREE, SlotState.PENDING, jobId)));

		tracker.notifyAllocationComplete(slotId, jobId);
		assertThat(tracker.getFreeSlots(), empty());
		assertThat(stateTransitions.remove(), is(new SlotStateTransition(slotId, SlotState.PENDING, SlotState.ALLOCATED, jobId)));

		tracker.notifyFree(slotId);

		assertThat(tracker.getFreeSlots(), contains(infoWithSlotId(slotId)));
		assertThat(stateTransitions.remove(), is(new SlotStateTransition(slotId, SlotState.ALLOCATED, SlotState.FREE, jobId)));
	}

	@Test
	public void testAllocationCompletionForDifferentJobThrowsIllegalStateException() {
		SlotTracker tracker = new DefaultSlotTracker((slot, previous, current, jobId) -> {});

		SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);

		tracker.addSlot(slotId, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

		tracker.notifyAllocationStart(slotId, new JobID());
		try {
			tracker.notifyAllocationComplete(slotId, new JobID());
			fail("Allocations must not be completed for a different job ID.");
		} catch (IllegalStateException expected) {
		}
	}

	@Test
	public void testAllocationCancellation() {
		Queue<SlotStateTransition> stateTransitions = new ArrayDeque<>();
		SlotTracker tracker = new DefaultSlotTracker((slot, previous, current, jobId) ->
			stateTransitions.add(new SlotStateTransition(slot.getSlotId(), previous, current, jobId)));

		SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);

		tracker.addSlot(slotId, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

		tracker.notifyAllocationStart(slotId, jobId);
		assertThat(tracker.getFreeSlots(), empty());
		assertThat(stateTransitions.remove(), is(new SlotStateTransition(slotId, SlotState.FREE, SlotState.PENDING, jobId)));

		tracker.notifyFree(slotId);
		assertThat(tracker.getFreeSlots(), contains(infoWithSlotId(slotId)));
		assertThat(stateTransitions.remove(), is(new SlotStateTransition(slotId, SlotState.PENDING, SlotState.FREE, jobId)));
	}

	@Test
	public void testSlotStatusProcessing() {
		SlotTracker tracker = new DefaultSlotTracker((slot, previous, current, jobId) -> {});
		SlotID slotId1 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);
		SlotID slotId2 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);
		SlotID slotId3 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 2);
		tracker.addSlot(slotId1, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
		tracker.addSlot(slotId2, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
		tracker.addSlot(slotId3, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, jobId);

		assertThat(tracker.getFreeSlots(), containsInAnyOrder(Arrays.asList(infoWithSlotId(slotId1), infoWithSlotId(slotId2))));

		// move slot2 to PENDING
		tracker.notifyAllocationStart(slotId2, jobId);

		tracker.notifySlotStatus(Arrays.asList(
			new SlotStatus(slotId1, ResourceProfile.ANY, jobId, new AllocationID()),
			new SlotStatus(slotId2, ResourceProfile.ANY, null, new AllocationID()),
			new SlotStatus(slotId3, ResourceProfile.ANY, null, new AllocationID())));

		// slot1 should now be allocated; slot2 should continue to be in a pending state; slot3 should be freed
		assertThat(tracker.getFreeSlots(), contains(infoWithSlotId(slotId3)));

		// if slot2 is not in a pending state, this will fail with an exception
		tracker.notifyAllocationComplete(slotId2, jobId);
	}

	/**
	 * Tests all state transitions that could (or should not) occur due to a slot status update. This test only checks
	 * the target state and job ID for state transitions, because the slot ID is not interesting and the slot state
	 * is not *actually* being updated. We assume the reconciler locks in a set of transitions given a source and target
	 * state, without worrying about the correctness of intermediate steps (because it shouldn't; and it would be a bit
	 * annoying to setup).
	 */
	@Test
	public void testSlotStatusReconciliation() {
		JobID jobId1 = new JobID();
		JobID jobId2 = new JobID();

		Queue<SlotStateTransition> stateTransitions = new ArrayDeque<>();

		DefaultSlotTracker.SlotStatusStateReconciler reconciler = new DefaultSlotTracker.SlotStatusStateReconciler(
			slot -> stateTransitions.add(new SlotStateTransition(slot.getSlotId(), slot.getState(), SlotState.FREE, slot.getJobId())),
			(slot, jobID) -> stateTransitions.add(new SlotStateTransition(slot.getSlotId(), slot.getState(), SlotState.PENDING, jobID)),
			(slot, jobID) -> stateTransitions.add(new SlotStateTransition(slot.getSlotId(), slot.getState(), SlotState.ALLOCATED, jobID)));

		{// free slot
			DeclarativeTaskManagerSlot slot = new DeclarativeTaskManagerSlot(new SlotID(ResourceID.generate(), 0), ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION);

			reconciler.executeStateTransition(slot, null);
			assertThat(stateTransitions, empty());

			reconciler.executeStateTransition(slot, jobId1);
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.PENDING, jobId1)));
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.ALLOCATED, jobId1)));
		}

		{// pending slot
			DeclarativeTaskManagerSlot slot = new DeclarativeTaskManagerSlot(new SlotID(ResourceID.generate(), 0), ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION);
			slot.startAllocation(jobId1);

			reconciler.executeStateTransition(slot, null);
			assertThat(stateTransitions, empty());

			reconciler.executeStateTransition(slot, jobId1);
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.ALLOCATED, jobId1)));
		}

		{// pending slot allocated to different job
			DeclarativeTaskManagerSlot slot = new DeclarativeTaskManagerSlot(new SlotID(ResourceID.generate(), 0), ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION);
			slot.startAllocation(jobId1);

			reconciler.executeStateTransition(slot, jobId2);
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.FREE, jobId1)));
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.PENDING, jobId2)));
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.ALLOCATED, jobId2)));
		}

		{// allocated slot
			DeclarativeTaskManagerSlot slot = new DeclarativeTaskManagerSlot(new SlotID(ResourceID.generate(), 0), ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION);
			slot.startAllocation(jobId1);
			slot.completeAllocation();

			reconciler.executeStateTransition(slot, jobId1);
			assertThat(stateTransitions, empty());

			reconciler.executeStateTransition(slot, null);
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.FREE, jobId1)));
		}

		{// allocated slot allocated to different job
			DeclarativeTaskManagerSlot slot = new DeclarativeTaskManagerSlot(new SlotID(ResourceID.generate(), 0), ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION);
			slot.startAllocation(jobId1);
			slot.completeAllocation();

			reconciler.executeStateTransition(slot, jobId2);
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.FREE, jobId1)));
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.PENDING, jobId2)));
			assertThat(stateTransitions.remove(), is(transitionWithTargetStateForJob(SlotState.ALLOCATED, jobId2)));
		}
	}

	private static class SlotStateTransition {

		private final SlotID slotId;
		private final SlotState oldState;
		private final SlotState newState;
		@Nullable
		private final JobID jobId;

		private SlotStateTransition(SlotID slotId, SlotState oldState, SlotState newState, @Nullable JobID jobId) {
			this.slotId = slotId;
			this.jobId = jobId;
			this.oldState = oldState;
			this.newState = newState;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SlotStateTransition that = (SlotStateTransition) o;
			return Objects.equals(slotId, that.slotId) &&
				oldState == that.oldState &&
				newState == that.newState &&
				Objects.equals(jobId, that.jobId);
		}

		@Override
		public String toString() {
			return "SlotStateTransition{" +
				"slotId=" + slotId +
				", oldState=" + oldState +
				", newState=" + newState +
				", jobId=" + jobId +
				'}';
		}
	}

	private static Matcher<SlotStateTransition> transitionWithTargetStateForJob(SlotState targetState, JobID jobId) {
		return new SlotStateTransitionMatcher(targetState, jobId);
	}

	private static class SlotStateTransitionMatcher extends TypeSafeMatcher<SlotStateTransition> {

		private final SlotState targetState;
		private final JobID jobId;

		private SlotStateTransitionMatcher(SlotState targetState, JobID jobId) {
			this.targetState = targetState;
			this.jobId = jobId;
		}

		@Override
		protected boolean matchesSafely(SlotStateTransition item) {
			return item.newState == targetState && jobId.equals(item.jobId);
		}

		@Override
		public void describeTo(Description description) {
			description
				.appendText("a transition with targetState=")
				.appendValue(targetState)
				.appendText(" and jobId=")
				.appendValue(jobId);
		}
	}

	private static Matcher<TaskManagerSlotInformation> infoWithSlotId(SlotID slotId) {
		return new TaskManagerSlotInformationMatcher(slotId);
	}

	private static class TaskManagerSlotInformationMatcher extends TypeSafeMatcher<TaskManagerSlotInformation> {

		private final SlotID slotId;

		private TaskManagerSlotInformationMatcher(SlotID slotId) {
			this.slotId = slotId;
		}

		@Override
		protected boolean matchesSafely(TaskManagerSlotInformation item) {
			return item.getSlotId().equals(slotId);
		}

		@Override
		public void describeTo(Description description) {
			description
				.appendText("a slot information with slotId=")
				.appendValue(slotId);
		}
	}
}
