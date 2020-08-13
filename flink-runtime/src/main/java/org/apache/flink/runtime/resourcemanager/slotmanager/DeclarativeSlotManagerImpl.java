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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.slotsbro.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link SlotManager}.
 */
public class DeclarativeSlotManagerImpl implements SlotManager {
	private static final Logger LOG = LoggerFactory.getLogger(DeclarativeSlotManagerImpl.class);

	static final AllocationID DUMMY_ALLOCATION_ID = new AllocationID();

	/** Scheduled executor for timeouts. */
	private final ScheduledExecutor scheduledExecutor;

	/** Timeout for slot requests to the task manager. */
	private final Time taskManagerRequestTimeout;

	/** Timeout after which an allocation is discarded. */
	private final Time slotAllocationTimeout;

	/** Timeout after which an unused TaskManager is released. */
	private final Time taskManagerTimeout;

	/** Map for all registered slots. */
	private final HashMap<SlotID, DeclarativeTaskManagerSlot> slots;

	/** Index of all currently pending slots. */
	private final HashMap<SlotID, DeclarativeTaskManagerSlot> pendingSlotAllocations;
	private final HashMap<SlotID, CompletableFuture<Acknowledge>> pendingSlotAllocationFutures;

	/** Index of all currently free slots. */
	private final LinkedHashMap<SlotID, DeclarativeTaskManagerSlot> freeSlots;

	/** All currently registered task managers. */
	private final HashMap<InstanceID, TaskManagerRegistration> taskManagerRegistrations;

	private final HashMap<TaskManagerSlotId, PendingTaskManagerSlot> pendingSlots;

	private final SlotMatchingStrategy slotMatchingStrategy;

	/** ResourceManager's id. */
	private ResourceManagerId resourceManagerId;

	/** Executor for future callbacks which have to be "synchronized". */
	private Executor mainThreadExecutor;

	/** Callbacks for resource (de-)allocations. */
	private ResourceActions resourceActions;

	private ScheduledFuture<?> taskManagerTimeoutsAndRedundancyCheck;

	private ScheduledFuture<?> slotRequestTimeoutCheck;

	/** True iff the component has been started. */
	private boolean started;

	/** Release task executor only when each produced result partition is either consumed or failed. */
	private final boolean waitResultConsumedBeforeRelease;

	/** Defines the max limitation of the total number of slots. */
	private final int maxSlotNum;

	/** Defines the number of redundant taskmanagers. */
	private final int redundantTaskManagerNum;

	/**
	 * If true, fail unfulfillable slot requests immediately. Otherwise, allow unfulfillable request to pend.
	 * A slot request is considered unfulfillable if it cannot be fulfilled by neither a slot that is already registered
	 * (including allocated ones) nor a pending slot that the {@link ResourceActions} can allocate.
	 * */
	private boolean failUnfulfillableRequest = true;

	/**
	 * The default resource spec of workers to request.
	 */
	private final WorkerResourceSpec defaultWorkerResourceSpec;

	private final int numSlotsPerWorker;

	private final ResourceProfile defaultSlotResourceProfile;

	private final SlotManagerMetricGroup slotManagerMetricGroup;

	private final DefaultRequirementsTracker resourceTracker;

	public DeclarativeSlotManagerImpl(
			ScheduledExecutor scheduledExecutor,
			SlotManagerConfiguration slotManagerConfiguration,
			SlotManagerMetricGroup slotManagerMetricGroup) {

		this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

		Preconditions.checkNotNull(slotManagerConfiguration);
		this.slotMatchingStrategy = slotManagerConfiguration.getSlotMatchingStrategy();
		this.taskManagerRequestTimeout = slotManagerConfiguration.getTaskManagerRequestTimeout();
		this.slotAllocationTimeout = slotManagerConfiguration.getSlotRequestTimeout();
		this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
		this.waitResultConsumedBeforeRelease = slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
		this.defaultWorkerResourceSpec = slotManagerConfiguration.getDefaultWorkerResourceSpec();
		this.numSlotsPerWorker = slotManagerConfiguration.getNumSlotsPerWorker();
		this.defaultSlotResourceProfile = generateDefaultSlotResourceProfile(defaultWorkerResourceSpec, numSlotsPerWorker);
		this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
		this.maxSlotNum = slotManagerConfiguration.getMaxSlotNum();
		this.redundantTaskManagerNum = slotManagerConfiguration.getRedundantTaskManagerNum();
		this.resourceTracker = new DefaultRequirementsTracker();

		slots = new HashMap<>(16);
		pendingSlotAllocations = new HashMap<>(16);
		pendingSlotAllocationFutures = new HashMap<>(16);
		freeSlots = new LinkedHashMap<>(16);
		taskManagerRegistrations = new HashMap<>(4);
		pendingSlots = new HashMap<>(16);

		resourceManagerId = null;
		resourceActions = null;
		mainThreadExecutor = null;
		taskManagerTimeoutsAndRedundancyCheck = null;
		slotRequestTimeoutCheck = null;

		started = false;
	}

	@Override
	public int getNumberRegisteredSlots() {
		return slots.size();
	}

	@Override
	public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (taskManagerRegistration != null) {
			return taskManagerRegistration.getNumberRegisteredSlots();
		} else {
			return 0;
		}
	}

	@Override
	public int getNumberFreeSlots() {
		return freeSlots.size();
	}

	@Override
	public int getNumberFreeSlotsOf(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (taskManagerRegistration != null) {
			return taskManagerRegistration.getNumberFreeSlots();
		} else {
			return 0;
		}
	}

	@Override
	public Map<WorkerResourceSpec, Integer> getRequiredResources() {
		final int pendingWorkerNum = MathUtils.divideRoundUp(pendingSlots.size(), numSlotsPerWorker);
		return pendingWorkerNum > 0 ?
			Collections.singletonMap(defaultWorkerResourceSpec, pendingWorkerNum) :
			Collections.emptyMap();
	}

	@Override
	public ResourceProfile getRegisteredResource() {
		return getResourceFromNumSlots(getNumberRegisteredSlots());
	}

	@Override
	public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
		return getResourceFromNumSlots(getNumberRegisteredSlotsOf(instanceID));
	}

	@Override
	public ResourceProfile getFreeResource() {
		return getResourceFromNumSlots(getNumberFreeSlots());
	}

	@Override
	public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
		return getResourceFromNumSlots(getNumberFreeSlotsOf(instanceID));
	}

	private ResourceProfile getResourceFromNumSlots(int numSlots) {
		if (numSlots < 0 || defaultSlotResourceProfile == null) {
			return ResourceProfile.UNKNOWN;
		} else {
			return defaultSlotResourceProfile.multiply(numSlots);
		}
	}

	@VisibleForTesting
	public int getNumberPendingTaskManagerSlots() {
		return pendingSlots.size();
	}

	@Override
	public int getNumberPendingSlotRequests() {
		throw new UnsupportedOperationException();
	}

	// ---------------------------------------------------------------------------------------------
	// Component lifecycle methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Starts the slot manager with the given leader id and resource manager actions.
	 *
	 * @param newResourceManagerId to use for communication with the task managers
	 * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
	 * @param newResourceActions to use for resource (de-)allocations
	 */
	@Override
	public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions) {
		LOG.info("Starting the SlotManager.");

		//this.resourceTracker.start(newResourceManagerId, newMainThreadExecutor, newResourceActions);
		this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
		mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
		resourceActions = Preconditions.checkNotNull(newResourceActions);

		started = true;

		taskManagerTimeoutsAndRedundancyCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				this::checkTaskManagerTimeoutsAndRedundancy),
			0L,
			taskManagerTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		slotRequestTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				this::checkSlotAllocationTimeouts),
			0L,
			slotAllocationTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		registerSlotManagerMetrics();
	}

	private void registerSlotManagerMetrics() {
		slotManagerMetricGroup.gauge(
			MetricNames.TASK_SLOTS_AVAILABLE,
			() -> (long) getNumberFreeSlots());
		slotManagerMetricGroup.gauge(
			MetricNames.TASK_SLOTS_TOTAL,
			() -> (long) getNumberRegisteredSlots());
	}

	/**
	 * Suspends the component. This clears the internal state of the slot manager.
	 */
	@Override
	public void suspend() {
		LOG.info("Suspending the SlotManager.");

		// stop the timeout checks for the TaskManagers and the SlotRequests
		if (taskManagerTimeoutsAndRedundancyCheck != null) {
			taskManagerTimeoutsAndRedundancyCheck.cancel(false);
			taskManagerTimeoutsAndRedundancyCheck = null;
		}

		if (slotRequestTimeoutCheck != null) {
			slotRequestTimeoutCheck.cancel(false);
			slotRequestTimeoutCheck = null;
		}

		resourceTracker.clear();

		ArrayList<InstanceID> registeredTaskManagers = new ArrayList<>(taskManagerRegistrations.keySet());

		for (InstanceID registeredTaskManager : registeredTaskManagers) {
			unregisterTaskManager(registeredTaskManager, new SlotManagerException("The slot manager is being suspended."));
		}

		resourceManagerId = null;
		resourceActions = null;
		started = false;
	}

	/**
	 * Closes the slot manager.
	 *
	 * @throws Exception if the close operation fails
	 */
	@Override
	public void close() throws Exception {
		LOG.info("Closing the SlotManager.");

		suspend();
		slotManagerMetricGroup.close();
	}

	// ---------------------------------------------------------------------------------------------
	// Public API
	// ---------------------------------------------------------------------------------------------

	@Override
	public void processResourceRequirements(ResourceRequirements resourceRequirements) {
		checkInit();
		resourceTracker.notifyResourceRequirements(resourceRequirements);
		checkResourceRequirements();
	}

	/**
	 * Registers a new task manager at the slot manager. This will make the task managers slots
	 * known and, thus, available for allocation.
	 *
	 * @param taskExecutorConnection for the new task manager
	 * @param initialSlotReport for the new task manager
	 * @return True if the task manager has not been registered before and is registered successfully; otherwise false
	 */
	@Override
	public boolean registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		checkInit();

		LOG.debug("Registering TaskManager {} under {} at the SlotManager.", taskExecutorConnection.getResourceID(), taskExecutorConnection.getInstanceID());

		// we identify task managers by their instance id
		if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
			reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
			return false;
		} else {
			if (isMaxSlotNumExceededAfterRegistration(initialSlotReport)) {
				LOG.info("The total number of slots exceeds the max limitation {}, release the excess resource.", maxSlotNum);
				resourceActions.releaseResource(taskExecutorConnection.getInstanceID(), new FlinkException("The total number of slots exceeds the max limitation."));
				return false;
			}

			// first register the TaskManager
			ArrayList<SlotID> reportedSlots = new ArrayList<>();

			for (SlotStatus slotStatus : initialSlotReport) {
				reportedSlots.add(slotStatus.getSlotID());
			}

			TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(
				taskExecutorConnection,
				reportedSlots);

			taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);

			// next register the new slots
			for (SlotStatus slotStatus : initialSlotReport) {
				registerSlot(
					slotStatus.getSlotID(),
					slotStatus.getJobID(),
					slotStatus.getResourceProfile(),
					taskExecutorConnection);
			}

			checkResourceRequirements();
			return true;
		}

	}

	@Override
	public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
		checkInit();

		LOG.debug("Unregister TaskManager {} from the SlotManager.", instanceId);

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (null != taskManagerRegistration) {
			internalUnregisterTaskManager(taskManagerRegistration, cause);
			checkResourceRequirements();

			return true;
		} else {
			LOG.debug("There is no task manager registered with instance ID {}. Ignoring this message.", instanceId);

			return false;
		}
	}

	/**
	 * Reports the current slot allocations for a task manager identified by the given instance id.
	 *
	 * @param instanceId identifying the task manager for which to report the slot status
	 * @param slotReport containing the status for all of its slots
	 * @return true if the slot status has been updated successfully, otherwise false
	 */
	@Override
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		checkInit();

		LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (null != taskManagerRegistration) {

			for (SlotStatus slotStatus : slotReport) {
				updateSlot(slotStatus.getSlotID(), slotStatus.getJobID());
			}
			checkResourceRequirements();
			return true;
		} else {
			LOG.debug("Received slot report for unknown task manager with instance id {}. Ignoring this report.", instanceId);

			return false;
		}
	}

	/**
	 * Free the given slot from the given allocation. If the slot is still allocated by the given
	 * allocation id, then the slot will be marked as free and will be subject to new slot requests.
	 *
	 * @param slotId identifying the slot to free
	 * @param allocationId with which the slot is presumably allocated
	 */
	@Override
	public void freeSlot(SlotID slotId, AllocationID allocationId) {
		checkInit();

		DeclarativeTaskManagerSlot slot = slots.get(slotId);

		if (null != slot) {
			if (slot.getState() == DeclarativeTaskManagerSlot.State.ALLOCATED) {
				TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

				if (taskManagerRegistration == null) {
					throw new IllegalStateException("Trying to free a slot from a TaskManager " +
						slot.getInstanceId() + " which has not been registered.");
				}

				updateStateForFreeSlot(slot);
				checkResourceRequirements();
			} else {
				LOG.debug("Slot {} has not been allocated.", slotId);
			}
		} else {
			LOG.debug("Trying to free a slot {} which has not been registered. Ignoring this message.", slotId);
		}
	}

	@Override
	public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
		if (!this.failUnfulfillableRequest && failUnfulfillableRequest) {
			// TODO: fail unfulfillable pending requests
		}
		this.failUnfulfillableRequest = failUnfulfillableRequest;
	}

	// ---------------------------------------------------------------------------------------------
	// Behaviour methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Finds a matching slot for a given resource profile. A matching slot has at least as many
	 * resources available as the given resource profile.
	 *
	 * <p>Note: If you want to change the behaviour of the slot manager wrt slot allocation and
	 * request fulfillment, then you should override this method.
	 *
	 * @param requestResourceProfile specifying the resource requirements for the a slot request
	 * @return A matching slot which fulfills the given resource profile. {@link Optional#empty()}
	 * if there is no such slot available.
	 */
	private Optional<DeclarativeTaskManagerSlot> findMatchingSlot(ResourceProfile requestResourceProfile) {
		final Optional<DeclarativeTaskManagerSlot> optionalMatchingSlot = slotMatchingStrategy.findMatchingSlot(
			requestResourceProfile,
			freeSlots.values(),
			this::getNumberRegisteredSlotsOf);

		optionalMatchingSlot.ifPresent(taskManagerSlot -> {
			// sanity check
			Preconditions.checkState(
				taskManagerSlot.getState() == DeclarativeTaskManagerSlot.State.FREE,
				"TaskManagerSlot %s is not in state FREE but %s.",
				taskManagerSlot.getSlotId(), taskManagerSlot.getState());

			freeSlots.remove(taskManagerSlot.getSlotId());
			pendingSlotAllocations.put(taskManagerSlot.getSlotId(), taskManagerSlot);
		});

		return optionalMatchingSlot;
	}

	private void checkResourceRequirements() {
		final Collection<ResourceRequirements> resourceAllocationInfo = resourceTracker.getExceedingOrRequiredResources();
		for (ResourceRequirements resourceRequirements : resourceAllocationInfo) {
			for (ResourceRequirement resourceRequirement : resourceRequirements.getResourceRequirements()) {
				if (resourceRequirement.getNumberOfRequiredSlots() > 0) {
					internalRequestSlots(resourceRequirements.getJobId(), resourceRequirements.getTargetAddress(), resourceRequirement);
				}
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal slot operations
	// ---------------------------------------------------------------------------------------------

	/**
	 * Registers a slot for the given task manager at the slot manager. The slot is identified by
	 * the given slot id. The given resource profile defines the available resources for the slot.
	 * The task manager connection can be used to communicate with the task manager.
	 *
	 * @param slotId identifying the slot on the task manager
	 * @param resourceProfile of the slot
	 * @param taskManagerConnection to communicate with the remote task manager
	 */
	private void registerSlot(
			SlotID slotId,
			JobID jobId,
			ResourceProfile resourceProfile,
			TaskExecutorConnection taskManagerConnection) {

		if (slots.containsKey(slotId)) {
			// remove the old slot first
			removeSlot(
				slotId,
				new SlotManagerException(
					String.format(
						"Re-registration of slot %s. This indicates that the TaskExecutor has re-connected.",
						slotId)));
		}

		final Optional<PendingTaskManagerSlot> pendingTaskManagerSlot = findExactlyMatchingPendingTaskManagerSlot(resourceProfile);
		if (pendingTaskManagerSlot.isPresent()) {
			pendingSlots.remove(pendingTaskManagerSlot.get().getTaskManagerSlotId());
		}
		createAndRegisterTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);
		updateSlot(slotId, jobId);
	}

	private Optional<PendingTaskManagerSlot> findExactlyMatchingPendingTaskManagerSlot(ResourceProfile resourceProfile) {
		for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
			if (isPendingSlotExactlyMatchingResourceProfile(pendingTaskManagerSlot, resourceProfile)) {
				return Optional.of(pendingTaskManagerSlot);
			}
		}

		return Optional.empty();
	}

	private void createAndRegisterTaskManagerSlot(SlotID slotId, ResourceProfile resourceProfile, TaskExecutorConnection taskManagerConnection) {
		final DeclarativeTaskManagerSlot slot = new DeclarativeTaskManagerSlot(
			slotId,
			resourceProfile,
			taskManagerConnection);
		slots.put(slotId, slot);
	}

	private boolean isPendingSlotExactlyMatchingResourceProfile(PendingTaskManagerSlot pendingTaskManagerSlot, ResourceProfile resourceProfile) {
		return pendingTaskManagerSlot.getResourceProfile().equals(resourceProfile);
	}

	private boolean isMaxSlotNumExceededAfterRegistration(SlotReport initialSlotReport) {
		// check if the total number exceed before matching pending slot.
		if (!isMaxSlotNumExceededAfterAdding(initialSlotReport.getNumSlotStatus())) {
			return false;
		}

		// check if the total number exceed slots after consuming pending slot.
		return isMaxSlotNumExceededAfterAdding(getNumNonPendingReportedNewSlots(initialSlotReport));
	}

	private int getNumNonPendingReportedNewSlots(SlotReport slotReport) {
		final Set<TaskManagerSlotId> matchingPendingSlots = new HashSet<>();

		for (SlotStatus slotStatus : slotReport) {
			for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
				if (!matchingPendingSlots.contains(pendingTaskManagerSlot.getTaskManagerSlotId()) &&
					isPendingSlotExactlyMatchingResourceProfile(pendingTaskManagerSlot, slotStatus.getResourceProfile())) {
					matchingPendingSlots.add(pendingTaskManagerSlot.getTaskManagerSlotId());
					break; // pendingTaskManagerSlot loop
				}
			}
		}
		return slotReport.getNumSlotStatus() - matchingPendingSlots.size();
	}

	/**
	 * Updates a slot with the given allocation id.
	 *
	 * @param slotId to update
	 * @param jobId specifying the job to which the slot is allocated
	 * @return True if the slot could be updated; otherwise false
	 */
	private boolean updateSlot(SlotID slotId, JobID jobId) {
		final DeclarativeTaskManagerSlot slot = slots.get(slotId);

		if (slot != null) {
			final TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

			if (taskManagerRegistration != null) {
				if (jobId == null) {
					updateStateForFreeSlot(slot);
				} else {
					updateStateForAllocatedSlot(slot, taskManagerRegistration, jobId);
				}

				return true;
			} else {
				throw new IllegalStateException("Trying to update a slot from a TaskManager " +
					slot.getInstanceId() + " which has not been registered.");
			}
		} else {
			LOG.debug("Trying to update unknown slot with slot id {}.", slotId);

			return false;
		}
	}

	private void internalFreeSlot(DeclarativeTaskManagerSlot slot) {
		final TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

		switch (slot.getState()) {
			case FREE:
				break;
			case PENDING:
				resourceTracker.notifyLostResource(slot.getJobId(), slot.getResourceProfile());
				slot.cancelAllocation();
				cancelAllocationFuture(slot.getSlotId());
				handleFreeSlot(slot);
				break;
			case ALLOCATED:
				resourceTracker.notifyLostResource(slot.getJobId(), slot.getResourceProfile());
				slot.freeSlot();
				taskManagerRegistration.freeSlot();
				handleFreeSlot(slot);
		}
	}

	private void updateStateForAllocatedSlot(DeclarativeTaskManagerSlot slot, TaskManagerRegistration taskManagerRegistration, JobID jobId) {
		switch (slot.getState()) {
			case PENDING:
				if (!jobId.equals(slot.getJobId())) {
					internalFreeSlot(slot);

					slot.startAllocation(jobId);
					resourceTracker.notifyAcquiredResource(jobId, slot.getResourceProfile());
					updateStateForAllocatedSlot(slot, taskManagerRegistration, jobId);
				} else {
					cancelAllocationFuture(slot.getSlotId());
					slot.completeAllocation();
					taskManagerRegistration.occupySlot();
				}
				break;
			case ALLOCATED:
				break;
			case FREE:
				// the slot is currently free --> it is stored in freeSlots
				freeSlots.remove(slot.getSlotId());
				slot.startAllocation(jobId);
				resourceTracker.notifyAcquiredResource(jobId, slot.getResourceProfile());
				slot.completeAllocation();
				taskManagerRegistration.occupySlot();

				break;
		}
	}

	private void updateStateForFreeSlot(DeclarativeTaskManagerSlot slot) {
		switch (slot.getState()) {
			case FREE:
				handleFreeSlot(slot);
				break;
			case PENDING:
				// don't do anything because we expect the slot to be allocated soon
				break;
			case ALLOCATED:
				internalFreeSlot(slot);
				break;
		}
	}

	/**
	 * Tries to allocate a slot for the given slot request. If there is no slot available, the
	 * resource manager is informed to allocate more resources.
	 *
	 * @param jobId job to allocate slots for
	 * @param targetAddress address of the jobmaster
	 * @param resourceRequirement required slots
	 */
	private void internalRequestSlots(JobID jobId, String targetAddress, ResourceRequirement resourceRequirement) {
		final ResourceProfile resourceProfile = resourceRequirement.getResourceProfile();

		boolean allRequirementsMayBeFulfilled = true;
		for (int x = 0; x < resourceRequirement.getNumberOfRequiredSlots(); x++) {
			final Optional<DeclarativeTaskManagerSlot> reservedSlot = findMatchingSlot(resourceProfile);
			if (reservedSlot.isPresent()) {
				allocateSlot(reservedSlot.get(), jobId, targetAddress, resourceProfile);
			} else {
				Optional<PendingTaskManagerSlot> pendingTaskManagerSlot = allocateResource(resourceProfile);
				if (!pendingTaskManagerSlot.isPresent()) {
					// this isn't really correct, since we are not reserving the pending slot
					// thus a single pending slot can "fulfill" any number of requirements, so long as the profiles fit
					if (!isFulfillableByPendingSlots(resourceProfile)) {
						allRequirementsMayBeFulfilled = false;
					}
				}
				// TODO: Rework how pending slots are handled; currently we repeatedly ask for pending slots until enough
				// TODO: were allocated to fulfill the current requirements, but we'll generally request
				// TODO: more than we actually need because we don't mark the soon(TM) fulfilled requirements as pending.
				// TODO: Basically, separate the request for new task executors from this method, and introduce another
				// TODO: component that uses _some_ heuristic to request new task executors. For matching resources, we
				// TODO: then only react to slot registrations by task executors.
			}
		}

		if (!allRequirementsMayBeFulfilled) {
			resourceActions.notifyNotEnoughResourcesAvailable(jobId, resourceTracker.getAcquiredResources(jobId));
		}
	}

	private boolean isFulfillableByPendingSlots(ResourceProfile resourceProfile) {
		for (PendingTaskManagerSlot slot : pendingSlots.values()) {
			if (slot.getResourceProfile().isMatching(resourceProfile)) {
				return true;
			}
		}

		return false;
	}

	private boolean isMaxSlotNumExceededAfterAdding(int numNewSlot) {
		return getNumberRegisteredSlots() + getNumberPendingTaskManagerSlots() + numNewSlot > maxSlotNum;
	}

	private void allocateRedundantTaskManagers(int number) {
		int allocatedNumber = allocateResources(number);
		if (number != allocatedNumber) {
			LOG.warn("Expect to allocate {} taskManagers. Actually allocate {} taskManagers.", number, allocatedNumber);
		}
	}

	/**
	 * Allocate a number of workers based on the input param.
	 * @param workerNum the number of workers to allocate.
	 * @return the number of allocated workers successfully.
	 */
	private int allocateResources(int workerNum) {
		int allocatedWorkerNum = 0;
		for (int i = 0; i < workerNum; ++i) {
			if (allocateResource(defaultSlotResourceProfile).isPresent()) {
				++allocatedWorkerNum;
			} else {
				break;
			}
		}
		return allocatedWorkerNum;
	}

	private Optional<PendingTaskManagerSlot> allocateResource(ResourceProfile requestedSlotResourceProfile) {
		final int numRegisteredSlots =  getNumberRegisteredSlots();
		final int numPendingSlots = getNumberPendingTaskManagerSlots();
		if (isMaxSlotNumExceededAfterAdding(numSlotsPerWorker)) {
			LOG.warn("Could not allocate {} more slots. The number of registered and pending slots is {}, while the maximum is {}.",
				numSlotsPerWorker, numPendingSlots + numRegisteredSlots, maxSlotNum);
			return Optional.empty();
		}

		if (!defaultSlotResourceProfile.isMatching(requestedSlotResourceProfile)) {
			// requested resource profile is unfulfillable
			return Optional.empty();
		}

		if (!resourceActions.allocateResource(defaultWorkerResourceSpec)) {
			// resource cannot be allocated
			return Optional.empty();
		}

		PendingTaskManagerSlot pendingTaskManagerSlot = null;
		for (int i = 0; i < numSlotsPerWorker; ++i) {
			pendingTaskManagerSlot = new PendingTaskManagerSlot(defaultSlotResourceProfile);
			pendingSlots.put(pendingTaskManagerSlot.getTaskManagerSlotId(), pendingTaskManagerSlot);
		}

		return Optional.of(Preconditions.checkNotNull(pendingTaskManagerSlot,
			"At least one pending slot should be created."));
	}

	/**
	 * Allocates the given slot for the given slot request. This entails sending a registration
	 * message to the task manager and treating failures.
	 *
	 * @param taskManagerSlot to allocate for the given slot request
	 * @param jobId job for which the slot should be allocated for
	 * @param targetAddress address of the job master
	 * @param resourceProfile resource profile for the slot
	 */
	private void allocateSlot(DeclarativeTaskManagerSlot taskManagerSlot, JobID jobId, String targetAddress, ResourceProfile resourceProfile) {
		Preconditions.checkState(taskManagerSlot.getState() == DeclarativeTaskManagerSlot.State.FREE);

		TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
		TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

		final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
		final SlotID slotId = taskManagerSlot.getSlotId();
		final InstanceID instanceID = taskManagerSlot.getInstanceId();

		taskManagerSlot.startAllocation(jobId);
		resourceTracker.notifyAcquiredResource(jobId, taskManagerSlot.getResourceProfile());
		pendingSlotAllocationFutures.put(slotId, completableFuture);

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceID);

		if (taskManagerRegistration == null) {
			throw new IllegalStateException("Could not find a registered task manager for instance id " +
				instanceID + '.');
		}

		taskManagerRegistration.markUsed();

		// RPC call to the task manager
		CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot(
			slotId,
			jobId,
			new AllocationID(),
			resourceProfile,
			targetAddress,
			resourceManagerId,
			taskManagerRequestTimeout);

		requestFuture.whenComplete(
			(Acknowledge acknowledge, Throwable throwable) -> {
				if (acknowledge != null) {
					completableFuture.complete(acknowledge);
				} else {
					completableFuture.completeExceptionally(throwable);
				}
			});

		completableFuture.whenCompleteAsync(
			(Acknowledge acknowledge, Throwable throwable) -> {
				try {
					pendingSlotAllocations.remove(slotId);
					if (acknowledge != null) {
						updateSlot(slotId, jobId);
					} else {
						if (throwable instanceof SlotOccupiedException) {
							SlotOccupiedException exception = (SlotOccupiedException) throwable;
							updateSlot(slotId, exception.getJobId());
						} else {
							if (throwable instanceof  CancellationException) {
								LOG.debug("Slot allocation request for slot {} has been cancelled.", slotId, throwable);
							} else {
								internalFreeSlot(taskManagerSlot);
							}
						}
					}
					checkResourceRequirements();
				} catch (Exception e) {
					LOG.error("Error while completing the slot allocation.", e);
				}
			},
			mainThreadExecutor);
	}

	private void cancelAllocationFuture(SlotID slotId) {
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = pendingSlotAllocationFutures.remove(slotId);
		// the future may be null if we are just re-playing the state transitions due to a slot report
		if (acknowledgeCompletableFuture != null) {
			acknowledgeCompletableFuture.cancel(false);
		}
	}

	/**
	 * Handles a free slot. It first tries to find a pending slot request which can be fulfilled.
	 * If there is no such request, then it will add the slot to the set of free slots.
	 *
	 * @param freeSlot to find a new slot request for
	 */
	private void handleFreeSlot(DeclarativeTaskManagerSlot freeSlot) {
		Preconditions.checkState(freeSlot.getState() == DeclarativeTaskManagerSlot.State.FREE);

		freeSlots.put(freeSlot.getSlotId(), freeSlot);
	}

	/**
	 * Removes the given set of slots from the slot manager.
	 *
	 * @param slotsToRemove identifying the slots to remove from the slot manager
	 * @param cause for removing the slots
	 */
	private void removeSlots(Iterable<SlotID> slotsToRemove, Exception cause) {
		for (SlotID slotId : slotsToRemove) {
			removeSlot(slotId, cause);
		}
	}

	/**
	 * Removes the given slot from the slot manager.
	 *
	 * @param slotId identifying the slot to remove
	 * @param cause for removing the slot
	 */
	private void removeSlot(SlotID slotId, Exception cause) {
		DeclarativeTaskManagerSlot slot = slots.remove(slotId);

		if (null != slot) {
			internalFreeSlot(slot);
			freeSlots.remove(slotId);
		} else {
			LOG.debug("There was no slot registered with slot id {}.", slotId);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal request handling methods
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	public static ResourceProfile generateDefaultSlotResourceProfile(WorkerResourceSpec workerResourceSpec, int numSlotsPerWorker) {
		return ResourceProfile.newBuilder()
			.setCpuCores(workerResourceSpec.getCpuCores().divide(numSlotsPerWorker))
			.setTaskHeapMemory(workerResourceSpec.getTaskHeapSize().divide(numSlotsPerWorker))
			.setTaskOffHeapMemory(workerResourceSpec.getTaskOffHeapSize().divide(numSlotsPerWorker))
			.setManagedMemory(workerResourceSpec.getManagedMemSize().divide(numSlotsPerWorker))
			.setNetworkMemory(workerResourceSpec.getNetworkMemSize().divide(numSlotsPerWorker))
			.build();
	}

	// ---------------------------------------------------------------------------------------------
	// Internal periodic check methods
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	void checkTaskManagerTimeoutsAndRedundancy() {
		if (!taskManagerRegistrations.isEmpty()) {
			long currentTime = System.currentTimeMillis();

			ArrayList<TaskManagerRegistration> timedOutTaskManagers = new ArrayList<>(taskManagerRegistrations.size());

			// first retrieve the timed out TaskManagers
			for (TaskManagerRegistration taskManagerRegistration : taskManagerRegistrations.values()) {
				if (currentTime - taskManagerRegistration.getIdleSince() >= taskManagerTimeout.toMilliseconds()) {
					// we collect the instance ids first in order to avoid concurrent modifications by the
					// ResourceActions.releaseResource call
					timedOutTaskManagers.add(taskManagerRegistration);
				}
			}

			int slotsDiff = redundantTaskManagerNum * numSlotsPerWorker - freeSlots.size();
			if (slotsDiff > 0) {
				// Keep enough redundant taskManagers from time to time.
				int requiredTaskManagers = MathUtils.divideRoundUp(slotsDiff, numSlotsPerWorker);
				allocateRedundantTaskManagers(requiredTaskManagers);
			} else {
				// second we trigger the release resource callback which can decide upon the resource release
				int maxReleaseNum = (-slotsDiff) / numSlotsPerWorker;
				releaseTaskExecutors(timedOutTaskManagers, Math.min(maxReleaseNum, timedOutTaskManagers.size()));
			}
		}
	}

	private void releaseTaskExecutors(ArrayList<TaskManagerRegistration> timedOutTaskManagers, int releaseNum) {
		for (int index = 0; index < releaseNum; ++index) {
			if (waitResultConsumedBeforeRelease) {
				releaseTaskExecutorIfPossible(timedOutTaskManagers.get(index));
			} else {
				releaseTaskExecutor(timedOutTaskManagers.get(index).getInstanceId());
			}
		}
	}

	private void releaseTaskExecutorIfPossible(TaskManagerRegistration taskManagerRegistration) {
		long idleSince = taskManagerRegistration.getIdleSince();
		taskManagerRegistration
			.getTaskManagerConnection()
			.getTaskExecutorGateway()
			.canBeReleased()
			.thenAcceptAsync(
				canBeReleased -> {
					InstanceID timedOutTaskManagerId = taskManagerRegistration.getInstanceId();
					boolean stillIdle = idleSince == taskManagerRegistration.getIdleSince();
					if (stillIdle && canBeReleased) {
						releaseTaskExecutor(timedOutTaskManagerId);
					}
				},
				mainThreadExecutor);
	}

	private void releaseTaskExecutor(InstanceID timedOutTaskManagerId) {
		final FlinkException cause = new FlinkException("TaskExecutor exceeded the idle timeout.");
		LOG.debug("Release TaskExecutor {} because it exceeded the idle timeout.", timedOutTaskManagerId);
		resourceActions.releaseResource(timedOutTaskManagerId, cause);
	}

	private void checkSlotAllocationTimeouts() {
		if (!pendingSlotAllocations.isEmpty()) {
			long currentTime = System.currentTimeMillis();

			for (Iterator<DeclarativeTaskManagerSlot> iterator = pendingSlotAllocations.values().iterator(); iterator.hasNext(); ) {
				DeclarativeTaskManagerSlot pendingSlotAllocation = iterator.next();

				if (currentTime - pendingSlotAllocation.getAllocationStartTimestamp() >= slotAllocationTimeout.toMilliseconds()) {
					internalFreeSlot(pendingSlotAllocation);
				}
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal utility methods
	// ---------------------------------------------------------------------------------------------

	private void internalUnregisterTaskManager(TaskManagerRegistration taskManagerRegistration, Exception cause) {
		Preconditions.checkNotNull(taskManagerRegistration);

		removeSlots(taskManagerRegistration.getSlots(), cause);
		taskManagerRegistrations.remove(taskManagerRegistration.getInstanceId());
	}

	private void checkInit() {
		Preconditions.checkState(started, "The slot manager has not been started.");
	}

	// ---------------------------------------------------------------------------------------------
	// Testing methods
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	int getNumResources(JobID jobId, JobResourceState state) {
		return resourceTracker.getNumResources(jobId, state);
	}

	@VisibleForTesting
	DeclarativeTaskManagerSlot getSlot(SlotID slotId) {
		return slots.get(slotId);
	}

	@VisibleForTesting
	boolean isTaskManagerIdle(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (null != taskManagerRegistration) {
			return taskManagerRegistration.isIdle();
		} else {
			return false;
		}
	}

	@Override
	@VisibleForTesting
	public void unregisterTaskManagersAndReleaseResources() {
		Iterator<Map.Entry<InstanceID, TaskManagerRegistration>> taskManagerRegistrationIterator =
				taskManagerRegistrations.entrySet().iterator();

		while (taskManagerRegistrationIterator.hasNext()) {
			TaskManagerRegistration taskManagerRegistration =
					taskManagerRegistrationIterator.next().getValue();

			taskManagerRegistrationIterator.remove();

			final FlinkException cause = new FlinkException("Triggering of SlotManager#unregisterTaskManagersAndReleaseResources.");
			internalUnregisterTaskManager(taskManagerRegistration, cause);
			resourceActions.releaseResource(taskManagerRegistration.getInstanceId(), cause);
		}
	}
}
