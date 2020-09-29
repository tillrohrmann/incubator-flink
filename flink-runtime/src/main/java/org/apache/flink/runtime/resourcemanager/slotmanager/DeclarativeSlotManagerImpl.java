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
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
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

	/** Timeout after which an unused TaskManager is released. */
	private final Time taskManagerTimeout;

	private final HashMap<SlotID, CompletableFuture<Acknowledge>> pendingSlotAllocationFutures;

	/** All currently registered task managers. */
	private final HashMap<InstanceID, TaskManagerRegistration> taskManagerRegistrations;

	private final HashMap<TaskManagerSlotId, PendingTaskManagerSlot> pendingSlots;

	private final DefaultSlotTracker slotTracker;
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

	private final ResourceTracker resourceTracker;

	private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();

	// TODO: add tracker constructor arguments
	public DeclarativeSlotManagerImpl(
			ScheduledExecutor scheduledExecutor,
			SlotManagerConfiguration slotManagerConfiguration,
			SlotManagerMetricGroup slotManagerMetricGroup) {

		this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

		Preconditions.checkNotNull(slotManagerConfiguration);
		this.taskManagerRequestTimeout = slotManagerConfiguration.getTaskManagerRequestTimeout();
		this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
		this.waitResultConsumedBeforeRelease = slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
		this.defaultWorkerResourceSpec = slotManagerConfiguration.getDefaultWorkerResourceSpec();
		this.numSlotsPerWorker = slotManagerConfiguration.getNumSlotsPerWorker();
		this.defaultSlotResourceProfile = generateDefaultSlotResourceProfile(defaultWorkerResourceSpec, numSlotsPerWorker);
		this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
		this.maxSlotNum = slotManagerConfiguration.getMaxSlotNum();
		this.redundantTaskManagerNum = slotManagerConfiguration.getRedundantTaskManagerNum();
		this.resourceTracker = new DefaultResourceTracker();

		pendingSlotAllocationFutures = new HashMap<>(16);
		taskManagerRegistrations = new HashMap<>(4);
		pendingSlots = new HashMap<>(16);

		this.slotTracker = new DefaultSlotTracker(createSlotStatusUpdateListener());
		slotMatchingStrategy = slotManagerConfiguration.getSlotMatchingStrategy();

		resourceManagerId = null;
		resourceActions = null;
		mainThreadExecutor = null;
		taskManagerTimeoutsAndRedundancyCheck = null;
		slotRequestTimeoutCheck = null;

		started = false;
	}

	private SlotStatusUpdateListener createSlotStatusUpdateListener() {
		return (taskManagerSlot, previous, current, jobId) -> {
			if (previous == SlotState.PENDING) {
				cancelAllocationFuture(taskManagerSlot.getSlotId());
			}

			if (current == SlotState.PENDING) {
				resourceTracker.notifyAcquiredResource(jobId, taskManagerSlot.getResourceProfile());
			}
			if (current == SlotState.FREE) {
				resourceTracker.notifyLostResource(jobId, taskManagerSlot.getResourceProfile());
			}

			if (current == SlotState.ALLOCATED) {
				taskManagerRegistrations.get(taskManagerSlot.getInstanceId()).occupySlot();
			}
			if (previous == SlotState.ALLOCATED && current == SlotState.FREE) {
				taskManagerRegistrations.get(taskManagerSlot.getInstanceId()).freeSlot();
			}
		};
	}

	@Override
	public int getNumberRegisteredSlots() {
		return taskManagerRegistrations.values().stream()
			.map(TaskManagerRegistration::getNumberRegisteredSlots)
			.reduce(0, Integer::sum);
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
		return slotTracker.getFreeSlots().size();
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

		registerSlotManagerMetrics();
	}

	private void registerSlotManagerMetrics() {
		// TODO: are there interesting opportunities for additional metrics?
		// TODO: job-scoped missing/acquired slots? time-since-requirements-(not-)fulfilled
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
		if (resourceRequirements.getResourceRequirements().isEmpty()) {
			jobMasterTargetAddresses.remove(resourceRequirements.getJobId());
		} else {
			jobMasterTargetAddresses.put(resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
		}
		resourceTracker.notifyResourceRequirements(resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
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
				findAndRemoveExactlyMatchingPendingTaskManagerSlot(slotStatus.getResourceProfile());

				slotTracker.addSlot(
					slotStatus.getSlotID(),
					slotStatus.getResourceProfile(),
					taskExecutorConnection,
					slotStatus.getJobID());
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
			slotTracker.notifySlotStatus(slotReport);
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

		slotTracker.notifyFree(slotId);
		checkResourceRequirements();
	}

	@Override
	// TODO: figure out whether this should just be a no-op
	public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
		if (!this.failUnfulfillableRequest && failUnfulfillableRequest) {
			// TODO: fail unfulfillable pending requests
		}
		this.failUnfulfillableRequest = failUnfulfillableRequest;
	}

	// ---------------------------------------------------------------------------------------------
	// Behaviour methods
	// ---------------------------------------------------------------------------------------------

	private void checkResourceRequirements() {
		final Map<JobID, Collection<ResourceRequirement>> resourceAllocationInfo = resourceTracker.getRequiredResources();
		for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements : resourceAllocationInfo.entrySet()) {
			JobID jobId = resourceRequirements.getKey();
			for (ResourceRequirement resourceRequirement : resourceRequirements.getValue()) {
				if (resourceRequirement.getNumberOfRequiredSlots() > 0) {
					internalRequestSlots(jobId, jobMasterTargetAddresses.get(jobId), resourceRequirement);
				}
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal slot operations
	// ---------------------------------------------------------------------------------------------

	private void findAndRemoveExactlyMatchingPendingTaskManagerSlot(ResourceProfile resourceProfile) {
		for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
			if (isPendingSlotExactlyMatchingResourceProfile(pendingTaskManagerSlot, resourceProfile)) {
				pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
				return;
			}
		}
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
			Collection<TaskManagerSlotInformation> freeSlots = slotTracker.getFreeSlots();

			final Optional<TaskManagerSlotInformation> reservedSlot = slotMatchingStrategy.findMatchingSlot(resourceProfile, freeSlots, this::getNumberRegisteredSlotsOf);
			if (reservedSlot.isPresent()) {
				allocateSlot(reservedSlot.get(), jobId, targetAddress, resourceProfile);
			} else {
				// TODO: don't return pending slot; instead check total set of required slots against set of pending slots
				// TODO: simple implementation would copy the pendingSlots collection, and go through a similar loop as for actual slots
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
			// TODO: this should only be done once per job; return a boolean here and merge it with the result for
			// TODO: other Requirements
			resourceActions.notifyNotEnoughResourcesAvailable(jobId, resourceTracker.getAcquiredResources(jobId));
		}
	}

	// TODO: remove; inherently flawed since one _big_ slot can fulfill all requirements
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

	// TODO: everything related to allocating a task executor should go into a separate component
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
	private void allocateSlot(TaskManagerSlotInformation taskManagerSlot, JobID jobId, String targetAddress, ResourceProfile resourceProfile) {
		LOG.debug("Allocate slot for job {} with resource profile {}.", jobId, resourceProfile);

		TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
		TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

		final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
		final SlotID slotId = taskManagerSlot.getSlotId();
		final InstanceID instanceID = taskManagerSlot.getInstanceId();

		slotTracker.notifyAllocationStart(slotId, jobId);

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
					if (acknowledge != null) {
						slotTracker.notifyAllocationComplete(slotId, jobId);
					} else {
						if (throwable instanceof SlotOccupiedException) {
							SlotOccupiedException exception = (SlotOccupiedException) throwable;
							slotTracker.notifySlotStatus(Collections.singleton(new SlotStatus(slotId, taskManagerSlot.getResourceProfile(), exception.getJobId(), exception.getAllocationId())));
						} else {
							if (throwable instanceof  CancellationException) {
								LOG.debug("Slot allocation request for slot {} has been cancelled.", slotId, throwable);
							} else {
								slotTracker.notifyFree(slotId);
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

			int slotsDiff = redundantTaskManagerNum * numSlotsPerWorker - getNumberFreeSlots();
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

	// ---------------------------------------------------------------------------------------------
	// Internal utility methods
	// ---------------------------------------------------------------------------------------------

	private void internalUnregisterTaskManager(TaskManagerRegistration taskManagerRegistration, Exception cause) {
		Preconditions.checkNotNull(taskManagerRegistration);

		slotTracker.removeSlots(taskManagerRegistration.getSlots());
		taskManagerRegistrations.remove(taskManagerRegistration.getInstanceId());
	}

	private void checkInit() {
		Preconditions.checkState(started, "The slot manager has not been started.");
	}

	// ---------------------------------------------------------------------------------------------
	// Testing methods
	// ---------------------------------------------------------------------------------------------

	// TODO: get rid of these methods
	@VisibleForTesting
	int getNumResources(JobID jobId, JobResourceState state) {
		switch (state) {
			case ACQUIRED:
				return resourceTracker.getAcquiredResources(jobId).stream().map(ResourceRequirement::getNumberOfRequiredSlots).reduce(0, Integer::sum);
			case MISSING:
				return resourceTracker.getRequiredResources().getOrDefault(jobId, Collections.emptyList()).stream().map(ResourceRequirement::getNumberOfRequiredSlots).reduce(0, Integer::sum);
			default:
				throw new IllegalArgumentException("Unknown job resource state " + state);
		}
	}

	@VisibleForTesting
	DeclarativeTaskManagerSlot getSlot(SlotID slotId) {
		return slotTracker.getSlot(slotId);
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
	}
}
