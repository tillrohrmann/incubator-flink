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
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Implementation of {@link SlotManager} supporting declarative slot management.
 */
public class DeclarativeSlotManager implements SlotManager {
	private static final Logger LOG = LoggerFactory.getLogger(DeclarativeSlotManager.class);

	static final AllocationID DUMMY_ALLOCATION_ID = new AllocationID();

	private final SlotTracker slotTracker;
	private final ResourceTracker resourceTracker;
	private final BiFunction<Executor, ResourceActions, TaskExecutorManager> taskExecutorManagerFactory;
	private TaskExecutorManager taskExecutorManager;

	/** Timeout for slot requests to the task manager. */
	private final Time taskManagerRequestTimeout;
	private ScheduledFuture<?> slotRequestTimeoutCheck;

	private final SlotMatchingStrategy slotMatchingStrategy;

	private final SlotManagerMetricGroup slotManagerMetricGroup;

	private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();
	private final HashMap<SlotID, CompletableFuture<Acknowledge>> pendingSlotAllocationFutures;

	/** ResourceManager's id. */
	private ResourceManagerId resourceManagerId;

	/** Executor for future callbacks which have to be "synchronized". */
	private Executor mainThreadExecutor;

	/** Callbacks for resource (de-)allocations. */
	private ResourceActions resourceActions;

	/** True iff the component has been started. */
	private boolean started;

	public DeclarativeSlotManager(
			ScheduledExecutor scheduledExecutor,
			SlotManagerConfiguration slotManagerConfiguration,
			SlotManagerMetricGroup slotManagerMetricGroup,
			ResourceTracker resourceTracker,
			SlotTracker slotTracker) {

		Preconditions.checkNotNull(slotManagerConfiguration);
		this.taskManagerRequestTimeout = slotManagerConfiguration.getTaskManagerRequestTimeout();
		this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
		this.resourceTracker = Preconditions.checkNotNull(resourceTracker);

		pendingSlotAllocationFutures = new HashMap<>(16);

		this.slotTracker = Preconditions.checkNotNull(slotTracker);
		slotTracker.registerSlotStatusUpdateListener(createSlotStatusUpdateListener());

		slotMatchingStrategy = slotManagerConfiguration.getSlotMatchingStrategy();

		taskExecutorManagerFactory = (executor, resourceActions) -> new TaskExecutorManager(
			slotManagerConfiguration.getDefaultWorkerResourceSpec(),
			slotManagerConfiguration.getNumSlotsPerWorker(),
			slotManagerConfiguration.getMaxSlotNum(),
			slotManagerConfiguration.isWaitResultConsumedBeforeRelease(),
			slotManagerConfiguration.getRedundantTaskManagerNum(),
			slotManagerConfiguration.getTaskManagerTimeout(),
			scheduledExecutor,
			executor,
			resourceActions);

		resourceManagerId = null;
		resourceActions = null;
		mainThreadExecutor = null;
		slotRequestTimeoutCheck = null;
		taskExecutorManager = null;

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
				taskExecutorManager.occupySlot(taskManagerSlot.getInstanceId());
			}
			if (previous == SlotState.ALLOCATED && current == SlotState.FREE) {
				taskExecutorManager.freeSlot(taskManagerSlot.getInstanceId());
			}
		};
	}

	private void cancelAllocationFuture(SlotID slotId) {
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = pendingSlotAllocationFutures.remove(slotId);
		// the future may be null if we are just re-playing the state transitions due to a slot report
		if (acknowledgeCompletableFuture != null) {
			acknowledgeCompletableFuture.cancel(false);
		}
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
		LOG.info("Starting the slot manager.");

		this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
		mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
		resourceActions = Preconditions.checkNotNull(newResourceActions);
		taskExecutorManager = taskExecutorManagerFactory.apply(newMainThreadExecutor, newResourceActions);

		started = true;

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
		LOG.info("Suspending the slot manager.");

		if (slotRequestTimeoutCheck != null) {
			slotRequestTimeoutCheck.cancel(false);
			slotRequestTimeoutCheck = null;
		}

		resourceTracker.clear();
		taskExecutorManager.close();

		for (InstanceID registeredTaskManager : taskExecutorManager.getTaskExecutors()) {
			unregisterTaskManager(registeredTaskManager, new SlotManagerException("The slot manager is being suspended."));
		}

		taskExecutorManager = null;
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
		LOG.info("Closing the slot manager.");

		suspend();
		slotManagerMetricGroup.close();
	}

	// ---------------------------------------------------------------------------------------------
	// Public API
	// ---------------------------------------------------------------------------------------------

	@Override
	public void processResourceRequirements(ResourceRequirements resourceRequirements) {
		checkInit();
		LOG.debug("Received resource requirements from job {}: {}", resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());

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
		LOG.debug("Registering task executor {} under {} at the slot manager.", taskExecutorConnection.getResourceID(), taskExecutorConnection.getInstanceID());

		// we identify task managers by their instance id
		if (taskExecutorManager.isTaskManagerRegistered(taskExecutorConnection.getInstanceID())) {
			LOG.debug("Task executor {} was already registered.", taskExecutorConnection.getResourceID());
			reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
			return false;
		} else {
			if (!taskExecutorManager.registerTaskManager(taskExecutorConnection, initialSlotReport)) {
				LOG.debug("Task executor {} could not be registered.", taskExecutorConnection.getResourceID());
				return false;
			}

			// register the new slots
			for (SlotStatus slotStatus : initialSlotReport) {
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

		LOG.debug("Unregistering task executor {} from the slot manager.", instanceId);

		if (taskExecutorManager.isTaskManagerRegistered(instanceId)) {
			slotTracker.removeSlots(taskExecutorManager.getSlotsOf(instanceId));
			taskExecutorManager.unregisterTaskExecutor(instanceId);
			checkResourceRequirements();

			return true;
		} else {
			LOG.debug("There is no task executor registered with instance ID {}. Ignoring this message.", instanceId);

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

		if (taskExecutorManager.isTaskManagerRegistered(instanceId)) {
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
		LOG.debug("Freeing slot {}.", slotId);

		slotTracker.notifyFree(slotId);
		checkResourceRequirements();
	}

	// ---------------------------------------------------------------------------------------------
	// Requirement matching
	// ---------------------------------------------------------------------------------------------

	private void checkResourceRequirements() {
		final Map<JobID, Collection<ResourceRequirement>> resourceAllocationInfo = resourceTracker.getRequiredResources();
		if (resourceAllocationInfo.isEmpty()) {
			return;
		}

		final Map<JobID, ResourceCounter> outstandingRequirements = new LinkedHashMap<>();
		for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements : resourceAllocationInfo.entrySet()) {
			JobID jobId = resourceRequirements.getKey();

			for (ResourceRequirement resourceRequirement : resourceRequirements.getValue()) {
				int numMissingSlots = internalRequestSlots(jobId, jobMasterTargetAddresses.get(jobId), resourceRequirement);
				if (numMissingSlots > 0) {
					outstandingRequirements
						.computeIfAbsent(jobId, ignored -> new ResourceCounter())
						.incrementCount(resourceRequirement.getResourceProfile(), numMissingSlots);
				}
			}
		}

		final ResourceCounter pendingSlots = new ResourceCounter(taskExecutorManager.getPendingTaskManagerSlots().stream().collect(
			Collectors.groupingBy(
				PendingTaskManagerSlot::getResourceProfile,
				Collectors.summingInt(x -> 1))));

		for (Map.Entry<JobID, ResourceCounter> unfulfilledRequirement : outstandingRequirements.entrySet()) {
			tryFulfillRequirementsWithPendingOrNewSlots(
				unfulfilledRequirement.getKey(),
				unfulfilledRequirement.getValue().getResourceProfilesWithCount(),
				pendingSlots);
		}
	}

	/**
	 * Tries to allocate slots for the given requirement. If there are not enough slots available, the
	 * resource manager is informed to allocate more resources.
	 *
	 * @param jobId job to allocate slots for
	 * @param targetAddress address of the jobmaster
	 * @param resourceRequirement required slots
	 * @return the number of missing slots
	 */
	private int internalRequestSlots(JobID jobId, String targetAddress, ResourceRequirement resourceRequirement) {
		final ResourceProfile requiredResource = resourceRequirement.getResourceProfile();
		Collection<TaskManagerSlotInformation> freeSlots = slotTracker.getFreeSlots();

		int numUnfulfilled = 0;
		for (int x = 0; x < resourceRequirement.getNumberOfRequiredSlots(); x++) {

			final Optional<TaskManagerSlotInformation> reservedSlot = slotMatchingStrategy.findMatchingSlot(requiredResource, freeSlots, this::getNumberRegisteredSlotsOf);
			if (reservedSlot.isPresent()) {
				// we do not need to modify freeSlots because it is indirectly modified by the allocation
				allocateSlot(reservedSlot.get(), jobId, targetAddress, requiredResource);
			} else {
				numUnfulfilled++;
			}
		}
		return numUnfulfilled;
	}

	/**
	 * Allocates the given slot. This entails sending a registration message to the task manager and treating failures.
	 *
	 * @param taskManagerSlot slot to allocate
	 * @param jobId job for which the slot should be allocated for
	 * @param targetAddress address of the job master
	 * @param resourceProfile resource profile for the requirement for which the slot is used
	 */
	private void allocateSlot(TaskManagerSlotInformation taskManagerSlot, JobID jobId, String targetAddress, ResourceProfile resourceProfile) {
		TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
		TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

		final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
		final SlotID slotId = taskManagerSlot.getSlotId();
		final InstanceID instanceId = taskManagerSlot.getInstanceId();

		LOG.debug("Starting allocation of slot {} for job {} with resource profile {}.", taskManagerSlot.getSlotId(), jobId, resourceProfile);

		slotTracker.notifyAllocationStart(slotId, jobId);

		if (!taskExecutorManager.isTaskManagerRegistered(instanceId)) {
			throw new IllegalStateException("Could not find a registered task manager for instance id " +
				instanceId + '.');
		}

		taskExecutorManager.markUsed(instanceId);

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
						LOG.trace("Completed allocation of slot {} for job {}.", slotId, jobId);
						slotTracker.notifyAllocationComplete(slotId, jobId);
					} else {
						if (throwable instanceof SlotOccupiedException) {
							SlotOccupiedException exception = (SlotOccupiedException) throwable;
							LOG.debug("Tried allocating slot {} for job {}, but it was already allocated for job {}.", slotId, jobId, exception.getJobId());
							// report as a slot status to force the state transition
							// this could be a problem if we ever assume the task executor always reports about all slots
							slotTracker.notifySlotStatus(Collections.singleton(new SlotStatus(slotId, taskManagerSlot.getResourceProfile(), exception.getJobId(), exception.getAllocationId())));
						} else {
							if (throwable instanceof CancellationException) {
								LOG.debug("Cancelled allocation of slot {} for job {}.", slotId, jobId, throwable);
							} else {
								LOG.warn("Slot allocation for slot {} for job {} failed.", slotId, jobId, throwable);
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

	private void tryFulfillRequirementsWithPendingOrNewSlots(JobID jobId, Map<ResourceProfile, Integer> missingResources, ResourceCounter pendingSlots) {
		for (Map.Entry<ResourceProfile, Integer> missingResource : missingResources.entrySet()) {
			ResourceProfile profile = missingResource.getKey();
			for (int i = 0; i < missingResource.getValue(); i++) {
				if (!tryFulfillWithPendingSlots(profile, pendingSlots)) {
					Optional<ResourceRequirement> newlyFulfillableRequirements = taskExecutorManager.allocateResource(profile);
					if (newlyFulfillableRequirements.isPresent()) {
						ResourceRequirement newSlots = newlyFulfillableRequirements.get();
						// reserve one of the new slots
						if (newSlots.getNumberOfRequiredSlots() > 1) {
							pendingSlots.incrementCount(newSlots.getResourceProfile(), newSlots.getNumberOfRequiredSlots() - 1);
						}
					} else {
						LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
						resourceActions.notifyNotEnoughResourcesAvailable(jobId, resourceTracker.getAcquiredResources(jobId));
						return;
					}
				}
			}
		}
	}

	private boolean tryFulfillWithPendingSlots(ResourceProfile resourceProfile, ResourceCounter pendingSlots) {
		Set<ResourceProfile> pendingSlotProfiles = pendingSlots.getResourceProfiles();

		// short-cut, pretty much only applicable to fine-grained resource management
		if (pendingSlotProfiles.contains(resourceProfile)) {
			pendingSlots.decrementCount(resourceProfile, 1);
			return true;
		}

		for (ResourceProfile pendingSlotProfile : pendingSlotProfiles) {
			if (pendingSlotProfile.isMatching(resourceProfile)) {
				pendingSlots.decrementCount(pendingSlotProfile, 1);
				return true;
			}
		}

		return false;
	}

	// ---------------------------------------------------------------------------------------------
	// Legacy APIs
	// ---------------------------------------------------------------------------------------------

	@Override
	public int getNumberRegisteredSlots() {
		return taskExecutorManager.getNumberRegisteredSlots();
	}

	@Override
	public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
		return taskExecutorManager.getNumberRegisteredSlotsOf(instanceId);
	}

	@Override
	public int getNumberFreeSlots() {
		return taskExecutorManager.getNumberFreeSlots();
	}

	@Override
	public int getNumberFreeSlotsOf(InstanceID instanceId) {
		return taskExecutorManager.getNumberFreeSlotsOf(instanceId);
	}

	@Override
	public Map<WorkerResourceSpec, Integer> getRequiredResources() {
		return taskExecutorManager.getRequiredResources();
	}

	@Override
	public ResourceProfile getRegisteredResource() {
		return taskExecutorManager.getRegisteredResource();
	}

	@Override
	public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
		return taskExecutorManager.getRegisteredResourceOf(instanceID);
	}

	@Override
	public ResourceProfile getFreeResource() {
		return taskExecutorManager.getFreeResource();
	}

	@Override
	public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
		return taskExecutorManager.getFreeResourceOf(instanceID);
	}

	@Override
	public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
		// we always send notifications if we cannot fulfill requests, and it is the responsibility of the JobManager
		// to handle it (e.g., by reducing requirements and failing outright)
	}

	@Override
	public int getNumberPendingSlotRequests() {
		throw new UnsupportedOperationException();
	}

	// ---------------------------------------------------------------------------------------------
	// Internal utility methods
	// ---------------------------------------------------------------------------------------------

	private void checkInit() {
		Preconditions.checkState(started, "The slot manager has not been started.");
	}

	// ---------------------------------------------------------------------------------------------
	// Testing methods
	// ---------------------------------------------------------------------------------------------

	@Override
	@VisibleForTesting
	public void unregisterTaskManagersAndReleaseResources() {
	}
}
