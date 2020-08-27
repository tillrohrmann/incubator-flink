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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slotsbro.ResourceRequirement;
import org.apache.flink.runtime.slotsbro.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link SlotPool} implementation which uses the {@link DeclarativeSlotPoolNg} to allocate slots.
 */
public class FutureSlotPool implements SlotPool {

	private static final Logger LOG = LoggerFactory.getLogger(FutureSlotPool.class);

	private final JobID jobId;

	private final DeclarativeSlotPoolNg declarativeSlotPool;

	private final Map<SlotRequestId, PendingRequest> pendingRequests;

	private final Map<SlotRequestId, AllocationID> fulfilledRequests;

	private final Set<ResourceID> registeredTaskManagers;

	private final Clock clock;

	private final Time rpcTimeout;

	private final Time idleSlotTimeout;

	private final Time batchSlotTimeout;

	private final Set<AllocationID> newSlotsSet = new HashSet<>();

	@Nullable
	private ComponentMainThreadExecutor componentMainThreadExecutor;

	@Nullable
	private String jobManagerAddress;

	@Nullable
	private JobMasterId jobMasterId;

	private ResourceManagerConnectionManager resourceManagerConnectionManager;

	private boolean isBatchSlotRequestTimeoutCheckDisabled;

	public FutureSlotPool(
			JobID jobId,
			DeclarativeSlotPoolNgFactory declarativeSlotPoolFactory,
			Clock clock,
			Time rpcTimeout,
			Time idleSlotTimeout,
			Time batchSlotTimeout) {
		this.jobId = jobId;
		this.declarativeSlotPool = declarativeSlotPoolFactory.create(
			this::declareResourceRequirements,
			this::newSlotsAreAvailable,
			idleSlotTimeout,
			rpcTimeout);
		this.clock = clock;
		this.rpcTimeout = rpcTimeout;
		this.idleSlotTimeout = idleSlotTimeout;
		this.batchSlotTimeout = batchSlotTimeout;
		this.pendingRequests = new LinkedHashMap<>();
		this.fulfilledRequests = new HashMap<>();
		this.registeredTaskManagers = new HashSet<>();
		this.resourceManagerConnectionManager = NoOpResourceManagerConnectionManager.INSTANCE;
		this.isBatchSlotRequestTimeoutCheckDisabled = false;
	}

	@Override
	public void start(JobMasterId jobMasterId, String newJobManagerAddress, ComponentMainThreadExecutor jmMainThreadScheduledExecutor) throws Exception {
		this.componentMainThreadExecutor = jmMainThreadScheduledExecutor;
		this.jobManagerAddress = newJobManagerAddress;
		this.jobMasterId = jobMasterId;
		this.resourceManagerConnectionManager = DefaultResourceManagerConnectionManager.create(componentMainThreadExecutor);

		componentMainThreadExecutor.schedule(this::checkIdleSlotTimeout, idleSlotTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		componentMainThreadExecutor.schedule(this::checkBatchSlotTimeout, batchSlotTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void suspend() {
		assertRunningInMainThread();
		LOG.info("Suspending FutureSlotPool.");

		declarativeSlotPool.printStatus();

		cancelPendingRequests();
		clearState();
	}

	private void clearState() {
		resourceManagerConnectionManager.close();
		resourceManagerConnectionManager = NoOpResourceManagerConnectionManager.INSTANCE;
		registeredTaskManagers.clear();
		jobManagerAddress = null;
		jobMasterId = null;
	}

	private void cancelPendingRequests() {
		ResourceCounter decreasedResourceRequirements = ResourceCounter.empty();

		final FlinkException cancelCause = new FlinkException("Cancelling all pending requests.");

		final Iterable<PendingRequest> pendingRequestsToFail = new ArrayList<>(pendingRequests.values());
		pendingRequests.clear();

		for (PendingRequest pendingRequest : pendingRequestsToFail) {
			pendingRequest.failRequest(cancelCause);
			decreasedResourceRequirements = decreasedResourceRequirements.add(pendingRequest.getResourceProfile(), 1);
		}

		declarativeSlotPool.decreaseResourceRequirementsBy(decreasedResourceRequirements);
	}

	@Override
	public void close() {
		LOG.info("Closing FutureSlotPool.");

		declarativeSlotPool.printStatus();

		cancelPendingRequests();
		releaseAllTaskManagers(new FlinkException("Closing FutureSlotPool."));
		clearState();
	}

	private void releaseAllTaskManagers(FlinkException cause) {
		for (ResourceID registeredTaskManager : registeredTaskManagers) {
			internalReleaseTaskManager(registeredTaskManager, cause);
		}

		registeredTaskManagers.clear();
	}

	@Override
	public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
		assertRunningInMainThread();

		resourceManagerConnectionManager.connect(resourceRequirements -> resourceManagerGateway.declareRequiredResources(jobMasterId, resourceRequirements, rpcTimeout));
		declareResourceRequirements(declarativeSlotPool.getResourceRequirements());
	}

	private void declareResourceRequirements(Collection<ResourceRequirement> resourceRequirements) {
		assertRunningInMainThread();

		LOG.debug("Declare new resource requirements for job {}: {}.", jobId, resourceRequirements);

		resourceManagerConnectionManager.declareResourceRequirements(new ResourceRequirements(jobId, jobManagerAddress, resourceRequirements));
	}

	@Override
	public void disconnectResourceManager() {
		assertRunningInMainThread();
		this.resourceManagerConnectionManager.disconnect();
	}

	@Override
	public boolean registerTaskManager(ResourceID resourceID) {
		assertRunningInMainThread();

		LOG.debug("Register new TaskExecutor {}.", resourceID);
		return registeredTaskManagers.add(resourceID);
	}

	@Override
	public boolean releaseTaskManager(ResourceID resourceId, Exception cause) {
		assertRunningInMainThread();

		if (registeredTaskManagers.remove(resourceId)) {
			internalReleaseTaskManager(resourceId, cause);
			return true;
		} else {
			return false;
		}
	}

	private void internalReleaseTaskManager(ResourceID resourceId, Exception cause) {
		LOG.debug("Release TaskManager {}.", resourceId, cause);
		declarativeSlotPool.failSlots(resourceId, cause);
	}

	@Override
	public Collection<SlotOffer> offerSlots(TaskManagerLocation taskManagerLocation, TaskManagerGateway taskManagerGateway, Collection<SlotOffer> offers) {
		assertRunningInMainThread();

		if (!registeredTaskManagers.contains(taskManagerLocation.getResourceID())) {
			return Collections.emptyList();
		}

		return declarativeSlotPool.offerSlots(offers, taskManagerLocation, taskManagerGateway, clock.relativeTimeMillis());
	}

	private void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
		// Avoid notifying new slots multiple times due to SchedulerImpl allocating and releasing slots
		// in order to find the best shared slot
		final Collection<PhysicalSlot> slotsToProcess = new ArrayList<>();
		for (PhysicalSlot newSlot : newSlots) {
			if (newSlotsSet.add(newSlot.getAllocationId())) {
				slotsToProcess.add(newSlot);
			}
		}

		for (PhysicalSlot newSlot : slotsToProcess) {
			final Optional<PendingRequest> matchingPendingRequest = findMatchingPendingRequest(newSlot);

			matchingPendingRequest.ifPresent(pendingRequest -> {
				Preconditions.checkNotNull(pendingRequests.remove(pendingRequest.getSlotRequestId()), "Cannot fulfill a non existing pending slot request.");
				fulfillPendingRequest(newSlot, pendingRequest);
			});

			newSlotsSet.remove(newSlot.getAllocationId());
		}

	}

	private void fulfillPendingRequest(PhysicalSlot newSlot, PendingRequest pendingRequest) {
		final SlotRequestId slotRequestId = pendingRequest.getSlotRequestId();
		final AllocationID allocationId = newSlot.getAllocationId();

		allocateFreeSlot(slotRequestId, allocationId);

		Preconditions.checkState(pendingRequest.fulfill(newSlot), "Pending requests must be fulfillable.");
	}

	private void allocateFreeSlot(SlotRequestId slotRequestId, AllocationID allocationId) {
		LOG.debug("Allocate slot {} for slot request id {}", allocationId, slotRequestId);
		declarativeSlotPool.allocateFreeSlot(allocationId);
		fulfilledRequests.put(slotRequestId, allocationId);
	}

	private PhysicalSlot allocateFreeSlotForResource(SlotRequestId slotRequestId, AllocationID allocationId, ResourceProfile requiredSlotProfile) {
		final PhysicalSlot physicalSlot = declarativeSlotPool.allocateFreeSlotForResource(allocationId, requiredSlotProfile);
		fulfilledRequests.put(slotRequestId, allocationId);

		return physicalSlot;
	}

	private Optional<PendingRequest> findMatchingPendingRequest(PhysicalSlot slot) {
		LOG.debug("Find matching pending request for slot {}.", slot);
		LOG.debug("Pending requests: {}", pendingRequests.values());
		final ResourceProfile resourceProfile = slot.getResourceProfile();

		for (PendingRequest pendingRequest : pendingRequests.values()) {
			if (resourceProfile.isMatching(pendingRequest.getResourceProfile())) {
				return Optional.of(pendingRequest);
			}
		}

		return Optional.empty();
	}

	@Override
	public Optional<ResourceID> failAllocation(AllocationID allocationID, Exception cause) {
		throw new UnsupportedOperationException("Please call failAllocation(ResourceID, AllocationID, Exception)");
	}

	@Override
	public Optional<ResourceID> failAllocation(@Nullable ResourceID resourceId, AllocationID allocationID, Exception cause) {
		assertRunningInMainThread();

		Preconditions.checkNotNull(resourceId, "The FutureSlotPool only supports failAllocation calls coming from the TaskExecutor.");

		declarativeSlotPool.failSlot(allocationID, cause);

		if (declarativeSlotPool.containsSlots(resourceId)) {
			return Optional.empty();
		} else {
			return Optional.of(resourceId);
		}
	}

	@Override
	public Collection<SlotInfoWithUtilization> getAvailableSlotsInformation() {
		assertRunningInMainThread();

		return declarativeSlotPool.getFreeSlotsInformation();
	}

	@Override
	public Collection<SlotInfo> getAllocatedSlotsInformation() {
		assertRunningInMainThread();

		@SuppressWarnings("unchecked")
		final Collection<SlotInfo> result = (Collection<SlotInfo>) declarativeSlotPool.getAllSlotsInformation();
		return result;
	}

	@Override
	public Optional<PhysicalSlot> allocateAvailableSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull AllocationID allocationID) {
		throw new UnsupportedOperationException("This method should not be used when using declarative resource management.");
	}

	@Override
	public Optional<PhysicalSlot> allocateAvailableSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull AllocationID allocationID, @Nullable ResourceProfile requiredSlotProfile) {
		assertRunningInMainThread();
		Preconditions.checkNotNull(requiredSlotProfile, "The requiredSlotProfile must not be null.");

		return Optional.of(allocateFreeSlotForResource(slotRequestId, allocationID, requiredSlotProfile));
	}

	@Override
	@Nonnull
	public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull ResourceProfile resourceProfile, @Nullable Time timeout) {
		assertRunningInMainThread();

		LOG.debug("Request new allocated slot with slot request id {} and resource profile {}", slotRequestId, resourceProfile);

		final PendingRequest pendingRequest = PendingRequest.createNormalRequest(slotRequestId, resourceProfile);

		if (timeout != null) {
			FutureUtils
				.orTimeout(
					pendingRequest.getSlotFuture(),
					timeout.toMilliseconds(),
					TimeUnit.MILLISECONDS,
					componentMainThreadExecutor)
				.whenComplete((physicalSlot, throwable) -> {
					if (throwable instanceof TimeoutException) {
						timeoutPendingSlotRequest(slotRequestId);
					}
				});
		}

		return internalRequestNewAllocatedSlot(pendingRequest);
	}

	private void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
		releaseSlot(slotRequestId, new TimeoutException("Pending slot request timed out in slot pool."));
	}

	@Override
	@Nonnull
	public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull ResourceProfile resourceProfile) {
		assertRunningInMainThread();

		LOG.debug("Request new allocated batch slot with slot request id {} and resource profile {}", slotRequestId, resourceProfile);

		final PendingRequest pendingRequest = PendingRequest.createBatchRequest(slotRequestId, resourceProfile);

		return internalRequestNewAllocatedSlot(pendingRequest);
	}

	private CompletableFuture<PhysicalSlot> internalRequestNewAllocatedSlot(PendingRequest pendingRequest) {
		pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);

		declarativeSlotPool.increaseResourceRequirementsBy(ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));

		return pendingRequest.getSlotFuture();
	}

	@Override
	public void disableBatchSlotRequestTimeoutCheck() {
		assertRunningInMainThread();

		isBatchSlotRequestTimeoutCheckDisabled = true;
	}

	@Override
	public AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId) {
		assertRunningInMainThread();

		// TODO: Can be improved by asking the DeclarativeSlotPool for slots belonging to taskManagerId
		final Collection<? extends SlotInfo> allocatedSlotsInformation = declarativeSlotPool.getAllSlotsInformation();

		final Collection<AllocatedSlotInfo> allocatedSlotInfos = new ArrayList<>();

		for (SlotInfo slotInfo : allocatedSlotsInformation) {
			if (slotInfo.getTaskManagerLocation().getResourceID().equals(taskManagerId)) {
				allocatedSlotInfos.add(new AllocatedSlotInfo(
					slotInfo.getPhysicalSlotNumber(),
					slotInfo.getAllocationId()));
			}
		}

		return new AllocatedSlotReport(jobId, allocatedSlotInfos);
	}

	@Override
	public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
		LOG.debug("Release slot with slot request id {}", slotRequestId);
		assertRunningInMainThread();

		final PendingRequest pendingRequest = pendingRequests.remove(slotRequestId);

		if (pendingRequest != null) {
			declarativeSlotPool.decreaseResourceRequirementsBy(ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
			pendingRequest.failRequest(new FlinkException(
				String.format("Pending slot request with %s has been released.", pendingRequest.getSlotRequestId()),
				cause));
		} else {
			final AllocationID allocationId = fulfilledRequests.remove(slotRequestId);

			if (allocationId != null) {
				declarativeSlotPool.releaseSlot(allocationId, cause, clock.relativeTimeMillis());
			} else {
				LOG.debug("Could not find slot which has fulfilled slot request {}. Ignoring the release operation.", slotRequestId);
			}
		}
	}

	private void assertRunningInMainThread() {
		if (componentMainThreadExecutor != null) {
			componentMainThreadExecutor.assertRunningInMainThread();
		} else {
			throw new IllegalStateException("The FutureSlotPool has not been started yet.");
		}
	}

	private void checkIdleSlotTimeout() {
		declarativeSlotPool.returnIdleSlots(clock.relativeTimeMillis());

		if (componentMainThreadExecutor != null) {
			componentMainThreadExecutor.schedule(this::checkIdleSlotTimeout, idleSlotTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	private void checkBatchSlotTimeout() {
		if (isBatchSlotRequestTimeoutCheckDisabled) {
			return;
		}

		final Collection<PendingRequest> pendingBatchRequests = getPendingBatchRequests();

		if (!pendingBatchRequests.isEmpty()) {
			final Set<ResourceProfile> allResourceProfiles = getResourceProfilesFromAllSlots();

			final Map<Boolean, List<PendingRequest>> fulfillableAndUnfulfillableRequests = pendingBatchRequests
				.stream()
				.collect(Collectors.partitioningBy(canBeFulfilledWithAnySlot(allResourceProfiles)));

			final List<PendingRequest> fulfillableRequests = fulfillableAndUnfulfillableRequests.get(true);
			final List<PendingRequest> unfulfillableRequests = fulfillableAndUnfulfillableRequests.get(false);

			final long currentTimestamp = clock.relativeTimeMillis();

			for (PendingRequest fulfillableRequest : fulfillableRequests) {
				fulfillableRequest.markFulfillable();
			}

			for (PendingRequest unfulfillableRequest : unfulfillableRequests) {
				unfulfillableRequest.markUnfulfillable(currentTimestamp);

				if (unfulfillableRequest.getUnfulfillableSince() + batchSlotTimeout.toMilliseconds() <= currentTimestamp) {
					timeoutPendingSlotRequest(unfulfillableRequest.getSlotRequestId());
				}
			}
		}

		if (componentMainThreadExecutor != null) {
			componentMainThreadExecutor.schedule(this::checkBatchSlotTimeout, batchSlotTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	private Set<ResourceProfile> getResourceProfilesFromAllSlots() {
		return Stream
			.concat(
				getAvailableSlotsInformation().stream(),
				getAllocatedSlotsInformation().stream())
			.map(SlotInfo::getResourceProfile)
			.collect(Collectors.toSet());
	}

	private Collection<PendingRequest> getPendingBatchRequests() {
		return pendingRequests.values().stream()
			.filter(PendingRequest::isBatchRequest)
			.collect(Collectors.toList());
	}

	private Predicate<PendingRequest> canBeFulfilledWithAnySlot(Set<ResourceProfile> allocatedResourceProfiles) {
		return pendingRequest -> {
			for (ResourceProfile allocatedResourceProfile : allocatedResourceProfiles) {
				if (allocatedResourceProfile.isMatching(pendingRequest.getResourceProfile())) {
					return true;
				}
			}

			return false;
		};
	}

	private static final class PendingRequest {

		private final SlotRequestId slotRequestId;

		private final ResourceProfile resourceProfile;

		private final CompletableFuture<PhysicalSlot> slotFuture;

		private final boolean isBatchRequest;

		private long unfulfillableSince;

		private PendingRequest(SlotRequestId slotRequestId, ResourceProfile resourceProfile, boolean isBatchRequest) {
			this.slotRequestId = slotRequestId;
			this.resourceProfile = resourceProfile;
			this.isBatchRequest = isBatchRequest;
			this.slotFuture = new CompletableFuture<>();
			this.unfulfillableSince = Long.MAX_VALUE;
		}

		static PendingRequest createBatchRequest(SlotRequestId slotRequestId, ResourceProfile resourceProfile) {
			return new PendingRequest(
				slotRequestId,
				resourceProfile,
				true);
		}

		static PendingRequest createNormalRequest(SlotRequestId slotRequestId, ResourceProfile resourceProfile) {
			return new PendingRequest(
				slotRequestId,
				resourceProfile,
				false);
		}

		SlotRequestId getSlotRequestId() {
			return slotRequestId;
		}

		ResourceProfile getResourceProfile() {
			return resourceProfile;
		}

		CompletableFuture<PhysicalSlot> getSlotFuture() {
			return slotFuture;
		}

		void cancelRequest() {
			slotFuture.completeExceptionally(new CancellationException("The slot request has been cancelled."));
		}

		void failRequest(Exception cause) {
			slotFuture.completeExceptionally(cause);
		}

		public boolean isBatchRequest() {
			return isBatchRequest;
		}

		public void markFulfillable() {
			this.unfulfillableSince = Long.MAX_VALUE;
		}

		public void markUnfulfillable(long currentTimestamp) {
			this.unfulfillableSince = currentTimestamp;
		}

		public long getUnfulfillableSince() {
			return unfulfillableSince;
		}

		public boolean fulfill(PhysicalSlot slot) {
			return slotFuture.complete(slot);
		}

		@Override
		public String toString() {
			return "PendingRequest{" +
				"slotRequestId=" + slotRequestId +
				", resourceProfile=" + resourceProfile +
				", isBatchRequest=" + isBatchRequest +
				", unfulfillableSince=" + unfulfillableSince +
				'}';
		}
	}
}
