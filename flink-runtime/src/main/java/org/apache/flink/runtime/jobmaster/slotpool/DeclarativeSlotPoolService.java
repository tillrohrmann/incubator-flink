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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

/** {@link SlotPoolService} implementation for the {@link DeclarativeSlotPool}. */
public class DeclarativeSlotPoolService implements SlotPoolService {

    private final JobID jobId;

    private final Time idleSlotTimeout;

    private final Time rpcTimeout;

    private final DeclarativeSlotPool declarativeSlotPool;

    private final HashSet<ResourceID> registeredTaskManagers;

    private DeclareResourceRequirementServiceConnectionManager
            resourceRequirementServiceConnectionManager =
                    NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;

    @Nullable private JobMasterId jobMasterId;

    @Nullable private String jobManagerAddress;

    private State state = State.CREATED;

    public DeclarativeSlotPoolService(JobID jobId, Time idleSlotTimeout, Time rpcTimeout) {
        this.jobId = jobId;
        this.idleSlotTimeout = idleSlotTimeout;
        this.rpcTimeout = rpcTimeout;
        this.registeredTaskManagers = new HashSet<>();

        this.declarativeSlotPool =
                new DefaultDeclarativeSlotPool(
                        jobId,
                        new DefaultAllocatedSlotPool(),
                        this::declareResourceRequirements,
                        idleSlotTimeout,
                        rpcTimeout);
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(declarativeSlotPool.getClass())) {
            return Optional.of(clazz.cast(declarativeSlotPool));
        }

        return Optional.empty();
    }

    @Override
    public void start(
            JobMasterId jobMasterId,
            String jobManagerAddress,
            ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        Preconditions.checkState(
                state == State.CREATED, "The DeclarativeSlotPoolService can only be started once.");

        this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
        this.jobManagerAddress = Preconditions.checkNotNull(jobManagerAddress);

        this.resourceRequirementServiceConnectionManager =
                DefaultDeclareResourceRequirementServiceConnectionManager.create(
                        mainThreadExecutor);

        state = State.STARTED;
    }

    private void assertHasBeenStarted() {
        Preconditions.checkState(
                state == State.STARTED, "The DeclarativeSlotPoolService has to be started.");
    }

    @Override
    public void suspend() {
        throw new UnsupportedOperationException("This method should not be needed.");
    }

    @Override
    public void close() {
        if (state != State.CLOSED) {

            if (resourceRequirementServiceConnectionManager != null) {
                resourceRequirementServiceConnectionManager.close();
            }

            releaseAllTaskManagers(
                    new FlinkException("The DeclarativeSlotPoolService is being closed."));

            state = State.CLOSED;
        }
    }

    @Override
    public Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers) {
        assertHasBeenStarted();
        return declarativeSlotPool.offerSlots(
                offers, taskManagerLocation, taskManagerGateway, System.nanoTime());
    }

    @Override
    public Optional<ResourceID> failAllocation(
            @Nullable ResourceID resourceID, AllocationID allocationId, Exception cause) {
        assertHasBeenStarted();
        Preconditions.checkNotNull(resourceID);

        declarativeSlotPool.releaseSlot(allocationId, cause);

        if (declarativeSlotPool.containsSlots(resourceID)) {
            return Optional.empty();
        } else {
            return Optional.of(resourceID);
        }
    }

    @Override
    public boolean registerTaskManager(ResourceID taskManagerId) {
        assertHasBeenStarted();

        return registeredTaskManagers.add(taskManagerId);
    }

    @Override
    public boolean releaseTaskManager(ResourceID resourceID, Exception cause) {
        assertHasBeenStarted();

        if (registeredTaskManagers.remove(resourceID)) {
            internalReleaseTaskManager(resourceID, cause);
            return true;
        }

        return false;
    }

    private void releaseAllTaskManagers(Exception cause) {
        for (ResourceID registeredTaskManager : registeredTaskManagers) {
            internalReleaseTaskManager(registeredTaskManager, cause);
        }

        registeredTaskManagers.clear();
    }

    private void internalReleaseTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();

        declarativeSlotPool.releaseSlots(taskManagerId, cause);
    }

    @Override
    public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.connect(
                resourceRequirements ->
                        resourceManagerGateway.declareRequiredResources(
                                jobMasterId, resourceRequirements, rpcTimeout));

        declareResourceRequirements(declarativeSlotPool.getResourceRequirements());
    }

    private void declareResourceRequirements(Collection<ResourceRequirement> resourceRequirements) {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.declareResourceRequirements(
                ResourceRequirements.create(jobId, jobManagerAddress, resourceRequirements));
    }

    @Override
    public void disconnectResourceManager() {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.disconnect();
    }

    @Override
    public AllocatedSlotReport createAllocatedSlotReport(ResourceID resourceID) {
        assertHasBeenStarted();

        final Collection<AllocatedSlotInfo> allocatedSlotInfos = new ArrayList<>();

        for (SlotInfo slotInfo : declarativeSlotPool.getAllSlotsInformation()) {
            if (slotInfo.getTaskManagerLocation().getResourceID().equals(resourceID)) {
                allocatedSlotInfos.add(
                        new AllocatedSlotInfo(
                                slotInfo.getPhysicalSlotNumber(), slotInfo.getAllocationId()));
            }
        }
        return new AllocatedSlotReport(jobId, allocatedSlotInfos);
    }

    private enum State {
        CREATED,
        STARTED,
        CLOSED,
    }
}
