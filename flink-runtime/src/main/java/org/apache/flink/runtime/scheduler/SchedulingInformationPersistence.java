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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Service for persisting scheduling information. */
public class SchedulingInformationPersistence {

    private static final Logger LOG =
            LoggerFactory.getLogger(SchedulingInformationPersistence.class);

    private final JobID jobId;

    private final File schedulingInformationFile;

    private final Map<ExecutionVertexID, SchedulingInformation> schedulingInformationCache;

    public SchedulingInformationPersistence(JobID jobId, File schedulingInformationFile) {
        this.jobId = jobId;
        this.schedulingInformationFile = schedulingInformationFile;
        this.schedulingInformationCache = new HashMap<>();

        try {
            restoreSchedulingInformation();
        } catch (IOException | ClassNotFoundException e) {
            LOG.debug("Could not restore the scheduling information for job {}.", jobId);
        }
    }

    public void store(Collection<SchedulingInformation> schedulingInformation) {
        for (SchedulingInformation information : schedulingInformation) {
            schedulingInformationCache.put(information.getExecutionVertexId(), information);
        }

        try {
            persistSchedulingInformation();
        } catch (IOException ioe) {
            LOG.debug("Could not persist the scheduling information for job {}.", jobId);
        }
    }

    public Optional<AllocationID> getLatestPriorAllocationId(ExecutionVertexID executionVertexID) {
        return Optional.ofNullable(schedulingInformationCache.get(executionVertexID))
                .map(SchedulingInformation::getAllocationId);
    }

    public Optional<TaskManagerLocation> getLatestPriorLocation(
            ExecutionVertexID executionVertexID) {
        return Optional.ofNullable(schedulingInformationCache.get(executionVertexID))
                .map(SchedulingInformation::getTaskManagerLocation);
    }

    public void persistSchedulingInformation() throws IOException {
        FileUtils.createParentDirectories(schedulingInformationFile);

        try (ObjectOutputStream oos =
                new ObjectOutputStream(new FileOutputStream(schedulingInformationFile))) {
            final Collection<SchedulingInformation> values = schedulingInformationCache.values();

            oos.writeInt(values.size());

            for (SchedulingInformation value : values) {
                oos.writeObject(value);
            }
        }
    }

    public void restoreSchedulingInformation() throws IOException, ClassNotFoundException {
        if (schedulingInformationFile.exists()) {
            try (ObjectInputStream ois =
                    new ObjectInputStream(new FileInputStream(schedulingInformationFile))) {
                final int numberEntries = ois.readInt();

                for (int i = 0; i < numberEntries; i++) {
                    final SchedulingInformation schedulingInformation =
                            (SchedulingInformation) ois.readObject();

                    schedulingInformationCache.put(
                            schedulingInformation.getExecutionVertexId(), schedulingInformation);
                }
            }

            LOG.debug("Restored scheduling information: {}.", schedulingInformationCache.values());
        }
    }

    public static SchedulingInformationPersistence from(JobID jobId, Configuration configuration) {
        final File jobDirectory =
                ClusterEntrypointUtils.getJobWorkingDirectory(jobId, configuration);

        final File schedulingInformationFile = new File(jobDirectory, "schedulingInformation");
        return new SchedulingInformationPersistence(jobId, schedulingInformationFile);
    }

    /** Scheduling information. */
    public static final class SchedulingInformation implements Serializable {
        private static final long serialVersionUID = -7065212133707540255L;

        private final ExecutionVertexID executionVertexId;

        private final AllocationID allocationId;

        private final TaskManagerLocation taskManagerLocation;

        private SchedulingInformation(
                ExecutionVertexID executionVertexId,
                AllocationID allocationId,
                TaskManagerLocation taskManagerLocation) {
            this.executionVertexId = executionVertexId;
            this.allocationId = allocationId;
            this.taskManagerLocation = taskManagerLocation;
        }

        public AllocationID getAllocationId() {
            return allocationId;
        }

        public ExecutionVertexID getExecutionVertexId() {
            return executionVertexId;
        }

        public TaskManagerLocation getTaskManagerLocation() {
            return taskManagerLocation;
        }

        public static SchedulingInformation create(
                ExecutionVertexID executionVertexId,
                AllocationID allocationId,
                TaskManagerLocation taskManagerLocation) {
            return new SchedulingInformation(executionVertexId, allocationId, taskManagerLocation);
        }

        @Override
        public String toString() {
            return "SchedulingInformation{"
                    + "executionVertexId="
                    + executionVertexId
                    + ", allocationId="
                    + allocationId
                    + ", taskManagerLocation="
                    + taskManagerLocation
                    + '}';
        }
    }
}
