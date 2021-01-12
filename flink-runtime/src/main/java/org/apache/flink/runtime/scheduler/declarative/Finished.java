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

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.util.ExceptionUtils;

public class Finished extends AbstractState {
    private final ArchivedExecutionGraph archivedExecutionGraph;

    private final JobStatusListener jobStatusListener;

    private final CompletedCheckpointStore completedCheckpointStore;

    private final CheckpointsCleaner checkpointsCleaner;

    private final CheckpointIDCounter checkpointIdCounter;

    private Finished(
            Context context,
            ArchivedExecutionGraph archivedExecutionGraph,
            JobStatusListener jobStatusListener,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter) {
        super(context);

        this.archivedExecutionGraph = archivedExecutionGraph;
        this.jobStatusListener = jobStatusListener;
        this.completedCheckpointStore = completedCheckpointStore;
        this.checkpointsCleaner = checkpointsCleaner;
        this.checkpointIdCounter = checkpointIdCounter;
    }

    @Override
    public void onEnter() {
        // set declared resources to 0
        // declarativeSlotPool.decreaseResourceRequirementsBy(0);

        stopCheckpointServicesSafely(archivedExecutionGraph.getState());

        if (jobStatusListener != null) {
            jobStatusListener.jobStatusChanges(
                    getContext().getJobGraph().getJobID(),
                    archivedExecutionGraph.getState(),
                    archivedExecutionGraph.getStatusTimestamp(archivedExecutionGraph.getState()),
                    archivedExecutionGraph.getFailureInfo() != null
                            ? archivedExecutionGraph.getFailureInfo().getException()
                            : null);
        }
    }

    private void stopCheckpointServicesSafely(JobStatus terminalState) {
        Exception exception = null;

        try {
            completedCheckpointStore.shutdown(terminalState, checkpointsCleaner, () -> {});
        } catch (Exception e) {
            exception = e;
        }

        try {
            checkpointIdCounter.shutdown(terminalState);
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            getLogger().warn("Failed to stop checkpoint services.", exception);
        }
    }

    @Override
    public State cancel() {
        return this;
    }

    @Override
    public State suspend(Throwable cause) {
        return this;
    }

    @Override
    public JobStatus getJobStatus() {
        return archivedExecutionGraph.getState();
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return archivedExecutionGraph;
    }

    @Override
    public State handleGlobalFailure(Throwable cause) {
        return this;
    }
}
