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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactoryNg;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.FlinkException;

import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Default {@link JobMasterServiceProcess} which is responsible for creating and running a {@link
 * JobMasterService}.
 */
public class DefaultJobMasterServiceProcess
        implements JobMasterServiceProcess, OnCompletionActions {

    private final Object lock = new Object();

    private final JobID jobId;

    private final CompletableFuture<JobMasterService> jobMasterServiceFuture;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
            new CompletableFuture<>();

    private final CompletableFuture<String> leaderAddressFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean isRunning = true;

    public DefaultJobMasterServiceProcess(
            JobID jobId,
            UUID leaderSessionId,
            JobMasterServiceFactoryNg jobMasterServiceFactory,
            Function<Throwable, ArchivedExecutionGraph> failedArchivedExecutionGraphFactory) {
        this.jobId = jobId;
        this.jobMasterServiceFuture =
                jobMasterServiceFactory.createJobMasterService(leaderSessionId, this);

        jobMasterServiceFuture.whenComplete(
                (jobMasterService, throwable) -> {
                    if (throwable != null) {
                        final JobInitializationException jobInitializationException =
                                new JobInitializationException(
                                        jobId, "Could not start the JobMaster.", throwable);
                        resultFuture.complete(
                                JobManagerRunnerResult.forInitializationFailure(
                                        new ExecutionGraphInfo(
                                                failedArchivedExecutionGraphFactory.apply(
                                                        jobInitializationException)),
                                        jobInitializationException));
                    } else {
                        registerJobMasterServiceFutures(jobMasterService);
                    }
                });
    }

    private void registerJobMasterServiceFutures(JobMasterService jobMasterService) {
        jobMasterGatewayFuture.complete(jobMasterService.getGateway());
        leaderAddressFuture.complete(jobMasterService.getAddress());

        jobMasterService
                .getTerminationFuture()
                .whenComplete(
                        (unused, throwable) -> {
                            synchronized (lock) {
                                if (isRunning) {
                                    jobMasterFailed(
                                            new FlinkException(
                                                    "Unexpected termination of the JobMasterService.",
                                                    throwable));
                                }
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (isRunning) {
                isRunning = false;

                resultFuture.completeExceptionally(new JobNotFinishedException(jobId));
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException("Process has been closed."));

                jobMasterServiceFuture.whenComplete(
                        (jobMasterService, throwable) -> {
                            if (throwable != null) {
                                // JobMasterService creation has failed. Nothing to stop then :-)
                                terminationFuture.complete(null);
                            } else {
                                FutureUtils.forward(
                                        jobMasterService.closeAsync(), terminationFuture);
                            }
                        });
            }
        }
        return terminationFuture;
    }

    @Override
    public boolean isInitialized() {
        return jobMasterServiceFuture.isDone()
                && !jobMasterServiceFuture.isCompletedExceptionally();
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture() {
        return jobMasterGatewayFuture;
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public CompletableFuture<String> getLeaderAddressFuture() {
        return leaderAddressFuture;
    }

    @Override
    public void jobReachedGloballyTerminalState(ExecutionGraphInfo executionGraphInfo) {
        resultFuture.complete(JobManagerRunnerResult.forSuccess(executionGraphInfo));
    }

    @Override
    public void jobFinishedByOther() {
        resultFuture.completeExceptionally(new JobNotFinishedException(jobId));
    }

    @Override
    public void jobMasterFailed(Throwable cause) {
        resultFuture.completeExceptionally(cause);
    }
}
