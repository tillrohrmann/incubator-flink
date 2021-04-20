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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.FlinkException;

import java.util.concurrent.CompletableFuture;

public class DefaultJobMasterServiceProcess implements JobMasterServiceProcess {

    private final Object lock = new Object();

    private final CompletableFuture<JobMasterService> jobMasterServiceFuture;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
            new CompletableFuture<>();

    private final CompletableFuture<String> leaderAddressFuture = new CompletableFuture<>();

    private boolean isRunning = true;

    public DefaultJobMasterServiceProcess(
            CompletableFuture<JobMasterService> jobMasterServiceFuture) {
        this.jobMasterServiceFuture =
                jobMasterServiceFuture.whenComplete(
                        (jobMasterService, initializationError) -> {
                            if (jobMasterService == null) {
                                resultFuture.complete(
                                        JobManagerRunnerResult.forInitializationFailure(
                                                initializationError));
                            } else {
                                jobMasterGatewayFuture.complete(jobMasterService.getGateway());
                                leaderAddressFuture.complete(jobMasterService.getAddress());
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (isRunning) {
                isRunning = false;

                resultFuture.complete(JobManagerRunnerResult.forJobNotFinished());
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException("Process has been closed."));

                if (isInitializationFailed()) {
                    terminationFuture.complete(null);
                } else {
                    FutureUtils.forward(
                            jobMasterServiceFuture.thenCompose(JobMasterService::closeAsync),
                            terminationFuture);
                }
            }
        }
        return terminationFuture;
    }

    private boolean isInitializationFailed() {
        return jobMasterGatewayFuture.isCompletedExceptionally();
    }

    @Override
    public boolean isInitialized() {
        return jobMasterServiceFuture.isDone();
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
    public JobID getJobId() {
        return null;
    }
}
