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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Leadership runner for the {@link JobMasterServiceProcess}. */
public class JobManagerLeadershipRunner implements JobManagerRunner, LeaderContender {

    private final Object lock = new Object();

    private final JobMasterServiceProcessFactory jobMasterServiceProcessFactory;

    private final LeaderElectionService leaderElectionService;

    private final FatalErrorHandler fatalErrorHandler;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    @GuardedBy("lock")
    private State state = State.RUNNING;

    @GuardedBy("lock")
    private CompletableFuture<Void> sequentialOperation = FutureUtils.completedVoidFuture();

    @GuardedBy("lock")
    private JobMasterServiceProcess jobMasterServiceProcess =
            JobMasterServiceProcess.waitingForLeadership();

    @GuardedBy("lock")
    private CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = new CompletableFuture<>();

    public JobManagerLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
            LeaderElectionService leaderElectionService,
            FatalErrorHandler fatalErrorHandler) {
        this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
        this.leaderElectionService = leaderElectionService;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (state != State.STOPPED) {
                state = State.STOPPED;

                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException(
                                "JobManagerLeadershipRunner is closed. Therefore, the corresponding JobMaster will never acquire the leadership."));
                resultFuture.complete(JobManagerRunnerResult.jobNotFinished());

                final CompletableFuture<Void> processTerminationFuture =
                        jobMasterServiceProcess.closeAsync();

                final CompletableFuture<Void> serviceTerminationFuture =
                        FutureUtils.runAfterwards(
                                processTerminationFuture, leaderElectionService::stop);

                FutureUtils.forward(serviceTerminationFuture, terminationFuture);
            }
        }

        return terminationFuture;
    }

    @Override
    public void start() throws Exception {
        leaderElectionService.start(this);
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        synchronized (lock) {
            return jobMasterGatewayFuture;
        }
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobMasterServiceProcessFactory.getJobId();
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        return getJobMasterGateway()
                .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout));
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        synchronized (lock) {
            if (state == State.RUNNING) {
                if (jobMasterServiceProcess.isInitialized()) {
                    return getJobMasterGateway()
                            .thenCompose(jobMasterGateway -> jobMasterGateway.requestJob(timeout));
                } else {
                    return CompletableFuture.completedFuture(
                            new ExecutionGraphInfo(
                                    ArchivedExecutionGraph.createFromInitializingJob(
                                            getJobID(),
                                            null,
                                            JobStatus.INITIALIZING,
                                            null,
                                            initializationTimestamp)));
                }
            } else {
                return resultFuture.thenApply(JobManagerRunnerResult::getExecutionGraphInfo);
            }
        }
    }

    @Override
    public boolean isInitialized() {
        synchronized (lock) {
            return jobMasterServiceProcess.isInitialized();
        }
    }

    @Override
    public void grantLeadership(UUID leaderSessionID) {
        runIfStateRunning(() -> onGrantLeadership(leaderSessionID));
    }

    @GuardedBy("lock")
    private void onGrantLeadership(UUID leaderSessionId) {
        sequentialOperation =
                sequentialOperation.thenRun(
                        () ->
                                runIfValidLeader(
                                        leaderSessionId,
                                        () -> createNewJobMasterProcess(leaderSessionId)));
    }

    @GuardedBy("lock")
    private void createNewJobMasterProcess(UUID leaderSessionId) {
        Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());

        jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);

        forwardIfValidLeader(
                leaderSessionId,
                jobMasterServiceProcess.getJobMasterGatewayFuture(),
                jobMasterGatewayFuture);
        forwardResultFuture(leaderSessionId, jobMasterServiceProcess);
        confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
    }

    private void confirmLeadership(
            UUID leaderSessionId, CompletableFuture<String> leaderAddressFuture) {
        FutureUtils.assertNoException(
                leaderAddressFuture.thenAccept(
                        address -> {
                            synchronized (lock) {
                                if (isValidLeader(leaderSessionId)) {
                                    leaderElectionService.confirmLeadership(
                                            leaderSessionId, address);
                                }
                            }
                        }));
    }

    private void forwardResultFuture(
            UUID leaderSessionId, JobMasterServiceProcess jobMasterServiceProcess) {
        jobMasterServiceProcess
                .getResultFuture()
                .whenComplete(
                        (jobManagerRunnerResult, throwable) -> {
                            synchronized (lock) {
                                if (isValidLeader(leaderSessionId)) {
                                    state = State.FINISHED;

                                    if (throwable != null) {
                                        resultFuture.completeExceptionally(throwable);
                                    } else {
                                        resultFuture.complete(jobManagerRunnerResult);
                                    }
                                }
                            }
                        });
    }

    @Override
    public void revokeLeadership() {
        runIfStateRunning(this::onRevokeLeadership);
    }

    @GuardedBy("lock")
    private void onRevokeLeadership() {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        ignored ->
                                callIfRunning(this::stopJobMasterServiceProcess)
                                        .orElse(FutureUtils.completedVoidFuture()));
    }

    @GuardedBy("lock")
    private CompletableFuture<Void> stopJobMasterServiceProcess() {
        final CompletableFuture<JobMasterGateway> newJobMasterGatewayFuture =
                new CompletableFuture<>();

        FutureUtils.forward(newJobMasterGatewayFuture, jobMasterGatewayFuture);
        jobMasterGatewayFuture = newJobMasterGatewayFuture;

        return jobMasterServiceProcess.closeAsync();
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(exception);
    }

    private void runIfStateRunning(Runnable action) {
        synchronized (lock) {
            if (isRunning()) {
                action.run();
            }
        }
    }

    private <T> Optional<T> callIfRunning(Supplier<T> supplier) {
        synchronized (lock) {
            if (isRunning()) {
                return Optional.of(supplier.get());
            } else {
                return Optional.empty();
            }
        }
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return state == State.RUNNING;
    }

    private void runIfValidLeader(UUID expectedLeaderId, Runnable action) {
        synchronized (lock) {
            if (isValidLeader(expectedLeaderId)) {
                action.run();
            }
        }
    }

    @GuardedBy("lock")
    private boolean isValidLeader(UUID expectedLeaderId) {
        return isRunning() && leaderElectionService.hasLeadership(expectedLeaderId);
    }

    private <T> void forwardIfValidLeader(
            UUID expectedLeaderId,
            CompletableFuture<? extends T> source,
            CompletableFuture<T> target) {
        source.whenComplete(
                (t, throwable) -> {
                    synchronized (lock) {
                        if (isValidLeader(expectedLeaderId)) {
                            if (throwable != null) {
                                target.completeExceptionally(throwable);
                            } else {
                                target.complete(t);
                            }
                        }
                    }
                });
    }

    enum State {
        RUNNING,
        STOPPED,
        FINISHED,
    }
}
