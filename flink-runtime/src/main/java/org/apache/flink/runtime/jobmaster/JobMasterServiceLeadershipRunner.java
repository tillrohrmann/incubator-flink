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
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Leadership runner for the {@link JobMasterServiceProcess}. */
public class JobMasterServiceLeadershipRunner implements JobManagerRunner, LeaderContender {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobMasterServiceLeadershipRunner.class);

    private final Object lock = new Object();

    private final JobMasterServiceProcessFactory jobMasterServiceProcessFactory;

    private final LeaderElectionService leaderElectionService;

    private final RunningJobsRegistry runningJobsRegistry;

    private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

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

    @GuardedBy("lock")
    private boolean hasCurrentLeaderBeenCancelled = false;

    public JobMasterServiceLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
            LeaderElectionService leaderElectionService,
            RunningJobsRegistry runningJobsRegistry,
            LibraryCacheManager.ClassLoaderLease classLoaderLease,
            FatalErrorHandler fatalErrorHandler) {
        this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
        this.leaderElectionService = leaderElectionService;
        this.runningJobsRegistry = runningJobsRegistry;
        this.classLoaderLease = classLoaderLease;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (state != State.STOPPED) {
                state = State.STOPPED;

                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException(
                                "JobMasterServiceLeadershipRunner is closed. Therefore, the corresponding JobMaster will never acquire the leadership."));
                resultFuture.completeExceptionally(new JobNotFinishedException(getJobID()));

                final CompletableFuture<Void> processTerminationFuture =
                        jobMasterServiceProcess.closeAsync();

                final CompletableFuture<Void> serviceTerminationFuture =
                        FutureUtils.runAfterwards(
                                processTerminationFuture,
                                () -> {
                                    classLoaderLease.release();
                                    leaderElectionService.stop();
                                });

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
        synchronized (lock) {
            hasCurrentLeaderBeenCancelled = true;
            return getJobMasterGateway()
                    .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout))
                    .exceptionally(
                            e -> {
                                throw new CompletionException(
                                        new FlinkException(
                                                "Cancellation failed.",
                                                ExceptionUtils.stripCompletionException(e)));
                            });
        }
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
                                    jobMasterServiceProcessFactory.createArchivedExecutionGraph(
                                            hasCurrentLeaderBeenCancelled
                                                    ? JobStatus.CANCELLING
                                                    : JobStatus.INITIALIZING,
                                            null)));
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
        runIfStateRunning(() -> startJobMasterProcessAsync(leaderSessionID));
    }

    @GuardedBy("lock")
    private void startJobMasterProcessAsync(UUID leaderSessionId) {
        sequentialOperation =
                sequentialOperation.thenRun(
                        () ->
                                runIfValidLeader(
                                        leaderSessionId,
                                        ThrowingRunnable.unchecked(
                                                () ->
                                                        verifyJobSchedulingStatusAndCreateJobMasterProcess(
                                                                leaderSessionId))));

        handleAsyncOperationError(sequentialOperation, "Could not start the job manager.");
    }

    @GuardedBy("lock")
    private void verifyJobSchedulingStatusAndCreateJobMasterProcess(UUID leaderSessionId)
            throws FlinkException {
        final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus =
                getJobSchedulingStatus();

        if (jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE) {
            jobAlreadyDone();
        } else {
            createNewJobMasterServiceProcess(leaderSessionId);
        }
    }

    private void jobAlreadyDone() {
        resultFuture.completeExceptionally(new JobNotFinishedException(getJobID()));
    }

    private RunningJobsRegistry.JobSchedulingStatus getJobSchedulingStatus() throws FlinkException {
        try {
            return runningJobsRegistry.getJobSchedulingStatus(getJobID());
        } catch (IOException e) {
            throw new FlinkException(
                    String.format(
                            "Could not retrieve the job scheduling status for job %s.", getJobID()),
                    e);
        }
    }

    @GuardedBy("lock")
    private void createNewJobMasterServiceProcess(UUID leaderSessionId) throws FlinkException {
        Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());

        LOG.debug(
                "Create new JobMasterServiceProcess because we were granted leadership under {}.",
                leaderSessionId);

        try {
            runningJobsRegistry.setJobRunning(getJobID());
        } catch (IOException e) {
            throw new FlinkException(
                    String.format(
                            "Failed to set the job %s to running in the running jobs registry.",
                            getJobID()),
                    e);
        }

        jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);

        forwardIfValidLeader(
                leaderSessionId,
                jobMasterServiceProcess.getJobMasterGatewayFuture(),
                jobMasterGatewayFuture);
        forwardResultFuture(leaderSessionId, jobMasterServiceProcess.getResultFuture());
        confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
    }

    private void confirmLeadership(
            UUID leaderSessionId, CompletableFuture<String> leaderAddressFuture) {
        FutureUtils.assertNoException(
                leaderAddressFuture.thenAccept(
                        address -> {
                            synchronized (lock) {
                                if (isValidLeader(leaderSessionId)) {
                                    LOG.debug("Confirm leadership {}.", leaderSessionId);
                                    leaderElectionService.confirmLeadership(
                                            leaderSessionId, address);
                                }
                            }
                        }));
    }

    private void forwardResultFuture(
            UUID leaderSessionId, CompletableFuture<JobManagerRunnerResult> resultFuture) {
        resultFuture.whenComplete(
                (jobManagerRunnerResult, throwable) -> {
                    synchronized (lock) {
                        if (isValidLeader(leaderSessionId)) {
                            onJobCompletion(jobManagerRunnerResult, throwable);
                        }
                    }
                });
    }

    @GuardedBy("lock")
    private void onJobCompletion(
            JobManagerRunnerResult jobManagerRunnerResult, Throwable throwable) {
        state = State.JOB_COMPLETED;

        if (throwable != null) {
            resultFuture.completeExceptionally(throwable);
            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "Could not retrieve JobMasterGateway because the JobMaster failed.",
                            throwable));
        } else {
            if (jobManagerRunnerResult.isSuccess()) {
                try {
                    runningJobsRegistry.setJobFinished(getJobID());
                } catch (IOException e) {
                    LOG.error(
                            "Could not un-register from high-availability services job {}."
                                    + "Other JobManager's may attempt to recover it and re-execute it.",
                            getJobID(),
                            e);
                }
            } else {
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException(
                                "Could not retrieve JobMasterGateway because the JobMaster initialization failed.",
                                jobManagerRunnerResult.getInitializationFailure()));
            }

            resultFuture.complete(jobManagerRunnerResult);
        }
    }

    @Override
    public void revokeLeadership() {
        runIfStateRunning(this::stopJobMasterServiceProcessAsync);
    }

    @GuardedBy("lock")
    private void stopJobMasterServiceProcessAsync() {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        ignored ->
                                callIfRunning(this::stopJobMasterServiceProcess)
                                        .orElse(FutureUtils.completedVoidFuture()));

        handleAsyncOperationError(sequentialOperation, "Could not suspend the job manager.");
    }

    @GuardedBy("lock")
    private CompletableFuture<Void> stopJobMasterServiceProcess() {
        LOG.debug("Stop current JobMasterServiceProcess because the leadership has been revoked.");

        jobMasterGatewayFuture.completeExceptionally(
                new FlinkException(
                        "Cannot obtain JobMasterGateway because the JobMaster lost leadership."));
        jobMasterGatewayFuture = new CompletableFuture<>();

        hasCurrentLeaderBeenCancelled = false;

        return jobMasterServiceProcess.closeAsync();
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(exception);
    }

    private void handleAsyncOperationError(CompletableFuture<Void> operation, String message) {
        operation.whenComplete(
                (unused, throwable) -> {
                    if (throwable != null) {
                        runIfStateRunning(
                                () ->
                                        handleJobManagerLeadershipRunnerError(
                                                new FlinkException(message, throwable)));
                    }
                });
    }

    private void handleJobManagerLeadershipRunnerError(Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            fatalErrorHandler.onFatalError(cause);
        } else {
            resultFuture.completeExceptionally(cause);
        }
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
        JOB_COMPLETED,
    }
}
