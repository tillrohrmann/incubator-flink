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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

public interface JobMasterServiceProcess extends AutoCloseableAsync {

    static JobMasterServiceProcess waitingForLeadership() {
        return WaitingForLeadership.INSTANCE;
    }

    boolean isInitialized();

    CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture();

    CompletableFuture<JobManagerRunnerResult> getResultFuture();

    CompletableFuture<String> getLeaderAddressFuture();

    enum WaitingForLeadership implements JobMasterServiceProcess {
        INSTANCE;

        @Override
        public CompletableFuture<Void> closeAsync() {
            return FutureUtils.completedVoidFuture();
        }

        @Override
        public boolean isInitialized() {
            return false;
        }

        @Override
        public CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture() {
            return failedOperationFuture();
        }

        @Override
        public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
            return failedOperationFuture();
        }

        @Override
        public CompletableFuture<String> getLeaderAddressFuture() {
            return failedOperationFuture();
        }

        @Nonnull
        private <T> CompletableFuture<T> failedOperationFuture() {
            return FutureUtils.completedExceptionally(
                    new FlinkException("Still waiting for the leadership."));
        }
    }
}
