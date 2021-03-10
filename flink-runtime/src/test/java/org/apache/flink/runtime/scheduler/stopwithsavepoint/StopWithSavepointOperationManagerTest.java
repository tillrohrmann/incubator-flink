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

package org.apache.flink.runtime.scheduler.stopwithsavepoint;

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StopWithSavepointOperations;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.util.TestLogger;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertThat;

/**
 * {@code StopWithSavepointOperationManagerTest} tests that {@link
 * StopWithSavepointOperationManager} applies the correct order expected by {@link
 * StopWithSavepointOperationHandler} regardless of the completion of the provided {@code
 * CompletableFutures}.
 */
public class StopWithSavepointOperationManagerTest extends TestLogger {

    @Test
    public void testCompletionInCorrectOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    completedSavepointFuture.complete(null);
                    terminatedExecutionStatesFuture.complete(null);
                });
    }

    @Test
    public void testCompletionInInverseOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    terminatedExecutionStatesFuture.complete(null);
                    completedSavepointFuture.complete(null);
                });
    }

    private void assertCorrectOrderOfProcessing(
            BiConsumer<CompletableFuture<CompletedCheckpoint>, CompletableFuture<ExecutionState>>
                    completion) {
        final CompletableFuture<CompletedCheckpoint> completedSavepointFuture =
                new CompletableFuture<>();
        final CompletableFuture<ExecutionState> terminatedExecutionStateFuture =
                new CompletableFuture<>();

        final TestingStopWithSavepointOperations testingStopWithSavepointOperations =
                new TestingStopWithSavepointOperations(completedSavepointFuture);
        final TestingStopWithSavepointOperationHandler stopWithSavepointTerminationHandler =
                new TestingStopWithSavepointOperationHandler();
        new StopWithSavepointOperationManager(
                        testingStopWithSavepointOperations, stopWithSavepointTerminationHandler)
                .trackStopWithSavepointWithTerminationFutures(
                        false,
                        "",
                        terminatedExecutionStateFuture.thenApply(Collections::singleton),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread());
        completion.accept(completedSavepointFuture, terminatedExecutionStateFuture);

        assertThat(
                stopWithSavepointTerminationHandler.getActualMethodCallOrder(),
                CoreMatchers.is(
                        Arrays.asList(
                                MethodCall.SavepointCreationTermination,
                                MethodCall.ExecutionTermination)));
    }

    private enum MethodCall {
        SavepointCreationTermination,
        AbortOperation,
        ExecutionTermination
    }

    private static class TestingStopWithSavepointOperationHandler
            implements StopWithSavepointOperationHandler {

        private final List<MethodCall> methodCalls = new ArrayList<>(2);

        @Override
        public CompletableFuture<String> getSavepointPathFuture() {
            return FutureUtils.completedExceptionally(
                    new Exception("The result is not relevant in this test."));
        }

        @Override
        public void handleSavepointCreation(
                CompletedCheckpoint completedSavepoint, Throwable throwable) {
            methodCalls.add(MethodCall.SavepointCreationTermination);
        }

        @Override
        public void handleExecutionsTermination(
                Collection<ExecutionState> terminatedExecutionStates) {
            methodCalls.add(MethodCall.ExecutionTermination);
        }

        @Override
        public void abortOperation(Throwable cause) {
            methodCalls.add(MethodCall.AbortOperation);
        }

        public List<MethodCall> getActualMethodCallOrder() {
            return methodCalls;
        }
    }

    private static class TestingStopWithSavepointOperations implements StopWithSavepointOperations {

        private final CompletableFuture<CompletedCheckpoint> savepointFuture;

        private TestingStopWithSavepointOperations(
                CompletableFuture<CompletedCheckpoint> savepointFuture) {
            this.savepointFuture = savepointFuture;
        }

        @Override
        public void startCheckpointScheduler() {}

        @Override
        public void stopCheckpointScheduler() {}

        @Override
        public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
                boolean terminate, @Nullable String targetLocation) {
            return savepointFuture;
        }
    }
}
