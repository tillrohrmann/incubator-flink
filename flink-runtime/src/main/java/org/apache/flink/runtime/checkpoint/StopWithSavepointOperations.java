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

package org.apache.flink.runtime.checkpoint;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * {@code StopWithSavepointOperations} provides methods for the stop-with-savepoint operation, such
 * as starting and stopping the periodic scheduling of checkpoints, or triggering a savepoint.
 */
public interface StopWithSavepointOperations {

    /** Starts the periodic scheduling if possible. */
    void startCheckpointScheduler();

    /** Stops the periodic scheduling if possible. */
    void stopCheckpointScheduler();

    /**
     * Triggers a synchronous savepoint with the given savepoint directory as a target.
     *
     * @param terminate flag indicating if the job should terminate or just suspend
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     */
    CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
            final boolean terminate, @Nullable final String targetLocation);
}
