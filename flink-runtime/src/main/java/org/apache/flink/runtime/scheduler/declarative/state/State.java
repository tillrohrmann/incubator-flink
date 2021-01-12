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

package org.apache.flink.runtime.scheduler.declarative.state;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;

import java.util.Optional;

public interface State {
    default void onEnter() {}

    default void onLeave() {}

    void cancel();

    void suspend(Throwable cause);

    JobStatus getJobStatus();

    ArchivedExecutionGraph getJob();

    void handleGlobalFailure(Throwable cause);

    Logger getLogger();

    default <T> Optional<T> as(Class<? extends T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }

    default <T, E extends Exception> void tryRun(
            Class<? extends T> clazz, ThrowingConsumer<T, E> action, String debugMessage) throws E {
        final Optional<? extends T> asOptional = as(clazz);

        if (asOptional.isPresent()) {
            action.accept(asOptional.get());
        } else {
            getLogger()
                    .debug(
                            "Cannot run '{}' because the actual state is {} and not {}.",
                            debugMessage,
                            this.getClass().getSimpleName(),
                            clazz.getSimpleName());
        }
    }

    default <T, V, E extends Exception> Optional<V> tryCall(
            Class<? extends T> clazz, FunctionWithException<T, V, E> action, String debugMessage)
            throws E {
        final Optional<? extends T> asOptional = as(clazz);

        if (asOptional.isPresent()) {
            return Optional.of(action.apply(asOptional.get()));
        } else {
            getLogger()
                    .debug(
                            "Cannot run '{}' because the actual state is {} and not {}.",
                            debugMessage,
                            this.getClass().getSimpleName(),
                            clazz.getSimpleName());
            return Optional.empty();
        }
    }
}
