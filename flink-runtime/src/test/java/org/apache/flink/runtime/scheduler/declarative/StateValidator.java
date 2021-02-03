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

import org.apache.flink.util.Preconditions;

import java.util.function.Consumer;

/**
 * TODO.
 *
 * @param <T>
 */
public class StateValidator<T> {

    private Runnable trap = () -> {};
    private Consumer<T> consumer = null;
    private final String stateName;

    public StateValidator(String stateName) {
        this.stateName = stateName;
    }

    public void validateInput(T input) {
        Preconditions.checkNotNull(consumer, "no consumer set. Unexpected state transition?");
        trap = () -> {};
        consumer.accept(input);
    }

    public void activate(Consumer<T> asserter) {
        consumer = Preconditions.checkNotNull(asserter);
        trap =
                () -> {
                    throw new AssertionError("no transition to " + stateName);
                };
    }

    public void close() {
        trap.run();
    }
}
