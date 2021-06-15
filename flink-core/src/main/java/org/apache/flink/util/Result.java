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

package org.apache.flink.util;

import javax.annotation.Nullable;

/**
 * Result class for transporting a success and error result.
 *
 * @param <T> type of the success
 * @param <E> type of the error
 */
public class Result<T, E> {

    @Nullable private final T success;

    @Nullable private final E error;

    protected Result(@Nullable T success, @Nullable E error) {
        Preconditions.checkArgument(
                success != null ^ error != null, "Either success or error must be non-null.");
        this.success = success;
        this.error = error;
    }

    public boolean isOk() {
        return success != null;
    }

    public boolean isError() {
        return error != null;
    }

    public E getError() {
        Preconditions.checkState(isError());
        return error;
    }

    public T getOk() {
        Preconditions.checkState(isOk());
        return success;
    }

    public static <T, E> Result<T, E> ok(T success) {
        return new Result<>(success, null);
    }

    public static <T, E> Result<T, E> error(E error) {
        return new Result<>(null, error);
    }
}
