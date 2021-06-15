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
 * Result class for transporting an empty success and an error result.
 *
 * @param <E> type of the failure case
 */
public class VoidResult<E> {

    @Nullable private final E error;

    protected VoidResult(@Nullable E error) {
        this.error = error;
    }

    public boolean isOk() {
        return error == null;
    }

    public boolean isError() {
        return error != null;
    }

    public E getError() {
        Preconditions.checkState(isError());
        return error;
    }

    public static <E> VoidResult<E> ok() {
        return new VoidResult<>(null);
    }

    public static <E> VoidResult<E> error(E error) {
        return new VoidResult<>(error);
    }
}
