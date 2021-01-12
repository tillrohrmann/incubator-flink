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

import org.slf4j.Logger;

public class Finished implements State {

    private final Context context;

    private final ArchivedExecutionGraph archivedExecutionGraph;

    private final Logger logger;

    private Finished(
            Context context, ArchivedExecutionGraph archivedExecutionGraph, Logger logger) {
        this.context = context;
        this.archivedExecutionGraph = archivedExecutionGraph;
        this.logger = logger;
    }

    @Override
    public void onEnter() {
        context.onFinished();
    }

    @Override
    public void cancel() {}

    @Override
    public void suspend(Throwable cause) {}

    @Override
    public JobStatus getJobStatus() {
        return archivedExecutionGraph.getState();
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return archivedExecutionGraph;
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {}

    @Override
    public Logger getLogger() {
        return logger;
    }

    interface Context {
        void onFinished();
    }
}
