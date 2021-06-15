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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.ChangeJobHeaders;
import org.apache.flink.runtime.rest.messages.job.ChangeJobRequestBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Rest handler for change job events. */
public class ChangeJobHandler
        extends AbstractRestHandler<
                DispatcherGateway, ChangeJobRequestBody, EmptyResponseBody, JobMessageParameters> {

    public ChangeJobHandler(
            GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders) {
        super(leaderRetriever, timeout, responseHeaders, ChangeJobHeaders.INSTANCE);
    }

    @Override
    protected CompletableFuture<EmptyResponseBody> handleRequest(
            @Nonnull HandlerRequest<ChangeJobRequestBody, JobMessageParameters> request,
            @Nonnull DispatcherGateway gateway)
            throws RestHandlerException {
        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);

        return gateway.changeJobParallelism(
                        jobId, request.getRequestBody().getJobVertexParallelism())
                .thenApply(ignored -> EmptyResponseBody.getInstance());
    }
}
