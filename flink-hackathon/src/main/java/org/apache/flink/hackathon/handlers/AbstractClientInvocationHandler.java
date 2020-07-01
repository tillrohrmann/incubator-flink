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

package org.apache.flink.hackathon.handlers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.hackathon.Utils;
import org.apache.flink.hackathon.invokables.InvocationUtils;
import org.apache.flink.hackathon.redis.RedisFuture;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.FunctionUtils;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * AbstractClientInvocationHandler.
 */
public abstract class AbstractClientInvocationHandler implements InvocationHandler {
	private final ClassLoader classLoader;
	private final Class<?> implementor;

	protected AbstractClientInvocationHandler(ClassLoader classLoader, Class<?> implementor) {
		this.classLoader = classLoader;
		this.implementor = implementor;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		final AbstractID futureId = new AbstractID();
		final JobGraph jobGraph = buildJobGraph(method, args, futureId);
		final CompletableFuture<JobID> jobSubmissionResult = submitJob(jobGraph);

		if (Void.TYPE.equals(method.getReturnType())) {
			try {
				jobSubmissionResult.get();
				return null;
			} catch (ExecutionException ee) {
				throw ExceptionUtils.stripExecutionException(ee);
			}
		} else {
			final RedisFuture<Object> resultFuture = new RedisFuture<>(futureId);
			jobSubmissionResult
				.thenCompose(ignored -> requestJobResult(jobGraph))
				.thenApply(FunctionUtils.uncheckedFunction(jobResult -> jobResult.toJobExecutionResult(classLoader)))
				.handle((jobExecutionResult, throwable) -> {
					if (throwable != null) {
						resultFuture.completeExceptionally(throwable);
					}

					stopJob(jobGraph.getJobID());
					return null;
				});

			if (Future.class.isAssignableFrom(method.getReturnType())) {
				return resultFuture;
			} else {
				return resultFuture.get();
			}
		}
	}

	protected void stopJob(JobID jobId) {}

	protected abstract CompletableFuture<JobResult> requestJobResult(JobGraph jobGraph);

	protected abstract CompletableFuture<JobID> submitJob(JobGraph jobGraph);

	private JobGraph buildJobGraph(Method method, Object[] args, AbstractID outputId) throws IOException {
		final JobGraph jobGraph = new JobGraph("Application: " + Utils.methodToString(method));
		final JobVertex jobVertex = InvocationUtils.createTaskVertex(method, args, implementor, outputId);
		jobGraph.addVertex(jobVertex);

		return jobGraph;
	}

}
