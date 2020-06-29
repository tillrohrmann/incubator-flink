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

package org.apache.flink.hackathon.invocation;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.hackathon.Utils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.FunctionUtils;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * MiniClusterInvocationHandler.
 */
public class MiniClusterInvocationHandler implements InvocationHandler {

	private final MiniCluster miniCluster;

	private final ClassLoader classLoader;

	private final Class<?> implementor;

	public MiniClusterInvocationHandler(MiniCluster miniCluster, Class<?> implementor) {
		this.miniCluster = miniCluster;
		this.classLoader = getClass().getClassLoader();
		this.implementor = implementor;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		final JobGraph jobGraph = buildJobGraph(method, args);
		final CompletableFuture<JobSubmissionResult> jobSubmissionResult = miniCluster.submitJob(jobGraph);

		if (Void.TYPE.equals(method.getReturnType())) {
			try {
				jobSubmissionResult.get();
				return null;
			} catch (ExecutionException ee) {
				throw ExceptionUtils.stripExecutionException(ee);
			}
		} else {
			final CompletableFuture<Object> resultFuture = jobSubmissionResult
				.thenCompose(ignored -> miniCluster.requestJobResult(jobGraph.getJobID()))
				.thenApply(FunctionUtils.uncheckedFunction(jobResult -> jobResult.toJobExecutionResult(classLoader)))
				.thenApply(jobExecutionResult -> jobExecutionResult.getAccumulatorResult("result"));

			if (Future.class.isAssignableFrom(method.getReturnType())) {
				return resultFuture;
			} else {
				return resultFuture.get();
			}
		}
	}

	private JobGraph buildJobGraph(Method method, Object[] args) throws IOException {
		final JobGraph jobGraph = new JobGraph("Application: " + Utils.methodToString(method));
		final JobVertex jobVertex = org.apache.flink.hackathon.invocation.Utils.createTaskVertex(method, args, implementor);
		jobGraph.addVertex(jobVertex);

		return jobGraph;
	}

}
