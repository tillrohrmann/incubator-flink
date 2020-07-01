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
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;

import java.util.concurrent.CompletableFuture;

/**
 * MiniClusterInvocationHandler.
 */
public class MiniClusterInvocationHandler extends AbstractClientInvocationHandler {

	private final MiniCluster miniCluster;

	public MiniClusterInvocationHandler(MiniCluster miniCluster, Class<?> implementor) {
		super(MiniClusterInvocationHandler.class.getClassLoader(), implementor);
		this.miniCluster = miniCluster;
	}

	@Override
	protected CompletableFuture<JobResult> requestJobResult(JobGraph jobGraph) {
		return miniCluster.requestJobResult(jobGraph.getJobID());
	}

	@Override
	protected CompletableFuture<JobID> submitJob(JobGraph jobGraph) {
		return miniCluster.submitJob(jobGraph).thenApply(JobSubmissionResult::getJobID);
	}
}
