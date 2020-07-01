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

package org.apache.flink.hackathon;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskexecutor.TaskExecutorActions;

import java.util.concurrent.CompletableFuture;

/**
 * DefaultApplicationContext.
 */
public class DefaultApplicationContext implements ApplicationContext {
	private final ClassLoader userCodeClassLoader;
	private final TaskExecutorActions taskExecutorActions;

	public DefaultApplicationContext(ClassLoader userCodeClassLoader, TaskExecutorActions taskExecutorActions) {
		this.userCodeClassLoader = userCodeClassLoader;
		this.taskExecutorActions = taskExecutorActions;
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return userCodeClassLoader;
	}

	@Override
	public <T extends Application<?>> ActorAddress startActor(Class<T> actorClass) {
		return null;
	}

	@Override
	public CompletableFuture<Acknowledge> executeTask(JobVertex taskVertex) {
		return taskExecutorActions.executeTask(taskVertex);
	}
}
