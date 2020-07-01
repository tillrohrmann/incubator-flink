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

import org.apache.flink.hackathon.Application;
import org.apache.flink.hackathon.ApplicationContext;
import org.apache.flink.hackathon.redis.RedisFuture;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ExceptionUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * RemoteTaskInvocationHandler.
 */
public class RemoteTaskInvocationHandler implements InvocationHandler {
	private final ApplicationContext applicationContext;
	private final Class<? extends Application<?>> implementor;

	public <W extends Application<?>> RemoteTaskInvocationHandler(ApplicationContext applicationContext, Class<W> implementor) {
		this.applicationContext = applicationContext;
		this.implementor = implementor;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		final AbstractID futureId;

		if (Void.TYPE.equals(method.getReturnType())) {
			futureId = null;
		} else {
			futureId = new AbstractID();
		}

		final JobVertex taskVertex = Utils.createTaskVertex(method, args, implementor, futureId);

		if (method.getName().startsWith("toString")) {
			System.out.println("damn it");
		}

		final CompletableFuture<Acknowledge> executeTaskFuture = applicationContext.executeTask(taskVertex);

		try {
			executeTaskFuture.get();
		} catch (ExecutionException ee) {
			throw ExceptionUtils.stripExecutionException(ee);
		}

		if (Void.TYPE.equals(method.getReturnType())) {
			return null;
		} else {
			return new RedisFuture<>(futureId);
		}
	}
}
