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

import org.apache.flink.hackathon.invocation.RemoteActorInvocationHandler;
import org.apache.flink.hackathon.invocation.RemoteTaskInvocationHandler;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Proxy;

/**
 * Application.
 * @param <T>
 */
public class Application<T> {
	private final Class<T> type;
	private final ApplicationContext applicationContext;

	public Application(Class<T> type, ApplicationContext applicationContext) {
		this.type = type;
		this.applicationContext = applicationContext;

		Preconditions.checkState(type.isAssignableFrom(getClass()));
	}

	T remoteTask() {
		return (T) remoteTask(type, getClass());
	}

	<V, W extends Application<V>> V remoteTask(Class<V> type, Class<W> implementor) {
		return (V) Proxy.newProxyInstance(
			applicationContext.getUserCodeClassLoader(),
			new Class<?>[]{type},
			new RemoteTaskInvocationHandler(applicationContext, implementor));
	}

	<V, W extends Application<V>> V remoteActor(Class<V> type, Class<W> implementor) {
		final ActorAddress actorAddress = applicationContext.startActor(implementor);

		return (V) Proxy.newProxyInstance(
			applicationContext.getUserCodeClassLoader(),
			new Class<?>[]{type},
			new RemoteActorInvocationHandler(actorAddress));
	}
}
