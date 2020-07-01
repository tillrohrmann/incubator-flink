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

import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.hackathon.handlers.RestClusterClientInvocationHandler;

import java.io.File;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * RemoteApplicationEnvironment.
 */
public class RemoteApplicationEnvironment implements ApplicationEnvironment {
	private final RestClusterClient<StandaloneClusterId> restClusterClient;
	private final Collection<File> jarFiles;

	public RemoteApplicationEnvironment(String restAddress, int restPort, Collection<File> jarFiles) throws Exception {
		final Configuration configuration = new Configuration();
		configuration.set(RestOptions.ADDRESS, restAddress);
		configuration.set(RestOptions.PORT, restPort);

		this.restClusterClient = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance());
		this.jarFiles = jarFiles;
	}

	@Override
	public <T, V extends Application<T>> T remote(Class<T> type, Class<V> implementor) {
		return (T) Proxy.newProxyInstance(
			getClass().getClassLoader(),
			new Class[]{type},
			new RestClusterClientInvocationHandler(implementor, restClusterClient, jarFiles));
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		restClusterClient.close();

		return CompletableFuture.completedFuture(null);
	}
}
