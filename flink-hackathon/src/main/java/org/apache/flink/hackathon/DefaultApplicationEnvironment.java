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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.hackathon.invocation.MiniClusterInvocationHandler;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;

/**
 * Default {@link ApplicationEnvironment}.
 */
public class DefaultApplicationEnvironment implements ApplicationEnvironment {

	private final MiniCluster miniCluster;

	public DefaultApplicationEnvironment() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setLong(WebOptions.REFRESH_INTERVAL, 100);
		this.miniCluster = new MiniCluster(new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumSlotsPerTaskManager(10)
			.build());
		miniCluster.start();
	}

	@Override
	public <T, V extends Application<T>> T remote(Class<T> type, Class<V> implementor) {
		return (T) Proxy.newProxyInstance(
			getClass().getClassLoader(),
			new Class[]{type},
			new MiniClusterInvocationHandler(miniCluster, implementor));
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return miniCluster.closeAsync();
	}
}
