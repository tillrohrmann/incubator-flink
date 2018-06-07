/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.resourcemanager;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.util.Config;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

/**
 * Kubernetes specific implementation of the {@link ResourceManager}.
 */
public class KubernetesResourceManager extends ResourceManager<ResourceID> {

	@Nullable
	private CoreV1Api kubernetesApi;

	public KubernetesResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler) {
		super(rpcService, resourceManagerEndpointId, resourceId, resourceManagerConfiguration, highAvailabilityServices, heartbeatServices, slotManager, metricRegistry, jobLeaderIdService, clusterInformation, fatalErrorHandler);

		kubernetesApi = null;
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		ApiClient apiClient = null;
		try {
			apiClient = Config.defaultClient();
		} catch (IOException e) {
			throw new ResourceManagerException("Could not create the Kubernetes ApiClient.", e);
		}

		kubernetesApi = new CoreV1Api(apiClient);
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) throws ResourceManagerException {

	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		final V1Container container = new V1Container()
			.name("taskmanager")
			.image("flink:native-kubernetes")
			.args(Collections.singletonList("taskmanager"))
			.env(Collections.singletonList(new V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value("flink-session-cluster")));

		final V1Pod pod = new V1Pod()
			.apiVersion("v1")
			.metadata(new V1ObjectMeta().name("flink-taskmanager-" + UUID.randomUUID()))
			.spec(new V1PodSpec().containers(Collections.singletonList(container)));

		try {
			kubernetesApi.createNamespacedPod("default", pod, "true");
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected ResourceID workerStarted(ResourceID resourceID) {
		return resourceID;
	}

	@Override
	public boolean stopWorker(ResourceID worker) {
		return true;
	}
}
