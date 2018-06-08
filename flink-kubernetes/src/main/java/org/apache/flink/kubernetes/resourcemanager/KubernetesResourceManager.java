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
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
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
import org.apache.flink.util.Preconditions;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.util.Config;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Kubernetes specific implementation of the {@link ResourceManager}.
 */
public class KubernetesResourceManager extends ResourceManager<KubernetesResourceManager.KubernetesWorkerNode> {
	public static final String ENV_RESOURCE_ID = "RESOURCE_ID";

	private final ConcurrentMap<ResourceID, KubernetesWorkerNode> workerNodeMap;

	@Nonnull
	private final String imageName;

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
			FatalErrorHandler fatalErrorHandler,
			@Nonnull String imageName) {
		super(rpcService, resourceManagerEndpointId, resourceId, resourceManagerConfiguration, highAvailabilityServices, heartbeatServices, slotManager, metricRegistry, jobLeaderIdService, clusterInformation, fatalErrorHandler);

		this.imageName = imageName;
		this.kubernetesApi = null;
		this.workerNodeMap = new ConcurrentHashMap<>();
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		ApiClient apiClient;
		try {
			apiClient = Config.fromCluster();
		} catch (IOException e) {
			throw new ResourceManagerException("Could not create the Kubernetes ApiClient.", e);
		}

		kubernetesApi = new CoreV1Api(apiClient);
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {

	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		Preconditions.checkNotNull(kubernetesApi);
		String podName = "flink-taskmanager-" + UUID.randomUUID();
		V1Pod pod = createTaskManagerPodManifest(podName, resourceProfile);
		log.debug(String.format("Creating pod %s:\n%s", podName, pod));
		try {
			pod = kubernetesApi.createNamespacedPod("default", pod, "true");
		} catch (ApiException e) {
			throw new RuntimeException("Failed to start task manager pod", e);
		}
		KubernetesWorkerNode worker = new KubernetesWorkerNode(pod);
		workerNodeMap.put(worker.getResourceID(), worker);
	}

	private V1Pod createTaskManagerPodManifest(String podName, ResourceProfile resourceProfile) {
		final V1Container container = new V1Container()
			.name("taskmanager")
			.image(imageName)
			.args(Collections.singletonList("taskmanager"))
			.env(Arrays.asList(
				new V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value(getRpcService().getAddress()),
				new V1EnvVar().name(ENV_RESOURCE_ID).value(podName)
			))
//			.resources(new V1ResourceRequirements().limits(ImmutableMap.of(
//				"cpu", Quantity.fromString(Double.toString(resourceProfile.getCpuCores())),
//				"memory", Quantity.fromString(String.format("%dMi", resourceProfile.getMemoryInMB()))
//			)))
			;

		return new V1Pod()
			.apiVersion("v1")
			.metadata(new V1ObjectMeta().name(podName))
			.spec(new V1PodSpec().containers(Collections.singletonList(container)));
	}

	@Override
	protected KubernetesWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
	}

	@Override
	public boolean stopWorker(KubernetesWorkerNode worker) {
		Preconditions.checkNotNull(kubernetesApi);
		String name = worker.pod.getMetadata().getName();
		String namespace = worker.pod.getMetadata().getNamespace();
		try {
			if (!podExists(name, namespace)) {
				log.debug(String.format("Deleting pod %s: does not exist", name));
				return true;
			}
			log.debug(String.format("Deleting pod %s:\n%s", name, worker.pod));
			kubernetesApi.deleteNamespacedPod(
				name,
				namespace,
				new V1DeleteOptions().kind("DeleteOptions").apiVersion("apiVersion").propagationPolicy("Background"),
				"true",
				0, false, "Background");
		} catch (ApiException e) {
			throw new RuntimeException("Failed to delete task manager pod", e);
		}
		return true;
	}

	private boolean podExists(String name, String namespace) throws ApiException {
		String cont = null;
		do {
			V1PodList pods = listPods(namespace, cont);
			for (V1Pod pod : pods.getItems()) {
				if (pod.getMetadata().getName().equals(name)) {
					return true;
				}
			}
			cont = pods.getMetadata().getContinue();
		} while (cont != null);
		return false;
	}

	private V1PodList listPods(String namespace, String cont) throws ApiException {
		Preconditions.checkNotNull(kubernetesApi);
		return kubernetesApi.listNamespacedPod(namespace, "true",
			cont, null, null, null,
			null, null, null, null);
	}

	/**
	 * Kubernetes specific implementation of the {@link ResourceIDRetrievable}.
	 */
	public static class KubernetesWorkerNode implements ResourceIDRetrievable {
		private final ResourceID resourceID;
		private final V1Pod pod;

		KubernetesWorkerNode(V1Pod pod) {
			this.pod = pod;
			Preconditions.checkNotNull(this.pod);
			this.resourceID = new ResourceID(pod.getMetadata().getName());
		}

		@Override
		public ResourceID getResourceID() {
			return resourceID;
		}
	}
}
