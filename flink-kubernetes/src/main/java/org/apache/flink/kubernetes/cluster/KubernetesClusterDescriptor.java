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

package org.apache.flink.kubernetes.cluster;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServicePort;
import io.kubernetes.client.models.V1ServiceSpec;
import io.kubernetes.client.util.Config;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.kubernetes.entrypoint.KubernetesJobClusterEntrypoint.KUBERNETES_CLUSTER_ID;
import static org.apache.flink.kubernetes.entrypoint.KubernetesJobClusterEntrypoint.KUBERNETES_IMAGE_NAME;

/**
 * Kubernetes specific {@link ClusterDescriptor} implementation.
 */
public class KubernetesClusterDescriptor implements ClusterDescriptor<String> {

	@Nonnull
	private final Configuration configuration;

	@Nonnull
	private final String imageName;

	@Nullable
	private final String className;

	@Nullable
	private final String userCodeJar;

	private final CoreV1Api kubernetesClient;

	public KubernetesClusterDescriptor(@Nonnull Configuration configuration, @Nonnull String imageName, @Nullable String className, @Nullable String userCodeJar) throws IOException {
		this.configuration = configuration;
		this.imageName = imageName;
		this.className = className;
		this.userCodeJar = userCodeJar;
		kubernetesClient = new CoreV1Api(Config.defaultClient());
	}

	@Override
	public String getClusterDescription() {
		return "Kubernetes cluster";
	}

	@Override
	public ClusterClient<String> retrieve(String clusterId) throws ClusterRetrieveException {
		final Configuration effectiveConfiguration = new Configuration();
		try {
			return new RestClusterClient<>(effectiveConfiguration, clusterId);
		} catch (Exception e) {
			throw new ClusterRetrieveException("Could not create the RestClusterClient.", e);
		}
	}

	@Override
	public ClusterClient<String> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
		final String clusterName = "flink-session-cluster-" + UUID.randomUUID();

		final List<String> args = new ArrayList<>(1);
		args.add("cluster");

		return deployClusterInternal(clusterName, args);
	}

	@Override
	public ClusterClient<String> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) throws ClusterDeploymentException {
		final String clusterName = "flink-job-cluster-" + UUID.randomUUID();

		final List<String> args = new ArrayList<>(5);
		args.add("job");

		if (className != null) {
			args.add("--class");
			args.add(className);
		}

		if (userCodeJar != null) {
			args.add("--userCodeJar");
			args.add(userCodeJar);
		}

		return deployClusterInternal(clusterName, args);
	}

	@Nonnull
	private ClusterClient<String> deployClusterInternal(String clusterName, List<String> args) throws ClusterDeploymentException {
		try {
			final Map<String, String> labels = new HashMap<>(2);
			labels.put("app", "flink");
			labels.put("cluster", clusterName);

			final V1ServicePort rpcPort = new V1ServicePort()
				.name("rpc")
				.port(6123);

			final V1ServicePort blobPort = new V1ServicePort()
				.name("blob")
				.port(6124);

			final V1ServicePort queryPort = new V1ServicePort()
				.name("query")
				.port(6125);

			final String uiPortName = "ui";
			final V1ServicePort uiPort = new V1ServicePort()
				.name(uiPortName)
				.port(8081);

			final V1ServiceSpec serviceSpec = new V1ServiceSpec()
				.type("NodePort")
				.ports(
					Arrays.asList(rpcPort, blobPort, queryPort, uiPort))
				.selector(labels);

			final V1Service service = new V1Service()
				.apiVersion("v1")
				.kind("Service")
				.metadata(new V1ObjectMeta().name(clusterName).labels(labels))
				.spec(serviceSpec);

			final V1Service namespacedService = kubernetesClient.createNamespacedService("default", service, "true");

			final List<V1ServicePort> ports = namespacedService.getSpec().getPorts();

			int uiNodePort = -1;

			for (V1ServicePort port : ports) {
				if (uiPortName.equals(port.getName())) {
					uiNodePort = port.getNodePort();
					break;
				}
			}

			if (uiNodePort == -1) {
				throw new FlinkException("Could not retrieve the ui node port.");
			}

			final V1Container clusterContainer = new V1Container()
				.name(clusterName)
				.image(imageName)
				.args(args)
				.env(
					Arrays.asList(
						new V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value(clusterName),
						new V1EnvVar().name(KUBERNETES_IMAGE_NAME).value(imageName),
						new V1EnvVar().name(KUBERNETES_CLUSTER_ID).value(clusterName)));

			final V1Pod pod = new V1Pod()
				.apiVersion("v1")
				.metadata(new V1ObjectMeta().name(clusterName).labels(labels))
				.spec(new V1PodSpec().containers(Collections.singletonList(clusterContainer)));

			kubernetesClient.createNamespacedPod("default", pod, "true");

			final Configuration modifiedConfiguration = new Configuration(configuration);

			modifiedConfiguration.setString(RestOptions.ADDRESS, URI.create(kubernetesClient.getApiClient().getBasePath()).getHost());
			modifiedConfiguration.setInteger(RestOptions.PORT, uiNodePort);

			return new RestClusterClient<>(modifiedConfiguration, clusterName);
		} catch (Exception e) {
			throw new ClusterDeploymentException("Could not create the Kubernetes cluster client.", e);
		}
	}

	@Override
	public void killCluster(String clusterId) throws FlinkException {
		try {
			kubernetesClient.deleteNamespacedService(clusterId, "default", "true");
			kubernetesClient.deleteCollectionNamespacedPod("default", "true", null, null, true, "app=flink,cluster=" + clusterId, 1000, null, null, false);
		} catch (ApiException e) {
			throw new FlinkException(String.format("Could not kill the cluster %s.", clusterId), e);
		}
	}

	@Override
	public void close() throws Exception {
		// noop
	}
}
