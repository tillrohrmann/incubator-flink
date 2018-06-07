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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kubernetes.resourcemanager.KubernetesResourceManager;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.io.FileUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Entrypoint for a Kubernetes job cluster.
 */

public class KubernetesJobClusterEntrypoint extends JobClusterEntrypoint {

	private static final int CONNECTION_TIMEOUT = 60000;

	private static final int READ_TIMEOUT = 60000;

	@Nonnull
	private final Path userCodeJarPath;

	@Nullable
	private String entrypointClassName;

	private final String[] args;

	private final int parallelism;

	public KubernetesJobClusterEntrypoint(Configuration configuration, @Nonnull Path userCodeJarPath, @Nullable String entrypointClassName) {
		super(configuration);
		this.userCodeJarPath = userCodeJarPath;
		this.entrypointClassName = entrypointClassName;
		args = new String[0];
		this.parallelism = 1;
	}

	@Override
	protected JobGraph retrieveJobGraph(Configuration configuration, BlobServer blobServer) throws FlinkException {
		final PackagedProgram packagedProgram = createPackagedProgram();
		try {
			final JobGraph jobGraph = PackagedProgramUtils.createJobGraph(packagedProgram, configuration, parallelism);
			jobGraph.setAllowQueuedScheduling(true);

			final PermanentBlobKey permanentBlobKey;
			final FileSystem fileSystem = userCodeJarPath.getFileSystem();
			try (InputStream inputStream = fileSystem.open(userCodeJarPath)) {
				permanentBlobKey = blobServer.putPermanent(jobGraph.getJobID(), inputStream);
			}

			jobGraph.addUserJarBlobKey(permanentBlobKey);

			return jobGraph;
		} catch (Exception e) {
			throw new FlinkException("Could not create the JobGraph from the provided user code jar.", e);
		}
	}

	private PackagedProgram createPackagedProgram() throws FlinkException {
		try {
			return new PackagedProgram(new File(userCodeJarPath.toUri()), entrypointClassName, args);
		} catch (ProgramInvocationException e) {
			throw new FlinkException("Could not create a PackagedProgram from the provided user code jar.", e);
		}
	}

	@Override
	protected void registerShutdownActions(CompletableFuture<ApplicationStatus> terminationFuture) {
		terminationFuture.thenAccept((status) -> System.exit(status.processExitCode()));
	}

	@Override
	protected ResourceManager<?> createResourceManager(Configuration configuration, ResourceID resourceId, RpcService rpcService, HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, MetricRegistry metricRegistry, FatalErrorHandler fatalErrorHandler, ClusterInformation clusterInformation, @Nullable String webInterfaceUrl) throws Exception {
		final ResourceManagerConfiguration resourceManagerConfiguration = ResourceManagerConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServicesConfiguration resourceManagerRuntimeServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration);
		final ResourceManagerRuntimeServices resourceManagerRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			resourceManagerRuntimeServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());

		return new KubernetesResourceManager(
			rpcService,
			FlinkResourceManager.RESOURCE_MANAGER_NAME,
			resourceId,
			resourceManagerConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			resourceManagerRuntimeServices.getSlotManager(),
			metricRegistry,
			resourceManagerRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler);
	}

	protected static KubernetesJobClusterConfiguration parseArguments(String[] args) {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		final ClusterConfiguration clusterConfiguration = ClusterEntrypoint.parseArguments(parameterTool);

		final String userCodeJar = parameterTool.get("userCodeJar", "file://jobs/job.jar");
		final String entrypointClassName = parameterTool.get("class");

		return new KubernetesJobClusterConfiguration(
			clusterConfiguration.getConfigDir(),
			clusterConfiguration.getRestPort(),
			userCodeJar,
			entrypointClassName);
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, KubernetesJobClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final KubernetesJobClusterConfiguration clusterConfiguration = parseArguments(args);

		Configuration configuration = loadConfiguration(clusterConfiguration);

		final String[] tmpDirs = ConfigurationUtils.parseTempDirectories(configuration);

		final Path userCodeJarPath = downloadIfRemote(clusterConfiguration.getUserCodeJar(), new File(tmpDirs[0]));

		KubernetesJobClusterEntrypoint entrypoint = new KubernetesJobClusterEntrypoint(configuration, userCodeJarPath, clusterConfiguration.getEntrypointClassName());

		entrypoint.startCluster();
	}

	protected static Path downloadIfRemote(String userCodeJar, File tmpDirectory) {
		final URI userCodeJarURI = URI.create(userCodeJar);

		try {
			final Path path = new Path(userCodeJarURI);
			path.getFileSystem();
			return path;
		} catch (IOException ignored) {
			// try whether we can transform the URI into a URL and download the resource
		}

		try {
			final URL userCodeJarURL = userCodeJarURI.toURL();

			final File destination = new File(tmpDirectory, "flink-job-" + UUID.randomUUID() + ".jar");

			FileUtils.copyURLToFile(userCodeJarURL, destination, CONNECTION_TIMEOUT, READ_TIMEOUT);

			return new Path(destination.toURI());
		} catch (Exception e) {
			throw new FlinkRuntimeException(String.format("Could not access the user code jar specified by %s.", userCodeJar), e);
		}
	}

	private static class KubernetesJobClusterConfiguration extends ClusterConfiguration {

		@Nonnull
		private final String userCodeJar;

		@Nullable
		private final String entrypointClassName;

		KubernetesJobClusterConfiguration(String configDir, int restPort, @Nonnull String userCodeJar, @Nullable String entrypointClassName) {
			super(configDir, restPort);
			this.userCodeJar = userCodeJar;
			this.entrypointClassName = entrypointClassName;
		}

		@Nonnull
		public String getUserCodeJar() {
			return userCodeJar;
		}

		@Nullable
		public String getEntrypointClassName() {
			return entrypointClassName;
		}
	}
}
