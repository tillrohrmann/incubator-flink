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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Tests the HA behaviour of the {@link Dispatcher}.
 */
public class DispatcherHATest extends TestLogger {
	private static final DispatcherId NULL_FENCING_TOKEN = DispatcherId.fromUuid(new UUID(0L, 0L));

	private static final Time timeout = Time.seconds(10L);

	private static TestingRpcService rpcService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@Before
	public void setup() {
		testingFatalErrorHandler = new TestingFatalErrorHandler();
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	@AfterClass
	public static void teardownClass() throws ExecutionException, InterruptedException {
		if (rpcService != null) {
			rpcService.stopService().get();
			rpcService = null;
		}
	}

	@Nonnull
	private HATestingDispatcher createDispatcherWithObservableFencingTokens(HighAvailabilityServices highAvailabilityServices, Queue<DispatcherId> fencingTokens) throws Exception {
		return createDispatcher(highAvailabilityServices, fencingTokens, createTestingJobManagerRunnerFactory());
	}

	@Nonnull
	private TestingJobManagerRunnerFactory createTestingJobManagerRunnerFactory() {
		return new TestingJobManagerRunnerFactory();
	}

	private HATestingDispatcher createDispatcher(HighAvailabilityServices haServices) throws Exception {
		return createDispatcher(
			haServices,
			new ArrayDeque<>(1),
			createTestingJobManagerRunnerFactory());
	}

	@Nonnull
	private HATestingDispatcher createDispatcher(
		HighAvailabilityServices highAvailabilityServices,
		@Nonnull Queue<DispatcherId> fencingTokens,
		JobManagerRunnerFactory jobManagerRunnerFactory) throws Exception {
		final Configuration configuration = new Configuration();

		TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		return new HATestingDispatcher(
			rpcService,
			UUID.randomUUID().toString(),
			DispatcherId.generate(),
			Collections.emptyList(),
			configuration,
			highAvailabilityServices,
			() -> CompletableFuture.completedFuture(resourceManagerGateway),
			new BlobServer(configuration, new VoidBlobStore()),
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			new MemoryArchivedExecutionGraphStore(),
			jobManagerRunnerFactory,
			testingFatalErrorHandler,
			fencingTokens);
	}

	@Nonnull
	static JobGraph createNonEmptyJobGraph() {
		final JobVertex noOpVertex = new JobVertex("NoOp vertex");
		noOpVertex.setInvokableClass(NoOpInvokable.class);
		final JobGraph jobGraph = new JobGraph(noOpVertex);
		jobGraph.setAllowQueuedScheduling(true);

		return jobGraph;
	}

	private static class HATestingDispatcher extends TestingDispatcher {

		@Nonnull
		private final Queue<DispatcherId> fencingTokens;

		HATestingDispatcher(
				RpcService rpcService,
				String endpointId,
				DispatcherId fencingToken,
				Collection<JobGraph> recoveredJobs,
				Configuration configuration,
				HighAvailabilityServices highAvailabilityServices,
				GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
				BlobServer blobServer,
				HeartbeatServices heartbeatServices,
				JobManagerMetricGroup jobManagerMetricGroup,
				@Nullable String metricQueryServiceAddress,
				ArchivedExecutionGraphStore archivedExecutionGraphStore,
				JobManagerRunnerFactory jobManagerRunnerFactory,
				FatalErrorHandler fatalErrorHandler,
				@Nonnull Queue<DispatcherId> fencingTokens) throws Exception {
			super(
				rpcService,
				endpointId,
				fencingToken,
				recoveredJobs,
				new DispatcherServices(
					configuration,
					highAvailabilityServices,
					resourceManagerGatewayRetriever,
					blobServer,
					heartbeatServices,
					jobManagerMetricGroup,
					archivedExecutionGraphStore,
					fatalErrorHandler,
					VoidHistoryServerArchivist.INSTANCE,
					metricQueryServiceAddress,
					highAvailabilityServices.getJobGraphStore(),
					jobManagerRunnerFactory));
			this.fencingTokens = fencingTokens;
		}

		@Override
		protected void setFencingToken(@Nullable DispatcherId newFencingToken) {
			super.setFencingToken(newFencingToken);

			final DispatcherId fencingToken;
			if (newFencingToken == null) {
				fencingToken = NULL_FENCING_TOKEN;
			} else {
				fencingToken = newFencingToken;
			}

			fencingTokens.offer(fencingToken);
		}
	}

	private static class BlockingJobGraphStore implements JobGraphStore {

		@Nonnull
		private final JobGraph jobGraph;

		@Nonnull
		private final OneShotLatch enterGetJobIdsLatch;

		@Nonnull
		private final OneShotLatch proceedGetJobIdsLatch;

		private BlockingJobGraphStore(@Nonnull JobGraph jobGraph, @Nonnull OneShotLatch enterGetJobIdsLatch, @Nonnull OneShotLatch proceedGetJobIdsLatch) {
			this.jobGraph = jobGraph;
			this.enterGetJobIdsLatch = enterGetJobIdsLatch;
			this.proceedGetJobIdsLatch = proceedGetJobIdsLatch;
		}

		@Override
		public void start(JobGraphListener jobGraphListener) {
		}

		@Override
		public void stop() {
		}

		@Nullable
		@Override
		public JobGraph recoverJobGraph(JobID jobId) {
			Preconditions.checkArgument(jobId.equals(jobGraph.getJobID()));

			return jobGraph;
		}

		@Override
		public void putJobGraph(JobGraph jobGraph) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public void removeJobGraph(JobID jobId) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public void releaseJobGraph(JobID jobId) {}

		@Override
		public Collection<JobID> getJobIds() throws Exception {
			enterGetJobIdsLatch.trigger();
			proceedGetJobIdsLatch.await();
			return Collections.singleton(jobGraph.getJobID());
		}
	}

	private static class FailingJobGraphStore implements JobGraphStore {
		private final JobID jobId = new JobID();

		private final Supplier<Exception> exceptionSupplier;

		private FailingJobGraphStore(Supplier<Exception> exceptionSupplier) {
			this.exceptionSupplier = exceptionSupplier;
		}

		@Override
		public void start(JobGraphListener jobGraphListener) {

		}

		@Override
		public void stop() {

		}

		@Nullable
		@Override
		public JobGraph recoverJobGraph(JobID jobId) throws Exception {
			throw exceptionSupplier.get();
		}

		@Override
		public void putJobGraph(JobGraph jobGraph) {

		}

		@Override
		public void removeJobGraph(JobID jobId) {

		}

		@Override
		public void releaseJobGraph(JobID jobId) {

		}

		@Override
		public Collection<JobID> getJobIds() {
			return Collections.singleton(jobId);
		}
	}
}
