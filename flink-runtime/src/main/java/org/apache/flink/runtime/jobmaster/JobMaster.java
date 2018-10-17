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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.StoppingException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyResolving;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.driver.DefaultExecutionGraphDriver;
import org.apache.flink.runtime.jobmaster.driver.ExecutionGraphDriver;
import org.apache.flink.runtime.jobmaster.driver.ExecutionGraphDriverException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.message.ClassloadingProps;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link JobGraph}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 * <ul>
 * <li>{@link #updateTaskExecutionState} updates the task execution state for
 * given task</li>
 * </ul>
 */
public class JobMaster extends FencedRpcEndpoint<JobMasterId> implements JobMasterGateway {

	/** Default names for Flink's distributed components. */
	public static final String JOB_MANAGER_NAME = "jobmanager";
	public static final String ARCHIVE_NAME = "archive";

	// ------------------------------------------------------------------------

	private final JobMasterConfiguration jobMasterConfiguration;

	private final ResourceID resourceId;

	private final JobGraph jobGraph;

	private final Time rpcTimeout;

	private final HighAvailabilityServices highAvailabilityServices;

	private final BlobServer blobServer;

	private final HeartbeatManager<AccumulatorReport, Void> taskManagerHeartbeatManager;

	private final HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

	private final ScheduledExecutorService scheduledExecutorService;

	private final OnCompletionActions jobCompletionActions;

	private final FatalErrorHandler fatalErrorHandler;

	private final ClassLoader userCodeLoader;

	private final SlotPool slotPool;

	private final SlotPoolGateway slotPoolGateway;

	private final RestartStrategy restartStrategy;

	private final ExecutionGraphDriver executionGraphDriver;

	// --------- ResourceManager --------

	private final LeaderRetrievalService resourceManagerLeaderRetriever;

	// --------- TaskManagers --------

	private final Map<ResourceID, Tuple2<TaskManagerLocation, TaskExecutorGateway>> registeredTaskManagers;

	// -------- Mutable fields ---------

	@Nullable
	private ResourceManagerAddress resourceManagerAddress;

	@Nullable
	private ResourceManagerConnection resourceManagerConnection;

	@Nullable
	private EstablishedResourceManagerConnection establishedResourceManagerConnection;

	// ------------------------------------------------------------------------

	public JobMaster(
			RpcService rpcService,
			JobMasterConfiguration jobMasterConfiguration,
			ResourceID resourceId,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityService,
			SlotPoolFactory slotPoolFactory,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices,
			BlobServer blobServer,
			JobManagerJobMetricGroupFactory jobMetricGroupFactory,
			OnCompletionActions jobCompletionActions,
			FatalErrorHandler fatalErrorHandler,
			ClassLoader userCodeLoader) throws Exception {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(JOB_MANAGER_NAME));

		final JobMasterGateway selfGateway = getSelfGateway(JobMasterGateway.class);

		this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
		this.resourceId = checkNotNull(resourceId);
		this.jobGraph = checkNotNull(jobGraph);
		this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.blobServer = checkNotNull(blobServer);
		this.scheduledExecutorService = jobManagerSharedServices.getScheduledExecutorService();
		this.jobCompletionActions = checkNotNull(jobCompletionActions);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.userCodeLoader = checkNotNull(userCodeLoader);

		this.taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new TaskManagerHeartbeatListener(selfGateway),
			rpcService.getScheduledExecutor(),
			log);

		this.resourceManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
				resourceId,
				new ResourceManagerHeartbeatListener(),
				rpcService.getScheduledExecutor(),
				log);

		final String jobName = jobGraph.getName();
		final JobID jid = jobGraph.getJobID();

		log.info("Initializing job {} ({}).", jobName, jid);

		final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
				jobGraph.getSerializedExecutionConfig()
						.deserializeValue(userCodeLoader)
						.getRestartStrategy();

		this.restartStrategy = RestartStrategyResolving.resolve(restartStrategyConfiguration,
			jobManagerSharedServices.getRestartStrategyFactory(),
			jobGraph.isCheckpointingEnabled());

		log.info("Using restart strategy {} for {} ({}).", this.restartStrategy, jobName, jid);

		resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();

		this.slotPool = checkNotNull(slotPoolFactory).createSlotPool(jobGraph.getJobID());

		this.slotPoolGateway = slotPool.getSelfGateway(SlotPoolGateway.class);

		this.registeredTaskManagers = new HashMap<>(4);

		this.executionGraphDriver = new DefaultExecutionGraphDriver(
			jobGraph,
			this::createExecutionGraph,
			jobMetricGroupFactory,
			jobManagerSharedServices.getBackPressureStatsTracker(),
			userCodeLoader);

		executionGraphDriver.getResultFuture().whenComplete((archivedExecutionGraph, throwable) -> {
			if (throwable != null) {
				handleJobMasterError(throwable);
			} else {
				jobReachedGloballyTerminalState(archivedExecutionGraph);
			}
		});

		this.resourceManagerConnection = null;
		this.establishedResourceManagerConnection = null;
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	@Override
	public void start() {
		throw new UnsupportedOperationException("Should never call start() without leader ID");
	}

	/**
	 * Start the rpc service and begin to run the job.
	 *
	 * @param newJobMasterId The necessary fencing token to run the job
	 * @param timeout for the operation
	 * @return Future acknowledge if the job could be started. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId, final Time timeout) throws Exception {
		// make sure we receive RPC and async calls
		super.start();

		return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), timeout);
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(JobMasterId, Time)} method once we take the leadership back again.
	 *
	 * <p>This method is executed asynchronously
	 *
	 * @param cause The reason of why this job been suspended.
	 * @param timeout for this operation
	 * @return Future acknowledge indicating that the job has been suspended. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> suspend(final Exception cause, final Time timeout) {
		CompletableFuture<Acknowledge> suspendFuture = callAsyncWithoutFencing(() -> suspendExecution(cause), timeout);

		stop();

		return suspendFuture;
	}

	/**
	 * Suspend the job and shutdown all other services including rpc.
	 */
	@Override
	public CompletableFuture<Void> postStop() {
		log.info("Stopping the JobMaster for job {}({}).", jobGraph.getName(), jobGraph.getJobID());

		// disconnect from all registered TaskExecutors
		final Set<ResourceID> taskManagerResourceIds = new HashSet<>(registeredTaskManagers.keySet());
		final FlinkException cause = new FlinkException("Stopping JobMaster for job " + jobGraph.getName() +
			'(' + jobGraph.getJobID() + ").");

		for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
			disconnectTaskManager(taskManagerResourceId, cause);
		}

		taskManagerHeartbeatManager.stop();
		resourceManagerHeartbeatManager.stop();

		// make sure there is a graceful exit
		suspendExecution(new FlinkException("JobManager is shutting down."));

		// shut down will internally release all registered slots
		slotPool.shutDown();

		return slotPool.getTerminationFuture();
	}

	//----------------------------------------------------------------------------------------------
	// RPC methods
	//----------------------------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		executionGraphDriver.cancel();
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> stop(Time timeout) {
		try {
			executionGraphDriver.stop();
		} catch (StoppingException e) {
			return FutureUtils.completedExceptionally(e);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> rescaleJob(
			int newParallelism,
			RescalingBehaviour rescalingBehaviour,
			Time timeout) {
		final ArrayList<JobVertexID> allOperators = new ArrayList<>(jobGraph.getNumberOfVertices());

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			allOperators.add(jobVertex.getID());
		}

		return rescaleOperators(allOperators, newParallelism, rescalingBehaviour, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> rescaleOperators(
			Collection<JobVertexID> operators,
			int newParallelism,
			RescalingBehaviour rescalingBehaviour,
			Time timeout) {

		return executionGraphDriver.rescaleOperators(
			operators,
			newParallelism,
			rescalingBehaviour);
	}

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Acknowledge the task execution state update
	 */
	@Override
	public CompletableFuture<Acknowledge> updateTaskExecutionState(
			final TaskExecutionState taskExecutionState) {
		checkNotNull(taskExecutionState, "taskExecutionState");

		if (executionGraphDriver.updateState(taskExecutionState)) {
			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			return FutureUtils.completedExceptionally(
				new ExecutionGraphException("The execution attempt " +
					taskExecutionState.getID() + " was not found."));
		}
	}

	@Override
	public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
			final JobVertexID vertexID,
			final ExecutionAttemptID executionAttempt) {
		final InputSplit nextInputSplit;
		try {
			nextInputSplit = executionGraphDriver.requestNextInputSplit(vertexID, executionAttempt);
		} catch (ExecutionGraphDriverException e) {
			log.warn("Could not obtain the next input split for job {}.", jobGraph.getJobID(), e);
			return FutureUtils.completedExceptionally(new FlinkException(
				String.format("Could not obtain next input split for job %s", jobGraph.getJobID()),
				e));
		}

		try {
			final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
			return CompletableFuture.completedFuture(new SerializedInputSplit(serializedInputSplit));
		} catch (Exception ex) {
			log.error("Could not serialize the next input split of class {}.", nextInputSplit.getClass(), ex);
			IOException reason = new IOException("Could not serialize the next input split of class " +
				nextInputSplit.getClass() + '.', ex);
			executionGraphDriver.fail(reason);
			return FutureUtils.completedExceptionally(reason);
		}
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionState(
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID resultPartitionId) {

		final Optional<ExecutionState> executionStateOptional = executionGraphDriver.requestPartitionState(
			intermediateResultId,
			resultPartitionId);

		return executionStateOptional
			.map(CompletableFuture::completedFuture)
			.orElseGet(() -> FutureUtils.completedExceptionally(new PartitionProducerDisposedException(resultPartitionId)));
	}

	@Override
	public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
			final ResultPartitionID partitionID,
			final Time timeout) {
		try {
			executionGraphDriver.scheduleOrUpdateConsumers(partitionID);
			return CompletableFuture.completedFuture(Acknowledge.get());
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> disconnectTaskManager(final ResourceID resourceID, final Exception cause) {
		log.debug("Disconnect TaskExecutor {} because: {}", resourceID, cause.getMessage());

		taskManagerHeartbeatManager.unmonitorTarget(resourceID);
		CompletableFuture<Acknowledge> releaseFuture = slotPoolGateway.releaseTaskManager(resourceID, cause);

		Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerConnection = registeredTaskManagers.remove(resourceID);

		if (taskManagerConnection != null) {
			taskManagerConnection.f1.disconnectJobManager(jobGraph.getJobID(), cause);
		}

		return releaseFuture;
	}

	// TODO: This method needs a leader session ID
	@Override
	public void acknowledgeCheckpoint(
			final JobID jobID,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointId,
			final CheckpointMetrics checkpointMetrics,
			final TaskStateSnapshot checkpointState) {

		getRpcService().execute(
			() -> executionGraphDriver.acknowledgeCheckpoint(
				jobID,
				executionAttemptID,
				checkpointId,
				checkpointMetrics,
				checkpointState));
	}

	// TODO: This method needs a leader session ID
	@Override
	public void declineCheckpoint(
			final JobID jobID,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointID,
			final Throwable reason) {
		getRpcService().execute(
			() -> executionGraphDriver.declineCheckpoint(
				jobID,
				executionAttemptID,
				checkpointID,
				reason));
	}

	@Override
	public CompletableFuture<KvStateLocation> requestKvStateLocation(final JobID jobId, final String registrationName) {
		// sanity check for the correct JobID
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Lookup key-value state for job {} with registration " +
					"name {}.", jobGraph.getJobID(), registrationName);
			}

			final KvStateLocationRegistry registry = executionGraphDriver.getKvStateLocationRegistry();
			final KvStateLocation location = registry.getKvStateLocation(registrationName);
			if (location != null) {
				return CompletableFuture.completedFuture(location);
			} else {
				return FutureUtils.completedExceptionally(new UnknownKvStateLocation(registrationName));
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Request of key-value state location for unknown job {} received.", jobId);
			}
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		}
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateRegistered(
			final JobID jobId,
			final JobVertexID jobVertexId,
			final KeyGroupRange keyGroupRange,
			final String registrationName,
			final KvStateID kvStateId,
			final InetSocketAddress kvStateServerAddress) {
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state registered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraphDriver.getKvStateLocationRegistry().notifyKvStateRegistered(
					jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress);

				return CompletableFuture.completedFuture(Acknowledge.get());
			} catch (Exception e) {
				log.error("Failed to notify KvStateRegistry about registration {}.", registrationName);
				return FutureUtils.completedExceptionally(e);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Notification about key-value state registration for unknown job {} received.", jobId);
			}
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		}
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName) {
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state unregistered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraphDriver.getKvStateLocationRegistry().notifyKvStateUnregistered(
					jobVertexId, keyGroupRange, registrationName);

				return CompletableFuture.completedFuture(Acknowledge.get());
			} catch (Exception e) {
				log.error("Failed to notify KvStateRegistry about registration {}.", registrationName, e);
				return FutureUtils.completedExceptionally(e);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Notification about key-value state deregistration for unknown job {} received.", jobId);
			}
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		}
	}

	@Override
	public CompletableFuture<ClassloadingProps> requestClassloadingProps() {
		return CompletableFuture.completedFuture(
			new ClassloadingProps(blobServer.getPort(),
				jobGraph.getUserJarBlobKeys(),
				jobGraph.getClasspaths()));
	}

	@Override
	public CompletableFuture<Collection<SlotOffer>> offerSlots(
			final ResourceID taskManagerId,
			final Collection<SlotOffer> slots,
			final Time timeout) {

		Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManager = registeredTaskManagers.get(taskManagerId);

		if (taskManager == null) {
			return FutureUtils.completedExceptionally(new Exception("Unknown TaskManager " + taskManagerId));
		}

		final TaskManagerLocation taskManagerLocation = taskManager.f0;
		final TaskExecutorGateway taskExecutorGateway = taskManager.f1;

		final RpcTaskManagerGateway rpcTaskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, getFencingToken());

		return slotPoolGateway.offerSlots(
			taskManagerLocation,
			rpcTaskManagerGateway,
			slots);
	}

	@Override
	public void failSlot(
			final ResourceID taskManagerId,
			final AllocationID allocationId,
			final Exception cause) {

		if (registeredTaskManagers.containsKey(taskManagerId)) {
			internalFailAllocation(allocationId, cause);
		} else {
			log.warn("Cannot fail slot " + allocationId + " because the TaskManager " +
			taskManagerId + " is unknown.");
		}
	}

	private void internalFailAllocation(AllocationID allocationId, Exception cause) {
		final CompletableFuture<SerializableOptional<ResourceID>> emptyTaskExecutorFuture = slotPoolGateway.failAllocation(allocationId, cause);

		emptyTaskExecutorFuture.thenAcceptAsync(
			resourceIdOptional -> resourceIdOptional.ifPresent(this::releaseEmptyTaskManager),
			getMainThreadExecutor());
	}

	private CompletableFuture<Acknowledge> releaseEmptyTaskManager(ResourceID resourceId) {
		return disconnectTaskManager(resourceId, new FlinkException(String.format("No more slots registered at JobMaster %s.", resourceId)));
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskManager(
			final String taskManagerRpcAddress,
			final TaskManagerLocation taskManagerLocation,
			final Time timeout) {

		final ResourceID taskManagerId = taskManagerLocation.getResourceID();

		if (registeredTaskManagers.containsKey(taskManagerId)) {
			final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
			return CompletableFuture.completedFuture(response);
		} else {
			return getRpcService()
				.connect(taskManagerRpcAddress, TaskExecutorGateway.class)
				.handleAsync(
					(TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
						if (throwable != null) {
							return new RegistrationResponse.Decline(throwable.getMessage());
						}

						slotPoolGateway.registerTaskManager(taskManagerId);
						registeredTaskManagers.put(taskManagerId, Tuple2.of(taskManagerLocation, taskExecutorGateway));

						// monitor the task manager as heartbeat target
						taskManagerHeartbeatManager.monitorTarget(taskManagerId, new HeartbeatTarget<Void>() {
							@Override
							public void receiveHeartbeat(ResourceID resourceID, Void payload) {
								// the task manager will not request heartbeat, so this method will never be called currently
							}

							@Override
							public void requestHeartbeat(ResourceID resourceID, Void payload) {
								taskExecutorGateway.heartbeatFromJobManager(resourceID);
							}
						});

						return new JMTMRegistrationSuccess(resourceId);
					},
					getMainThreadExecutor());
		}
	}

	@Override
	public void disconnectResourceManager(
			final ResourceManagerId resourceManagerId,
			final Exception cause) {

		if (isConnectingToResourceManager(resourceManagerId)) {
			reconnectToResourceManager(cause);
		}
	}

	private boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
		return resourceManagerAddress != null
				&& resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
	}

	@Override
	public void heartbeatFromTaskManager(final ResourceID resourceID, AccumulatorReport accumulatorReport) {
		taskManagerHeartbeatManager.receiveHeartbeat(resourceID, accumulatorReport);
	}

	@Override
	public void heartbeatFromResourceManager(final ResourceID resourceID) {
		resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
	}

	@Override
	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		final AccessExecutionGraph currentExecutionGraph = executionGraphDriver.getExecutionGraph();
		return CompletableFuture.supplyAsync(() -> WebMonitorUtils.createDetailsForJob(currentExecutionGraph), scheduledExecutorService);
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		return CompletableFuture.completedFuture(executionGraphDriver.getState());
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
		return CompletableFuture.completedFuture(ArchivedExecutionGraph.createFrom(executionGraphDriver.getExecutionGraph()));
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
			@Nullable final String targetDirectory,
			final boolean cancelJob,
			final Time timeout) {
		if (cancelJob) {
			executionGraphDriver.stopCheckpointScheduler();
		}

		final CompletableFuture<String> savepointFuture = executionGraphDriver.triggerSavepoint(targetDirectory);

		return savepointFuture.thenApplyAsync(path -> {
			if (cancelJob) {
				log.info("Savepoint stored in {}. Now cancelling {}.", path, jobGraph.getJobID());
				executionGraphDriver.cancel();
			}
			return path;
		}, getMainThreadExecutor())
			.exceptionally(throwable -> {
				if (cancelJob) {
					executionGraphDriver.startCheckpointScheduler();
				}
				throw new CompletionException(throwable);
			});
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(final JobVertexID jobVertexId) {
		return executionGraphDriver.requestOperatorBackPressureStats(jobVertexId);
	}

	@Override
	public void notifyAllocationFailure(AllocationID allocationID, Exception cause) {
		internalFailAllocation(allocationID, cause);
	}

	//----------------------------------------------------------------------------------------------
	// Internal methods
	//----------------------------------------------------------------------------------------------

	//-- job starting and stopping  -----------------------------------------------------------------

	private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {
		validateRunsInMainThread();

		checkNotNull(newJobMasterId, "The new JobMasterId must not be null.");

		if (Objects.equals(getFencingToken(), newJobMasterId)) {
			log.info("Already started the job execution with JobMasterId {}.", newJobMasterId);

			return Acknowledge.get();
		}

		setNewFencingToken(newJobMasterId);

		startJobMasterServices();
		scheduleExecutionGraph();

		return Acknowledge.get();
	}

	private void startJobMasterServices() throws Exception {
		// start the slot pool make sure the slot pool now accepts messages for this leader
		slotPool.start(getFencingToken(), getAddress());

		//TODO: Remove once the ZooKeeperLeaderRetrieval returns the stored address upon start
		// try to reconnect to previously known leader
		reconnectToResourceManager(new FlinkException("Starting JobMaster component."));

		// job is ready to go, try to establish connection with resource manager
		//   - activate leader retrieval for the resource manager
		//   - on notification of the leader, the connection will be established and
		//     the slot pool will start requesting slots
		resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
	}

	private void setNewFencingToken(JobMasterId newJobMasterId) {
		if (getFencingToken() != null) {
			log.info("Restarting old job with JobMasterId {}. The new JobMasterId is {}.", getFencingToken(), newJobMasterId);

			// first we have to suspend the current execution
			suspendExecution(new FlinkException("Old job with JobMasterId " + getFencingToken() +
				" is restarted with a new JobMasterId " + newJobMasterId + '.'));
		}

		// set new leader id
		setFencingToken(newJobMasterId);
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(JobMasterId, Time)} method once we take the leadership back again.
	 *
	 * @param cause The reason of why this job been suspended.
	 */
	private Acknowledge suspendExecution(final Exception cause) {
		validateRunsInMainThread();

		if (getFencingToken() == null) {
			log.debug("Job has already been suspended or shutdown.");
			return Acknowledge.get();
		}

		// not leader anymore --> set the JobMasterId to null
		setFencingToken(null);

		try {
			resourceManagerLeaderRetriever.stop();
		} catch (Throwable t) {
			log.warn("Failed to stop resource manager leader retriever when suspending.", t);
		}

		suspendExecutionGraphDriver(cause);

		// the slot pool stops receiving messages and clears its pooled slots
		slotPoolGateway.suspend();

		// disconnect from resource manager:
		closeResourceManagerConnection(cause);

		return Acknowledge.get();
	}

	private void scheduleExecutionGraph() throws Exception {
		log.info("Starting execution of job {} ({})", jobGraph.getName(), jobGraph.getJobID());
		executionGraphDriver.start();
	}

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph, JobManagerJobMetricGroup currentJobManagerJobMetricGroup) throws JobExecutionException, JobException {
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobMasterConfiguration.getConfiguration(),
			scheduledExecutorService,
			scheduledExecutorService,
			slotPool.getSlotProvider(),
			userCodeLoader,
			highAvailabilityServices.getCheckpointRecoveryFactory(),
			rpcTimeout,
			restartStrategy,
			currentJobManagerJobMetricGroup,
			blobServer,
			jobMasterConfiguration.getSlotRequestTimeout(),
			log);
	}

	private void suspendExecutionGraphDriver(Exception cause) {
		executionGraphDriver.suspend(cause);
	}

	//----------------------------------------------------------------------------------------------

	private void handleJobMasterError(final Throwable cause) {
		if (ExceptionUtils.isJvmFatalError(cause)) {
			log.error("Fatal error occurred on JobManager.", cause);
			// The fatal error handler implementation should make sure that this call is non-blocking
			fatalErrorHandler.onFatalError(cause);
		} else {
			jobCompletionActions.jobMasterFailed(cause);
		}
	}

	private void jobReachedGloballyTerminalState(final ArchivedExecutionGraph archivedExecutionGraph) {
		Preconditions.checkArgument(archivedExecutionGraph.getState().isGloballyTerminalState());
		scheduledExecutorService.execute(() -> jobCompletionActions.jobReachedGloballyTerminalState(archivedExecutionGraph));
	}

	private void notifyOfNewResourceManagerLeader(final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
		resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);

		reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
	}

	@Nullable
	private ResourceManagerAddress createResourceManagerAddress(@Nullable String newResourceManagerAddress, @Nullable ResourceManagerId resourceManagerId) {
		if (newResourceManagerAddress != null) {
			// the contract is: address == null <=> id == null
			checkNotNull(resourceManagerId);
			return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
		} else {
			return null;
		}
	}

	private void reconnectToResourceManager(Exception cause) {
		closeResourceManagerConnection(cause);
		tryConnectToResourceManager();
	}

	private void tryConnectToResourceManager() {
		if (resourceManagerAddress != null) {
			connectToResourceManager();
		}
	}

	private void connectToResourceManager() {
		assert(resourceManagerAddress != null);
		assert(resourceManagerConnection == null);
		assert(establishedResourceManagerConnection == null);

		log.info("Connecting to ResourceManager {}", resourceManagerAddress);

		resourceManagerConnection = new ResourceManagerConnection(
			log,
			jobGraph.getJobID(),
			resourceId,
			getAddress(),
			getFencingToken(),
			resourceManagerAddress.getAddress(),
			resourceManagerAddress.getResourceManagerId(),
			scheduledExecutorService);

		resourceManagerConnection.start();
	}

	private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
		final ResourceManagerId resourceManagerId = success.getResourceManagerId();

		// verify the response with current connection
		if (resourceManagerConnection != null
				&& Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

			log.info("JobManager successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

			final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

			final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

			establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
				resourceManagerGateway,
				resourceManagerResourceId);

			slotPoolGateway.connectToResourceManager(resourceManagerGateway);

			resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {
					resourceManagerGateway.heartbeatFromJobManager(resourceID);
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					// request heartbeat will never be called on the job manager side
				}
			});
		} else {
			log.debug("Ignoring resource manager connection to {} because its a duplicate or outdated.", resourceManagerId);

		}
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (establishedResourceManagerConnection != null) {
			dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
			establishedResourceManagerConnection = null;
		}

		if (resourceManagerConnection != null) {
			// stop a potentially ongoing registration process
			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}
	}

	private void dissolveResourceManagerConnection(EstablishedResourceManagerConnection establishedResourceManagerConnection, Exception cause) {
		final ResourceID resourceManagerResourceID = establishedResourceManagerConnection.getResourceManagerResourceID();

		if (log.isDebugEnabled()) {
			log.debug("Close ResourceManager connection {}.", resourceManagerResourceID, cause);
		} else {
			log.info("Close ResourceManager connection {}: {}.", resourceManagerResourceID, cause.getMessage());
		}

		resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

		ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
		resourceManagerGateway.disconnectJobManager(jobGraph.getJobID(), cause);
		slotPoolGateway.disconnectResourceManager();
	}

	//----------------------------------------------------------------------------------------------
	// Utility classes
	//----------------------------------------------------------------------------------------------

	private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			runAsync(
				() -> notifyOfNewResourceManagerLeader(
					leaderAddress,
					ResourceManagerId.fromUuidOrNull(leaderSessionID)));
		}

		@Override
		public void handleError(final Exception exception) {
			handleJobMasterError(new Exception("Fatal error in the ResourceManager leader service", exception));
		}
	}

	//----------------------------------------------------------------------------------------------

	private class ResourceManagerConnection
			extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> {
		private final JobID jobID;

		private final ResourceID jobManagerResourceID;

		private final String jobManagerRpcAddress;

		private final JobMasterId jobMasterId;

		ResourceManagerConnection(
				final Logger log,
				final JobID jobID,
				final ResourceID jobManagerResourceID,
				final String jobManagerRpcAddress,
				final JobMasterId jobMasterId,
				final String resourceManagerAddress,
				final ResourceManagerId resourceManagerId,
				final Executor executor) {
			super(log, resourceManagerAddress, resourceManagerId, executor);
			this.jobID = checkNotNull(jobID);
			this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
			this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
			this.jobMasterId = checkNotNull(jobMasterId);
		}

		@Override
		protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> generateRegistration() {
			return new RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess>(
					log, getRpcService(), "ResourceManager", ResourceManagerGateway.class,
					getTargetAddress(), getTargetLeaderId()) {
				@Override
				protected CompletableFuture<RegistrationResponse> invokeRegistration(
						ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) {
					Time timeout = Time.milliseconds(timeoutMillis);

					return gateway.registerJobManager(
						jobMasterId,
						jobManagerResourceID,
						jobManagerRpcAddress,
						jobID,
						timeout);
				}
			};
		}

		@Override
		protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
			runAsync(() -> {
				// filter out outdated connections
				//noinspection ObjectEquality
				if (this == resourceManagerConnection) {
					establishResourceManagerConnection(success);
				}
			});
		}

		@Override
		protected void onRegistrationFailure(final Throwable failure) {
			handleJobMasterError(failure);
		}
	}

	//----------------------------------------------------------------------------------------------

	private class TaskManagerHeartbeatListener implements HeartbeatListener<AccumulatorReport, Void> {

		private final JobMasterGateway jobMasterGateway;

		private TaskManagerHeartbeatListener(JobMasterGateway jobMasterGateway) {
			this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		}

		@Override
		public void notifyHeartbeatTimeout(ResourceID resourceID) {
			jobMasterGateway.disconnectTaskManager(
				resourceID,
				new TimeoutException("Heartbeat of TaskManager with id " + resourceID + " timed out."));
		}

		@Override
		public void reportPayload(ResourceID resourceID, AccumulatorReport payload) {
			for (AccumulatorSnapshot snapshot : payload.getAccumulatorSnapshots()) {
				executionGraphDriver.updateAccumulators(snapshot);
			}
		}

		@Override
		public CompletableFuture<Void> retrievePayload(ResourceID resourceID) {
			return CompletableFuture.completedFuture(null);
		}
	}

	private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceId) {
			runAsync(() -> {
				log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);

				if (establishedResourceManagerConnection != null && establishedResourceManagerConnection.getResourceManagerResourceID().equals(resourceId)) {
					reconnectToResourceManager(
						new JobMasterException(
							String.format("The heartbeat of ResourceManager with id %s timed out.", resourceId)));
				}
			});
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since the payload is of type Void
		}

		@Override
		public CompletableFuture<Void> retrievePayload(ResourceID resourceID) {
			return CompletableFuture.completedFuture(null);
		}
	}

	@VisibleForTesting
	RestartStrategy getRestartStrategy() {
		return restartStrategy;
	}

	@VisibleForTesting
	public ExecutionGraphDriver getExecutionGraphDriver() {
		return executionGraphDriver;
	}
}
