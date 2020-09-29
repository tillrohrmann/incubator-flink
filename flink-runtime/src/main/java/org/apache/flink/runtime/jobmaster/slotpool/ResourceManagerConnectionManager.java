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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirements;

import java.util.concurrent.CompletableFuture;

/**
 * Component which is responsible to manage the connection to the ResourceManager and
 * to declare new resource requirements.
 *
 * <p>The resource manager connection manager can only send resource requirements to the
 * resource manager if it is connected.
 */
public interface ResourceManagerConnectionManager {

	/**
	 * Connect to the resource manager identified by the given {@link ResourceManagerGateway}.
	 *
	 * @param declareResourceRequirementsService declareResourceRequirementsService to declare new resource requirements
	 */
	void connect(DeclareResourceRequirementsService declareResourceRequirementsService);

	/**
	 * Disconnect from the current resource manager.
	 */
	void disconnect();

	/**
	 * Declares the given resource requirements at the connected resource manager. If the resource
	 * manager connection manager is not connected, then this call will be ignored.
	 *
	 * @param resourceRequirements resourceRequirements to declare at the connected resource manager
	 */
	void declareResourceRequirements(ResourceRequirements resourceRequirements);

	/**
	 * Close the resource manager connection manager. A closed manager is not supposed to be
	 * used again.
	 */
	void close();

	interface DeclareResourceRequirementsService {
		CompletableFuture<Acknowledge> declareResourceRequirements(ResourceRequirements resourceRequirements);
	}
}
