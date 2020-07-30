/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.slotsbro;

import org.apache.flink.api.common.JobID;

import java.io.Serializable;
import java.util.Collection;

/**
 * TODO: Add javadoc.
 */
public class ResourceRequirements implements Serializable {

	private static final long serialVersionUID = 3350991568345837268L;

	private final JobID jobId;

	private final String targetAddress;

	private final Collection<ResourceRequirement> resourceRequirements;

	public ResourceRequirements(JobID jobId, String targetAddress, Collection<ResourceRequirement> resourceRequirements) {
		this.jobId = jobId;
		this.targetAddress = targetAddress;
		this.resourceRequirements = resourceRequirements;
	}

	public JobID getJobId() {
		return jobId;
	}

	public String getTargetAddress() {
		return targetAddress;
	}

	public Collection<ResourceRequirement> getResourceRequirements() {
		return resourceRequirements;
	}
}

