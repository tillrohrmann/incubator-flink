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

package org.apache.flink.runtime.rescaling;

import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.util.AutoCloseableAsync;

import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@link OperatorRescalingPolicy} allows to trigger rescaling of operators.
 */
public interface OperatorRescalingPolicy extends AutoCloseableAsync {

	/**
	 * This method is called by the {@link JobMaster} to ask whether the
	 * job shall be rescaled.
	 *
	 * @param rescalingContext context to access information about the operator and its metrics
	 * @return new parallelism
	 */
	int rescaleTo(OperatorRescalingContext rescalingContext);

	/**
	 * Context.
	 */
	interface OperatorRescalingContext {
		int getCurrentParallelism();
	}

	/**
	 * Factory.
	 */
	interface Factory extends Serializable {
		OperatorRescalingPolicy create(ScheduledExecutorService scheduledExecutorService) throws Exception;
	}
}
