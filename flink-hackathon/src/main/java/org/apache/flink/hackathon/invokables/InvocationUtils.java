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

package org.apache.flink.hackathon.invokables;

import org.apache.flink.hackathon.ActorAddress;
import org.apache.flink.hackathon.Application;
import org.apache.flink.hackathon.Utils;
import org.apache.flink.hackathon.messages.RemoteCall;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * InvocationUtils.
 */
public class InvocationUtils {
	public static JobVertex createTaskVertex(Method method, Object[] args, Class<?> implementor, AbstractID outptuId) throws IOException {
		final JobVertex jobVertex = new JobVertex("Method: " + Utils.methodToString(method));
		jobVertex.setInvokableClass(TaskInvokable.class);
		jobVertex.setParallelism(1);

		final TaskConfiguration taskConfiguration = new TaskConfiguration(jobVertex.getConfiguration());
		taskConfiguration.setRemoteCall(RemoteCall.create(implementor, method, args));
		taskConfiguration.setOutputId(outptuId);

		return jobVertex;
	}

	public static <T extends Application<?>> JobVertex createActorVertex(Class<T> actorClass, ActorAddress actorAddress) throws IOException {
		final JobVertex jobVertex = new JobVertex("Actor: " + Utils.actorToString(actorClass));
		jobVertex.setInvokableClass(ActorInvokable.class);
		jobVertex.setParallelism(1);

		final ActorConfiguration actorConfiguration = new ActorConfiguration(jobVertex.getConfiguration());
		actorConfiguration.setActorClass(actorClass.getName());
		actorConfiguration.setActorAddress(actorAddress);

		return jobVertex;
	}
}
