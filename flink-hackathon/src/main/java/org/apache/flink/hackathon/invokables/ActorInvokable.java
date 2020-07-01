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
import org.apache.flink.hackathon.messages.ActorCall;
import org.apache.flink.hackathon.messages.StopActor;
import org.apache.flink.hackathon.redis.RedisUtils;
import org.apache.flink.runtime.execution.Environment;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * ActorInvokable.
 */
public class ActorInvokable extends AbstractApplicationInvokable {

	private static final Logger LOG = LoggerFactory.getLogger(ActorInvokable.class);

	/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public ActorInvokable(Environment environment) {
		super(environment);
	}

	@Override
	public void invoke() throws Exception {
		final ActorConfiguration actorConfiguration = new ActorConfiguration(getTaskConfiguration());

		final String actorClassName = actorConfiguration.getActorClassName();

		final Application<?> application = loadAndInstantiateApplicationClass(actorClassName);

		LOG.info("Instantiated actor {}.", actorClassName);

		final RedissonClient redissonClient = RedisUtils.createClient();

		final ActorAddress actorAddress = actorConfiguration.getActorAddress(getUserCodeClassLoader());

		final RBlockingQueue<Object> requestQueue = redissonClient.getBlockingQueue(actorAddress.toString());

		while (true) {
			final Object request = requestQueue.take();

			if (request instanceof StopActor) {
				break;
			} else if (request instanceof ActorCall) {
				final ActorCall actorCall = (ActorCall) request;

				final Method method = application.getClass().getMethod(actorCall.getMethodName(), actorCall.getArgumentTypes());

				final Object result = method.invoke(application, actorCall.getArguments());

				if (!Void.TYPE.equals(method.getReturnType())) {
					outputResult(result, actorCall.getOutputId());
				}

			} else {
				LOG.info("Unknown request: {}", request);
			}
		}
	}
}
