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

package org.apache.flink.hackathon.handlers;

import org.apache.flink.hackathon.ActorAddress;
import org.apache.flink.hackathon.messages.ActorCall;
import org.apache.flink.hackathon.redis.RedisFuture;
import org.apache.flink.hackathon.redis.RedisUtils;
import org.apache.flink.util.AbstractID;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.Future;

/**
 * RemoteActorInvocationHandler.
 */
public class RemoteActorInvocationHandler implements InvocationHandler {

	private final RBlockingQueue<Object> blockingQueue;

	public RemoteActorInvocationHandler(ActorAddress actorAddress) {
		final RedissonClient redissonClient = RedisUtils.createClient();
		blockingQueue = redissonClient.getBlockingQueue(actorAddress.toString());
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		final AbstractID outputId = new AbstractID();
		final ActorCall actorCall = ActorCall.create(method, args, outputId);

		blockingQueue.put(actorCall);

		if (Void.TYPE.equals(method.getReturnType())) {
			return null;
		} else {
			final RedisFuture<Object> redisFuture = new RedisFuture<>(outputId);
			if (Future.class.isAssignableFrom(method.getReturnType())) {
				return redisFuture;
			} else {
				return redisFuture.get();
			}
		}
	}
}
