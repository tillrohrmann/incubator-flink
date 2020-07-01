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

import org.apache.flink.hackathon.Application;
import org.apache.flink.hackathon.ApplicationContext;
import org.apache.flink.hackathon.DefaultApplicationContext;
import org.apache.flink.hackathon.FutureReference;
import org.apache.flink.hackathon.FutureValue;
import org.apache.flink.hackathon.redis.RedisFuture;
import org.apache.flink.hackathon.redis.RedisUtils;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.util.AbstractID;

import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

import java.lang.reflect.Constructor;
import java.util.concurrent.Future;

/**
 * AbstractAppliationInvokable.
 */
public abstract class AbstractApplicationInvokable extends AbstractInvokable {

	protected  final ApplicationContext applicationContext;

	/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public AbstractApplicationInvokable(Environment environment) {
		super(environment);
		applicationContext = new DefaultApplicationContext(getUserCodeClassLoader(), getEnvironment().getTaskExecutorActions());
	}

	protected Class<? extends Application> loadApplicationClass(String targetClassName) throws ClassNotFoundException {
		final Class<?> clazz = getUserCodeClassLoader().loadClass(targetClassName);

		final Class<? extends Application> targetClass;

		if (Application.class.isAssignableFrom(clazz)) {
			targetClass = clazz.asSubclass(Application.class);
		} else {
			throw new RuntimeException("Target class does not implement Application.");
		}
		return targetClass;
	}

	protected Application<?> instantiateApplication(Class<? extends Application> targetClass) throws Exception {
		final Constructor<? extends Application> constructor = targetClass.getConstructor(ApplicationContext.class);

		return constructor.newInstance(applicationContext);
	}

	protected Application<?> loadAndInstantiateApplicationClass(String className) throws Exception {
		final Class<? extends Application> applicationClass = loadApplicationClass(className);
		return instantiateApplication(applicationClass);
	}

	protected void outputResult(Object result, AbstractID outputId) throws InterruptedException, java.util.concurrent.ExecutionException {
		final RedissonClient redissonClient = RedisUtils.createClient();
		final RBucket<Object> bucket = redissonClient.getBucket(outputId.toString());

		if (result instanceof RedisFuture) {
			bucket.set(FutureReference.of(((RedisFuture<?>) result).getFutureId()));
		} else {
			final Object value;
			if (result instanceof Future) {
				value = ((Future<?>) result).get();
			} else {
				value = result;
			}

			bucket.set(FutureValue.of(value));
		}
	}
}
