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

package org.apache.flink.hackathon;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * TaskInvokable.
 */
public class TaskInvokable extends AbstractInvokable {

	private final ApplicationContext applicationContext;

	/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public TaskInvokable(Environment environment) {
		super(environment);
		applicationContext = new DefaultApplicationContext(getUserCodeClassLoader(), getEnvironment().getTaskExecutorActions());
	}

	@Override
	public void invoke() throws Exception  {
		final ApplicationConfig applicationConfig = new ApplicationConfig(getTaskConfiguration());
		final RemoteCall remoteCall = applicationConfig.getRemoteCall(getUserCodeClassLoader());
		final String targetClassName = remoteCall.getTargetClass();

		final Class<?> clazz = getUserCodeClassLoader().loadClass(targetClassName);

		final Class<? extends Application> targetClass;

		if (Application.class.isAssignableFrom(clazz)) {
			targetClass = clazz.asSubclass(Application.class);
		} else {
			throw new RuntimeException("Target class does not implement Application.");
		}

		final Application<?> application = instantiateApplication(targetClass);

		final Method method = targetClass.getMethod(remoteCall.getMethodName(), remoteCall.getArgumentTypes());

		final Object result = method.invoke(application, remoteCall.getArguments());
	}

	private Application<?> instantiateApplication(Class<? extends Application> targetClass) throws Exception {
		final Constructor<? extends Application> constructor = targetClass.getConstructor(ApplicationContext.class);

		return constructor.newInstance(applicationContext);
	}
}
