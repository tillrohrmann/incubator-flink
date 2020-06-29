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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * RemoteCall.
 */
public class RemoteCall implements Serializable {

	private static final long serialVersionUID = -7504890723710499509L;

	private final String targetClassName;
	private final String methodName;

	@Nullable
	private final Class<?>[] argumentTypes;

	@Nullable
	private final Object[] arguments;

	public RemoteCall(String targetClassName, String methodName, Class<?>[] argumentTypes, Object[] arguments) {
		this.targetClassName = targetClassName;
		this.methodName = methodName;
		this.argumentTypes = argumentTypes;
		this.arguments = arguments;
	}

	public String getTargetClass() {
		return targetClassName;
	}

	public String getMethodName() {
		return methodName;
	}

	@Nullable
	public Class<?>[] getArgumentTypes() {
		return argumentTypes;
	}

	@Nullable
	public Object[] getArguments() {
		return arguments;
	}

	public static RemoteCall create(Class<?> implementor, Method method, Object[] args) {
		final String methodName = method.getName();
		final Class<?>[] argumentTypes;

		if (args != null) {
			argumentTypes = new Class<?>[args.length];

			for (int i = 0; i < args.length; i++) {
				argumentTypes[i] = args[i].getClass();
			}
		} else {
			argumentTypes = null;
		}

		return new RemoteCall(implementor.getName(), methodName, argumentTypes, args);
	}
}
