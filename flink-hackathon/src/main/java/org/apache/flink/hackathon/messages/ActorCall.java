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

package org.apache.flink.hackathon.messages;

import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Actor call.
 */
public class ActorCall implements Serializable {
	private static final long serialVersionUID = 7726567850037921677L;

	private final String methodName;

	@Nullable
	private final Class<?>[] argumentTypes;
	@Nullable
	private final Object[] arguments;
	@Nullable
	private final AbstractID outputId;

	public ActorCall(String methodName, @Nullable Class<?>[] argumentTypes, @Nullable Object[] arguments, @Nullable AbstractID outputId) {
		this.methodName = methodName;
		this.argumentTypes = argumentTypes;
		this.arguments = arguments;
		this.outputId = outputId;
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
		return MessageUtils.resolveRedisFutures(arguments);
	}

	@Nullable
	public AbstractID getOutputId() {
		return outputId;
	}

	public static ActorCall create(Method method, Object[] arguments, @Nullable AbstractID outputId) {
		return new ActorCall(
			method.getName(),
			method.getParameterTypes(),
			MessageUtils.replaceRedisFutures(arguments),
			outputId);
	}

	@Override
	public String toString() {
		return "ActorCall{" +
			"methodName='" + methodName + '\'' +
			", argumentTypes=" + Arrays.toString(argumentTypes) +
			", arguments=" + Arrays.toString(arguments) +
			", outputId=" + outputId +
			'}';
	}
}
