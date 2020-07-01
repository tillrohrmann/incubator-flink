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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.hackathon.messages.RemoteCall;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * TaskConfiguration.
 */
public class TaskConfiguration {
	private final Configuration configuration;

	private static final String REMOTE_CALL = "remoteCall";
	private static final String METHOD_NAME = "methodName";
	private static final String OUTPUT_ID = "outputID";

	public TaskConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public void setRemoteCall(RemoteCall remoteCall) throws IOException {
		configuration.setString(METHOD_NAME, remoteCall.getMethodName());
		InstantiationUtil.writeObjectToConfig(remoteCall, configuration, REMOTE_CALL);
	}

	public RemoteCall getRemoteCall(ClassLoader classLoader) throws IOException, ClassNotFoundException {
		return InstantiationUtil.readObjectFromConfig(configuration, REMOTE_CALL, classLoader);
	}

	public void setOutputId(AbstractID outputId) {
		configuration.setBytes(OUTPUT_ID, outputId.getBytes());
	}

	@Nullable
	public AbstractID getOutputId() {
		final byte[] bytes = configuration.getBytes(OUTPUT_ID, null);

		if (bytes == null) {
			return null;
		} else {
			return new AbstractID(bytes);
		}
	}
}
