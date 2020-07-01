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
import org.apache.flink.hackathon.ActorAddress;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * ActorConfiguration.
 */
public class ActorConfiguration {

	private static final String ACTOR_ADDRESS = "actorAddress";
	private static final String ACTOR_CLASS_NAME = "actorClassName";

	private final Configuration configuration;

	public ActorConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public void setActorAddress(ActorAddress actorAddress) throws IOException {
		InstantiationUtil.writeObjectToConfig(actorAddress, configuration, ACTOR_ADDRESS);
	}

	public ActorAddress getActorAddress(ClassLoader classLoader) throws IOException, ClassNotFoundException {
		return InstantiationUtil.readObjectFromConfig(configuration, ACTOR_ADDRESS, classLoader);
	}

	public void setActorClass(String actorClassName) {
		configuration.setString(ACTOR_CLASS_NAME, actorClassName);
	}

	public String getActorClassName() {
		return Preconditions.checkNotNull(configuration.getString(ACTOR_CLASS_NAME, null));
	}
}
