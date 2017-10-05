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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

public class LeaderHandlerRequest<T extends RestfulGateway, R extends RequestBody, M extends MessageParameters> extends HandlerRequest<R, M> {

	private final T leaderGateway;

	public LeaderHandlerRequest(
			R requestBody,
			M messageParameters,
			Map<String, String> receivedPathParameters,
			Map<String, List<String>> receivedQueryParameters,
			T leaderGateway) throws HandlerRequestException {
		super(requestBody, messageParameters, receivedPathParameters, receivedQueryParameters);

		this.leaderGateway = Preconditions.checkNotNull(leaderGateway);
	}

	public T getLeaderGateway() {
		return leaderGateway;
	}
}
