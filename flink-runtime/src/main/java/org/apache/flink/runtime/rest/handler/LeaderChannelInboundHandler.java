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

import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler which extracts the leader gateway from the request.
 *
 * @param <T> type of the gateway
 */
public abstract class LeaderChannelInboundHandler<T extends RestfulGateway> extends SimpleChannelInboundHandler<Routed> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		if (routed.request() instanceof LeaderHttpRequest) {
			@SuppressWarnings("unchecked")
			LeaderHttpRequest<T> leaderHttpRequest = ((LeaderHttpRequest<T>) routed.request());

			respondAsLeader(
				ctx,
				new Routed(
					routed.target(),
					routed.notFound(),
					leaderHttpRequest.getHttpRequest(),
					routed.path(),
					routed.pathParams(),
					routed.queryParams()),
				leaderHttpRequest.getLeaderGateway());
		} else {
			log.error("Implementation error: Received a request that wasn't a LeaderHttpRequest.");
			HandlerUtils.sendErrorResponse(ctx, routed.request(), new ErrorResponseBody("Bad request received."), HttpResponseStatus.BAD_REQUEST);
		}
	}

	protected abstract void respondAsLeader(ChannelHandlerContext channelHandlerContext, Routed msg, T gateway) throws Exception;
}
