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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

/**
 * Adapter which does the handshaking for {@link AbstractWebSocketHandler}.
 *
 * @param <T> type of the leader gateway
 * @param <R> type of the request
 * @param <P> type of the response
 * @param <M> type of the message parameters
 */
@ChannelHandler.Sharable
public class WebSocketInstantiator<T extends RestfulGateway, R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends RedirectHandler<T> {

	private final MessageHeaders<R, P, M> messageHeaders;

	private final AbstractWebSocketHandler.WebSocketHandlerFactory<T, R, P, M> webSocketHandlerFactory;

	protected WebSocketInstantiator(
			@Nonnull CompletableFuture<String> localAddressFuture,
			@Nonnull GatewayRetriever<? extends T> leaderRetriever,
			@Nonnull Time timeout,
			MessageHeaders<R, P, M> messageHeaders,
			AbstractWebSocketHandler.WebSocketHandlerFactory<T, R, P, M> webSocketHandlerFactory) {
		super(localAddressFuture, leaderRetriever, timeout);

		this.messageHeaders = Preconditions.checkNotNull(messageHeaders);
		this.webSocketHandlerFactory = Preconditions.checkNotNull(webSocketHandlerFactory);
	}

	@Override
	protected void respondAsLeader(ChannelHandlerContext channelHandlerContext, Routed routed, T gateway) throws Exception {
		// Handshaker for the protocol
		channelHandlerContext.pipeline().addAfter(
			channelHandlerContext.name(),
			channelHandlerContext.name() + "_WEB_SOCKET_PROTOCOL",
			new WebSocketServerProtocolHandler(routed.path()));

		final HandlerRequest<EmptyRequestBody, M> handlerRequest = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			messageHeaders.getUnresolvedMessageParameters(),
			routed.pathParams(),
			routed.queryParams());

		AbstractWebSocketHandler<R, P, M> webSocketHandler = webSocketHandlerFactory.createWebSocketHandler(
			channelHandlerContext,
			messageHeaders.getRequestClass(),
			handlerRequest,
			gateway);

		channelHandlerContext.pipeline().addAfter(
			channelHandlerContext.name() + "_WEB_SOCKET_PROTOCOL",
			channelHandlerContext.name() + "_WEB_SOCKET_HANDLER",
			webSocketHandler);

		// let the WebSocketServerProtocolHandler handle the handshake
		routed.retain();
		channelHandlerContext.fireChannelRead(routed.request());
	}
}
