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

import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketFrame;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.ServerHandshakeStateEvent.HANDSHAKE_COMPLETE;

/**
 * Base class for web socket handler.
 *
 * @param <R> type of the request
 * @param <P> type of the response
 * @param <M> type of the message parameters
 */
public abstract class AbstractWebSocketHandler<R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends ChannelDuplexHandler {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final ChannelHandlerContext channelHandlerContext;

	private final Class<R> requestClass;

	protected final HandlerRequest<EmptyRequestBody, M> handlerRequest;

	protected AbstractWebSocketHandler(
		ChannelHandlerContext channelHandlerContext,
		Class<R> requestClass,
		HandlerRequest<EmptyRequestBody, M> handlerRequest) {

		this.channelHandlerContext = Preconditions.checkNotNull(channelHandlerContext);
		this.requestClass = Preconditions.checkNotNull(requestClass);

		this.handlerRequest = Preconditions.checkNotNull(handlerRequest);
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (Objects.equals(evt, HANDSHAKE_COMPLETE)) {
			channelActive();
		}

		ctx.fireUserEventTriggered(evt);
	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
		closeHandler();
		super.close(ctx, future);
	}


	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof WebSocketFrame) {
			final WebSocketFrame webSocketFrame = ((WebSocketFrame) msg);

			if (webSocketFrame instanceof CloseWebSocketFrame) {
				closeHandler();
			} else if (webSocketFrame instanceof PingWebSocketFrame) {
				ctx.channel().writeAndFlush(new PongWebSocketFrame(webSocketFrame.content()));
				return;
			} else if (webSocketFrame instanceof PongWebSocketFrame) {
				// Pong frames need to get ignored
				return;
			} else {
				final R request;

				try {
					request = deserializeWebSocketFrame(webSocketFrame);
				} catch (IOException ioe) {
					log.warn("Could not deserialize web socket request.", ioe);

					throw new FlinkException("Could not deserialize web socket request.", ioe);
				}

				readRequest(request);
			}
		} else {
			ctx.fireChannelRead(msg);
		}
	}

	protected R deserializeWebSocketFrame(WebSocketFrame webSocketFrame) throws IOException {
		final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

		try(final ByteBufInputStream is = new ByteBufInputStream(webSocketFrame.content())) {
			return objectMapper.readValue(is, requestClass);
		}
	}

	protected void sendResponse(P response) throws IOException {
		final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

		byte[] bytes = objectMapper.writeValueAsBytes(response);
		ByteBuf byteBuf = Unpooled.copiedBuffer(bytes);

		final BinaryWebSocketFrame webSocketFrame = new BinaryWebSocketFrame(byteBuf);

		channelHandlerContext.writeAndFlush(webSocketFrame);
	}

	/**
	 * Called when the web socket connection is closed by the sender.
	 */
	abstract void closeHandler();

	/**
	 * Called when the web socket channel has been established.
	 */
	abstract void channelActive();

	/**
	 * Called when the handler receives a new request from the sender.
	 *
	 * @param request from the sender
	 */
	abstract void readRequest(R request);

	interface WebSocketHandlerFactory<T extends RestfulGateway, R extends RequestBody, P extends ResponseBody, M extends MessageParameters> {
		AbstractWebSocketHandler<R, P, M> createWebSocketHandler(
			ChannelHandlerContext ctx,
			Class<R> requestClass,
			HandlerRequest<EmptyRequestBody, M> handlerRequest,
			T gateway);
	}
}
