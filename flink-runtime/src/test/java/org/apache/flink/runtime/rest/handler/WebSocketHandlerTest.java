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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpClientCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketVersion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class WebSocketHandlerTest extends TestLogger {

	@Test
	public void testWebSocketHandler() throws Exception {
		final Configuration configuration = new Configuration();
		final Time timeout = Time.seconds(10L);
		final RestfulGateway gateway = mock(RestfulGateway.class);
		final CompletableFuture<String> restAddressFuture = new CompletableFuture<>();
		when(gateway.requestRestAddress(any(Time.class))).thenReturn(restAddressFuture);

		final TestingGatewayRetriever<RestfulGateway> leaderRetriever = new TestingGatewayRetriever<>(gateway);
		final RestServerEndpointConfiguration restServerConfiguration = RestServerEndpointConfiguration.fromConfiguration(configuration);
		final RestServerEndpoint restServerEndpoint = new WebSocketRestServer(restServerConfiguration, leaderRetriever, timeout);

		restServerEndpoint.start();
		restAddressFuture.complete(restServerEndpoint.getRestAddress());
		final URI uri = new URI(restServerEndpoint.getRestAddress() + WebSocketMessageHeaders.URL);

		final EventLoopGroup group = new NioEventLoopGroup();

		try {
			final WebSocketClientHandler handler =
				new WebSocketClientHandler(
					WebSocketClientHandshakerFactory.newHandshaker(
						uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders()));

			Bootstrap b = new Bootstrap();
			b.group(group)
				.channel(NioSocketChannel.class)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) {
						ChannelPipeline p = ch.pipeline();
						p.addLast(
							new HttpClientCodec(),
							new HttpObjectAggregator(8192),
							handler);
					}
				});

			Channel ch = b.connect(uri.getHost(), restServerEndpoint.getServerAddress().getPort()).sync().channel();

			// wait for the handshake to complete
			handler.getHandshakeFuture().get();

			final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

			byte[] bytes = mapper.writeValueAsBytes(new WebSocketRequest(1, "Foobar"));

			ByteBuf byteBuf = Unpooled.copiedBuffer(bytes);

			ch.writeAndFlush(new BinaryWebSocketFrame(byteBuf));

			// wait for some responses
			Thread.sleep(1000L);

			ch.writeAndFlush(new CloseWebSocketFrame());
			ch.closeFuture().sync();
		} finally {
			restServerEndpoint.shutdown(timeout);
		}
	}

	private static class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

		private final WebSocketClientHandshaker handshaker;

		private final CompletableFuture<Boolean> handshakeFuture;

		private WebSocketClientHandler(WebSocketClientHandshaker handshaker) {
			this.handshaker = Preconditions.checkNotNull(handshaker);
			this.handshakeFuture = new CompletableFuture<>();
		}

		public CompletableFuture<Boolean> getHandshakeFuture() {
			return handshakeFuture;
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) {
			handshaker.handshake(ctx.channel());
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) {
			System.out.println("WebSocket Client disconnected!");
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
			final Channel ch = ctx.channel();
			// complete the handshake
			if (!handshaker.isHandshakeComplete()) {
				try {
					handshaker.finishHandshake(ch, (FullHttpResponse) msg);
					System.out.println("WebSocket Client connected!");
					handshakeFuture.complete(true);
				} catch (WebSocketHandshakeException e) {
					System.out.println("WebSocket Client failed to connect");
					handshakeFuture.completeExceptionally(e);
				}
				return;
			}

			if (msg instanceof WebSocketFrame) {
				final WebSocketFrame webSocketFrame = ((WebSocketFrame) msg);

				try (ByteBufInputStream is = new ByteBufInputStream(webSocketFrame.content())) {
					final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

					WebSocketResponse socketResponse = mapper.readValue(is, WebSocketResponse.class);

					System.out.println(socketResponse);
				}
			}
		}
	}


	private static class TestingGatewayRetriever<T extends RpcGateway> implements GatewayRetriever<T> {

		private final CompletableFuture<T> completedFuture;

		private TestingGatewayRetriever(T gateway) {
			completedFuture = CompletableFuture.completedFuture(gateway);
		}

		@Override
		public CompletableFuture<T> getFuture() {
			return completedFuture;
		}
	}

	private static class WebSocketRequest implements RequestBody {

		private static final String FIELD_NAME_ID = "id";

		private static final String FIELD_NAME_PAYLOAD = "payload";

		@JsonProperty(FIELD_NAME_ID)
		private final int id;

		@JsonProperty(FIELD_NAME_PAYLOAD)
		private final String payload;

		@JsonCreator
		private WebSocketRequest(
				@JsonProperty(FIELD_NAME_ID) int id,
				@JsonProperty(FIELD_NAME_PAYLOAD) String payload) {
			this.id = id;
			this.payload = Preconditions.checkNotNull(payload);
		}

		public String getPayload() {
			return payload;
		}

		@Override
		public String toString() {
			return "WebSocketRequest{" +
				"id=" + id +
				", payload='" + payload + '\'' +
				'}';
		}
	}

	private static class WebSocketResponse implements ResponseBody {
		private static final String FIELD_NAME_PAYLOAD = "payload";

		@JsonProperty(FIELD_NAME_PAYLOAD)
		private final String payload;

		@JsonCreator
		private WebSocketResponse(
				@JsonProperty(FIELD_NAME_PAYLOAD) String payload) {
			this.payload = Preconditions.checkNotNull(payload);
		}

		@Override
		public String toString() {
			return "WebSocketResponse{" +
				"payload='" + payload + '\'' +
				'}';
		}
	}

	private static class WebSocketMessageHeaders implements MessageHeaders<WebSocketRequest, WebSocketResponse, EmptyMessageParameters> {

		private static final WebSocketMessageHeaders INSTANCE = new WebSocketMessageHeaders();
		private static final String URL = "/websocket";

		private WebSocketMessageHeaders() {}

		@Override
		public Class<WebSocketRequest> getRequestClass() {
			return WebSocketRequest.class;
		}

		@Override
		public Class<WebSocketResponse> getResponseClass() {
			return WebSocketResponse.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public EmptyMessageParameters getUnresolvedMessageParameters() {
			return EmptyMessageParameters.getInstance();
		}

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.GET;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return URL;
		}

		public static WebSocketMessageHeaders getInstance() {
			return INSTANCE;
		}
	}

	private static class WebSocketRestServer extends RestServerEndpoint {

		final GatewayRetriever<RestfulGateway> leaderRetriever;
		final Time timeout;

		WebSocketRestServer(RestServerEndpointConfiguration configuration, GatewayRetriever<RestfulGateway> leaderRetriever, Time timeout) {
			super(configuration);

			this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
			this.timeout = Preconditions.checkNotNull(timeout);
		}

		@Override
		protected Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
			final WebSocketInstantiator<RestfulGateway, WebSocketRequest, WebSocketResponse, EmptyMessageParameters> webSocketHandlerHandshaker = new WebSocketInstantiator<>(
				restAddressFuture,
				leaderRetriever,
				timeout,
				WebSocketMessageHeaders.getInstance(),
				new WebSocketHandler.Factory());

			return Collections.singleton(Tuple2.of(WebSocketMessageHeaders.getInstance(), webSocketHandlerHandshaker));
		}
	}

	private static class WebSocketHandler extends AbstractWebSocketHandler<WebSocketRequest, WebSocketResponse, EmptyMessageParameters> {

		private ScheduledFuture<?> scheduledFuture;

		protected WebSocketHandler(
				ChannelHandlerContext channelHandlerContext,
				Class<WebSocketRequest> requestClass,
				HandlerRequest<EmptyRequestBody, EmptyMessageParameters> handlerRequest) {
			super(channelHandlerContext, requestClass, handlerRequest);
		}

		@Override
		void closeHandler() {
			System.out.println("Closing WebSocketHandler");

			if (scheduledFuture != null) {
				scheduledFuture.cancel(false);
				scheduledFuture = null;
			}
		}

		@Override
		void channelActive() {
			System.out.println("Web socket channel active.");

			final Random random = new Random();

			scheduledFuture = TestingUtils.defaultScheduledExecutor().scheduleWithFixedDelay(
				() -> {
					try {
						sendResponse(new WebSocketResponse("Payload_" + random.nextInt()));
					} catch (IOException e) {
						e.printStackTrace();
					}
				},
				200L,
				200L,
				TimeUnit.MILLISECONDS);
		}

		@Override
		void readRequest(WebSocketRequest request) {
			System.out.println("Reading web socket request: " + request);

			try {
				sendResponse(new WebSocketResponse(request.getPayload()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private static class Factory implements WebSocketHandlerFactory<RestfulGateway, WebSocketRequest, WebSocketResponse, EmptyMessageParameters> {

			@Override
			public AbstractWebSocketHandler<WebSocketRequest, WebSocketResponse, EmptyMessageParameters> createWebSocketHandler(
					ChannelHandlerContext ctx,
					Class<WebSocketRequest> requestClass,
					HandlerRequest<EmptyRequestBody, EmptyMessageParameters> handlerRequest,
					RestfulGateway gateway) {
				return new WebSocketHandler(ctx, requestClass, handlerRequest);
			}
		}
	}
}
