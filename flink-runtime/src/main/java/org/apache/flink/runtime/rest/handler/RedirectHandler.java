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
import org.apache.flink.runtime.rest.handler.util.HandlerRedirectUtils;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.KeepAliveWrite;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link SimpleChannelInboundHandler} which encapsulates the redirection logic for the
 * REST endpoints.
 *
 * @param <T> type of the leader to retrieve
 */
@ChannelHandler.Sharable
public class RedirectHandler<T extends RestfulGateway> extends SimpleChannelInboundHandler<HttpRequest> {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final CompletableFuture<String> localAddressFuture;

	protected final GatewayRetriever<? extends T> leaderRetriever;

	protected final Time timeout;

	private String localAddress;

	public RedirectHandler(
			@Nonnull CompletableFuture<String> localAddressFuture,
			@Nonnull GatewayRetriever<? extends T> leaderRetriever,
			@Nonnull Time timeout) {
		this.localAddressFuture = Preconditions.checkNotNull(localAddressFuture);
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.timeout = Preconditions.checkNotNull(timeout);
		localAddress = null;
	}

	@Override
	protected void channelRead0(
		ChannelHandlerContext channelHandlerContext,
		HttpRequest httpRequest) throws Exception {

		if (localAddressFuture.isDone()) {
			if (localAddress == null) {
				try {
					localAddress = localAddressFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
				} catch (Exception e) {
					logger.error("Could not obtain local address.", e);

					HandlerUtils.sendErrorResponse(
						channelHandlerContext,
						httpRequest,
						new ErrorResponseBody("Fatal error. Could not obtain local address. Please try to refresh."),
						HttpResponseStatus.INTERNAL_SERVER_ERROR);

					return;
				}
			}

			try {
				OptionalConsumer<? extends T> optLeaderConsumer = OptionalConsumer.of(leaderRetriever.getNow());

				optLeaderConsumer.ifPresent(
					gateway -> {
						CompletableFuture<Optional<String>> optRedirectAddressFuture = HandlerRedirectUtils.getRedirectAddress(
							localAddress,
							gateway,
							timeout);

						// retain the message for the asynchronous handler
						ReferenceCountUtil.retain(httpRequest);

						optRedirectAddressFuture.whenCompleteAsync(
							(Optional<String> optRedirectAddress, Throwable throwable) -> {
								HttpResponse response;
								try {
									if (throwable != null) {
										logger.error("Could not retrieve the redirect address.", throwable);

										HandlerUtils.sendErrorResponse(
											channelHandlerContext,
											httpRequest,
											new ErrorResponseBody("Could not retrieve the redirect address of the current leader. Please try to refresh."),
											HttpResponseStatus.INTERNAL_SERVER_ERROR);
									} else if (optRedirectAddress.isPresent()) {
										response = HandlerRedirectUtils.getRedirectResponse(
											optRedirectAddress.get(),
											new QueryStringDecoder(httpRequest.getUri()).path());

										KeepAliveWrite.flush(channelHandlerContext, httpRequest, response);
									} else {
										ReferenceCountUtil.retain(httpRequest);

										channelHandlerContext.fireChannelRead(new LeaderHttpRequest<T>(gateway, httpRequest));
									}
								} finally {
									// release the message after processing it asynchronously
									ReferenceCountUtil.release(httpRequest);
								}
							}
						, channelHandlerContext.executor());
					}
				).ifNotPresent(
					() ->
						HandlerUtils.sendErrorResponse(
							channelHandlerContext,
							httpRequest,
							new ErrorResponseBody("Service temporarily unavailable due to an ongoing leader election. Please refresh."),
							HttpResponseStatus.SERVICE_UNAVAILABLE));

			} catch (Throwable throwable) {
				logger.warn("Error occurred while processing web request.", throwable);

				HandlerUtils.sendErrorResponse(
					channelHandlerContext,
					httpRequest,
					new ErrorResponseBody("Error occurred in RedirectHandler: " + throwable.getMessage() + '.'),
					HttpResponseStatus.INTERNAL_SERVER_ERROR);
			}
		} else {
			HandlerUtils.sendErrorResponse(
				channelHandlerContext,
				httpRequest,
				new ErrorResponseBody("Local address has not been resolved. This indicates an internal error."),
				HttpResponseStatus.INTERNAL_SERVER_ERROR);
		}
	}
}
