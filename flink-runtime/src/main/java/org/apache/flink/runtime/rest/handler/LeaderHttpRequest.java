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

import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.DecoderResult;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCounted;

/**
 * HttpRequest which has leader information attached.
 *
 * @param <T> type of the leader gateway
 */
public class LeaderHttpRequest<T extends RestfulGateway> implements HttpRequest, ReferenceCounted {

	private final T leaderGateway;

	private final HttpRequest httpRequest;

	private final ReferenceCounted requestAsReferenceCounted;

	public LeaderHttpRequest(
			T leaderGateway,
			HttpRequest httpRequest) {
		this.leaderGateway = Preconditions.checkNotNull(leaderGateway);
		this.httpRequest = Preconditions.checkNotNull(httpRequest);

		requestAsReferenceCounted = (httpRequest instanceof ReferenceCounted) ? ((ReferenceCounted) httpRequest) : null;
	}

	public T getLeaderGateway() {
		return leaderGateway;
	}

	public HttpRequest getHttpRequest() {
		return httpRequest;
	}

	// -----------------------------------------------------------------
	// Reference counting
	// -----------------------------------------------------------------

	@Override
	public int refCnt() {
		return (requestAsReferenceCounted == null) ? 0 : requestAsReferenceCounted.refCnt();
	}

	@Override
	public boolean release() {
		return (requestAsReferenceCounted == null) || requestAsReferenceCounted.release();
	}

	@Override
	public boolean release(int arg0) {
		return (requestAsReferenceCounted == null) || requestAsReferenceCounted.release(arg0);
	}

	@Override
	public ReferenceCounted retain() {
		if (requestAsReferenceCounted != null) {
			requestAsReferenceCounted.retain();
		}
		return this;
	}

	@Override
	public ReferenceCounted retain(int arg0) {
		if (requestAsReferenceCounted != null) {
			requestAsReferenceCounted.retain(arg0);
		}
		return this;
	}

	// -----------------------------------------------------------------
	// Http request methods
	// -----------------------------------------------------------------

	@Override
	public HttpMethod getMethod() {
		return httpRequest.getMethod();
	}

	@Override
	public HttpRequest setMethod(HttpMethod method) {
		return httpRequest.setMethod(method);
	}

	@Override
	public String getUri() {
		return httpRequest.getUri();
	}

	@Override
	public HttpRequest setUri(String uri) {
		return httpRequest.setUri(uri);
	}

	@Override
	public HttpVersion getProtocolVersion() {
		return httpRequest.getProtocolVersion();
	}

	@Override
	public HttpRequest setProtocolVersion(HttpVersion version) {
		return httpRequest.setProtocolVersion(version);
	}

	@Override
	public HttpHeaders headers() {
		return httpRequest.headers();
	}

	@Override
	public DecoderResult getDecoderResult() {
		return httpRequest.getDecoderResult();
	}

	@Override
	public void setDecoderResult(DecoderResult result) {
		httpRequest.setDecoderResult(result);
	}
}
