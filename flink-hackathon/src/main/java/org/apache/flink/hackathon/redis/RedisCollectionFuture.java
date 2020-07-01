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

package org.apache.flink.hackathon.redis;

import org.apache.flink.hackathon.FutureReference;
import org.apache.flink.hackathon.FutureValue;
import org.apache.flink.util.AbstractID;

import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * RedisCollectionFuture.
 * @param <T>
 */
public class RedisCollectionFuture<T> implements Future<Collection<T>> {

	private final CompletableFuture<Collection<T>> result;

	private final Collection<AbstractID> futureIds;

	public RedisCollectionFuture(Collection<AbstractID> futureIds) {
		this.futureIds = futureIds;
		this.result = new CompletableFuture<>();
	}

	public Collection<AbstractID> getFutureIds() {
		return futureIds;
	}

	public boolean completeExceptionally(Throwable throwable) {
		return result.completeExceptionally(throwable);
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return result.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return result.isCancelled();
	}

	@Override
	public boolean isDone() {
		return result.isDone();
	}

	@Override
	public Collection<T> get() throws InterruptedException, ExecutionException {
		final RedissonClient redissonClient = RedisUtils.createClient();
		final Collection<T> results = new ArrayList<>(futureIds.size());

		for (AbstractID futureId : futureIds) {
			RBucket<Object> bucket = redissonClient.getBucket(futureId.toString());
			boolean resolved = false;

			while (true) {
				while (true) {
					final Object redisValue = bucket.get();

					if (redisValue == null) {
						break;
					} else if (redisValue instanceof FutureReference) {
						final AbstractID referencedFutureId = ((FutureReference) redisValue).getReferencedFutureId();
						bucket = redissonClient.getBucket(referencedFutureId.toString());
					} else if (redisValue instanceof FutureValue) {
						results.add(((FutureValue<T>) redisValue).getValue());
						resolved = true;
						break;
					} else {
						throw new IllegalStateException(String.format("Unknown redis value type: %s", redisValue.getClass().getName()));
					}
				}

				if (resolved || result.isDone()) {
					break;
				} else {
					Thread.sleep(100);
				}
			}
		}

		result.complete(results);

		return result.get();
	}

	@Override
	public Collection<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return null;
	}
}
