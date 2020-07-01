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

package org.apache.flink.hackathon;

import org.apache.flink.types.Either;
import org.apache.flink.util.AbstractID;

import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RedisFuture.
 * @param <V>
 */
public class RedisFuture<V> implements Future<V>, Serializable {
	private static final long serialVersionUID = 5077310196600837247L;

	private final AtomicReference<Either<V, Throwable>> value;

	private final AbstractID futureId;

	public RedisFuture(AbstractID futureId) {
		this.futureId = futureId;
		this.value = new AtomicReference<>();
	}

	public AbstractID getFutureId() {
		return futureId;
	}

	public boolean completeExceptionally(Throwable throwable) {
		return value.compareAndSet(null, Either.Right(throwable));
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return value.get() != null;
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://localhost:6379");
		final RedissonClient redissonClient = Redisson.create(config);

		final RMap<AbstractID, Object> hackathon = redissonClient.getMap("hackathon");
		AbstractID currentFutureId = futureId;

		while (true) {
			while (true) {
				final Object redisValue = hackathon.get(currentFutureId);

				if (redisValue == null) {
					break;
				} else if (redisValue instanceof FutureReference) {
					currentFutureId = ((FutureReference) redisValue).getReferencedFutureId();
				} else if (redisValue instanceof FutureValue) {
					value.compareAndSet(null, Either.Left(((FutureValue<V>) redisValue).getValue()));
					break;
				} else {
					throw new IllegalStateException(String.format("Unknown redis value type: %s", value.getClass().getName()));
				}
			}

			final Either<V, Throwable> eitherValue = this.value.get();

			if (eitherValue != null) {
				if (eitherValue.isLeft()) {
					return eitherValue.left();
				} else {
					throw new ExecutionException(eitherValue.right());
				}
			}

			Thread.sleep(100);
		}
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return null;
	}
}
