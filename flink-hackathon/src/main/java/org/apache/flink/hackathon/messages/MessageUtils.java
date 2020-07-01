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

package org.apache.flink.hackathon.messages;

import org.apache.flink.hackathon.redis.RedisCollectionFuture;
import org.apache.flink.hackathon.redis.RedisFuture;
import org.apache.flink.util.AbstractID;

import java.io.Serializable;
import java.util.Collection;

/**
 * Message utils.
 */
public class MessageUtils {
	static Object[] replaceRedisFutures(Object[] objects) {
		if (objects != null) {
			final Object[] newObjects = new Object[objects.length];

			for (int i = 0; i < newObjects.length; i++) {
				if (objects[i] instanceof RedisFuture) {
					newObjects[i] = new RedisFutureId(((RedisFuture<?>) objects[i]).getFutureId());
				} else if (objects[i] instanceof RedisCollectionFuture) {
					newObjects[i] = new RedisFutureIds(((RedisCollectionFuture<?>) objects[i]).getFutureIds());
				} else {
					newObjects[i] = objects[i];
				}
			}

			return newObjects;
		} else {
			return null;
		}
	}

	static Object[] resolveRedisFutures(Object[] objects) {
		if (objects != null) {
			final Object[] newObjects = new Object[objects.length];

			for (int i = 0; i < newObjects.length; i++) {
				if (objects[i] instanceof RedisFutureId) {
					newObjects[i] = new RedisFuture<>(((RedisFutureId) objects[i]).futureId);
				} else if (objects[i] instanceof RedisFutureIds) {
					newObjects[i] = new RedisCollectionFuture<>(((RedisFutureIds) objects[i]).futureIds);
				} else {
					newObjects[i] = objects[i];
				}
			}

			return newObjects;
		} else {
			return null;
		}
	}

	static class RedisFutureId implements Serializable {

		private static final long serialVersionUID = 1133891731570787447L;

		private final AbstractID futureId;

		RedisFutureId(AbstractID futureId) {
			this.futureId = futureId;
		}
	}

	static class RedisFutureIds implements Serializable {
		private static final long serialVersionUID = 1913146266406104118L;

		private final Collection<AbstractID> futureIds;

		RedisFutureIds(Collection<AbstractID> futureIds) {
			this.futureIds = futureIds;
		}
	}
}
