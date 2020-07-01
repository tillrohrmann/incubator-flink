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

import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.util.Collection;

/**
 * ApplicationEnvironment.
 */
public interface ApplicationEnvironment extends AutoCloseableAsync {
	static ApplicationEnvironment getRemoteEnvironment(String restAddress, int restPort, Collection<File> jarFiles) throws Exception {
		return new RemoteApplicationEnvironment(restAddress, restPort, jarFiles);
	}

	<T, V extends Application<T>> T remote(Class<T> type, Class<V> implementor);

	static ApplicationEnvironment getEnvironment() {
		try {
			return new DefaultApplicationEnvironment();
		} catch (Exception e) {
			ExceptionUtils.rethrow(e);
			return null;
		}
	}
}
