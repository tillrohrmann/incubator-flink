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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Tests for the {@link KubernetesJobClusterEntrypoint}.
 */
public class KubernetesJobClusterEntrypointTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testDownloadIfRemote() throws IOException {
		final File tmpDir = temporaryFolder.newFolder();

		final Path path = KubernetesJobClusterEntrypoint.downloadIfRemote("http://central.maven.org/maven2/org/apache/flink/flink-core/1.5.0/flink-core-1.5.0.jar", tmpDir);
	}

	@Test
	public void testProgramExecution() {
		final Configuration configuration = new Configuration();
		final Path userCodeJarPath = new Path(URI.create("file:///Users/till/work/flink/workspace/flink/build-target/examples/streaming/WindowJoin.jar"));
		final KubernetesJobClusterEntrypoint kubernetesJobClusterEntrypoint = new KubernetesJobClusterEntrypoint(configuration, userCodeJarPath, "foobar");

		kubernetesJobClusterEntrypoint.startCluster();
	}
}
